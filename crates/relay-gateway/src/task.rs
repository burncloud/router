use core::fmt::Debug;
use core::future::Future;
use std::sync::Arc;

use billing::errors::WalletError;
use billing::model::ModelUsage;
use billing::ports::{Pricing, Wallet};
use identity::model::Principal;
use kernel::model::{ModelId, Money, RequestId};
use kernel::ports::Clock;
use router::ports::{Adapter, ByteStream, Upstream, UsageMeter};
use runtime::events::{ActualCost, SettlementCapped, UsageUnreadable};

use crate::channel::{Message, Sender};
use crate::gateway::Services;
use crate::ports::{Fault, PriceSource, Report, Shortfall, Spawn, Stage};

type Meter<S> = <<S as Services>::Adapter as Adapter>::Meter;
type Prices<S> = <<S as Services>::Prices as PriceSource>::Prices;

pub(crate) struct Job<S: Services> {
    pub(crate) principal: Principal,
    pub(crate) request: RequestId,
    pub(crate) held: Money,
    pub(crate) model: ModelId,
    pub(crate) upstream_body: Vec<u8>,
    pub(crate) streaming: bool,
    pub(crate) meter: Meter<S>,
    pub(crate) prices: Prices<S>,
}

struct Forward {
    sender: Option<Sender>,
}

impl Forward {
    async fn send(&mut self, message: Message) {
        if let Some(sender) = &mut self.sender {
            if sender.send(message).await.is_err() {
                self.sender = None;
            }
        }
    }
}

pub(crate) async fn relay<S: Services>(services: Arc<S>, job: Job<S>, sender: Sender) {
    let services = &*services;
    let Job {
        principal,
        request,
        held,
        model,
        upstream_body,
        streaming,
        mut meter,
        prices,
    } = job;
    let mut forward = Forward {
        sender: Some(sender),
    };
    let reply = match services.upstream().call(upstream_body, streaming).await {
        Ok(reply) => reply,
        Err(error) => {
            fault(services, Stage::Upstream, &principal, &request, 1, &error);
            release(services, &principal, &request).await;
            forward.send(Message::Failed).await;
            return;
        }
    };
    let success = (200..300).contains(&reply.status);
    forward.send(Message::Head(reply.status)).await;
    if !success {
        release(services, &principal, &request).await;
    }
    let mut body = reply.body;
    let finished = loop {
        let chunk = match body.next_chunk().await {
            Some(Ok(bytes)) => Some(bytes),
            Some(Err(_)) => None,
            None => break true,
        };
        let Some(bytes) = chunk else {
            forward.send(Message::Broken).await;
            break false;
        };
        if success {
            let _ = meter.feed(&bytes);
        }
        forward.send(Message::Chunk(bytes)).await;
    };
    drop(forward);
    if !success {
        return;
    }
    let ended = finished && meter.ended();
    match meter.finish().ok() {
        Some(tokens) => {
            let actual = prices.price(&ModelUsage { model, tokens }).ok();
            settle(services, &principal, &request, held, actual).await;
        }
        None if ended => services.report().usage_unreadable(&UsageUnreadable {
            request,
            account: principal.account,
            key: principal.key,
            held,
            at: services.clock().now(),
        }),
        None => release(services, &principal, &request).await,
    }
}

fn fault<S: Services>(
    services: &S,
    stage: Stage,
    principal: &Principal,
    request: &RequestId,
    attempts: u32,
    detail: &dyn Debug,
) {
    services.report().fault(&Fault {
        stage,
        request: Some(request.clone()),
        account: Some(principal.account.clone()),
        attempts,
        detail: format!("{detail:?}"),
    });
}

fn capped(held: Money, actual: Option<Money>) -> Option<(ActualCost, Shortfall)> {
    match actual {
        Some(actual) if actual.currency != held.currency => {
            Some((ActualCost::Priced(actual), Shortfall::CurrencyMismatch))
        }
        Some(actual) => actual
            .minor_units
            .checked_sub(held.minor_units)
            .filter(|shortfall| *shortfall > 0)
            .map(|shortfall| (ActualCost::Priced(actual), Shortfall::Minor(shortfall))),
        None => Some((ActualCost::Overflow, Shortfall::Overflow)),
    }
}

async fn settle<S: Services>(
    services: &S,
    principal: &Principal,
    request: &RequestId,
    held: Money,
    actual: Option<Money>,
) {
    let amount = match (actual, capped(held, actual)) {
        (_, Some((cost, shortfall))) => {
            services.report().settlement_capped(
                &SettlementCapped {
                    request: request.clone(),
                    account: principal.account.clone(),
                    key: principal.key.clone(),
                    held,
                    actual: cost,
                    at: services.clock().now(),
                },
                shortfall,
            );
            held
        }
        (Some(actual), None) => actual,
        (None, None) => held,
    };
    let wallet = services.wallet();
    retry(services, Stage::Settle, principal, request, || {
        wallet.settle(&principal.account, request, amount)
    })
    .await;
}

async fn release<S: Services>(services: &S, principal: &Principal, request: &RequestId) {
    let wallet = services.wallet();
    retry(services, Stage::Release, principal, request, || {
        wallet.release(&principal.account, request)
    })
    .await;
}

async fn retry<S, F, Fut, E>(
    services: &S,
    stage: Stage,
    principal: &Principal,
    request: &RequestId,
    mut attempt: F,
) where
    S: Services,
    F: FnMut() -> Fut + Send,
    Fut: Future<Output = Result<(), WalletError<E>>> + Send,
    E: Debug,
{
    let config = services.config();
    let mut attempts = 1;
    loop {
        match attempt().await {
            Ok(()) => return,
            Err(WalletError::Rejected(rejection)) => {
                fault(services, stage, principal, request, attempts, &rejection);
                return;
            }
            Err(WalletError::Storage(error)) if attempts >= config.settle_attempts.get() => {
                fault(services, stage, principal, request, attempts, &error);
                return;
            }
            Err(WalletError::Storage(_)) => {
                attempts += 1;
                services.spawner().sleep(config.retry_delay).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use kernel::model::{Currency, Money};
    use runtime::events::ActualCost;

    use super::capped;
    use crate::ports::Shortfall;

    fn money(currency: Currency, minor_units: u64) -> Money {
        Money {
            currency,
            minor_units,
        }
    }

    #[test]
    fn capping_compares_within_one_currency() {
        let held = money(Currency::Usd, 10);
        assert_eq!(capped(held, Some(money(Currency::Usd, 10))), None);
        assert_eq!(capped(held, Some(money(Currency::Usd, 3))), None);
        assert_eq!(
            capped(held, Some(money(Currency::Usd, 11))),
            Some((
                ActualCost::Priced(money(Currency::Usd, 11)),
                Shortfall::Minor(1)
            ))
        );
        assert_eq!(
            capped(held, Some(money(Currency::Cny, 3))),
            Some((
                ActualCost::Priced(money(Currency::Cny, 3)),
                Shortfall::CurrencyMismatch
            ))
        );
        assert_eq!(
            capped(held, None),
            Some((ActualCost::Overflow, Shortfall::Overflow))
        );
    }
}
