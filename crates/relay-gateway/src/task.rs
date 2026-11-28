use core::fmt::Debug;
use core::future::Future;
use std::sync::Arc;

use billing::errors::{BillingRejection, WalletError};
use billing::model::ModelUsage;
use billing::ports::{Pricing, Wallet};
use identity::model::Principal;
use kernel::model::{ModelId, Money, RequestId, UnixMillis};
use kernel::ports::Clock;
use router::ports::{Adapter, ByteStream, Upstream, UsageMeter};
use runtime::events::{ActualCost, SettlementCapped, UsageUnreadable};

use crate::channel::{Message, Sender};
use crate::gateway::Services;
use crate::ports::{Fault, MeterFailed, PriceSource, Report, Shortfall, Spawn, Stage};
use crate::ratio::Watch;

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

pub(crate) async fn relay<S: Services>(
    services: Arc<S>,
    watch: Arc<Watch>,
    job: Job<S>,
    sender: Sender,
) {
    let services = &*services;
    let watch = &*watch;
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
            count(services, watch, false, services.clock().now());
            return;
        }
    };
    let success = (200..300).contains(&reply.status);
    forward.send(Message::Head(reply.status)).await;
    if !success {
        release(services, &principal, &request).await;
    }
    let mut body = reply.body;
    let mut meter_failed = false;
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
            meter_failed |= meter.feed(&bytes).is_err();
        }
        forward.send(Message::Chunk(bytes)).await;
    };
    drop(forward);
    if !success {
        count(services, watch, false, services.clock().now());
        return;
    }
    let ended = finished && meter.ended();
    match meter.finish().ok() {
        Some(tokens) => {
            let actual = prices.price(&ModelUsage { model, tokens }).ok();
            settle(services, &principal, &request, held, actual).await;
            count(services, watch, false, services.clock().now());
        }
        None if ended => {
            let at = services.clock().now();
            services.report().usage_unreadable(&UsageUnreadable {
                request,
                account: principal.account,
                key: principal.key,
                held,
                at,
            });
            count(services, watch, true, at);
        }
        None => {
            if meter_failed {
                services.report().meter_failed(&MeterFailed {
                    request: request.clone(),
                    account: principal.account.clone(),
                    key: principal.key.clone(),
                    held,
                    at: services.clock().now(),
                });
            }
            release(services, &principal, &request).await;
            count(services, watch, false, services.clock().now());
        }
    }
}

fn count<S: Services>(services: &S, watch: &Watch, unreadable: bool, at: UnixMillis) {
    if let Some(event) = watch.record(unreadable, at) {
        services.report().usage_unreadable_ratio(&event);
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
    retry(
        services,
        Stage::Settle,
        principal,
        request,
        BillingRejection::AlreadySettled,
        || wallet.settle(&principal.account, request, amount),
    )
    .await;
}

async fn release<S: Services>(services: &S, principal: &Principal, request: &RequestId) {
    let wallet = services.wallet();
    retry(
        services,
        Stage::Release,
        principal,
        request,
        BillingRejection::AlreadyReleased,
        || wallet.release(&principal.account, request),
    )
    .await;
}

async fn retry<S, F, Fut, E>(
    services: &S,
    stage: Stage,
    principal: &Principal,
    request: &RequestId,
    done: BillingRejection,
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
            Err(WalletError::Rejected(rejection)) if attempts > 1 && rejection == done => return,
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
