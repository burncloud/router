use core::fmt::Debug;
use core::time::Duration;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;

use billing::errors::{BillingRejection, WalletError};
use billing::model::KeyCap;
use billing::ports::{Pricing, Wallet};
use identity::model::{Admission, Principal};
use identity::ports::{ApiKeys, RateLimiter};
use kernel::model::{AccountId, Currency, RequestId};
use kernel::ports::Clock;
use router::ports::{Adapter, Upstream};
use runtime::errors::Rejection;
use runtime::model::Reply;
use runtime::ports::Gateway;

use crate::channel::{channel, Message, ReplyBody};
use crate::error::GatewayError;
use crate::period::since;
use crate::ports::{Fault, PriceSource, Report, RequestIds, Spawn, Stage};
use crate::ratio::{UnreadableThreshold, Watch};
use crate::task::{relay, Job};

/// `Idempotency-Key` 的最大字节数。
pub const MAX_IDEMPOTENCY_KEY_LEN: usize = 255;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GatewayConfig {
    /// 后台任务和 `Reply::body` 之间最多缓存的块数。
    pub channel_capacity: NonZeroUsize,
    /// 结算或释放遇到存储失败时，连同第一次在内最多尝试的次数。用完后冻结留给对账。
    pub settle_attempts: NonZeroU32,
    pub retry_delay: Duration,
    /// 已决次数和读不出用量的比例。默认是 `UnreadableThreshold::DEFAULT`。
    pub unreadable_threshold: UnreadableThreshold,
}

/// 编排用到的全部端口。实现方把各个实现放进一个结构体里交给 [`RelayGateway`]。
pub trait Services: Send + Sync + 'static {
    type Keys: ApiKeys<Error: Debug + Send> + Sync;
    type Limiter: RateLimiter<Error: Debug + Send> + Sync;
    type Adapter: Adapter<Error: Send, Meter: 'static> + Sync;
    type Upstream: Upstream<Error: Debug + Send, Body: 'static> + Sync;
    type Prices: PriceSource;
    type Wallet: Wallet<Error: Debug + Send> + Sync;
    type Clock: Clock + Sync;
    type Spawn: Spawn;
    type Ids: RequestIds;
    type Report: Report;

    fn keys(&self) -> &Self::Keys;
    fn limiter(&self) -> &Self::Limiter;
    fn adapter(&self) -> &Self::Adapter;
    fn upstream(&self) -> &Self::Upstream;
    fn prices(&self) -> &Self::Prices;
    fn wallet(&self) -> &Self::Wallet;
    fn clock(&self) -> &Self::Clock;
    fn spawner(&self) -> &Self::Spawn;
    fn ids(&self) -> &Self::Ids;
    fn report(&self) -> &Self::Report;
    fn config(&self) -> &GatewayConfig;
}

type AdapterError<S> = <<S as Services>::Adapter as Adapter>::Error;
type PricingError<S> = <<<S as Services>::Prices as PriceSource>::Prices as Pricing>::Error;
type Failure<S> = GatewayError<AdapterError<S>, PricingError<S>>;

pub struct RelayGateway<S> {
    services: Arc<S>,
    watch: Arc<Watch>,
}

impl<S> Clone for RelayGateway<S> {
    fn clone(&self) -> Self {
        Self {
            services: Arc::clone(&self.services),
            watch: Arc::clone(&self.watch),
        }
    }
}

impl<S> core::fmt::Debug for RelayGateway<S> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("RelayGateway")
            .finish_non_exhaustive()
    }
}

impl<S: Services> RelayGateway<S> {
    #[must_use]
    pub fn new(services: S) -> Self {
        let watch = Arc::new(Watch::new(services.config().unreadable_threshold));
        Self {
            services: Arc::new(services),
            watch,
        }
    }

    #[must_use]
    pub fn services(&self) -> &S {
        &self.services
    }

    fn fault(&self, stage: Stage, principal: Option<&Principal>, detail: &dyn Debug) {
        self.services.report().fault(&Fault {
            stage,
            request: None,
            account: principal.map(|principal| principal.account.clone()),
            attempts: 1,
            detail: format!("{detail:?}"),
        });
    }

    async fn admit(&self, key: &str) -> Result<(Principal, Option<KeyCap>), Failure<S>> {
        let services = &*self.services;
        let principal = match services.keys().authenticate(key).await {
            Ok(Some(principal)) => principal,
            Ok(None) => return Err(Rejection::Unauthenticated.into()),
            Err(error) => {
                self.fault(Stage::Authenticate, None, &error);
                return Err(GatewayError::Identity);
            }
        };
        let limit = match services.keys().limit(&principal.key).await {
            Ok(Some(limit)) => limit,
            Ok(None) => return Err(Rejection::UnknownKey.into()),
            Err(error) => {
                self.fault(Stage::Limit, Some(&principal), &error);
                return Err(GatewayError::Identity);
            }
        };
        if let Some(per_minute) = limit.requests_per_minute {
            let now = services.clock().now();
            match services
                .limiter()
                .admit(&principal.key, per_minute, now)
                .await
            {
                Ok(Admission::Allowed) => {}
                Ok(Admission::Throttled) => return Err(Rejection::RateLimited.into()),
                Err(error) => {
                    self.fault(Stage::Admit, Some(&principal), &error);
                    return Err(GatewayError::Identity);
                }
            }
        }
        let cap = limit.spend.map(|spend| KeyCap {
            since: since(spend.period, services.clock().now()),
            limit: spend.amount,
        });
        Ok((principal, cap))
    }

    fn request_id(&self, idempotency_key: Option<&str>) -> Result<RequestId, Failure<S>> {
        match idempotency_key {
            None => Ok(self.services.ids().next_id()),
            Some(key) if valid_idempotency_key(key) => Ok(RequestId(key.to_owned())),
            Some(_) => Err(GatewayError::InvalidIdempotencyKey),
        }
    }
}

fn valid_idempotency_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= MAX_IDEMPOTENCY_KEY_LEN
        && key.bytes().all(|byte| byte.is_ascii_graphic())
}

fn hold_failure<S: Services>(
    rejection: &WalletError<<S::Wallet as Wallet>::Error>,
) -> Option<Failure<S>> {
    match rejection {
        WalletError::Rejected(BillingRejection::InsufficientBalance) => {
            Some(Rejection::InsufficientBalance.into())
        }
        WalletError::Rejected(BillingRejection::LimitExceeded) => {
            Some(Rejection::LimitExceeded.into())
        }
        WalletError::Rejected(BillingRejection::DuplicateRequest) => {
            Some(Rejection::DuplicateRequest.into())
        }
        WalletError::Rejected(BillingRejection::CurrencyMismatch) => {
            Some(Rejection::CurrencyMismatch.into())
        }
        WalletError::Rejected(
            reason @ (BillingRejection::HoldNotFound
            | BillingRejection::SettlementExceedsHold
            | BillingRejection::AlreadySettled
            | BillingRejection::AlreadyReleased
            | BillingRejection::Unbalanced
            | BillingRejection::Overflow
            | BillingRejection::HoldMismatch),
        ) => Some(GatewayError::Billing(*reason)),
        WalletError::Storage(_) => None,
    }
}

impl<S: Services> Gateway for RelayGateway<S> {
    type Error = Failure<S>;
    type Body = ReplyBody;

    async fn handle(
        &self,
        key: &str,
        idempotency_key: Option<&str>,
        body: &[u8],
    ) -> Result<Reply<ReplyBody>, Failure<S>> {
        let services = &*self.services;
        let (principal, cap) = self.admit(key).await?;
        let request = self.request_id(idempotency_key)?;
        let prepared = services
            .adapter()
            .prepare(body)
            .map_err(GatewayError::Prepare)?;
        let prices = match services.prices().snapshot().await {
            Ok(prices) => prices,
            Err(error) => {
                self.fault(Stage::Prices, Some(&principal), &error);
                return Err(GatewayError::PricesUnavailable);
            }
        };
        let held = prices.price(&prepared.limit).map_err(GatewayError::Price)?;
        if held.currency != Currency::Usd {
            return Err(GatewayError::UnsupportedCurrency);
        }
        if held.minor_units == 0 {
            return Err(GatewayError::ZeroPrice);
        }
        let account: &AccountId = &principal.account;
        let hold_id = match services
            .wallet()
            .hold(account, &request, &principal.key, cap, held)
            .await
        {
            Ok(hold_id) => hold_id,
            Err(error) => {
                if let Some(failure) = hold_failure::<S>(&error) {
                    return Err(failure);
                }
                self.fault(Stage::Hold, Some(&principal), &error);
                return Err(GatewayError::Storage);
            }
        };
        let (sender, mut receiver) = channel(services.config().channel_capacity);
        let streaming = prepared.streaming;
        services.spawner().spawn(relay(
            Arc::clone(&self.services),
            Arc::clone(&self.watch),
            Job {
                principal,
                request,
                hold_id,
                held,
                model: prepared.limit.model,
                upstream_body: prepared.upstream_body,
                streaming,
                meter: prepared.meter,
                prices,
            },
            sender,
        ));
        match receiver.recv().await {
            Some(Message::Head(status)) => Ok(Reply {
                status,
                streaming,
                body: ReplyBody::new(receiver),
            }),
            Some(Message::Failed) => Err(GatewayError::Upstream),
            Some(Message::Chunk(_) | Message::Broken) | None => Err(GatewayError::Internal),
        }
    }
}
