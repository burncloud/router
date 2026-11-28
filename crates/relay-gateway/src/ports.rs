use core::future::Future;
use core::time::Duration;

use billing::model::ModelUsage;
use billing::ports::Pricing;
use kernel::model::{AccountId, KeyId, Money, RequestId, UnixMillis};
use runtime::events::{SettlementCapped, UsageUnreadable};

/// 客户端断开后仍要跑完的任务交给它，`relay-server` 用 tokio 实现。
pub trait Spawn: Send + Sync + 'static {
    /// 任务必须跑到结束，不能因为调用方丢弃 `Reply::body` 而取消。
    fn spawn<F>(&self, task: F)
    where
        F: Future<Output = ()> + Send + 'static;

    /// 结算或释放存储失败后，两次重试之间等待用。
    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> + Send;
}

/// 每个请求取一次快照，冻结和结算都用同一份。
pub trait PriceSource: Send + Sync + 'static {
    type Prices: Pricing<Usage = ModelUsage, Error: Send> + Send + 'static;
    type Error: core::fmt::Debug + Send;

    /// # Errors
    ///
    /// 价格表读不出来，或有零价、非 USD 等坏行时返回 `Self::Error`，不能用部分价格表继续。
    fn snapshot(&self) -> impl Future<Output = Result<Self::Prices, Self::Error>> + Send;
}

/// 请求没带 `Idempotency-Key` 时生成 `RequestId`。
pub trait RequestIds: Send + Sync + 'static {
    /// 每次返回的值都不能与之前的重复。
    fn next_id(&self) -> RequestId;
}

/// 实际费用比冻结额多出的部分。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Shortfall {
    Minor(u64),
    Overflow,
    CurrencyMismatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Stage {
    Authenticate,
    Limit,
    Admit,
    Prices,
    Hold,
    Upstream,
    Settle,
    Release,
}

/// `detail` 是底层错误的 `Debug` 输出，不含客户 key 和请求内容。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Fault {
    pub stage: Stage,
    pub request: Option<RequestId>,
    pub account: Option<AccountId>,
    pub attempts: u32,
    pub detail: String,
}

/// meter 已在 `feed` 中报错，且上游未正常结束。计费仍是释放、不扣费。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MeterFailed {
    pub request: RequestId,
    pub account: AccountId,
    pub key: KeyId,
    pub held: Money,
    pub at: UnixMillis,
}

/// 首期写结构化日志：`usage_unreadable`、`settlement_capped`、`meter_failed` 和 `fault` 记 error 级。
pub trait Report: Send + Sync + 'static {
    fn usage_unreadable(&self, event: &UsageUnreadable);

    fn settlement_capped(&self, event: &SettlementCapped, shortfall: Shortfall);

    fn meter_failed(&self, event: &MeterFailed);

    fn fault(&self, fault: &Fault);
}
