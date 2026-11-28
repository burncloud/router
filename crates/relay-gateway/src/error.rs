use billing::errors::BillingRejection;
use runtime::errors::Rejection;

/// `A` 是 `Adapter::Error`，`P` 是价格快照的 `Pricing::Error`。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GatewayError<A, P> {
    #[error("request rejected: {0:?}")]
    Rejected(Rejection),
    #[error("invalid idempotency key")]
    InvalidIdempotencyKey,
    #[error("request cannot be prepared")]
    Prepare(A),
    #[error("prices are unavailable")]
    PricesUnavailable,
    #[error("request cannot be priced")]
    Price(P),
    #[error("price currency is not USD")]
    UnsupportedCurrency,
    #[error("price is zero")]
    ZeroPrice,
    #[error("hold rejected: {0:?}")]
    Billing(BillingRejection),
    #[error("identity storage failed")]
    Identity,
    #[error("wallet storage failed")]
    Storage,
    #[error("upstream failed before headers")]
    Upstream,
    #[error("relay task ended without a reply")]
    Internal,
}

impl<A, P> From<Rejection> for GatewayError<A, P> {
    fn from(rejection: Rejection) -> Self {
        Self::Rejected(rejection)
    }
}
