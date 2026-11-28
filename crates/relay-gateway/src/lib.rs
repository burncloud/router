#![forbid(unsafe_code)]
//! 中转请求的编排：鉴权、限流、冻结、调用上游、边读边转发、按真实用量结算。只依赖 next 的契约，不做 IO，执行器由调用方注入。

mod channel;
mod error;
mod gateway;
mod period;
mod ratio;
mod task;

pub use channel::{BodyInterrupted, ReplyBody};
pub use error::GatewayError;
pub use gateway::{GatewayConfig, RelayGateway, Services, MAX_IDEMPOTENCY_KEY_LEN};
pub use period::since;
pub use ratio::{InvalidUnreadableThreshold, UnreadableThreshold};
pub use runtime::events::{Fault, MeterFailed, Shortfall, Stage, UsageUnreadableRatio};
pub use runtime::ports::{PriceSource, Report, RequestIds, Spawn};

#[cfg(test)]
mod manifest {
    use kernel::model::{AccountId, Currency, KeyId, Money, RequestId, UnixMillis};

    #[test]
    fn assembly_ports_are_the_next_contract() {
        let stage: crate::Stage = runtime::events::Stage::Prices;
        assert_eq!(stage, crate::Stage::Prices);
        let shortfall: crate::Shortfall = runtime::events::Shortfall::Minor(2);
        assert_eq!(shortfall, crate::Shortfall::Minor(2));
        let at = UnixMillis(1);
        let request = RequestId(String::from("r"));
        let account = AccountId(String::from("a"));
        let key = KeyId(String::from("k"));
        let ratio = runtime::events::UsageUnreadableRatio {
            decided: 4,
            unreadable: 1,
            window: 100,
            ratio_num: 1,
            ratio_den: 100,
            at,
        };
        let same_ratio: crate::UsageUnreadableRatio = ratio;
        assert_eq!(same_ratio, ratio);
        let fault = runtime::events::Fault {
            stage: runtime::events::Stage::Hold,
            request: Some(request.clone()),
            account: Some(account.clone()),
            attempts: 1,
            detail: String::from("x"),
        };
        let same_fault: crate::Fault = fault.clone();
        assert_eq!(same_fault, fault);
        let failed = runtime::events::MeterFailed {
            request,
            account,
            key,
            held: Money {
                currency: Currency::Usd,
                minor_units: 3,
            },
            at,
        };
        let same_failed: crate::MeterFailed = failed.clone();
        assert_eq!(same_failed, failed);
    }

    #[test]
    fn dependencies_stay_pure() {
        let manifest = include_str!("../Cargo.toml");
        let bans = include_str!("../bans.toml");
        let mut names = Vec::new();
        let mut section = "";
        for line in manifest.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with('[') {
                section = trimmed;
                continue;
            }
            if trimmed.is_empty() {
                continue;
            }
            let Some((name, _)) = trimmed.split_once('=') else {
                continue;
            };
            if section == "[dependencies]" || section == "[dev-dependencies]" {
                names.push(name.trim());
            }
        }
        assert_eq!(
            names,
            [
                "billing",
                "identity",
                "kernel",
                "router",
                "runtime",
                "thiserror",
                "proptest"
            ]
        );
        for name in [
            "sqlx",
            "tokio",
            "reqwest",
            "axum",
            "hyper",
            "postgres",
            "relay-openai",
        ] {
            assert!(!names.contains(&name), "{name}");
        }
        for name in ["sqlx", "tokio", "reqwest", "axum", "hyper", "postgres"] {
            let needle = format!("name = \"{name}\"");
            assert!(bans.contains(&needle), "{name}");
        }
    }
}
