#![forbid(unsafe_code)]
//! 中转请求的编排：鉴权、限流、冻结、调用上游、边读边转发、按真实用量结算。只依赖 next 的契约，不做 IO，执行器由调用方注入。

mod channel;
mod error;
mod gateway;
mod period;
mod ports;
mod task;

pub use channel::{BodyInterrupted, ReplyBody};
pub use error::GatewayError;
pub use gateway::{GatewayConfig, RelayGateway, Services, MAX_IDEMPOTENCY_KEY_LEN};
pub use period::since;
pub use ports::{Fault, PriceSource, Report, RequestIds, Shortfall, Spawn, Stage};

#[cfg(test)]
mod manifest {
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
