#![forbid(unsafe_code)]
//! `OpenAI` chat completions 协议的纯逻辑：校验并改写请求、估算冻结上限、从响应体读用量。没有 IO。

mod adapter;
mod error;
mod meter;
mod sse;
mod usage;

pub use adapter::{ModelLimit, OpenAiChat};
pub use error::{ConfigError, MeterError, PrepareError};
pub use meter::OpenAiMeter;

#[cfg(test)]
mod manifest {
    #[test]
    fn dependencies_stay_pure() {
        let manifest = include_str!("../Cargo.toml");
        let bans = include_str!("../bans.toml");
        let mut names = Vec::new();
        let mut in_dependencies = false;
        for line in manifest.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with('[') {
                in_dependencies = trimmed == "[dependencies]";
                continue;
            }
            if !in_dependencies || trimmed.is_empty() {
                continue;
            }
            let Some((name, _)) = trimmed.split_once('=') else {
                continue;
            };
            names.push(name.trim());
        }
        assert_eq!(
            names,
            [
                "billing",
                "kernel",
                "router",
                "serde",
                "serde_json",
                "thiserror"
            ]
        );
        for name in ["sqlx", "tokio", "reqwest", "axum", "hyper", "postgres"] {
            assert!(!names.contains(&name), "{name}");
            let needle = format!("name = \"{name}\"");
            assert!(bans.contains(&needle), "{name}");
        }
    }
}
