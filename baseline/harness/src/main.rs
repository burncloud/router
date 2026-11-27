//! 旧中转行为基线录制工具。
//!
//! 用旧仓库生产服务挂载的同一个入口 `burncloud_router::create_router_app`，对进程内脚本化假上游回放固定场景，
//! 记录状态码、规范化响应体、金额变化、`router_logs` 行、`WARN`/`ERROR` 日志和耗时。
//! 夹具是旧仓库 `crates/traffic/router/tests/common.rs`，由 `baseline-tool` 从 Cargo 按钉死 rev 检出的源码复制到 `old/`，不入库。
//!
//! 用法：`old-relay-baseline <out.json> <raw.log>`，需要环境变量 `OLD_COMMIT`（40 位 SHA）。

#[rustfmt::skip]
#[path = "../old/common.rs"]
#[allow(
    unused_imports,
    clippy::pedantic,
    clippy::panic,
    reason = "旧仓库的测试夹具，按 Cargo.toml 钉死的 rev 原样编译，不在本仓库修改"
)]
mod common;

mod logbuf;
mod normalize;
mod snapshot;
mod upstream;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use burncloud_commerce_billing::{BillingPriceModel, PriceInput};
use burncloud_database::sqlx::{self, AnyPool};
use futures_util::StreamExt;
use serde_json::{json, Map, Value};

use crate::logbuf::LogBuf;
use crate::normalize::{norm_body, norm_json, norm_log_line};
use crate::snapshot::{logs_after, max_log_id, settle_wait, snapshot};
use crate::upstream::Hits;

const MAIN_KEY: &str = "sk-baseline-main";
const MAIN_USER: &str = "baseline-user";
const WALLET0_KEY: &str = "sk-baseline-wallet-zero";
const WALLET0_USER: &str = "baseline-wallet-zero";
const BROKE_KEY: &str = "sk-baseline-quota-exhausted";
const BROKE_USER: &str = "baseline-quota-exhausted";
const RL_KEY: &str = "sk-baseline-ratelimit";
const RL_USER: &str = "baseline-ratelimit";
const UPSTREAM_KEY: &str = "sk-upstream-fake";
const IN_PRICE: i64 = 2_000_000_000;
const OUT_PRICE: i64 = 8_000_000_000;
const UNIT_COST: i64 = 10 * IN_PRICE / 1_000_000 + 20 * OUT_PRICE / 1_000_000;
const FAIL_CH: i32 = 9103;
const OK_FAILOVER_CH: i32 = 9104;
const SCHEDULER_POLICIES: &str = r#"{"default":{"type":"passthrough"}}"#;
const RATE_LIMIT_REQUESTS: usize = 140;
const PRICED_MODELS: [&str; 7] = [
    "gpt-baseline",
    "gpt-nousage",
    "gpt-failover",
    "claude-baseline",
    "gemini-baseline",
    "gpt-ratelimit",
    "gpt-long",
];

struct Scn {
    name: &'static str,
    expected_diff: Option<&'static str>,
    user: &'static str,
    key: &'static str,
    path: &'static str,
    auth: (&'static str, String),
    body: Value,
    disconnect_after_first_chunk: bool,
}

struct Ctx {
    client: reqwest::Client,
    pool: AnyPool,
    logbuf: LogBuf,
    hits: Hits,
    r_port: u16,
    ports: [u16; 2],
    raw_log_path: String,
}

impl Ctx {
    fn url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{path}", self.r_port)
    }

    fn append_raw_log(&self, name: &str, lines: &str) -> anyhow::Result<()> {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.raw_log_path)?
            .write_all(format!("\n===== {name} =====\n{lines}").as_bytes())?;
        Ok(())
    }

    fn warn_error(&self, lines: &str) -> Vec<String> {
        lines
            .lines()
            .filter(|l| {
                let l = l.trim_start();
                l.starts_with("WARN") || l.starts_with("ERROR")
            })
            .map(|l| norm_log_line(l, &self.ports))
            .collect()
    }
}

fn chat(model: &str, stream: bool) -> Value {
    let mut b = json!({"model": model, "messages": [{"role": "user", "content": "baseline ping"}]});
    if stream {
        b["stream"] = json!(true);
    }
    b
}

fn bearer(key: &str) -> (&'static str, String) {
    ("authorization", format!("Bearer {key}"))
}

fn failover_session() -> anyhow::Result<String> {
    let score = |key: &str, ch: i32| {
        let mut h = DefaultHasher::new();
        key.hash(&mut h);
        ch.hash(&mut h);
        h.finish()
    };
    (0..4096)
        .map(|i| format!("baseline-failover-session-{i}"))
        .find(|key| {
            let fail = score(key, FAIL_CH);
            let ok = score(key, OK_FAILOVER_CH);
            fail > ok && fail.saturating_sub(ok) > u64::MAX / 3
        })
        .context("no failover session")
}

fn main_scenario(name: &'static str, expected_diff: Option<&'static str>, body: Value) -> Scn {
    Scn {
        name,
        expected_diff,
        user: MAIN_USER,
        key: MAIN_KEY,
        path: "/v1/chat/completions",
        auth: bearer(MAIN_KEY),
        body,
        disconnect_after_first_chunk: false,
    }
}

fn scenarios(session: &str) -> Vec<Scn> {
    let main = main_scenario;
    let mut all = vec![
        main("openai_nonstream_ok", None, chat("gpt-baseline", false)),
        main(
            "openai_stream_ok",
            Some("预期不同：旧系统在响应头返回时就读 usage 结算，流末 usage 读不到，记 0 且 cost_status 为空；新系统必须按流末 usage 结算"),
            chat("gpt-baseline", true),
        ),
        Scn {
            name: "anthropic_messages_ok",
            expected_diff: None,
            user: MAIN_USER,
            key: MAIN_KEY,
            path: "/v1/messages",
            auth: ("x-api-key", MAIN_KEY.into()),
            body: json!({"model": "claude-baseline", "max_tokens": 64, "messages": [{"role": "user", "content": "baseline ping"}]}),
            disconnect_after_first_chunk: false,
        },
        Scn {
            name: "gemini_generate_ok",
            expected_diff: None,
            user: MAIN_USER,
            key: MAIN_KEY,
            path: "/v1beta/models/gemini-baseline:generateContent",
            auth: ("x-goog-api-key", MAIN_KEY.into()),
            body: json!({"contents": [{"role": "user", "parts": [{"text": "baseline ping"}]}]}),
            disconnect_after_first_chunk: false,
        },
        Scn {
            name: "insufficient_key_quota",
            expected_diff: None,
            user: BROKE_USER,
            key: BROKE_KEY,
            path: "/v1/chat/completions",
            auth: bearer(BROKE_KEY),
            body: chat("gpt-baseline", false),
            disconnect_after_first_chunk: false,
        },
        Scn {
            name: "wallet_balance_zero_unlimited_key",
            expected_diff: Some("预期不同：旧系统不看钱包余额，只累加 key used_quota，余额 0 照样 200；新系统按钱包冻结，余额 0 必须拒绝"),
            user: WALLET0_USER,
            key: WALLET0_KEY,
            path: "/v1/chat/completions",
            auth: bearer(WALLET0_KEY),
            body: chat("gpt-baseline", false),
            disconnect_after_first_chunk: false,
        },
        main(
            "no_price_model",
            Some("预期不同（仅日志）：旧系统 400 拒绝、不收费，这部分要保持；但旧日志把它记成 cost_status=upstream_error，新系统应记为无价格拒绝"),
            chat("gpt-noprice", false),
        ),
        main(
            "openai_nonstream_no_usage",
            Some("预期不同：旧系统 200 且记 0、cost_status 为空；新系统缺 usage 不得记 0：冻结保留，最多 30 分钟后由对账按上游账单补扣或释放，发 UsageUnreadable"),
            chat("gpt-nousage", false),
        ),
        main(
            "openai_stream_no_usage",
            Some("预期不同：旧系统 200 且记 0；新系统缺 usage 不得记 0：冻结保留，最多 30 分钟后由对账按上游账单补扣或释放，发 UsageUnreadable"),
            chat("gpt-nousage", true),
        ),
        main(
            "failover_500_then_ok",
            None,
            json!({"model": "gpt-failover", "conversation_id": session, "messages": [{"role": "user", "content": "baseline ping"}]}),
        ),
    ];
    all.extend(stream_scenarios());
    all
}

fn stream_scenarios() -> Vec<Scn> {
    vec![
        Scn {
            name: "anthropic_messages_stream",
            expected_diff: Some("预期不同：和 OpenAI 流式同一个问题，旧系统 200 但记 0（usage 在 message_start/message_delta 里，没读到）；新系统按流末 usage 结算"),
            user: MAIN_USER,
            key: MAIN_KEY,
            path: "/v1/messages",
            auth: ("x-api-key", MAIN_KEY.into()),
            body: json!({"model": "claude-baseline", "max_tokens": 64, "stream": true, "messages": [{"role": "user", "content": "baseline ping"}]}),
            disconnect_after_first_chunk: false,
        },
        Scn {
            name: "gemini_stream_generate",
            expected_diff: Some("预期不同：旧系统转发时丢了 ?alt=sse 且按 body.stream 判断流式，上游回 JSON 数组后质量检查失败，502 不收费；新系统应 200 SSE 并按 usageMetadata 结算（需真实上游样本确认）"),
            user: MAIN_USER,
            key: MAIN_KEY,
            path: "/v1beta/models/gemini-baseline:streamGenerateContent?alt=sse",
            auth: ("x-goog-api-key", MAIN_KEY.into()),
            body: json!({"contents": [{"role": "user", "parts": [{"text": "baseline ping"}]}]}),
            disconnect_after_first_chunk: false,
        },
        Scn {
            disconnect_after_first_chunk: true,
            ..main_scenario(
                "openai_stream_client_disconnect",
                Some("预期不同：旧系统客户端断开后也断开上游、记 0；新系统继续读完上游按实际用量结算，没有用量就保留冻结并发事件（已定，最终以真实上游样本确认）"),
                chat("gpt-long", true),
            )
        },
    ]
}

async fn seed(
    db: &burncloud_database::Database,
    pool: &AnyPool,
    up_port: u16,
) -> anyhow::Result<()> {
    burncloud_service_user::UserDatabase::init(db).await?;
    common::ensure_l6_observability_columns(pool).await?;
    common::ensure_cost_status_column(pool).await?;
    common::ensure_error_type_column(pool).await?;
    for (key, user) in [
        (MAIN_KEY, MAIN_USER),
        (WALLET0_KEY, WALLET0_USER),
        (BROKE_KEY, BROKE_USER),
        (RL_KEY, RL_USER),
    ] {
        common::insert_router_token(db, key, user, "default", None, None).await?;
    }
    for (user, usd) in [
        (MAIN_USER, 10_000_000_000_i64),
        (WALLET0_USER, 0),
        (BROKE_USER, 10_000_000_000),
        (RL_USER, 10_000_000_000),
    ] {
        sqlx::query("UPDATE user_accounts SET balance_usd = ?, balance_cny = 0 WHERE id = ?")
            .bind(usd)
            .bind(user)
            .execute(pool)
            .await?;
    }
    for (key, remain, used) in [
        (MAIN_KEY, 100_000_000_000_i64, 0_i64),
        (WALLET0_KEY, -1, 0),
        (BROKE_KEY, 1_000, 1_000),
        (RL_KEY, -1, 0),
    ] {
        sqlx::query("UPDATE user_api_keys SET remain_quota = ?, used_quota = ? WHERE key = ?")
            .bind(remain)
            .bind(used)
            .bind(key)
            .execute(pool)
            .await?;
    }
    let base = |p: &str| format!("http://127.0.0.1:{up_port}/{p}");
    let channels = [
        (9101, "openai", base("openai"), "gpt-baseline", 1),
        (
            9102,
            "openai-nousage",
            base("openai-nousage"),
            "gpt-nousage",
            1,
        ),
        (FAIL_CH, "fail-500", base("fail"), "gpt-failover", 1),
        (
            OK_FAILOVER_CH,
            "failover-ok",
            base("openai/failover-ok"),
            "gpt-failover",
            1,
        ),
        (9105, "anthropic", base("anthropic"), "claude-baseline", 14),
        (9106, "gemini", base("gemini"), "gemini-baseline", 24),
        (9107, "openai-noprice", base("openai"), "gpt-noprice", 1),
        (9108, "openai-rl", base("openai"), "gpt-ratelimit", 1),
        (9109, "openai-long", base("openai-long"), "gpt-long", 1),
    ];
    for (id, name, url, model, ty) in &channels {
        common::insert_test_channel(pool, *id, name, url, UPSTREAM_KEY, model, "default").await?;
        sqlx::query("UPDATE channel_providers SET type = ? WHERE id = ?")
            .bind(*ty)
            .bind(*id)
            .execute(pool)
            .await?;
    }
    for model in PRICED_MODELS {
        let price = PriceInput {
            model: model.into(),
            currency: "USD".into(),
            input_price: IN_PRICE,
            output_price: OUT_PRICE,
            ..Default::default()
        };
        BillingPriceModel::upsert(db, &price).await?;
    }
    Ok(())
}

async fn run_scenario(ctx: &Ctx, s: Scn) -> anyhow::Result<(Value, Value)> {
    tracing::info!("=== BASELINE SCENARIO {} ===", s.name);
    ctx.logbuf.take();
    ctx.hits.clear();
    let before = snapshot(&ctx.pool, s.user, s.key).await?;
    let log_id = max_log_id(&ctx.pool).await?;
    let t0 = Instant::now();
    let resp = ctx
        .client
        .post(ctx.url(s.path))
        .header(s.auth.0, &s.auth.1)
        .json(&s.body)
        .send()
        .await?;
    let status = resp.status().as_u16();
    let ctype = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_owned();
    let mut ttfb = None;
    let mut raw = Vec::new();
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let c = chunk?;
        if ttfb.is_none() && !c.is_empty() {
            ttfb = Some(t0.elapsed().as_secs_f64() * 1000.0);
        }
        raw.extend_from_slice(&c);
        if s.disconnect_after_first_chunk {
            break;
        }
    }
    drop(stream);
    let total = t0.elapsed().as_secs_f64() * 1000.0;
    if s.disconnect_after_first_chunk {
        tokio::time::sleep(Duration::from_millis(1500)).await;
    }
    settle_wait(&ctx.pool, s.user, s.key).await?;
    let after = snapshot(&ctx.pool, s.user, s.key).await?;
    let rows = logs_after(&ctx.pool, log_id, &ctx.ports).await?;
    let lines = ctx.logbuf.take();
    let raw = String::from_utf8_lossy(&raw);
    let hits = ctx.hits.hits();
    let body = if s.disconnect_after_first_chunk {
        json!("<client disconnected after first chunk>")
    } else {
        norm_body(&raw, &ctx.ports)
    };
    let result = json!({
        "name": s.name,
        "expected_diff": s.expected_diff,
        "request": {"path": s.path, "auth_header": s.auth.0, "body": norm_json(&s.body, &ctx.ports)},
        "response": {"status": status, "content_type": ctype, "body": body},
        "upstream_hits": hits,
        "upstream_streams_completed": ctx.hits.completed(),
        "money_delta_nano": before.delta(&after),
        "router_logs": rows,
        "warn_error_logs": ctx.warn_error(&lines),
    });
    ctx.append_raw_log(s.name, &lines)?;
    Ok((result, json!({"ttfb_ms": ttfb, "total_ms": total})))
}

async fn rate_limit(ctx: &Ctx) -> anyhow::Result<(Value, Value)> {
    tracing::info!("=== BASELINE SCENARIO rate_limit ===");
    ctx.logbuf.take();
    let before = snapshot(&ctx.pool, RL_USER, RL_KEY).await?;
    let log_id = max_log_id(&ctx.pool).await?;
    let (mut ok, mut limited, mut other) = (0_u32, 0_u32, Vec::new());
    let mut first_429 = None;
    let t0 = Instant::now();
    for _ in 0..RATE_LIMIT_REQUESTS {
        let r = ctx
            .client
            .post(ctx.url("/v1/chat/completions"))
            .header("authorization", format!("Bearer {RL_KEY}"))
            .json(&chat("gpt-ratelimit", false))
            .send()
            .await?;
        let status = r.status().as_u16();
        let body = r.text().await?;
        match status {
            200 => ok += 1,
            429 => {
                limited += 1;
                if first_429.is_none() {
                    first_429 = Some(norm_body(&body, &ctx.ports));
                }
            }
            s => other.push(s),
        }
    }
    let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;
    settle_wait(&ctx.pool, RL_USER, RL_KEY).await?;
    let after = snapshot(&ctx.pool, RL_USER, RL_KEY).await?;
    let rows = logs_after(&ctx.pool, log_id, &ctx.ports).await?;
    let charged = after.key_used_quota.unwrap_or(0) - before.key_used_quota.unwrap_or(0);
    let logged_429 = rows
        .iter()
        .filter(|r| r["status_code"] == json!(429))
        .count();
    let lines = ctx.logbuf.take();
    ctx.append_raw_log("rate_limit", &lines)?;
    let result = json!({
        "name": "rate_limit",
        "expected_diff": null,
        "response": {"saw_429": limited > 0, "first_429_body": first_429, "non_200_non_429_statuses": other},
        "invariants": {
            "charged_equals_successes_times_unit_cost": charged == i64::from(ok) * UNIT_COST,
            "unit_cost_nano": UNIT_COST,
            "rate_limited_requests_logged_to_router_logs": logged_429 > 0,
            "balance_usd_delta": snapshot::delta(before.balance_usd, after.balance_usd),
        },
    });
    let nondet = json!({"rate_limit": {
        "ok": ok,
        "limited": limited,
        "elapsed_ms": elapsed_ms,
        "charged_nano": charged,
        "router_logs_rows": rows.len(),
        "logged_429": logged_429,
    }});
    Ok((result, nondet))
}

fn percentile(sorted: &[f64], permille: usize) -> f64 {
    let idx = (sorted.len().saturating_sub(1) * permille + 500) / 1000;
    sorted.get(idx).copied().unwrap_or(f64::NAN)
}

async fn perf(ctx: &Ctx, n: usize) -> anyhow::Result<Map<String, Value>> {
    let mut out = Map::new();
    if n == 0 {
        return Ok(out);
    }
    for (label, stream) in [("openai_nonstream_ok", false), ("openai_stream_ok", true)] {
        let (mut ttfbs, mut totals) = (Vec::with_capacity(n), Vec::with_capacity(n));
        for _ in 0..n {
            let t0 = Instant::now();
            let r = ctx
                .client
                .post(ctx.url("/v1/chat/completions"))
                .header("authorization", format!("Bearer {MAIN_KEY}"))
                .json(&chat("gpt-baseline", stream))
                .send()
                .await?;
            let mut st = r.bytes_stream();
            let mut first = None;
            while let Some(c) = st.next().await {
                let c = c?;
                if first.is_none() && !c.is_empty() {
                    first = Some(t0.elapsed().as_secs_f64() * 1000.0);
                }
            }
            ttfbs.push(first.unwrap_or(f64::NAN));
            totals.push(t0.elapsed().as_secs_f64() * 1000.0);
            tokio::time::sleep(Duration::from_millis(110)).await;
        }
        ttfbs.sort_by(f64::total_cmp);
        totals.sort_by(f64::total_cmp);
        out.insert(
            label.into(),
            json!({
                "n": n,
                "ttfb_p50_ms": percentile(&ttfbs, 500),
                "ttfb_p95_ms": percentile(&ttfbs, 950),
                "total_p50_ms": percentile(&totals, 500),
                "total_p95_ms": percentile(&totals, 950),
            }),
        );
    }
    Ok(out)
}

fn old_commit() -> anyhow::Result<String> {
    let commit =
        std::env::var("OLD_COMMIT").context("OLD_COMMIT 未设置，请用 baseline-tool 运行")?;
    anyhow::ensure!(
        commit.len() == 40 && commit.bytes().all(|b| b.is_ascii_hexdigit()),
        "OLD_COMMIT 必须是 40 位十六进制 SHA：{commit}"
    );
    Ok(commit)
}

async fn run(out_path: String, raw_log_path: String, old_commit: String) -> anyhow::Result<()> {
    let logbuf = LogBuf::default();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new("info"))
        .with_ansi(false)
        .without_time()
        .with_writer(logbuf.clone())
        .try_init()
        .map_err(|e| anyhow::anyhow!("tracing init: {e}"))?;

    let (db, pool, db_url) = common::setup_db().await?;
    let hits: Hits = Arc::default();
    let up = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let up_port = up.local_addr()?.port();
    tokio::spawn(upstream::serve(up, Arc::clone(&hits)));
    seed(&db, &pool, up_port).await?;
    let session = failover_session()?;

    let rl = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let r_port = rl.local_addr()?.port();
    common::start_test_server_on(rl, &db_url).await;
    let ctx = Ctx {
        client: reqwest::Client::new(),
        pool,
        logbuf,
        hits,
        r_port,
        ports: [up_port, r_port],
        raw_log_path,
    };
    let startup_logs = ctx.logbuf.take();

    let mut results = Vec::new();
    let mut timing = Map::new();
    for s in scenarios(&session) {
        let name = s.name;
        let (result, t) = run_scenario(&ctx, s).await?;
        results.push(result);
        timing.insert(name.into(), t);
    }
    let (rl_result, nondet) = rate_limit(&ctx).await?;
    results.push(rl_result);

    let n = std::env::var("BASELINE_PERF")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let perf = perf(&ctx, n).await?;
    let out = json!({
        "meta": {
            "old_repo": "burncloud/burncloud",
            "old_commit": old_commit,
            "entry": "burncloud_router::create_router_app (+ internal_app merge), fixtures from tests/common.rs",
            "scheduler_policies": SCHEDULER_POLICIES,
            "prices_nano_per_1m": {"input": IN_PRICE, "output": OUT_PRICE},
            "upstream_usage": {"prompt": 10, "completion": 20}
        },
        "compared": {"scenarios": results},
        "not_compared": {
            "perf": perf,
            "build_profile": if cfg!(debug_assertions) { "debug" } else { "release" },
            "timing_ms": timing,
            "nondeterministic": nondet,
            "startup_warn_error": startup_logs
                .lines()
                .filter(|l| l.contains("WARN") || l.contains("ERROR"))
                .map(|l| norm_log_line(l, &ctx.ports))
                .collect::<Vec<_>>()
        },
    });
    std::fs::write(&out_path, serde_json::to_string_pretty(&out)?)?;
    eprintln!("wrote {out_path}");
    Ok(())
}

fn start() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let usage = "usage: old-relay-baseline <out.json> <raw.log>";
    let out_path = args.next().context(usage)?;
    let raw_log_path = args.next().context(usage)?;
    let old_commit = old_commit()?;
    std::env::set_var("SCHEDULER_POLICIES", SCHEDULER_POLICIES);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let result = rt.block_on(run(out_path, raw_log_path, old_commit));
    rt.shutdown_timeout(Duration::from_secs(1));
    result
}

fn main() -> ExitCode {
    match start() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e:?}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{failover_session, percentile, scenarios, UNIT_COST};

    #[test]
    fn percentile_matches_nearest_rank_rounding() {
        let v: Vec<f64> = (0..200).map(f64::from).collect();
        assert!((percentile(&v, 500) - 100.0).abs() < f64::EPSILON);
        assert!((percentile(&v, 950) - 189.0).abs() < f64::EPSILON);
        assert!(percentile(&[], 500).is_nan());
    }

    #[test]
    fn unit_cost_is_180000_nano() {
        assert_eq!(UNIT_COST, 180_000);
    }

    #[test]
    fn failover_session_is_found_and_stable() -> anyhow::Result<()> {
        assert_eq!(failover_session()?, failover_session()?);
        Ok(())
    }

    #[test]
    fn thirteen_scenarios_with_unique_names() {
        let s = scenarios("s");
        let mut names: Vec<_> = s.iter().map(|x| x.name).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), 13);
    }
}
