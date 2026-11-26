use std::time::{Duration, Instant};

use burncloud_database::sqlx::{self, AnyPool};
use serde_json::{json, Value};

use crate::normalize::norm_str;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Snapshot {
    pub(crate) balance_usd: Option<i64>,
    pub(crate) balance_cny: Option<i64>,
    pub(crate) key_used_quota: Option<i64>,
    pub(crate) key_remain_quota: Option<i64>,
    pub(crate) router_token_used_quota: Option<i64>,
}

impl Snapshot {
    pub(crate) fn delta(&self, after: &Self) -> Value {
        json!({
            "balance_usd": delta(self.balance_usd, after.balance_usd),
            "balance_cny": delta(self.balance_cny, after.balance_cny),
            "key_used_quota": delta(self.key_used_quota, after.key_used_quota),
            "key_remain_quota": delta(self.key_remain_quota, after.key_remain_quota),
            "router_token_used_quota": delta(self.router_token_used_quota, after.router_token_used_quota),
        })
    }
}

pub(crate) async fn snapshot(pool: &AnyPool, user: &str, key: &str) -> anyhow::Result<Snapshot> {
    let (balance_usd, balance_cny) = sqlx::query_as::<_, (Option<i64>, Option<i64>)>(
        "SELECT CAST(balance_usd AS INTEGER), CAST(balance_cny AS INTEGER) FROM user_accounts WHERE id = ?",
    )
    .bind(user)
    .fetch_optional(pool)
    .await?
    .unwrap_or((None, None));
    let (key_used_quota, key_remain_quota) = sqlx::query_as::<_, (Option<i64>, Option<i64>)>(
        "SELECT CAST(used_quota AS INTEGER), CAST(remain_quota AS INTEGER) FROM user_api_keys WHERE key = ?",
    )
    .bind(key)
    .fetch_optional(pool)
    .await?
    .unwrap_or((None, None));
    let router_token_used_quota = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT CAST(used_quota AS INTEGER) FROM router_tokens WHERE token = ?",
    )
    .bind(key)
    .fetch_optional(pool)
    .await?
    .flatten();
    Ok(Snapshot {
        balance_usd,
        balance_cny,
        key_used_quota,
        key_remain_quota,
        router_token_used_quota,
    })
}

pub(crate) fn delta(before: Option<i64>, after: Option<i64>) -> Value {
    match (before, after) {
        (Some(b), Some(a)) => json!(a - b),
        _ => json!({"before": before, "after": after}),
    }
}

pub(crate) async fn max_log_id(pool: &AnyPool) -> anyhow::Result<i64> {
    Ok(
        sqlx::query_scalar::<_, Option<i64>>("SELECT MAX(id) FROM router_logs")
            .fetch_one(pool)
            .await?
            .unwrap_or(0),
    )
}

type LogRow = (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<String>,
    Option<String>,
    Option<String>,
);

pub(crate) async fn logs_after(
    pool: &AnyPool,
    id: i64,
    ports: &[u16],
) -> anyhow::Result<Vec<Value>> {
    let rows: Vec<LogRow> = sqlx::query_as(
        "SELECT user_id, path, upstream_id, CAST(status_code AS INTEGER), CAST(prompt_tokens AS INTEGER), \
         CAST(completion_tokens AS INTEGER), CAST(cost AS INTEGER), model, cost_status, error_type \
         FROM router_logs WHERE id > ? ORDER BY id",
    )
    .bind(id)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| {
            json!({
                "user_id": r.0,
                "path": r.1.map(|p| norm_str(&p, ports)),
                "upstream_id": r.2,
                "status_code": r.3,
                "prompt_tokens": r.4,
                "completion_tokens": r.5,
                "cost_nano": r.6,
                "model": r.7,
                "cost_status": r.8,
                "error_type": r.9
            })
        })
        .collect())
}

pub(crate) async fn settle_wait(pool: &AnyPool, user: &str, key: &str) -> anyhow::Result<()> {
    let mut last = (max_log_id(pool).await?, snapshot(pool, user, key).await?);
    let mut stable = 0;
    let start = Instant::now();
    while stable < 3 && start.elapsed() < Duration::from_secs(6) {
        tokio::time::sleep(Duration::from_millis(150)).await;
        let now = (max_log_id(pool).await?, snapshot(pool, user, key).await?);
        if now == last {
            stable += 1;
        } else {
            stable = 0;
            last = now;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::delta;
    use serde_json::json;

    #[test]
    fn delta_reports_both_sides_when_a_value_is_missing() {
        assert_eq!(delta(Some(5), Some(180_005)), json!(180_000));
        assert_eq!(delta(None, Some(1)), json!({"before": null, "after": 1}));
    }
}
