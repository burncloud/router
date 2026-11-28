use identity::model::LimitPeriod;
use kernel::model::UnixMillis;

const DAY_MILLIS: u64 = 86_400_000;

/// `Lifetime` 是 `UnixMillis(0)`，`Daily`、`Monthly` 是 `now` 所在 UTC 自然日、自然月的起点。
#[must_use]
pub fn since(period: LimitPeriod, now: UnixMillis) -> UnixMillis {
    match period {
        LimitPeriod::Lifetime => UnixMillis(0),
        LimitPeriod::Daily => UnixMillis(now.0 - now.0 % DAY_MILLIS),
        LimitPeriod::Monthly => {
            let days = now.0 / DAY_MILLIS;
            UnixMillis((days + 1 - day_of_month(days)) * DAY_MILLIS)
        }
    }
}

fn day_of_month(days: u64) -> u64 {
    let day_of_era = (days + 719_468) % 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted_month = (5 * day_of_year + 2) / 153;
    day_of_year - (153 * shifted_month + 2) / 5 + 1
}
