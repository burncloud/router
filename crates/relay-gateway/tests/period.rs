use identity::model::LimitPeriod;
use kernel::model::UnixMillis;
use proptest::prelude::*;
use relay_gateway::since;

const DAY: u64 = 86_400_000;

#[test]
fn known_dates() {
    let cases = [
        (0, 0, 0),
        (1_709_208_000_000, 1_709_164_800_000, 1_706_745_600_000),
        (1_735_689_599_999, 1_735_603_200_000, 1_733_011_200_000),
        (1_735_689_600_000, 1_735_689_600_000, 1_735_689_600_000),
        (951_825_600_000, 951_782_400_000, 949_363_200_000),
        (4_107_542_400_000, 4_107_542_400_000, 4_107_542_400_000),
        (1_795_000_000_000, 1_794_960_000_000, 1_793_491_200_000),
    ];
    for (now, day, month) in cases {
        let now = UnixMillis(now);
        assert_eq!(since(LimitPeriod::Lifetime, now), UnixMillis(0));
        assert_eq!(since(LimitPeriod::Daily, now), UnixMillis(day), "{now:?}");
        assert_eq!(
            since(LimitPeriod::Monthly, now),
            UnixMillis(month),
            "{now:?}"
        );
    }
}

proptest! {
    #[test]
    fn month_start_is_the_first_midnight_of_the_month(now in 0_u64..10_000_000_000_000) {
        let start = since(LimitPeriod::Monthly, UnixMillis(now)).0;
        let day = since(LimitPeriod::Daily, UnixMillis(now)).0;
        prop_assert!(start <= day && day <= now);
        prop_assert_eq!(start % DAY, 0);
        prop_assert!(now - start < 31 * DAY);
        if start >= DAY {
            let previous = since(LimitPeriod::Monthly, UnixMillis(start - 1)).0;
            prop_assert!(start - previous >= 28 * DAY);
            prop_assert!(start - previous <= 31 * DAY);
        }
        prop_assert_eq!(since(LimitPeriod::Monthly, UnixMillis(start)).0, start);
    }
}
