use std::sync::Mutex;

use kernel::model::UnixMillis;

use runtime::events::UsageUnreadableRatio;

/// 已决请求里读不出用量的告警阈值：窗口、分子、分母。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UnreadableThreshold {
    window: u64,
    numerator: u64,
    denominator: u64,
}

/// 窗口为 0、分母为 0，或分子大于分母。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, thiserror::Error)]
#[error("unreadable threshold is invalid")]
pub struct InvalidUnreadableThreshold;

impl UnreadableThreshold {
    /// 至少 100 次已决，且读不出的比例达到 1/100。
    pub const DEFAULT: Self = Self {
        window: 100,
        numerator: 1,
        denominator: 100,
    };

    /// # Errors
    ///
    /// 窗口为 0、分母为 0，或分子大于分母。
    pub const fn new(
        window: u64,
        numerator: u64,
        denominator: u64,
    ) -> Result<Self, InvalidUnreadableThreshold> {
        if window == 0 || denominator == 0 || numerator > denominator {
            return Err(InvalidUnreadableThreshold);
        }
        Ok(Self {
            window,
            numerator,
            denominator,
        })
    }
}

#[derive(Debug)]
pub(crate) struct Watch {
    threshold: UnreadableThreshold,
    state: Mutex<State>,
}

#[derive(Debug)]
struct State {
    decided: u64,
    unreadable: u64,
    above: bool,
}

impl Watch {
    pub(crate) const fn new(threshold: UnreadableThreshold) -> Self {
        Self {
            threshold,
            state: Mutex::new(State {
                decided: 0,
                unreadable: 0,
                above: false,
            }),
        }
    }

    pub(crate) fn record(&self, unreadable: bool, at: UnixMillis) -> Option<UsageUnreadableRatio> {
        let mut state = match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        state.decided = state.decided.saturating_add(1);
        if unreadable {
            state.unreadable = state.unreadable.saturating_add(1);
        }
        let above = exceeds(state.decided, state.unreadable, self.threshold);
        let rising = above && !state.above;
        state.above = above;
        if !rising {
            return None;
        }
        Some(UsageUnreadableRatio {
            decided: state.decided,
            unreadable: state.unreadable,
            window: self.threshold.window,
            ratio_num: self.threshold.numerator,
            ratio_den: self.threshold.denominator,
            at,
        })
    }
}

fn exceeds(decided: u64, unreadable: u64, threshold: UnreadableThreshold) -> bool {
    decided >= threshold.window
        && u128::from(unreadable) * u128::from(threshold.denominator)
            >= u128::from(decided) * u128::from(threshold.numerator)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use kernel::model::UnixMillis;
    use proptest::prelude::*;

    use super::{exceeds, Watch};
    use crate::ratio::{InvalidUnreadableThreshold, UnreadableThreshold};

    fn must(window: u64, numerator: u64, denominator: u64) -> UnreadableThreshold {
        match UnreadableThreshold::new(window, numerator, denominator) {
            Ok(threshold) => threshold,
            Err(error) => panic!("{error}"),
        }
    }

    #[test]
    fn invalid_thresholds_are_rejected() {
        let rejected = [(0, 1, 1), (1, 1, 0), (1, 2, 1), (0, u64::MAX, 0)];
        for (window, numerator, denominator) in rejected {
            assert_eq!(
                UnreadableThreshold::new(window, numerator, denominator),
                Err(InvalidUnreadableThreshold)
            );
        }
        assert_eq!(
            UnreadableThreshold::new(100, 1, 100),
            Ok(UnreadableThreshold::DEFAULT)
        );
        assert!(UnreadableThreshold::new(8, 0, 1).is_ok());
        assert!(UnreadableThreshold::new(8, 8, 8).is_ok());
    }

    #[test]
    fn threshold_math_uses_wide_multiplication() {
        let default = UnreadableThreshold::DEFAULT;
        let full = must(1, u64::MAX, u64::MAX);
        let rare = must(1, 1, u64::MAX);
        let half = must(1, 1, 2);
        let rows = [
            (99, 99, default, false),
            (100, 1, default, true),
            (100, 0, default, false),
            (101, 1, default, false),
            (u64::MAX, u64::MAX, full, true),
            (u64::MAX, u64::MAX - 1, full, false),
            (u64::MAX, 1, rare, true),
            (u64::MAX, 0, rare, false),
            (u64::MAX, u64::MAX / 2, half, false),
            (u64::MAX, u64::MAX / 2 + 1, half, true),
            (3, 0, must(4, 0, 5), false),
            (4, 0, must(4, 0, 5), true),
        ];
        for (decided, unreadable, threshold, expected) in rows {
            assert_eq!(exceeds(decided, unreadable, threshold), expected);
        }
    }

    #[test]
    fn default_threshold_alerts_on_rising_edges_only() {
        let at = UnixMillis(4);
        let early = Watch::new(UnreadableThreshold::DEFAULT);
        for _ in 0..99 {
            assert!(early.record(true, at).is_none());
        }
        let Some(hit) = early.record(true, at) else {
            panic!("missing edge");
        };
        assert_eq!((hit.decided, hit.unreadable), (100, 100));
        assert!(early.record(true, at).is_none());
        let watch = Watch::new(UnreadableThreshold::DEFAULT);
        for _ in 0..99 {
            assert!(watch.record(false, at).is_none());
        }
        let Some(first) = watch.record(true, at) else {
            panic!("missing edge");
        };
        assert_eq!((first.decided, first.unreadable, first.at), (100, 1, at));
        assert_eq!(
            (first.window, first.ratio_num, first.ratio_den),
            (100, 1, 100)
        );
        assert!(watch.record(true, at).is_none());
        for _ in 0..100 {
            assert!(watch.record(false, at).is_none());
        }
        let Some(second) = watch.record(true, at) else {
            panic!("missing second edge");
        };
        assert_eq!((second.decided, second.unreadable), (202, 3));
    }

    #[test]
    fn concurrent_crossing_alerts_once() {
        let watch = Arc::new(Watch::new(must(32, 1, 2)));
        let alerts = AtomicUsize::new(0);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let watch = Arc::clone(&watch);
                let alerts = &alerts;
                scope.spawn(move || {
                    for _ in 0..20 {
                        if watch.record(true, UnixMillis(0)).is_some() {
                            alerts.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                });
            }
        });
        assert_eq!(alerts.load(Ordering::SeqCst), 1);
    }

    proptest! {
        #[test]
        fn alerts_are_exactly_the_rising_edges(
            window in 1u64..64,
            numerator in 0u64..32,
            denominator in 1u64..32,
            steps in proptest::collection::vec(any::<bool>(), 0..40),
        ) {
            prop_assume!(numerator <= denominator);
            let Ok(threshold) = UnreadableThreshold::new(window, numerator, denominator) else {
                panic!("threshold");
            };
            let watch = Watch::new(threshold);
            let mut decided = 0u64;
            let mut unreadable = 0u64;
            let mut above = false;
            for step in steps {
                decided += 1;
                unreadable += u64::from(step);
                let event = watch.record(step, UnixMillis(decided));
                let now = exceeds(decided, unreadable, threshold);
                prop_assert_eq!(event.is_some(), now && !above);
                if let Some(event) = event {
                    prop_assert_eq!((event.decided, event.unreadable), (decided, unreadable));
                    prop_assert_eq!(event.at, UnixMillis(decided));
                }
                above = now;
            }
        }
    }
}
