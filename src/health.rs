//! Subscription health (Extension 2): per-source message rate and the periodic-recycle
//! schedule. Pure; the arbiter owns the state and turns verdicts into actions.

use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
};

use crate::model::ConnId;

/// Rule 1: a replica ahead of the active source for this long means the active one is
/// stale for that market.
pub const LAG_LIMIT_US: i64 = 2_000_000;

/// Rule 2: a rate below this percent of the baseline is a collapse.
const COLLAPSE_PCT: i64 = 10;
/// Baseline floor (milli-messages per minute): quieter markets have no baseline.
const MIN_BASELINE_MILLI: i64 = 1_000;
/// Minutes of history before a baseline counts.
const WARMUP_MINUTES: u32 = 5;
/// At most one rate-driven resubscribe per subscription per this long.
const RESUB_COOLDOWN_US: i64 = 600_000_000;

/// Rule 4: each socket is recycled every 30–45 minutes, with at least this long between
/// any two recycles.
const RECYCLE_MIN_US: i64 = 30 * 60_000_000;
const RECYCLE_JITTER_US: i64 = 15 * 60_000_000;
pub const RECYCLE_SPACING_US: i64 = 30_000_000;

/// Messages per minute for one (market, connection), as an integer EWMA in
/// milli-messages per minute.
#[derive(Debug, Default, Clone)]
pub struct Rate {
    minute_count: i64,
    ewma_milli: i64,
    minutes: u32,
    low_minutes: i64,
    last_resub_us: i64,
}

impl Rate {
    pub fn hit(&mut self) {
        self.minute_count += 1;
    }

    /// Close a one-minute window. True = the rate collapsed below 10% of its baseline for
    /// `max(1 min, time for 5 baseline messages)`: resubscribe this market here.
    pub fn on_minute(&mut self, now_us: i64) -> bool {
        let got = self.minute_count * 1000;
        self.minute_count = 0;
        self.minutes += 1;
        let baseline = self.ewma_milli;
        let has_baseline = self.minutes > WARMUP_MINUTES && baseline >= MIN_BASELINE_MILLI;
        if has_baseline && got * 100 < baseline * COLLAPSE_PCT {
            // The baseline is frozen while collapsed, or the EWMA would decay into it.
            self.low_minutes += 1;
            let need = ((5_000 + baseline - 1) / baseline).max(1);
            if self.low_minutes >= need && now_us - self.last_resub_us >= RESUB_COOLDOWN_US {
                self.low_minutes = 0;
                self.last_resub_us = now_us;
                return true;
            }
            return false;
        }
        self.low_minutes = 0;
        self.ewma_milli = if self.minutes == 1 {
            got
        } else {
            (baseline * 4 + got) / 5
        };
        false
    }
}

/// When each connection is next due for a full reconnect.
#[derive(Debug, Default)]
pub struct Recycler {
    next_at: HashMap<ConnId, i64>,
    last_us: i64,
}

impl Recycler {
    /// Is `conn` due, and has enough time passed since the last recycle anywhere? A
    /// connection seen for the first time is scheduled, not due.
    pub fn due(&mut self, conn: ConnId, now_us: i64) -> bool {
        let at = *self
            .next_at
            .entry(conn)
            .or_insert_with(|| now_us + interval(conn, now_us));
        now_us >= at && now_us - self.last_us >= RECYCLE_SPACING_US
    }

    pub fn recycled(&mut self, conn: ConnId, now_us: i64) {
        self.last_us = now_us;
        self.next_at.insert(conn, now_us + interval(conn, now_us));
    }
}

fn interval(conn: ConnId, now_us: i64) -> i64 {
    let mut h = DefaultHasher::new();
    (conn, now_us).hash(&mut h);
    RECYCLE_MIN_US + (h.finish() % RECYCLE_JITTER_US as u64) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIN: i64 = 60_000_000;

    fn minutes(r: &mut Rate, start: i64, per_minute: i64, n: i64) -> Vec<i64> {
        let mut fired = Vec::new();
        for i in 0..n {
            for _ in 0..per_minute {
                r.hit();
            }
            let now = start + i * MIN;
            if r.on_minute(now) {
                fired.push(now);
            }
        }
        fired
    }

    #[test]
    fn collapse_after_baseline_resubscribes_then_cools_down() {
        let mut r = Rate::default();
        assert!(minutes(&mut r, 0, 100, 10).is_empty()); // baseline 100/min
        // 5/baseline is far under a minute, so one collapsed minute is enough.
        let fired = minutes(&mut r, 10 * MIN, 2, 10);
        assert_eq!(fired, vec![10 * MIN]); // then the 10-minute cooldown holds
        let fired = minutes(&mut r, 25 * MIN, 2, 2);
        assert_eq!(fired, vec![25 * MIN]);
    }

    #[test]
    fn slow_baseline_needs_time_for_five_messages() {
        let mut r = Rate::default();
        minutes(&mut r, 0, 2, 10); // baseline 2/min: 5 messages take 2.5 minutes
        let fired = minutes(&mut r, 10 * MIN, 0, 4);
        assert_eq!(fired, vec![12 * MIN]); // the third quiet minute
    }

    #[test]
    fn quiet_markets_and_new_subscriptions_are_exempt() {
        let mut never = Rate::default();
        assert!(minutes(&mut never, 0, 0, 30).is_empty());
        let mut warming = Rate::default();
        minutes(&mut warming, 0, 100, 3);
        assert!(minutes(&mut warming, 3 * MIN, 0, 2).is_empty());
    }

    #[test]
    fn recycle_is_scheduled_then_spaced() {
        let mut r = Recycler::default();
        assert!(!r.due(0, 0) && !r.due(1, 0));
        let late = 46 * MIN;
        assert!(r.due(0, late) && r.due(1, late));
        r.recycled(0, late);
        assert!(!r.due(1, late + 1)); // spacing
        assert!(r.due(1, late + RECYCLE_SPACING_US));
        assert!(!r.due(0, late + RECYCLE_SPACING_US)); // rescheduled 30–45 min out
    }
}
