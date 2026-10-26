//! Output checker: decode an SBE capture with the starter reader and prove, per symbol,
//! that deltas and snapshots agree.
//!
//! - Every symbol must start with a snapshot.
//! - Updates are applied as absolute quantities.
//! - A later snapshot must equal the rebuilt book, unless the collector logged a
//!   non-periodic cause for it (`--events`: subscribe, resync, exchange clear). Periodic
//!   and shutdown snapshots are not logged per market, so they must always match.
//! - Consecutive snapshots of a symbol are at most 61s apart while it is live (a
//!   `Removed` snapshot ends it; a re-add starts it again). A longer interval is
//!   accepted only if the collector logged a gap (no valid source) inside it; those are
//!   reported as `gap_intervals`.
//! - `backtest_timestamp` regressions per symbol are reported (not fatal).
//!
//! Prints a JSON summary; exit 0 only if there is nothing unexplained.

use std::{
    collections::{BTreeMap, HashMap},
    io::Write,
    path::PathBuf,
};

use clap::Parser;
use engineering_challenge::util::{
    AlignedSbeReader, OrderbookSchema,
    dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain, SnapshotDomain},
};
use serde::Serialize;

const MAX_SNAPSHOT_INTERVAL_US: i64 = 61_000_000;

#[derive(Parser)]
struct Args {
    capture: PathBuf,
    /// The collector's stderr log; explains non-periodic snapshots.
    #[arg(long)]
    events: Option<PathBuf>,
    /// Zero-pad a capture whose last chunk was cut off (collector killed) before reading.
    #[arg(long)]
    repair: bool,
}

type Book = BTreeMap<(u8, i64), i64>;

#[derive(Default, Serialize)]
struct Symbol {
    snapshots: u64,
    updates: u64,
    explained_mismatches: u64,
    unexplained_mismatches: u64,
    periodic_checked: u64,
    updates_before_snapshot: u64,
    interval_violations: u64,
    gap_intervals: u64,
    max_interval_us: i64,
    timestamp_regressions: u64,
    #[serde(skip)]
    book: Option<Book>,
    #[serde(skip)]
    last_snapshot_us: Option<i64>,
    /// The last snapshot was a removal: the symbol was not live until this one.
    #[serde(skip)]
    ended: bool,
    #[serde(skip)]
    last_ts: i64,
}

/// `(market, recv_us) -> cause` of every snapshot the collector logged with a cause.
fn read_events(log: &str) -> anyhow::Result<HashMap<(String, i64), String>> {
    let mut set = HashMap::new();
    for line in log.lines() {
        if !line.contains(" snapshot: snapshot ") {
            continue;
        }
        let field = |k: &str| {
            line.split_whitespace()
                .find_map(|t| t.strip_prefix(k))
                .map(str::to_owned)
        };
        if let (Some(m), Some(us), Some(cause)) =
            (field("market="), field("recv_us="), field("cause="))
        {
            set.insert((m, us.parse()?), cause);
        }
    }
    Ok(set)
}

/// Gap start times per market (`gap market=… since_us=…` lines).
fn read_gaps(log: &str) -> anyhow::Result<HashMap<String, Vec<i64>>> {
    let mut gaps: HashMap<String, Vec<i64>> = HashMap::new();
    for line in log.lines() {
        if !line.contains(" gap: gap ") {
            continue;
        }
        let field = |k: &str| line.split_whitespace().find_map(|t| t.strip_prefix(k));
        if let (Some(m), Some(us)) = (field("market="), field("since_us=")) {
            gaps.entry(m.to_owned()).or_default().push(us.parse()?);
        }
    }
    Ok(gaps)
}

/// The checks, one message at a time.
#[derive(Default)]
struct Checker {
    events: HashMap<(String, i64), String>,
    gaps: HashMap<String, Vec<i64>>,
    symbols: HashMap<String, Symbol>,
    messages: u64,
    lag: Lag,
    /// First few problems, for diagnosis: (kind, symbol, previous_us, this_us).
    examples: Vec<(&'static str, String, i64, i64)>,
}

impl Checker {
    fn snapshot(&mut self, s: &SnapshotDomain) {
        self.messages += 1;
        let at = s.received_at.timestamp_micros();
        let st = self.symbols.entry(s.symbol.clone()).or_default();
        st.snapshots += 1;
        let mut snap = Book::new();
        snap.extend(s.bids.iter().map(|l| ((0, l.price_level), l.quantity)));
        snap.extend(s.asks.iter().map(|l| ((1, l.price_level), l.quantity)));
        let cause = self.events.get(&(s.symbol.clone(), at));
        let explained = cause.is_some();
        if let Some(book) = &st.book {
            if !explained {
                st.periodic_checked += 1;
            }
            if *book != snap {
                if explained {
                    st.explained_mismatches += 1;
                } else {
                    st.unexplained_mismatches += 1;
                }
            }
        }
        if let Some(prev) = st.last_snapshot_us.filter(|_| !st.ended) {
            st.max_interval_us = st.max_interval_us.max(at - prev);
            let in_gap = self.gaps.get(&s.symbol).is_some_and(|g| {
                g.iter()
                    .any(|&t| prev - MAX_SNAPSHOT_INTERVAL_US <= t && t <= at)
            });
            if at - prev > MAX_SNAPSHOT_INTERVAL_US && in_gap {
                st.gap_intervals += 1;
            } else if at - prev > MAX_SNAPSHOT_INTERVAL_US {
                st.interval_violations += 1;
                if self.examples.len() < 10 {
                    self.examples.push(("interval", s.symbol.clone(), prev, at));
                }
            }
        }
        st.last_snapshot_us = Some(at);
        st.ended = cause.is_some_and(|c| c == "Removed");
        st.book = Some(snap);
        if check_ts(st, at) && self.examples.len() < 10 {
            self.examples
                .push(("regression_snapshot", s.symbol.clone(), st.last_ts, at));
        }
    }

    fn update(&mut self, u: &PriceLevelUpdateDomain) {
        self.messages += 1;
        let st = self.symbols.entry(u.symbol.clone()).or_default();
        st.updates += 1;
        let at = u.backtest_timestamp.timestamp_micros();
        self.lag.add(at - u.exchange_timestamp.timestamp_micros());
        if check_ts(st, at) && self.examples.len() < 10 {
            self.examples
                .push(("regression_update", u.symbol.clone(), st.last_ts, at));
        }
        let Some(book) = st.book.as_mut() else {
            st.updates_before_snapshot += 1;
            return;
        };
        let key = (u.side as u8, u.price_level);
        if u.quantity == 0 {
            book.remove(&key);
        } else {
            book.insert(key, u.quantity);
        }
    }

    fn sum(&self, f: fn(&Symbol) -> u64) -> u64 {
        self.symbols.values().map(f).sum()
    }

    /// Mismatches, updates before a first snapshot and interval violations: anything
    /// that makes the capture fail.
    fn unexplained(&self) -> u64 {
        self.sum(|s| s.unexplained_mismatches)
            + self.sum(|s| s.updates_before_snapshot)
            + self.sum(|s| s.interval_violations)
    }

    fn summary(&self) -> serde_json::Value {
        serde_json::json!({
            "ok": self.unexplained() == 0,
            "messages": self.messages,
            "symbols": self.symbols.len(),
            "snapshots": self.sum(|s| s.snapshots),
            "updates": self.sum(|s| s.updates),
            "min_snapshots_per_symbol": self.symbols.values().map(|s| s.snapshots).min(),
            "periodic_checked": self.sum(|s| s.periodic_checked),
            "explained_mismatches": self.sum(|s| s.explained_mismatches),
            "unexplained_mismatches": self.sum(|s| s.unexplained_mismatches),
            "updates_before_snapshot": self.sum(|s| s.updates_before_snapshot),
            "interval_violations": self.sum(|s| s.interval_violations),
            "gap_intervals": self.sum(|s| s.gap_intervals),
            "max_snapshot_interval_us": self.symbols.values().map(|s| s.max_interval_us).max(),
            "timestamp_regressions": self.sum(|s| s.timestamp_regressions),
            "events_loaded": self.events.len(),
            "receive_minus_exchange_us": self.lag.summary(),
            "examples": self.examples,
            "per_symbol": self.symbols.iter().collect::<BTreeMap<_, _>>(),
        })
    }
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let mut c = Checker::default();
    if let Some(p) = &args.events {
        let log = std::fs::read_to_string(p)?;
        c.events = read_events(&log)?;
        c.gaps = read_gaps(&log)?;
    }
    if args.repair {
        let added = engineering_challenge::writer::pad_capture(
            &args.capture,
            engineering_challenge::writer::CHUNK,
        )?;
        eprintln!("repair: padded {added} bytes");
    }
    let mut r = AlignedSbeReader::<OrderbookSchema>::open(&args.capture)?;
    while let Some(msg) = r.read_msg()? {
        match msg {
            AnyMessage::Snapshot(s) => c.snapshot(&SnapshotDomain::try_from_decoder(s)?),
            AnyMessage::PriceLevelUpdate(u) => {
                c.update(&PriceLevelUpdateDomain::try_from_decoder(u)?)
            }
            _ => anyhow::bail!("unknown message type in capture"),
        }
    }
    writeln!(
        std::io::stdout(),
        "{}",
        serde_json::to_string_pretty(&c.summary())?
    )?;
    std::process::exit(if c.unexplained() == 0 { 0 } else { 1 });
}

/// Histogram of update receive time minus exchange time, 100 µs buckets over ±10 s
/// (includes any host clock offset, so compare runs, not absolute values).
struct Lag {
    buckets: Vec<u64>,
    n: u64,
}

const LAG_BUCKET_US: i64 = 100;
const LAG_SPAN_US: i64 = 10_000_000;

impl Default for Lag {
    fn default() -> Self {
        Self {
            buckets: vec![0; (2 * LAG_SPAN_US / LAG_BUCKET_US + 1) as usize],
            n: 0,
        }
    }
}

impl Lag {
    fn add(&mut self, us: i64) {
        let i = (us.clamp(-LAG_SPAN_US, LAG_SPAN_US) + LAG_SPAN_US) / LAG_BUCKET_US;
        self.buckets[i as usize] += 1;
        self.n += 1;
    }

    /// p-th percentile (0..=100), bucket lower edge.
    fn pct(&self, p: u64) -> Option<i64> {
        let want = (self.n * p).div_ceil(100).max(1);
        let mut seen = 0;
        self.buckets
            .iter()
            .position(|&c| {
                seen += c;
                seen >= want
            })
            .map(|i| i as i64 * LAG_BUCKET_US - LAG_SPAN_US)
    }

    fn summary(&self) -> serde_json::Value {
        if self.n == 0 {
            return serde_json::Value::Null;
        }
        serde_json::json!({
            "n": self.n, "p1": self.pct(1), "p10": self.pct(10), "p50": self.pct(50),
            "p90": self.pct(90), "p99": self.pct(99),
        })
    }
}

/// Records `ts`; true if it went back in time for this symbol.
fn check_ts(st: &mut Symbol, ts: i64) -> bool {
    let back = ts < st.last_ts;
    if back {
        st.timestamp_regressions += 1;
    }
    st.last_ts = st.last_ts.max(ts);
    back
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;
    use engineering_challenge::{
        Side,
        util::dtos::challenge_orderbook::{SnapshotAsksEntryDomain, SnapshotBidsEntryDomain},
    };

    const S: i64 = 1_000_000; // one second in µs
    const T0: i64 = 1_700_000_000 * S;

    fn snap(at: i64, bid_qty: i64) -> SnapshotDomain {
        let t = DateTime::from_timestamp_micros(T0 + at).unwrap();
        SnapshotDomain {
            received_at: t,
            exchange_timestamp: t,
            bids: vec![SnapshotBidsEntryDomain {
                price_level: 400_000,
                quantity: bid_qty,
            }],
            asks: vec![SnapshotAsksEntryDomain {
                price_level: 600_000,
                quantity: 3,
            }],
            symbol: "M".into(),
        }
    }

    fn upd(at: i64, qty: i64) -> PriceLevelUpdateDomain {
        let t = DateTime::from_timestamp_micros(T0 + at).unwrap();
        PriceLevelUpdateDomain {
            backtest_timestamp: t,
            exchange_timestamp: t,
            price_level: 400_000,
            quantity: qty,
            side: Side::Buy,
            symbol: "M".into(),
        }
    }

    fn cause(c: &mut Checker, at: i64, why: &str) {
        c.events.insert(("M".into(), T0 + at), why.into());
    }

    #[test]
    fn a_periodic_snapshot_must_match_the_rebuilt_book() {
        let mut c = Checker::default();
        c.snapshot(&snap(0, 5));
        c.update(&upd(S, 7));
        c.snapshot(&snap(60 * S, 7)); // matches
        assert_eq!((c.unexplained(), c.sum(|s| s.periodic_checked)), (0, 1));
        c.snapshot(&snap(120 * S, 9)); // the book says 7
        assert_eq!(c.sum(|s| s.unexplained_mismatches), 1);
        assert_eq!(c.unexplained(), 1);
    }

    #[test]
    fn a_logged_cause_explains_a_mismatch() {
        let mut c = Checker::default();
        cause(&mut c, 30 * S, "Failover");
        c.snapshot(&snap(0, 5));
        c.snapshot(&snap(30 * S, 9));
        assert_eq!(c.sum(|s| s.explained_mismatches), 1);
        assert_eq!(
            c.sum(|s| s.periodic_checked),
            0,
            "explained snapshots are not periodic"
        );
        assert_eq!(c.unexplained(), 0);
    }

    #[test]
    fn an_update_before_the_first_snapshot_fails() {
        let mut c = Checker::default();
        c.update(&upd(0, 1));
        c.snapshot(&snap(S, 5));
        assert_eq!(c.sum(|s| s.updates_before_snapshot), 1);
        assert_eq!(c.unexplained(), 1);
    }

    #[test]
    fn snapshots_more_than_61s_apart_fail() {
        let mut c = Checker::default();
        c.snapshot(&snap(0, 5));
        c.snapshot(&snap(61 * S, 5)); // exactly the limit: fine
        assert_eq!(c.unexplained(), 0);
        c.snapshot(&snap(61 * S + 61 * S + 1, 5)); // 1 µs over
        assert_eq!(c.sum(|s| s.interval_violations), 1);
        assert_eq!(c.examples[0].0, "interval");
    }

    #[test]
    fn a_long_interval_with_a_logged_gap_is_explained() {
        let mut c = Checker::default();
        c.gaps.insert("M".into(), vec![T0 + 30 * S]);
        c.snapshot(&snap(0, 5));
        c.snapshot(&snap(200 * S, 5));
        assert_eq!((c.sum(|s| s.gap_intervals), c.unexplained()), (1, 0));
    }

    #[test]
    fn a_removal_ends_the_symbol_until_it_returns() {
        let mut c = Checker::default();
        cause(&mut c, 10 * S, "Removed");
        c.snapshot(&snap(0, 5));
        c.snapshot(&snap(10 * S, 5)); // terminal snapshot
        c.snapshot(&snap(600 * S, 5)); // re-added 10 min later: no interval violation
        assert_eq!(c.unexplained(), 0);
        c.snapshot(&snap(700 * S, 5)); // live again: the 61 s rule applies
        assert_eq!(c.sum(|s| s.interval_violations), 1);
    }

    #[test]
    fn timestamp_regressions_are_reported_not_fatal() {
        let mut c = Checker::default();
        c.snapshot(&snap(10 * S, 5));
        c.update(&upd(9 * S, 6));
        assert_eq!(c.sum(|s| s.timestamp_regressions), 1);
        assert_eq!(c.unexplained(), 0);
        assert_eq!(c.examples[0].0, "regression_update");
    }

    #[test]
    fn reads_the_collectors_event_and_gap_lines() {
        let log = "2026-10-26T00:57:00.512195Z  INFO snapshot: snapshot market=KX-1 cause=Subscribed recv_us=1792976220458914
2026-10-26T00:57:01.000000Z  INFO gap: gap market=KX-1 since_us=1792976221000000
2026-10-26T00:57:02.000000Z  INFO engineering_challenge::conn: connected conn=0 markets=5
2026-10-26T00:57:03.000000Z  INFO snapshot: snapshot market=KX-2 cause=Removed recv_us=5";
        let ev = read_events(log).unwrap();
        assert_eq!(ev.len(), 2);
        assert_eq!(ev[&("KX-1".to_owned(), 1792976220458914)], "Subscribed");
        assert_eq!(ev[&("KX-2".to_owned(), 5)], "Removed");
        assert_eq!(read_gaps(log).unwrap()["KX-1"], vec![1792976221000000]);
        assert!(read_events("x INFO snapshot: snapshot market=A cause=B recv_us=nope").is_err());
    }

    #[test]
    fn lag_percentiles() {
        let mut l = Lag::default();
        for us in (1..=100).map(|i| i * 1_000) {
            l.add(us); // 1 ms .. 100 ms
        }
        l.add(-50_000_000); // clamped into the lowest bucket
        assert_eq!(l.pct(50), Some(50_000));
        assert_eq!(l.pct(99), Some(99_000));
        assert_eq!(l.pct(0), Some(-LAG_SPAN_US));
        assert_eq!(l.pct(1), Some(1_000));
        assert!(Lag::default().summary().is_null());
    }
}
