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
fn read_events(path: &PathBuf) -> anyhow::Result<HashMap<(String, i64), String>> {
    let mut set = HashMap::new();
    for line in std::fs::read_to_string(path)?.lines() {
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
fn read_gaps(path: &PathBuf) -> anyhow::Result<HashMap<String, Vec<i64>>> {
    let mut gaps: HashMap<String, Vec<i64>> = HashMap::new();
    for line in std::fs::read_to_string(path)?.lines() {
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

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let (events, gaps) = match &args.events {
        Some(p) => (read_events(p)?, read_gaps(p)?),
        None => (HashMap::new(), HashMap::new()),
    };
    if args.repair {
        let added = engineering_challenge::writer::pad_capture(
            &args.capture,
            engineering_challenge::writer::CHUNK,
        )?;
        eprintln!("repair: padded {added} bytes");
    }
    let mut r = AlignedSbeReader::<OrderbookSchema>::open(&args.capture)?;
    let mut symbols: HashMap<String, Symbol> = HashMap::new();
    let mut messages = 0u64;
    let mut lag = Lag::default();
    // First few problems, for diagnosis: (kind, symbol, previous_us, this_us).
    let mut examples: Vec<(&str, String, i64, i64)> = Vec::new();
    while let Some(msg) = r.read_msg()? {
        messages += 1;
        match msg {
            AnyMessage::Snapshot(s) => {
                let s = SnapshotDomain::try_from_decoder(s)?;
                let at = s.received_at.timestamp_micros();
                let st = symbols.entry(s.symbol.clone()).or_default();
                st.snapshots += 1;
                let mut snap = Book::new();
                snap.extend(s.bids.iter().map(|l| ((0, l.price_level), l.quantity)));
                snap.extend(s.asks.iter().map(|l| ((1, l.price_level), l.quantity)));
                let cause = events.get(&(s.symbol.clone(), at));
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
                    let in_gap = gaps.get(&s.symbol).is_some_and(|g| {
                        g.iter()
                            .any(|&t| prev - MAX_SNAPSHOT_INTERVAL_US <= t && t <= at)
                    });
                    if at - prev > MAX_SNAPSHOT_INTERVAL_US && in_gap {
                        st.gap_intervals += 1;
                    } else if at - prev > MAX_SNAPSHOT_INTERVAL_US {
                        st.interval_violations += 1;
                        if examples.len() < 10 {
                            examples.push(("interval", s.symbol.clone(), prev, at));
                        }
                    }
                }
                st.last_snapshot_us = Some(at);
                st.ended = cause.is_some_and(|c| c == "Removed");
                st.book = Some(snap);
                if check_ts(st, at) && examples.len() < 10 {
                    examples.push(("regression_snapshot", s.symbol.clone(), st.last_ts, at));
                }
            }
            AnyMessage::PriceLevelUpdate(u) => {
                let u = PriceLevelUpdateDomain::try_from_decoder(u)?;
                let st = symbols.entry(u.symbol.clone()).or_default();
                st.updates += 1;
                let at = u.backtest_timestamp.timestamp_micros();
                lag.add(at - u.exchange_timestamp.timestamp_micros());
                if check_ts(st, at) && examples.len() < 10 {
                    examples.push(("regression_update", u.symbol.clone(), st.last_ts, at));
                }
                let Some(book) = st.book.as_mut() else {
                    st.updates_before_snapshot += 1;
                    continue;
                };
                let key = (u.side as u8, u.price_level);
                if u.quantity == 0 {
                    book.remove(&key);
                } else {
                    book.insert(key, u.quantity);
                }
            }
            _ => anyhow::bail!("unknown message type in capture"),
        }
    }
    let sum = |f: fn(&Symbol) -> u64| symbols.values().map(f).sum::<u64>();
    let unexplained = sum(|s| s.unexplained_mismatches)
        + sum(|s| s.updates_before_snapshot)
        + sum(|s| s.interval_violations);
    let summary = serde_json::json!({
        "ok": unexplained == 0,
        "messages": messages,
        "symbols": symbols.len(),
        "snapshots": sum(|s| s.snapshots),
        "updates": sum(|s| s.updates),
        "min_snapshots_per_symbol": symbols.values().map(|s| s.snapshots).min(),
        "periodic_checked": sum(|s| s.periodic_checked),
        "explained_mismatches": sum(|s| s.explained_mismatches),
        "unexplained_mismatches": sum(|s| s.unexplained_mismatches),
        "updates_before_snapshot": sum(|s| s.updates_before_snapshot),
        "interval_violations": sum(|s| s.interval_violations),
        "gap_intervals": sum(|s| s.gap_intervals),
        "max_snapshot_interval_us": symbols.values().map(|s| s.max_interval_us).max(),
        "timestamp_regressions": sum(|s| s.timestamp_regressions),
        "events_loaded": events.len(),
        "receive_minus_exchange_us": lag.summary(),
        "examples": examples,
        "per_symbol": symbols.iter().collect::<BTreeMap<_, _>>(),
    });
    writeln!(
        std::io::stdout(),
        "{}",
        serde_json::to_string_pretty(&summary)?
    )?;
    std::process::exit(if unexplained == 0 { 0 } else { 1 });
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
