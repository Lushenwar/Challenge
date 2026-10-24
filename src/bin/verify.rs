//! Output checker: decode an SBE capture with the starter reader and prove, per symbol,
//! that deltas and snapshots agree.
//!
//! - Every symbol must start with a snapshot.
//! - Updates are applied as absolute quantities.
//! - A later snapshot must equal the rebuilt book, unless the collector logged a
//!   non-periodic cause for it (`--events`: subscribe, resync, exchange clear). Periodic
//!   and shutdown snapshots are not logged per market, so they must always match.
//! - Consecutive snapshots of a symbol are at most 61s apart.
//! - `backtest_timestamp` regressions per symbol are reported (not fatal).
//!
//! Prints a JSON summary; exit 0 only if there is nothing unexplained.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
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
    max_interval_us: i64,
    timestamp_regressions: u64,
    #[serde(skip)]
    book: Option<Book>,
    #[serde(skip)]
    last_snapshot_us: Option<i64>,
    #[serde(skip)]
    last_ts: i64,
}

/// `(market, recv_us)` of every snapshot the collector logged with a cause.
fn read_events(path: &PathBuf) -> anyhow::Result<HashSet<(String, i64)>> {
    let mut set = HashSet::new();
    for line in std::fs::read_to_string(path)?.lines() {
        if !line.contains(" snapshot: snapshot ") {
            continue;
        }
        let field = |k: &str| {
            line.split_whitespace()
                .find_map(|t| t.strip_prefix(k))
                .map(str::to_owned)
        };
        if let (Some(m), Some(us)) = (field("market="), field("recv_us=")) {
            set.insert((m, us.parse()?));
        }
    }
    Ok(set)
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let events = match &args.events {
        Some(p) => read_events(p)?,
        None => HashSet::new(),
    };
    let mut r = AlignedSbeReader::<OrderbookSchema>::open(&args.capture)?;
    let mut symbols: HashMap<String, Symbol> = HashMap::new();
    let mut messages = 0u64;
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
                let explained = events.contains(&(s.symbol.clone(), at));
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
                if let Some(prev) = st.last_snapshot_us {
                    st.max_interval_us = st.max_interval_us.max(at - prev);
                    if at - prev > MAX_SNAPSHOT_INTERVAL_US {
                        st.interval_violations += 1;
                    }
                }
                st.last_snapshot_us = Some(at);
                st.book = Some(snap);
                check_ts(st, at);
            }
            AnyMessage::PriceLevelUpdate(u) => {
                let u = PriceLevelUpdateDomain::try_from_decoder(u)?;
                let st = symbols.entry(u.symbol.clone()).or_default();
                st.updates += 1;
                check_ts(st, u.backtest_timestamp.timestamp_micros());
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
        "max_snapshot_interval_us": symbols.values().map(|s| s.max_interval_us).max(),
        "timestamp_regressions": sum(|s| s.timestamp_regressions),
        "events_loaded": events.len(),
        "per_symbol": symbols.iter().collect::<BTreeMap<_, _>>(),
    });
    writeln!(
        std::io::stdout(),
        "{}",
        serde_json::to_string_pretty(&summary)?
    )?;
    std::process::exit(if unexplained == 0 { 0 } else { 1 });
}

fn check_ts(st: &mut Symbol, ts: i64) {
    if ts < st.last_ts {
        st.timestamp_regressions += 1;
    }
    st.last_ts = st.last_ts.max(ts);
}
