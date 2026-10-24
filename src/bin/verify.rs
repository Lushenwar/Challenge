//! Output checker: decode an SBE capture with the starter reader and check, per symbol,
//! that deltas and snapshots agree. Prints a JSON summary; exit 0 only if clean.

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

#[derive(Parser)]
struct Args {
    capture: PathBuf,
}

type Book = BTreeMap<(u8, i64), i64>;

#[derive(Default, Serialize)]
struct Symbol {
    snapshots: u64,
    updates: u64,
    /// Snapshot that differs from the book rebuilt from earlier output.
    mismatches: u64,
    updates_before_snapshot: u64,
    timestamp_regressions: u64,
    #[serde(skip)]
    book: Option<Book>,
    #[serde(skip)]
    last_ts: i64,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let mut r = AlignedSbeReader::<OrderbookSchema>::open(&args.capture)?;
    let mut symbols: HashMap<String, Symbol> = HashMap::new();
    let mut messages = 0u64;
    while let Some(msg) = r.read_msg()? {
        messages += 1;
        match msg {
            AnyMessage::Snapshot(s) => {
                let s = SnapshotDomain::try_from_decoder(s)?;
                let st = symbols.entry(s.symbol.clone()).or_default();
                st.snapshots += 1;
                let mut snap = Book::new();
                snap.extend(s.bids.iter().map(|l| ((0, l.price_level), l.quantity)));
                snap.extend(s.asks.iter().map(|l| ((1, l.price_level), l.quantity)));
                if st.book.as_ref().is_some_and(|b| *b != snap) {
                    st.mismatches += 1;
                }
                st.book = Some(snap);
                check_ts(st, s.received_at.timestamp_micros());
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
    let clean = sum(|s| s.updates_before_snapshot) == 0 && sum(|s| s.timestamp_regressions) == 0;
    let summary = serde_json::json!({
        "messages": messages,
        "symbols": symbols.len(),
        "snapshots": sum(|s| s.snapshots),
        "updates": sum(|s| s.updates),
        "min_snapshots_per_symbol": symbols.values().map(|s| s.snapshots).min(),
        "mismatches": sum(|s| s.mismatches),
        "updates_before_snapshot": sum(|s| s.updates_before_snapshot),
        "timestamp_regressions": sum(|s| s.timestamp_regressions),
        "per_symbol": symbols.iter().collect::<BTreeMap<_, _>>(),
    });
    writeln!(
        std::io::stdout(),
        "{}",
        serde_json::to_string_pretty(&summary)?
    )?;
    std::process::exit(if clean { 0 } else { 1 });
}

fn check_ts(st: &mut Symbol, ts: i64) {
    if ts < st.last_ts {
        st.timestamp_regressions += 1;
    }
    st.last_ts = st.last_ts.max(ts);
}
