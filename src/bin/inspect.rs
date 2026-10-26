//! Summarize an SBE capture as JSON: message counts, symbols, time range, snapshots per
//! symbol, chunks and padding. `--repair` first zero-pads a last chunk cut off by a killed
//! collector (see `writer::pad_capture`). For the correctness check, use `verify`.

use std::{
    collections::HashMap,
    io::Write,
    path::{Path, PathBuf},
};

use clap::Parser;
use engineering_challenge::{
    util::{
        AlignedSbeReader, OrderbookSchema,
        dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain, SnapshotDomain},
    },
    writer::{CHUNK, pad_capture},
};
use serde_json::json;

#[derive(Parser)]
#[command(about = "Summarize an SBE capture")]
struct Args {
    capture: PathBuf,
    /// Zero-pad a cut-off last chunk first, so the whole capture is read.
    #[arg(long)]
    repair: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if args.repair {
        let added = pad_capture(&args.capture, CHUNK)?;
        let _ = writeln!(std::io::stderr(), "repair: padded {added} bytes");
    }
    let summary = inspect(&args.capture)?;
    writeln!(
        std::io::stdout(),
        "{}",
        serde_json::to_string_pretty(&summary)?
    )?;
    Ok(())
}

#[derive(Default)]
struct Sym {
    snapshots: u64,
    updates: u64,
}

fn inspect(path: &Path) -> anyhow::Result<serde_json::Value> {
    let file_len = std::fs::metadata(path)?.len();
    let mut r = AlignedSbeReader::<OrderbookSchema>::open(path)?;
    let mut syms: HashMap<String, Sym> = HashMap::new();
    let (mut snapshots, mut updates, mut payload) = (0u64, 0u64, 0u64);
    let (mut first, mut last) = (i64::MAX, i64::MIN);
    while let Some(msg) = r.read_msg()? {
        let (symbol, at, bytes, is_snap) = match msg {
            AnyMessage::Snapshot(s) => {
                let s = SnapshotDomain::try_from_decoder(s)?;
                let n = s.encoded_length_with_header()?;
                (s.symbol, s.received_at.timestamp_micros(), n, true)
            }
            AnyMessage::PriceLevelUpdate(u) => {
                let u = PriceLevelUpdateDomain::try_from_decoder(u)?;
                let n = u.encoded_length_with_header()?;
                (u.symbol, u.backtest_timestamp.timestamp_micros(), n, false)
            }
            _ => anyhow::bail!("unknown message type in capture"),
        };
        payload += bytes as u64;
        first = first.min(at);
        last = last.max(at);
        let s = syms.entry(symbol).or_default();
        if is_snap {
            snapshots += 1;
            s.snapshots += 1;
        } else {
            updates += 1;
            s.updates += 1;
        }
    }
    let full_chunks = file_len / CHUNK as u64;
    let per_symbol = |f: fn(&Sym) -> u64| {
        let v: Vec<u64> = syms.values().map(f).collect();
        json!({"min": v.iter().min(), "max": v.iter().max()})
    };
    let mut busiest: Vec<(&String, u64)> = syms.iter().map(|(k, s)| (k, s.updates)).collect();
    busiest.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
    busiest.truncate(5);
    Ok(json!({
        "file_bytes": file_len,
        "chunks": full_chunks,
        // Bytes past the last whole chunk are invisible to the starter reader.
        "unread_tail_bytes": file_len % CHUNK as u64,
        "messages": snapshots + updates,
        "snapshots": snapshots,
        "updates": updates,
        "symbols": syms.len(),
        "first_us": (first != i64::MAX).then_some(first),
        "last_us": (last != i64::MIN).then_some(last),
        "duration_s": (last >= first).then(|| (last - first) / 1_000_000),
        "payload_bytes": payload,
        // Zero padding inside the readable chunks (each flush pads to a whole chunk).
        "padding_bytes": (full_chunks * CHUNK as u64).saturating_sub(payload),
        "snapshots_per_symbol": per_symbol(|s| s.snapshots),
        "updates_per_symbol": per_symbol(|s| s.updates),
        "busiest_symbols": busiest,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;
    use engineering_challenge::{
        Side,
        util::dtos::challenge_orderbook::{SnapshotAsksEntryDomain, SnapshotBidsEntryDomain},
        writer::ChunkWriter,
    };

    #[test]
    fn summarizes_a_capture_and_its_cut_off_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.sbe");
        let t = |s: i64| {
            DateTime::from_timestamp_micros(1_700_000_000_000_000 + s * 1_000_000).unwrap()
        };
        let mut w = ChunkWriter::create(&path, CHUNK).unwrap();
        for sym in ["A", "B"] {
            w.write_msg(&SnapshotDomain {
                received_at: t(0),
                exchange_timestamp: t(0),
                bids: vec![SnapshotBidsEntryDomain {
                    price_level: 1,
                    quantity: 1,
                }],
                asks: vec![SnapshotAsksEntryDomain {
                    price_level: 2,
                    quantity: 1,
                }],
                symbol: sym.into(),
            })
            .unwrap();
        }
        for i in 1..=3 {
            w.write_msg(&PriceLevelUpdateDomain {
                backtest_timestamp: t(i * 10),
                exchange_timestamp: t(i * 10),
                price_level: 1,
                quantity: i,
                side: Side::Buy,
                symbol: "A".into(),
            })
            .unwrap();
        }
        w.write_pending().unwrap();
        drop(w); // cut off
        let s = inspect(&path).unwrap();
        assert_eq!(
            (s["chunks"].as_u64(), s["messages"].as_u64()),
            (Some(0), Some(0))
        );
        assert!(s["unread_tail_bytes"].as_u64().unwrap() > 0);
        pad_capture(&path, CHUNK).unwrap();
        let s = inspect(&path).unwrap();
        assert_eq!(s["chunks"], 1);
        assert_eq!(s["unread_tail_bytes"], 0);
        assert_eq!(
            (s["snapshots"].as_u64(), s["updates"].as_u64()),
            (Some(2), Some(3))
        );
        assert_eq!(s["symbols"], 2);
        assert_eq!(s["duration_s"], 30);
        assert_eq!(s["updates_per_symbol"]["max"], 3);
        assert_eq!(s["busiest_symbols"][0][0], "A");
        let (payload, padding) = (
            s["payload_bytes"].as_u64().unwrap(),
            s["padding_bytes"].as_u64().unwrap(),
        );
        assert_eq!(payload + padding, CHUNK as u64);
    }
}
