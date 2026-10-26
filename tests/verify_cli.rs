//! `verify` as a command: a capture cut off mid-chunk (collector killed) is unreadable
//! by the starter reader until `--repair` pads it; then every message checks out.

use std::process::Command;

use chrono::DateTime;
use engineering_challenge::{
    Side,
    util::dtos::challenge_orderbook::{
        PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
    },
    writer::{CHUNK, ChunkWriter},
};

fn verify(args: &[&std::ffi::OsStr]) -> (i32, serde_json::Value) {
    let out = Command::new(env!("CARGO_BIN_EXE_verify"))
        .args(args)
        .output()
        .unwrap();
    (
        out.status.code().unwrap(),
        serde_json::from_slice(&out.stdout).unwrap(),
    )
}

#[test]
fn repair_makes_a_killed_capture_fully_readable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cut.sbe");
    let t = |us: i64| DateTime::from_timestamp_micros(1_700_000_000_000_000 + us).unwrap();
    let mut w = ChunkWriter::create(&path, CHUNK).unwrap();
    w.write_msg(&SnapshotDomain {
        received_at: t(0),
        exchange_timestamp: t(0),
        bids: vec![SnapshotBidsEntryDomain {
            price_level: 400_000,
            quantity: 5,
        }],
        asks: vec![SnapshotAsksEntryDomain {
            price_level: 600_000,
            quantity: 3,
        }],
        symbol: "M".into(),
    })
    .unwrap();
    for i in 1..=20 {
        w.write_msg(&PriceLevelUpdateDomain {
            backtest_timestamp: t(i),
            exchange_timestamp: t(i),
            price_level: 400_000,
            quantity: i,
            side: Side::Buy,
            symbol: "M".into(),
        })
        .unwrap();
    }
    w.write_pending().unwrap();
    drop(w); // killed: the chunk was never padded

    let (code, before) = verify(&[path.as_os_str()]);
    assert_eq!(
        (code, before["messages"].as_u64()),
        (0, Some(0)),
        "the partial chunk is invisible"
    );
    let (code, after) = verify(&[path.as_os_str(), "--repair".as_ref()]);
    assert_eq!(code, 0, "{after}");
    assert_eq!(after["messages"], 21);
    assert_eq!(after["updates_before_snapshot"], 0);
    assert_eq!(std::fs::metadata(&path).unwrap().len(), CHUNK as u64);
}
