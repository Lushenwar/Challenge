//! Recorded frames → ConnState → Arbiter → starter AlignedSbeWriter, then the real
//! `verify` binary on the capture. A clean capture must verify; the same capture with one
//! delta dropped must not.

use std::{io::Write, path::Path, process::Command};

use engineering_challenge::{
    arbiter::Arbiter,
    conn::{Action, ConnState},
    model::{ConnEvent, OutMsg, SnapCause},
    util::AlignedSbeWriter,
    writer::CHUNK,
};
use serde::Deserialize;

const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/kalshi/prod-2026-10-24.jsonl"
);
const MINUTE_US: i64 = 60_000_000;

#[derive(Deserialize)]
struct Line {
    conn: usize,
    frame: Option<String>,
    ctrl: Option<String>,
    cmd: Option<String>,
    recv_us: Option<i64>,
}

/// Replays conn 0 of the fixture with minute boundaries injected from recorded time.
/// Returns the output messages and the event log lines the collector would write.
fn run_pipeline() -> (Vec<OutMsg>, String) {
    let lines: Vec<Line> = std::fs::read_to_string(FIXTURE)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let mut tickers: Vec<String> = lines
        .iter()
        .filter(|l| l.conn == 0)
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l.cmd.as_deref()?).ok())
        .filter(|c| c["params"]["channels"][0] == "orderbook_delta")
        .flat_map(|c| {
            c["params"]["market_tickers"]
                .as_array()
                .cloned()
                .unwrap_or_default()
        })
        .filter_map(|t| t.as_str().map(String::from))
        .collect();
    tickers.sort();
    tickers.dedup();

    let mut conn = ConnState::new(0, &tickers);
    let mut arb = Arbiter::default();
    let (mut out, mut events) = (Vec::new(), String::new());
    let mut next_minute = None;
    for l in lines.into_iter().filter(|l| l.conn == 0) {
        let Some(recv_us) = l.recv_us else { continue };
        let boundary = *next_minute.get_or_insert((recv_us / MINUTE_US + 1) * MINUTE_US);
        if recv_us >= boundary {
            arb.snapshot_all(boundary, SnapCause::Periodic, &mut out);
            next_minute = Some(boundary + MINUTE_US);
        }
        if l.ctrl.as_deref() == Some("connected") {
            arb.on_event(ConnEvent::Disconnected { conn: 0 }, &mut out);
            conn.subscribe_cmds();
        }
        let Some(frame) = l.frame else { continue };
        for a in conn.on_frame(&frame, recv_us) {
            let Action::Event(e) = a else { continue };
            if let ConnEvent::Snapshot {
                market,
                cause,
                recv_us,
                ..
            } = &e
            {
                // Same shape as the collector's `snapshot` log line.
                events += &format!(
                    "INFO snapshot: snapshot market={market} cause={cause:?} recv_us={recv_us}\n"
                );
            }
            arb.on_event(e, &mut out);
        }
    }
    assert_eq!(conn.stats.seq_gaps, 0);
    (out, events)
}

fn write_capture(msgs: &[OutMsg], path: &Path) {
    let mut w = AlignedSbeWriter::create(path, CHUNK).unwrap();
    for m in msgs {
        match m {
            OutMsg::Snapshot(s) => w.write_msg(s).unwrap(),
            OutMsg::Update(u) => w.write_msg(u).unwrap(),
        }
    }
    w.flush().unwrap();
}

/// Runs `verify` and returns (exit success, summary JSON).
fn verify(msgs: &[OutMsg], events: &str) -> (bool, serde_json::Value) {
    let dir = tempfile::tempdir().unwrap();
    let (cap, ev) = (dir.path().join("cap.sbe"), dir.path().join("events.log"));
    write_capture(msgs, &cap);
    std::fs::File::create(&ev)
        .unwrap()
        .write_all(events.as_bytes())
        .unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_verify"))
        .arg(&cap)
        .arg("--events")
        .arg(&ev)
        .output()
        .unwrap();
    (
        out.status.success(),
        serde_json::from_slice(&out.stdout).expect("verify prints JSON"),
    )
}

#[test]
fn clean_capture_verifies() {
    let (msgs, events) = run_pipeline();
    let (ok, summary) = verify(&msgs, &events);
    assert!(ok, "{summary:#}");
    assert_eq!(summary["messages"], msgs.len());
    assert!(
        summary["periodic_checked"].as_u64().unwrap() >= 4,
        "{summary:#}"
    );
    assert!(summary["updates"].as_u64().unwrap() > 100);
}

#[test]
fn dropped_delta_fails_verify() {
    let (mut msgs, events) = run_pipeline();
    // Drop an update whose level is not touched again before its symbol's next snapshot,
    // and that next snapshot is periodic (so nothing in the event log explains it).
    let next_is_periodic = |i: usize| {
        let OutMsg::Update(u) = &msgs[i] else {
            return false;
        };
        msgs[i + 1..].iter().find_map(|m| match m {
            OutMsg::Snapshot(s) if s.symbol == u.symbol => {
                Some(s.received_at.timestamp_micros() % MINUTE_US == 0)
            }
            OutMsg::Update(v)
                if v.symbol == u.symbol && v.side == u.side && v.price_level == u.price_level =>
            {
                Some(false)
            }
            _ => None,
        }) == Some(true)
    };
    let victim = (0..msgs.len())
        .find(|&i| next_is_periodic(i))
        .expect("a droppable update");
    msgs.remove(victim);
    let (ok, summary) = verify(&msgs, &events);
    assert!(
        !ok,
        "verify accepted a capture with a dropped delta: {summary:#}"
    );
    assert!(summary["unexplained_mismatches"].as_u64().unwrap() >= 1);
}
