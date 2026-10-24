//! Recorded frames → ConnState → Arbiter → starter AlignedSbeWriter → AlignedSbeReader.
//! The capture must decode completely, start every symbol with a snapshot, and every
//! periodic snapshot must equal the book rebuilt from the output's own updates.

use std::collections::{BTreeMap, HashMap};

use engineering_challenge::{
    Side,
    arbiter::Arbiter,
    conn::{Action, ConnState},
    model::{ConnEvent, OutMsg, SnapCause},
    util::{
        AlignedSbeReader, AlignedSbeWriter, OrderbookSchema, dtos::challenge_orderbook::AnyMessage,
    },
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

/// Replays conn 0 of the fixture. Returns the messages and which ones are periodic.
fn run_pipeline() -> (Vec<OutMsg>, Vec<bool>) {
    let lines: Vec<Line> = std::fs::read_to_string(FIXTURE)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let mut tickers: Vec<String> = lines
        .iter()
        .filter(|l| l.conn == 0)
        .filter_map(|l| l.cmd.as_deref())
        .filter_map(|c| serde_json::from_str::<serde_json::Value>(c).ok())
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
    assert!(!tickers.is_empty());

    let mut conn = ConnState::new(0, &tickers);
    let mut arb = Arbiter::default();
    let (mut out, mut periodic) = (Vec::new(), Vec::new());
    let mut next_minute = None;
    let mut push = |arb_out: &mut Vec<OutMsg>, is_periodic: bool, out: &mut Vec<OutMsg>| {
        for m in arb_out.drain(..) {
            out.push(m);
            periodic.push(is_periodic);
        }
    };
    let mut buf = Vec::new();
    for l in lines.into_iter().filter(|l| l.conn == 0) {
        let Some(recv_us) = l.recv_us else { continue };
        let boundary = *next_minute.get_or_insert((recv_us / MINUTE_US + 1) * MINUTE_US);
        if recv_us >= boundary {
            arb.snapshot_all(boundary, SnapCause::Periodic, &mut buf);
            push(&mut buf, true, &mut out);
            next_minute = Some(boundary + MINUTE_US);
        }
        if l.ctrl.as_deref() == Some("connected") {
            arb.on_event(ConnEvent::Disconnected { conn: 0 }, &mut buf);
            conn.subscribe_cmds();
        }
        if let Some(frame) = l.frame {
            for a in conn.on_frame(&frame, recv_us) {
                if let Action::Event(e) = a {
                    arb.on_event(e, &mut buf);
                }
            }
        }
        push(&mut buf, false, &mut out);
    }
    assert_eq!(conn.stats.seq_gaps, 0);
    (out, periodic)
}

type Book = BTreeMap<(u8, i64), i64>;

#[test]
fn fixture_through_collector_decodes_and_periodic_snapshots_match() {
    let (msgs, periodic) = run_pipeline();
    let file = tempfile::NamedTempFile::new().unwrap();
    let mut w =
        AlignedSbeWriter::create(file.path(), engineering_challenge::writer::CHUNK).unwrap();
    for m in &msgs {
        match m {
            OutMsg::Snapshot(s) => w.write_msg(s).unwrap(),
            OutMsg::Update(u) => w.write_msg(u).unwrap(),
        }
    }
    w.flush().unwrap();
    drop(w);

    let mut r = AlignedSbeReader::<OrderbookSchema>::open(file.path()).unwrap();
    let mut books: HashMap<String, Book> = HashMap::new();
    let (mut i, mut checked, mut updates) = (0, 0, 0);
    while let Some(msg) = r.read_msg().unwrap() {
        match msg {
            AnyMessage::Snapshot(s) => {
                let s = engineering_challenge::util::dtos::challenge_orderbook::SnapshotDomain::try_from_decoder(s).unwrap();
                let mut snap = Book::new();
                snap.extend(
                    s.bids
                        .iter()
                        .map(|l| ((Side::Buy as u8, l.price_level), l.quantity)),
                );
                snap.extend(
                    s.asks
                        .iter()
                        .map(|l| ((Side::Sell as u8, l.price_level), l.quantity)),
                );
                if periodic[i] {
                    assert_eq!(
                        books.get(&s.symbol),
                        Some(&snap),
                        "periodic snapshot != output-rebuilt book for {}",
                        s.symbol
                    );
                    checked += 1;
                }
                books.insert(s.symbol, snap);
            }
            AnyMessage::PriceLevelUpdate(u) => {
                let u = engineering_challenge::util::dtos::challenge_orderbook::PriceLevelUpdateDomain::try_from_decoder(u).unwrap();
                let book = books
                    .get_mut(&u.symbol)
                    .expect("update before the symbol's first snapshot");
                if u.quantity == 0 {
                    book.remove(&(u.side as u8, u.price_level));
                } else {
                    book.insert((u.side as u8, u.price_level), u.quantity);
                }
                updates += 1;
            }
            _ => panic!("unexpected message type"),
        }
        i += 1;
    }
    assert_eq!(i, msgs.len(), "every written message decodes");
    assert!(
        checked >= 4 && updates > 100,
        "checked {checked} periodic snapshots, {updates} updates"
    );
}
