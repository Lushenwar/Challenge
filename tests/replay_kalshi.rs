//! Replays recorded Kalshi frames through codec → book. At every exchange snapshot for a
//! market whose book was rebuilt from earlier frames, the rebuilt book must equal it.
//! Only a seq gap (or reconnect) on that market's sid may excuse a difference, and such
//! books are never compared: they are invalid until the next snapshot.
//!
//! One exchange behavior is not a mismatch: when a market closes, Kalshi sends an
//! unsolicited (no `id`) snapshot with both sides empty. That resets the book and is
//! counted as `clears`.
//!
//! A market's deltas only start flowing at its own subscription snapshot (the first
//! unsolicited one). A `get_snapshot` reply that arrives before that is not a base for
//! deltas, so it is ignored here (`early_requested`), exactly as the collector must.

use std::collections::{HashMap, HashSet};

use engineering_challenge::{
    book::OrderBook,
    exchange::kalshi::codec::{Decoded, decode},
};
use serde::Deserialize;

#[derive(Deserialize)]
struct Line {
    conn: usize,
    frame: Option<String>,
    ctrl: Option<String>,
}

#[derive(Default, Debug)]
struct Stats {
    frames: usize,
    snapshots: usize,
    deltas: usize,
    compared: usize,
    clears: usize,
    early_requested: usize,
    mismatches: Vec<String>,
    gaps: usize,
    dropped_invalid: usize,
    crossed: usize,
    lifecycle: usize,
    controls: usize,
}

#[derive(Default)]
struct Conn {
    next_seq: HashMap<u64, u64>,
    /// ticker → (sid, book); a missing entry means Pending/Invalid.
    books: HashMap<String, (u64, OrderBook)>,
    /// Markets whose subscription snapshot has arrived since connect.
    streaming: HashSet<String>,
}

fn replay(text: &str) -> Stats {
    let mut s = Stats::default();
    let mut conns: HashMap<usize, Conn> = HashMap::new();
    for line in text.lines() {
        let l: Line = serde_json::from_str(line).expect("recorder line");
        let c = conns.entry(l.conn).or_default();
        if l.ctrl.as_deref() == Some("connected") {
            *c = Conn::default();
        }
        let Some(frame) = l.frame else { continue };
        s.frames += 1;
        let d = decode(&frame).unwrap_or_else(|e| panic!("{e:#}: {frame}"));
        if let Some((sid, seq)) = d.sid_seq() {
            let expected = c.next_seq.insert(sid, seq + 1);
            if expected.is_some_and(|e| e != seq) {
                s.gaps += 1;
                c.books.retain(|_, (book_sid, _)| *book_sid != sid);
            }
        }
        match d {
            Decoded::Snapshot {
                sid,
                seq,
                id,
                ticker,
                bids,
                asks,
                ..
            } => {
                s.snapshots += 1;
                if id.is_none() {
                    c.streaming.insert(ticker.clone());
                } else if !c.streaming.contains(&ticker) {
                    s.early_requested += 1;
                    continue;
                }
                let snap = OrderBook::from_levels(&bids, &asks).expect("valid snapshot");
                if let Some((_, rebuilt)) = c.books.get(&ticker) {
                    s.compared += 1;
                    if *rebuilt != snap {
                        if id.is_none() && snap == OrderBook::default() {
                            s.clears += 1;
                        } else {
                            s.mismatches.push(format!("{ticker} seq={seq} id={id:?}"));
                        }
                    }
                }
                c.books.insert(ticker, (sid, snap));
            }
            Decoded::Delta {
                ticker,
                side,
                price,
                delta,
                ..
            } => {
                s.deltas += 1;
                match c.books.get_mut(&ticker) {
                    Some((_, book)) => {
                        book.apply_delta(side, price, delta)
                            .expect("delta on valid book");
                        s.crossed += usize::from(book.is_crossed());
                    }
                    None => s.dropped_invalid += 1,
                }
            }
            Decoded::Lifecycle { .. } => s.lifecycle += 1,
            _ => s.controls += 1,
        }
    }
    s
}

fn check(s: &Stats) {
    assert!(s.snapshots > 0 && s.deltas > 0, "{s:?}");
    assert!(
        s.compared > 0,
        "no snapshot was compared against a rebuilt book: {s:?}"
    );
    assert!(
        s.mismatches.is_empty(),
        "rebuilt book != exchange snapshot: {s:?}"
    );
    // Real Kalshi books are never crossed after a delta; a crossed book here means the
    // NO→ask inversion is wrong somewhere.
    assert_eq!(s.crossed, 0, "{s:?}");
}

#[test]
fn replays_committed_fixtures() {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/kalshi");
    let mut n = 0;
    for entry in std::fs::read_dir(dir).expect("fixtures/kalshi exists") {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|e| e == "jsonl") {
            let s = replay(&std::fs::read_to_string(&path).unwrap());
            eprintln!("{}: {s:?}", path.display());
            check(&s);
            n += 1;
        }
    }
    assert!(n > 0, "no fixtures found in {dir}");
}

/// Large local recordings: `REPLAY_FILE=~/rec/x.jsonl cargo test -- --ignored`.
#[test]
#[ignore]
fn replays_local_recording() {
    let path = std::env::var("REPLAY_FILE").expect("set REPLAY_FILE");
    let s = replay(&std::fs::read_to_string(&path).unwrap());
    eprintln!("{path}: {s:?}");
    check(&s);
}
