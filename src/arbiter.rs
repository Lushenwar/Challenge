//! Decides what reaches the output. Phase 3: one source per market, so it forwards that
//! source and adds minute and shutdown snapshots. It keeps an `emitted` copy of every
//! book: exactly what the output stream currently says.

use std::{collections::HashMap, time::Duration};

use chrono::{DateTime, Utc};
use crossbeam::channel::{Sender, TrySendError};
use serde::Serialize;
use tokio::sync::mpsc;

use crate::{
    book::OrderBook,
    model::{ConnEvent, OutMsg, SnapCause, now_us},
    util::dtos::challenge_orderbook::{
        PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
    },
};

#[derive(Debug, Default, Clone, Serialize)]
pub struct ArbiterStats {
    pub markets_seen: u64,
    pub snapshots: u64,
    pub updates: u64,
    pub periodic_rounds: u64,
    pub dropped_no_book: u64,
    pub gaps: u64,
    pub gap_total_us: i64,
    pub gap_max_us: i64,
    pub writer_blocked_us: i64,
    pub lifecycle_events: u64,
}

#[derive(Default)]
struct Emitted {
    /// `None` = no valid source; the market is silent.
    book: Option<OrderBook>,
    exch_us: i64,
    gap_since: Option<i64>,
}

#[derive(Default)]
pub struct Arbiter {
    markets: HashMap<String, Emitted>,
    pub stats: ArbiterStats,
}

impl Arbiter {
    pub fn on_event(&mut self, ev: ConnEvent, out: &mut Vec<OutMsg>) {
        match ev {
            ConnEvent::Snapshot {
                market,
                book,
                recv_us,
                exch_us,
                cause,
                ..
            } => {
                let e = self.markets.entry(market.clone()).or_insert_with(|| {
                    self.stats.markets_seen += 1;
                    Emitted::default()
                });
                if let Some(since) = e.gap_since.take() {
                    let gap = recv_us - since;
                    self.stats.gaps += 1;
                    self.stats.gap_total_us += gap;
                    self.stats.gap_max_us = self.stats.gap_max_us.max(gap);
                    tracing::info!(%market, gap_ms = gap / 1000, "gap closed");
                }
                // One line per non-periodic snapshot: `verify --events` pairs these up.
                tracing::info!(target: "snapshot", %market, cause = ?cause, recv_us, "snapshot");
                out.push(snapshot_msg(&market, &book, recv_us, exch_us));
                self.stats.snapshots += 1;
                e.exch_us = exch_us;
                e.book = Some(book);
            }
            ConnEvent::Update {
                market,
                side,
                price,
                qty,
                recv_us,
                exch_us,
                ..
            } => {
                let Some(e) = self.markets.get_mut(&market) else {
                    return;
                };
                let Some(book) = e.book.as_mut() else {
                    self.stats.dropped_no_book += 1;
                    return;
                };
                book.set_level(side, price, qty);
                e.exch_us = exch_us;
                self.stats.updates += 1;
                out.push(OutMsg::Update(PriceLevelUpdateDomain {
                    backtest_timestamp: ts(recv_us),
                    exchange_timestamp: ts(exch_us),
                    price_level: price,
                    quantity: qty,
                    side,
                    symbol: market,
                }));
            }
            ConnEvent::Invalidated { market, .. } => {
                if let Some(e) = self.markets.get_mut(&market) {
                    start_gap(e);
                }
            }
            // ponytail: single source per market in Phase 3, so a dropped connection
            // silences every market; Phase 6 fails over to a replica instead.
            ConnEvent::Disconnected { .. } => self.markets.values_mut().for_each(start_gap),
            ConnEvent::Lifecycle { market, event, .. } => {
                self.stats.lifecycle_events += 1;
                tracing::debug!(%market, ?event, "lifecycle");
            }
        }
    }

    /// A snapshot of every market that currently has a valid book.
    pub fn snapshot_all(&mut self, now_us: i64, cause: SnapCause, out: &mut Vec<OutMsg>) {
        let mut markets: Vec<_> = self
            .markets
            .iter()
            .filter(|(_, e)| e.book.is_some())
            .collect();
        markets.sort_by(|a, b| a.0.cmp(b.0));
        for (market, e) in markets {
            out.push(snapshot_msg(
                market,
                e.book.as_ref().expect("filtered"),
                now_us,
                e.exch_us,
            ));
            self.stats.snapshots += 1;
        }
        tracing::info!(cause = ?cause, now_us, "snapshot round");
    }
}

fn start_gap(e: &mut Emitted) {
    if e.book.take().is_some() {
        e.gap_since = Some(now_us());
    }
}

fn ts(us: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_micros(us).expect("timestamp in range")
}

fn snapshot_msg(market: &str, book: &OrderBook, received_us: i64, exch_us: i64) -> OutMsg {
    let (bids, asks) = book.levels();
    OutMsg::Snapshot(SnapshotDomain {
        received_at: ts(received_us),
        exchange_timestamp: ts(exch_us),
        bids: bids
            .into_iter()
            .map(|(price_level, quantity)| SnapshotBidsEntryDomain {
                price_level,
                quantity,
            })
            .collect(),
        asks: asks
            .into_iter()
            .map(|(price_level, quantity)| SnapshotAsksEntryDomain {
                price_level,
                quantity,
            })
            .collect(),
        symbol: market.to_owned(),
    })
}

/// Time until the next wall-clock minute boundary (`:00` seconds).
fn until_next_minute() -> Duration {
    let us = now_us();
    Duration::from_micros((60_000_000 - us.rem_euclid(60_000_000)) as u64)
}

/// The arbiter task. Ends when every connection has dropped its sender, then writes a
/// final snapshot of every valid book and closes the writer channel.
pub async fn run(mut events: mpsc::Receiver<ConnEvent>, writer: Sender<OutMsg>) -> ArbiterStats {
    let mut a = Arbiter::default();
    let mut buf = Vec::new();
    let minute = tokio::time::sleep(until_next_minute());
    tokio::pin!(minute);
    loop {
        tokio::select! {
            ev = events.recv() => match ev {
                Some(ev) => a.on_event(ev, &mut buf),
                None => break,
            },
            _ = &mut minute => {
                a.stats.periodic_rounds += 1;
                a.snapshot_all(now_us(), SnapCause::Periodic, &mut buf);
                minute.as_mut().reset(tokio::time::Instant::now() + until_next_minute());
            }
        }
        send_all(&writer, &mut buf, &mut a.stats);
    }
    a.snapshot_all(now_us(), SnapCause::Shutdown, &mut buf);
    send_all(&writer, &mut buf, &mut a.stats);
    a.stats
}

/// Hand messages to the writer thread. A full channel blocks (back-pressure); output is
/// never dropped.
fn send_all(writer: &Sender<OutMsg>, buf: &mut Vec<OutMsg>, stats: &mut ArbiterStats) {
    for msg in buf.drain(..) {
        match writer.try_send(msg) {
            Ok(()) => {}
            Err(TrySendError::Full(msg)) => {
                let t = std::time::Instant::now();
                let sent = tokio::task::block_in_place(|| writer.send(msg));
                let waited = t.elapsed();
                stats.writer_blocked_us += waited.as_micros() as i64;
                if waited > Duration::from_secs(1) {
                    tracing::error!(?waited, "writer channel full for over 1s");
                }
                if sent.is_err() {
                    tracing::error!("writer gone; output lost");
                }
            }
            Err(TrySendError::Disconnected(_)) => tracing::error!("writer gone; output lost"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Side;

    fn book(bid: i64) -> OrderBook {
        OrderBook::from_levels(&[(bid, 5)], &[(900_000, 1)]).unwrap()
    }

    #[test]
    fn forwards_snapshot_updates_and_minute_snapshot_reflects_them() {
        let mut a = Arbiter::default();
        let mut out = Vec::new();
        let market = "T".to_string();
        a.on_event(
            ConnEvent::Snapshot {
                conn: 0,
                market: market.clone(),
                book: book(400_000),
                recv_us: 1,
                exch_us: 2,
                cause: SnapCause::Subscribed,
            },
            &mut out,
        );
        a.on_event(
            ConnEvent::Update {
                conn: 0,
                market: market.clone(),
                side: Side::Buy,
                price: 400_000,
                qty: 0,
                recv_us: 3,
                exch_us: 4,
            },
            &mut out,
        );
        a.snapshot_all(10, SnapCause::Periodic, &mut out);
        assert_eq!(out.len(), 3);
        let OutMsg::Snapshot(s) = &out[2] else {
            panic!()
        };
        assert!(s.bids.is_empty());
        assert_eq!(s.asks.len(), 1);
        assert_eq!(s.received_at, ts(10));
        assert_eq!(s.exchange_timestamp, ts(4)); // latest event reflected
    }

    #[test]
    fn invalid_market_is_silent_until_next_snapshot() {
        let mut a = Arbiter::default();
        let mut out = Vec::new();
        let m = "T".to_string();
        a.on_event(
            ConnEvent::Snapshot {
                conn: 0,
                market: m.clone(),
                book: book(1),
                recv_us: 1,
                exch_us: 1,
                cause: SnapCause::Subscribed,
            },
            &mut out,
        );
        a.on_event(
            ConnEvent::Invalidated {
                conn: 0,
                market: m.clone(),
            },
            &mut out,
        );
        a.on_event(
            ConnEvent::Update {
                conn: 0,
                market: m.clone(),
                side: Side::Buy,
                price: 1,
                qty: 9,
                recv_us: 2,
                exch_us: 2,
            },
            &mut out,
        );
        a.snapshot_all(3, SnapCause::Periodic, &mut out);
        assert_eq!(out.len(), 1); // only the first snapshot
        assert_eq!(a.stats.dropped_no_book, 1);
        a.on_event(
            ConnEvent::Snapshot {
                conn: 0,
                market: m,
                book: book(2),
                recv_us: now_us(),
                exch_us: 4,
                cause: SnapCause::Resync,
            },
            &mut out,
        );
        assert_eq!(out.len(), 2);
        assert_eq!(a.stats.gaps, 1);
    }
}
