//! Decides what reaches the output. Each market can arrive on several connections
//! (replicas). The arbiter mirrors every source's book from its events, forwards only
//! the active source, and on failover writes the new source's book as a snapshot before
//! any of its deltas. What has been written always equals the active source's book.

use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};

use chrono::{DateTime, Utc};
use crossbeam::channel::{Sender, TrySendError};
use serde::Serialize;
use tokio::sync::mpsc;

use crate::{
    book::OrderBook,
    exchange::kalshi::codec::Lifecycle,
    model::{ConnEvent, ConnId, OutMsg, SnapCause, now_us},
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
    pub failovers: u64,
    pub gaps: u64,
    pub gap_total_us: i64,
    pub gap_max_us: i64,
    pub writer_blocked_us: i64,
    pub lifecycle_events: u64,
    pub removed: u64,
}

#[derive(Default)]
struct Source {
    /// `None` = Pending or Invalid on that connection.
    book: Option<OrderBook>,
    /// Exchange time of the latest event this source applied: the freshness signal
    /// (Kalshi frames carry no per-market clock).
    exch_us: i64,
    /// Local receive time of that event.
    recv_us: i64,
}

#[derive(Default)]
struct Market {
    sources: BTreeMap<ConnId, Source>,
    active: Option<ConnId>,
    gap_since: Option<i64>,
    /// Terminal snapshot written; waiting for the other replicas to drop it too.
    removed: bool,
    /// Latest timestamp written for this symbol; output time never goes back.
    last_out_us: i64,
}

impl Market {
    /// Freshest valid source, lowest ConnId on ties.
    fn best(&self) -> Option<ConnId> {
        self.sources
            .iter()
            .filter(|(_, s)| s.book.is_some())
            .max_by_key(|(c, s)| (s.exch_us, std::cmp::Reverse(**c)))
            .map(|(c, _)| *c)
    }
}

#[derive(Default)]
pub struct Arbiter {
    markets: HashMap<String, Market>,
    /// Lifecycle events for the control task, drained by [`run`].
    pub lifecycle: Vec<(String, Lifecycle)>,
    /// `(market, cause, received_at)` of every non-periodic snapshot written; [`run`]
    /// logs them to stderr, where `verify --events` pairs them with the capture.
    pub snapshot_log: Vec<(String, SnapCause, i64)>,
    pub stats: ArbiterStats,
}

impl Arbiter {
    pub fn on_event(&mut self, ev: ConnEvent, out: &mut Vec<OutMsg>) {
        match ev {
            ConnEvent::Snapshot {
                conn,
                market,
                book,
                recv_us,
                exch_us,
                cause,
            } => {
                let m = self.markets.entry(market.clone()).or_insert_with(|| {
                    self.stats.markets_seen += 1;
                    Market::default()
                });
                if m.removed {
                    return;
                }
                let src = m.sources.entry(conn).or_default();
                src.book = Some(book);
                src.exch_us = exch_us;
                src.recv_us = recv_us;
                if m.active.is_none() || m.active == Some(conn) {
                    m.active = Some(conn);
                    close_gap(m, &market, recv_us, &mut self.stats);
                    let log = &mut self.snapshot_log;
                    emit(&market, m, conn, recv_us, cause, out, log, &mut self.stats);
                    m.last_out_us = m.last_out_us.max(recv_us);
                }
            }
            ConnEvent::Update {
                conn,
                market,
                side,
                price,
                qty,
                recv_us,
                exch_us,
            } => {
                let Some(m) = self.markets.get_mut(&market) else {
                    return;
                };
                let Some(src) = m.sources.get_mut(&conn) else {
                    return;
                };
                let Some(book) = src.book.as_mut() else {
                    return;
                };
                book.set_level(side, price, qty);
                src.exch_us = exch_us;
                src.recv_us = recv_us;
                if m.active == Some(conn) && !m.removed {
                    self.stats.updates += 1;
                    m.last_out_us = m.last_out_us.max(recv_us);
                    out.push(OutMsg::Update(PriceLevelUpdateDomain {
                        backtest_timestamp: ts(recv_us),
                        exchange_timestamp: ts(exch_us),
                        price_level: price,
                        quantity: qty,
                        side,
                        symbol: market,
                    }));
                }
            }
            ConnEvent::Invalidated { conn, market } => self.lose(conn, &market, out),
            ConnEvent::Disconnected { conn } => {
                let mut markets: Vec<String> = self
                    .markets
                    .iter()
                    .filter(|(_, m)| m.sources.contains_key(&conn))
                    .map(|(k, _)| k.clone())
                    .collect();
                markets.sort();
                for market in markets {
                    self.lose(conn, &market, out);
                }
            }
            ConnEvent::Lifecycle { market, event, .. } => {
                self.stats.lifecycle_events += 1;
                self.lifecycle.push((market, event));
            }
            ConnEvent::Removed { conn, market } => {
                let Some(m) = self.markets.get_mut(&market) else {
                    return;
                };
                m.sources.remove(&conn);
                if !m.removed {
                    m.removed = true;
                    let now = now_us();
                    let exch_us = m
                        .active
                        .and_then(|c| m.sources.get(&c))
                        .map_or(now, |s| s.exch_us);
                    self.snapshot_log
                        .push((market.clone(), SnapCause::Removed, now));
                    out.push(snapshot_msg(&market, &OrderBook::default(), now, exch_us));
                    self.stats.snapshots += 1;
                    self.stats.removed += 1;
                }
                if m.sources.is_empty() {
                    self.markets.remove(&market);
                }
            }
        }
    }

    /// `conn`'s book for `market` is gone. If it was the active source, fail over to the
    /// freshest valid replica (snapshot first), or go silent and start a gap.
    fn lose(&mut self, conn: ConnId, market: &str, out: &mut Vec<OutMsg>) {
        let Some(m) = self.markets.get_mut(market) else {
            return;
        };
        // The gap (if any) starts at the last data we had from the lost source.
        let mut last_data = 0;
        if let Some(src) = m.sources.get_mut(&conn) {
            src.book = None;
            last_data = src.recv_us;
        }
        if m.active != Some(conn) || m.removed {
            return;
        }
        match m.best() {
            Some(next) => {
                m.active = Some(next);
                self.stats.failovers += 1;
                // When the new source's book reached this state, but never before what
                // was already written: its queued updates and the next periodic
                // snapshot then stay in time order.
                let at = m.sources[&next].recv_us.max(m.last_out_us);
                m.last_out_us = at;
                let log = &mut self.snapshot_log;
                emit(
                    market,
                    m,
                    next,
                    at,
                    SnapCause::Failover,
                    out,
                    log,
                    &mut self.stats,
                );
            }
            None => {
                m.active = None;
                m.gap_since = Some(last_data);
            }
        }
    }

    /// A snapshot of every market that currently has a valid active source.
    pub fn snapshot_all(&mut self, now_us: i64, cause: SnapCause, out: &mut Vec<OutMsg>) {
        let mut markets: Vec<_> = self
            .markets
            .iter_mut()
            .filter(|(_, m)| !m.removed)
            .filter_map(|(k, m)| {
                let src = m.sources.get(&m.active?)?;
                let (book, exch_us) = (src.book.as_ref()?, src.exch_us);
                m.last_out_us = m.last_out_us.max(now_us);
                Some((k, book, exch_us))
            })
            .collect();
        markets.sort_by(|a, b| a.0.cmp(b.0));
        for (market, book, exch_us) in markets {
            out.push(snapshot_msg(market, book, now_us, exch_us));
            self.stats.snapshots += 1;
        }
        tracing::info!(cause = ?cause, now_us, "snapshot round");
    }
}

fn close_gap(m: &mut Market, market: &str, now: i64, stats: &mut ArbiterStats) {
    if let Some(since) = m.gap_since.take() {
        let gap = now - since;
        stats.gaps += 1;
        stats.gap_total_us += gap;
        stats.gap_max_us = stats.gap_max_us.max(gap);
        tracing::info!(%market, gap_ms = gap / 1000, "gap closed");
    }
}

/// Write `conn`'s book for `market` as a snapshot.
#[allow(clippy::too_many_arguments)]
fn emit(
    market: &str,
    m: &Market,
    conn: ConnId,
    at: i64,
    cause: SnapCause,
    out: &mut Vec<OutMsg>,
    log: &mut Vec<(String, SnapCause, i64)>,
    stats: &mut ArbiterStats,
) {
    let src = &m.sources[&conn];
    log.push((market.to_owned(), cause, at));
    out.push(snapshot_msg(
        market,
        src.book.as_ref().expect("valid source"),
        at,
        src.exch_us,
    ));
    stats.snapshots += 1;
}

/// One line per non-periodic snapshot: `verify --events` pairs these up.
fn log_snapshot(market: &str, cause: SnapCause, at: i64) {
    tracing::info!(target: "snapshot", %market, cause = ?cause, recv_us = at, "snapshot");
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
pub async fn run(
    mut events: mpsc::Receiver<ConnEvent>,
    writer: Sender<OutMsg>,
    lifecycle: mpsc::UnboundedSender<(String, Lifecycle)>,
) -> ArbiterStats {
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
        for (market, cause, at) in a.snapshot_log.drain(..) {
            log_snapshot(&market, cause, at);
        }
        send_all(&writer, &mut buf, &mut a.stats);
        for ev in a.lifecycle.drain(..) {
            let _ = lifecycle.send(ev); // control may already be gone at shutdown
        }
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
    fn snap(conn: ConnId, bid: i64, exch_us: i64) -> ConnEvent {
        ConnEvent::Snapshot {
            conn,
            market: "T".into(),
            book: book(bid),
            recv_us: exch_us,
            exch_us,
            cause: SnapCause::Subscribed,
        }
    }
    fn update(conn: ConnId, qty: i64, exch_us: i64) -> ConnEvent {
        ConnEvent::Update {
            conn,
            market: "T".into(),
            side: Side::Buy,
            price: 400_000,
            qty,
            recv_us: exch_us,
            exch_us,
        }
    }
    fn bids(m: &OutMsg) -> Vec<(i64, i64)> {
        let OutMsg::Snapshot(s) = m else {
            panic!("not a snapshot: {m:?}")
        };
        s.bids.iter().map(|l| (l.price_level, l.quantity)).collect()
    }

    #[test]
    fn forwards_only_the_active_source() {
        let mut a = Arbiter::default();
        let mut out = Vec::new();
        a.on_event(snap(0, 400_000, 1), &mut out);
        a.on_event(snap(1, 400_000, 1), &mut out); // backup: silent
        a.on_event(update(1, 7, 2), &mut out); // backup: silent
        a.on_event(update(0, 7, 2), &mut out); // the same change on the active source
        assert_eq!(out.len(), 2);
        assert!(matches!(&out[1], OutMsg::Update(u) if u.quantity == 7));
        a.snapshot_all(10, SnapCause::Periodic, &mut out);
        assert_eq!(bids(&out[2]), vec![(400_000, 7)]);
    }

    #[test]
    fn disconnect_fails_over_with_a_snapshot_and_no_gap() {
        let mut a = Arbiter::default();
        let mut out = Vec::new();
        a.on_event(snap(0, 400_000, 1), &mut out);
        a.on_event(snap(1, 400_000, 1), &mut out);
        a.on_event(update(1, 9, 3), &mut out); // the backup has seen more
        a.on_event(ConnEvent::Disconnected { conn: 0 }, &mut out);
        assert_eq!(bids(out.last().unwrap()), vec![(400_000, 9)]);
        assert_eq!((a.stats.failovers, a.stats.gaps), (1, 0));
        a.on_event(update(1, 4, 4), &mut out); // now forwarded
        assert!(matches!(out.last().unwrap(), OutMsg::Update(u) if u.quantity == 4));
        // The old source coming back is a backup: silent.
        let n = out.len();
        a.on_event(snap(0, 400_000, 5), &mut out);
        assert_eq!(out.len(), n);
    }

    #[test]
    fn failover_to_a_quiet_backup_never_goes_back_in_time() {
        let mut a = Arbiter::default();
        let mut out = Vec::new();
        a.on_event(snap(0, 400_000, 1), &mut out);
        a.on_event(snap(1, 400_000, 1), &mut out); // the backup's last data: t=1
        a.on_event(update(0, 6, 50), &mut out); // the active source wrote t=50
        a.on_event(ConnEvent::Disconnected { conn: 0 }, &mut out);
        let OutMsg::Snapshot(s) = out.last().unwrap() else {
            panic!()
        };
        assert_eq!(s.received_at, ts(50));
        // A quiet market's latest output is often a periodic snapshot.
        a.snapshot_all(90, SnapCause::Periodic, &mut out);
        a.on_event(snap(0, 400_000, 2), &mut out); // conn 0 back, as a backup
        a.on_event(ConnEvent::Disconnected { conn: 1 }, &mut out);
        let OutMsg::Snapshot(s) = out.last().unwrap() else {
            panic!()
        };
        assert_eq!(s.received_at, ts(90));
    }

    #[test]
    fn no_valid_replica_means_a_gap_until_any_source_returns() {
        let mut a = Arbiter::default();
        let mut out = Vec::new();
        a.on_event(snap(0, 400_000, 1), &mut out);
        a.on_event(
            ConnEvent::Invalidated {
                conn: 0,
                market: "T".into(),
            },
            &mut out,
        );
        a.on_event(update(0, 3, 2), &mut out); // invalid source: dropped
        a.snapshot_all(3, SnapCause::Periodic, &mut out);
        assert_eq!(out.len(), 1);
        a.on_event(snap(1, 300_000, now_us()), &mut out);
        assert_eq!(bids(out.last().unwrap()), vec![(300_000, 5)]);
        assert_eq!(a.stats.gaps, 1);
    }

    #[test]
    fn removal_writes_one_terminal_snapshot_across_replicas() {
        let mut a = Arbiter::default();
        let mut out = Vec::new();
        a.on_event(snap(0, 400_000, 1), &mut out);
        a.on_event(snap(1, 400_000, 1), &mut out);
        a.on_event(
            ConnEvent::Removed {
                conn: 0,
                market: "T".into(),
            },
            &mut out,
        );
        assert!(bids(out.last().unwrap()).is_empty());
        a.on_event(update(1, 2, 2), &mut out); // the other replica has not removed it yet
        a.on_event(
            ConnEvent::Removed {
                conn: 1,
                market: "T".into(),
            },
            &mut out,
        );
        assert_eq!(out.len(), 2);
        assert_eq!(a.stats.removed, 1);
        assert!(a.markets.is_empty());
    }
}
