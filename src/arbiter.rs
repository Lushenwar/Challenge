//! Decides what reaches the output. Each market can arrive on several connections
//! (replicas). The arbiter mirrors every source's book from its events, forwards only
//! the active source, and on failover writes the new source's book as a snapshot before
//! any of its deltas. What has been written always equals the active source's book.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    time::Duration,
};

use chrono::{DateTime, Utc};
use crossbeam::channel::{Sender, TrySendError};
use serde::Serialize;
use tokio::sync::mpsc;

use crate::{
    book::OrderBook,
    health::{self, Rate, Recycler},
    model::{ConnEvent, ConnId, ControlMsg, OutMsg, SnapCause, now_us},
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
    /// Failovers because the active source fell behind a replica (health rule 1).
    pub failovers_stale: u64,
    pub resubscribes_lag: u64,
    pub resubscribes_rate: u64,
    pub recycles: u64,
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
    /// Exchange time of the latest *delta*: what replicas are compared on. Snapshots
    /// carry Kalshi's send time, not an event time, so they do not count.
    delta_us: i64,
    rate: Rate,
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
    /// Since when a replica has been ahead of the active source.
    lag_since: Option<i64>,
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

    /// Rule 1 bookkeeping after a delta: is a valid replica ahead of the active source?
    fn track_lag(&mut self, now_us: i64) -> bool {
        let Some(a) = self.active else {
            self.lag_since = None;
            return false;
        };
        let active = self.sources.get(&a).map_or(0, |s| s.delta_us);
        let ahead = self
            .sources
            .iter()
            .any(|(c, s)| *c != a && s.book.is_some() && s.delta_us > active);
        if ahead {
            self.lag_since.get_or_insert(now_us);
        } else {
            self.lag_since = None;
        }
        ahead
    }
}

#[derive(Default)]
pub struct Arbiter {
    markets: HashMap<String, Market>,
    /// Messages for the control task (lifecycle events, health actions), drained by
    /// [`run`].
    pub control: Vec<ControlMsg>,
    /// Markets with a replica ahead of the active source.
    lagging: HashSet<String>,
    conns: BTreeSet<ConnId>,
    recycler: Recycler,
    last_recycle_check: i64,
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
                self.conns.insert(conn);
                // A fresh snapshot already holds every delta its replicas applied so far,
                // so for rule 1 it has caught up with them.
                let caught_up = m
                    .sources
                    .iter()
                    .filter(|(c, _)| **c != conn)
                    .map(|(_, s)| s.delta_us)
                    .max();
                let src = m.sources.entry(conn).or_default();
                src.delta_us = src.delta_us.max(caught_up.unwrap_or(0));
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
                src.delta_us = exch_us;
                src.rate.hit();
                if m.track_lag(recv_us) {
                    self.lagging.insert(market.clone());
                }
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
                self.control.push(ControlMsg::Lifecycle(market, event));
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

    /// Health rules 1 and 4; call often (every 250 ms).
    pub fn on_tick(&mut self, now_us: i64, out: &mut Vec<OutMsg>) {
        let lagging: Vec<String> = self.lagging.drain().collect();
        for market in lagging {
            let Some(m) = self.markets.get_mut(&market) else {
                continue;
            };
            let (Some(since), Some(stale), false) = (m.lag_since, m.active, m.removed) else {
                continue; // caught up, silent, or removed
            };
            if now_us - since < health::LAG_LIMIT_US {
                self.lagging.insert(market);
                continue;
            }
            let next = m
                .sources
                .iter()
                .filter(|(c, s)| **c != stale && s.book.is_some())
                .max_by_key(|(c, s)| (s.delta_us, std::cmp::Reverse(**c)))
                .map(|(c, _)| *c);
            let Some(next) = next else { continue };
            m.lag_since = None;
            m.active = Some(next);
            let at = m.sources[&next].recv_us.max(m.last_out_us);
            m.last_out_us = at;
            let log = &mut self.snapshot_log;
            emit(
                &market,
                m,
                next,
                at,
                SnapCause::Stale,
                out,
                log,
                &mut self.stats,
            );
            self.stats.failovers += 1;
            self.stats.failovers_stale += 1;
            self.stats.resubscribes_lag += 1;
            tracing::warn!(%market, stale, next, "active source fell behind; failed over");
            self.control.push(ControlMsg::Resubscribe {
                conn: stale,
                market,
            });
        }
        if now_us - self.last_recycle_check >= 10_000_000 {
            self.last_recycle_check = now_us;
            let conns: Vec<ConnId> = self.conns.iter().copied().collect();
            for conn in conns {
                if self.recycler.due(conn, now_us) && self.safe_to_recycle(conn) {
                    self.recycler.recycled(conn, now_us);
                    self.stats.recycles += 1;
                    tracing::info!(conn, "recycling connection");
                    self.control.push(ControlMsg::Recycle(conn));
                    break;
                }
            }
        }
    }

    /// Rule 4 guard: every market on `conn` has another valid replica.
    pub fn safe_to_recycle(&self, conn: ConnId) -> bool {
        self.markets
            .values()
            .filter(|m| !m.removed && m.sources.contains_key(&conn))
            .all(|m| {
                m.sources
                    .iter()
                    .any(|(c, s)| *c != conn && s.book.is_some())
            })
    }

    /// Health rule 2; call once per minute. Never resubscribes the active source while a
    /// valid replica exists: that would open a gap, and a silent active source is caught
    /// by rule 1 through its replica. With one replica, the only source is resubscribed.
    pub fn health_minute(&mut self, now_us: i64) {
        for (market, m) in &mut self.markets {
            if m.removed {
                continue;
            }
            let valid = m.sources.values().filter(|s| s.book.is_some()).count();
            let active = m.active;
            for (conn, src) in &mut m.sources {
                let collapsed = src.rate.on_minute(now_us);
                let spare = active != Some(*conn) || valid == 1;
                if collapsed && src.book.is_some() && spare {
                    self.stats.resubscribes_rate += 1;
                    tracing::warn!(%market, conn, "message rate collapsed; resubscribing");
                    let (conn, market) = (*conn, market.clone());
                    self.control.push(ControlMsg::Resubscribe { conn, market });
                }
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
    control: mpsc::UnboundedSender<ControlMsg>,
) -> ArbiterStats {
    let mut a = Arbiter::default();
    let mut buf = Vec::new();
    let minute = tokio::time::sleep(until_next_minute());
    tokio::pin!(minute);
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    loop {
        tokio::select! {
            ev = events.recv() => match ev {
                Some(ev) => a.on_event(ev, &mut buf),
                None => break,
            },
            _ = &mut minute => {
                a.stats.periodic_rounds += 1;
                let now = now_us();
                a.snapshot_all(now, SnapCause::Periodic, &mut buf);
                a.health_minute(now);
                minute.as_mut().reset(tokio::time::Instant::now() + until_next_minute());
            }
            _ = tick.tick() => a.on_tick(now_us(), &mut buf),
        }
        for (market, cause, at) in a.snapshot_log.drain(..) {
            log_snapshot(&market, cause, at);
        }
        send_all(&writer, &mut buf, &mut a.stats);
        for msg in a.control.drain(..) {
            let _ = control.send(msg); // control may already be gone at shutdown
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

    /// Phase 7 (a): the active source goes silent for a market while its replica keeps
    /// moving; failover happens once the replica has been ahead for 2s, not before.
    #[test]
    fn silenced_active_source_fails_over_within_2s_of_replica_moving_ahead() {
        let mut a = Arbiter::default();
        let mut out = Vec::new();
        a.on_event(snap(0, 400_000, 1), &mut out);
        a.on_event(snap(1, 400_000, 1), &mut out);
        // Both see a delta at t=500: no lag.
        a.on_event(update(1, 6, 500), &mut out);
        a.on_event(update(0, 6, 500), &mut out);
        a.on_tick(10_000_000, &mut out);
        assert_eq!(a.stats.failovers_stale, 0);
        // Now only the replica moves.
        let t = 20_000_000;
        a.on_event(update(1, 9, t), &mut out);
        a.on_tick(t + health::LAG_LIMIT_US - 1, &mut out);
        assert_eq!(a.stats.failovers_stale, 0);
        a.on_tick(t + health::LAG_LIMIT_US, &mut out);
        assert_eq!(a.stats.failovers_stale, 1);
        assert_eq!(bids(out.last().unwrap()), vec![(400_000, 9)]); // the replica's book
        assert!(a.control.contains(&ControlMsg::Resubscribe {
            conn: 0,
            market: "T".into()
        }));
        // Its deltas are forwarded from now on; the stale source's are not.
        let n = out.len();
        a.on_event(update(0, 1, t + 3_000_000), &mut out);
        assert_eq!(out.len(), n);
        a.on_event(update(1, 2, t + 3_000_000), &mut out);
        assert_eq!(out.len(), n + 1);
    }

    #[test]
    fn a_replica_that_catches_up_is_not_failed_over() {
        let mut a = Arbiter::default();
        let mut out = Vec::new();
        a.on_event(snap(0, 400_000, 1), &mut out);
        a.on_event(snap(1, 400_000, 1), &mut out);
        a.on_event(update(1, 6, 1_000), &mut out); // replica first...
        a.on_event(update(0, 6, 1_000), &mut out); // ...active a moment later
        a.on_tick(10_000_000, &mut out);
        assert_eq!(a.stats.failovers_stale, 0);
        assert!(a.control.is_empty());
    }

    /// Phase 7 (b): a collapsed message rate on a source triggers a resubscribe there.
    #[test]
    fn rate_collapsed_market_is_resubscribed() {
        const MIN: i64 = 60_000_000;
        let mut a = Arbiter::default();
        let mut out = Vec::new();
        a.on_event(snap(0, 400_000, 1), &mut out);
        for minute in 0..10 {
            for i in 0..100 {
                a.on_event(update(0, i + 1, minute * MIN + i), &mut out);
            }
            a.health_minute((minute + 1) * MIN);
        }
        assert!(a.control.is_empty());
        a.on_event(update(0, 5, 10 * MIN + 1), &mut out); // 1 message instead of ~100
        a.health_minute(11 * MIN);
        assert_eq!(
            a.control,
            vec![ControlMsg::Resubscribe {
                conn: 0,
                market: "T".into()
            }]
        );
        assert_eq!(a.stats.resubscribes_rate, 1);
    }

    /// Found live: a quiet market must not have both replicas resubscribed at once.
    #[test]
    fn rate_collapse_with_a_valid_replica_spares_the_active_source() {
        const MIN: i64 = 60_000_000;
        let mut a = Arbiter::default();
        let mut out = Vec::new();
        a.on_event(snap(0, 400_000, 1), &mut out);
        a.on_event(snap(1, 400_000, 1), &mut out);
        for minute in 0..10 {
            for i in 0..100 {
                let t = minute * MIN + i;
                a.on_event(update(0, i + 1, t), &mut out);
                a.on_event(update(1, i + 1, t), &mut out);
            }
            a.health_minute((minute + 1) * MIN);
        }
        a.health_minute(11 * MIN); // the market goes quiet on both
        assert_eq!(
            a.control,
            vec![ControlMsg::Resubscribe {
                conn: 1,
                market: "T".into()
            }]
        );
    }

    /// Found live: a source that just took a snapshot is not "behind" a replica that
    /// applied, as a delta, a change the snapshot already contains.
    #[test]
    fn a_fresh_snapshot_is_not_stale() {
        let mut a = Arbiter::default();
        let mut out = Vec::new();
        a.on_event(snap(1, 400_000, 1), &mut out); // conn 1 active
        a.on_event(update(1, 6, 1_000), &mut out);
        a.on_event(ConnEvent::Disconnected { conn: 1 }, &mut out); // gap
        a.on_event(snap(0, 400_000, 2_000), &mut out); // conn 0 closes the gap
        a.on_event(snap(1, 400_000, 3_000), &mut out); // conn 1 back as backup
        a.on_event(update(1, 7, 4_000), &mut out); // ...and conn 0 sees it a moment later
        a.on_event(update(0, 7, 4_000), &mut out);
        a.on_tick(60_000_000, &mut out);
        assert_eq!(a.stats.failovers_stale, 0);
    }

    /// Phase 7 (c): a socket is never recycled while one of its markets has no other
    /// valid replica.
    #[test]
    fn recycle_waits_until_every_market_has_another_valid_replica() {
        const MIN: i64 = 60_000_000;
        let mut a = Arbiter::default();
        let mut out = Vec::new();
        let book_on = |conn, market: &str| ConnEvent::Snapshot {
            conn,
            market: market.into(),
            book: book(400_000),
            recv_us: 1,
            exch_us: 1,
            cause: SnapCause::Subscribed,
        };
        a.on_event(book_on(0, "T"), &mut out);
        a.on_event(book_on(1, "T"), &mut out);
        a.on_event(book_on(0, "U"), &mut out);
        a.on_event(book_on(1, "U"), &mut out);
        a.on_event(
            ConnEvent::Invalidated {
                conn: 1,
                market: "U".into(),
            },
            &mut out,
        );
        a.on_tick(10_000_000, &mut out); // schedules both sockets 30–45 min out
        let recycled = |a: &Arbiter, c| a.control.contains(&ControlMsg::Recycle(c));
        let mut now = 10_000_000 + 46 * MIN;
        for _ in 0..20 {
            a.on_tick(now, &mut out);
            now += 10_000_000;
        }
        // U lives only on socket 0, so socket 0 must not be recycled; socket 1 may be.
        assert!(!recycled(&a, 0));
        assert!(!a.safe_to_recycle(0));
        assert!(recycled(&a, 1));
        // Once U is valid on socket 1 again, socket 0 can go.
        a.on_event(book_on(1, "U"), &mut out);
        assert!(a.safe_to_recycle(0));
        for _ in 0..5 {
            a.on_tick(now, &mut out);
            now += 10_000_000;
        }
        assert!(recycled(&a, 0));
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
