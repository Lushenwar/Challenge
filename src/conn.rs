//! One websocket connection: subscribe, check `seq` per sid (where the venue has it),
//! keep its own copy of every book, and report facts to the arbiter. It never decides
//! what is written. Venue specifics come from its [`Session`].
//!
//! [`ConnState`] is the pure per-frame logic (driven by replay tests); [`run`] is the
//! socket loop around it.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::Message;

use crate::{
    book::OrderBook,
    exchange::{Change, Decoded, Exchange, Session},
    model::{ConnEvent, ConnId, SnapCause, now_us},
};

/// A requested resync that has not arrived after this long means the subscription is
/// broken: reconnect.
pub const RESYNC_TIMEOUT_US: i64 = 15_000_000;
const PING_EVERY: Duration = Duration::from_secs(10);
/// No frame of any kind (data, ping, pong) for this long = a dead socket. Not "no pong
/// within 10s": a pong queues behind bulk data (a socket's initial snapshots can take
/// 15s+ to download), so a pong deadline falsely kills busy sockets.
const SILENT_FOR: Duration = Duration::from_secs(20);

#[derive(Debug, Default, Clone, Serialize)]
pub struct ConnStats {
    pub connects: u64,
    pub frames: u64,
    pub snapshots: u64,
    pub deltas: u64,
    pub dropped_invalid: u64,
    pub early_snapshots: u64,
    pub seq_gaps: u64,
    pub bad_deltas: u64,
    pub decode_errors: u64,
    pub resync_requests: u64,
    pub resubscribes: u64,
    pub exch_ts_fallbacks: u64,
    pub crossed: u64,
    pub stalls: u64,
    /// Venue `error` frames by code.
    pub errors: BTreeMap<i64, u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    Event(ConnEvent),
    /// A command to send on the socket.
    Send(String),
}

#[derive(Default)]
struct Market {
    sid: Option<u64>,
    /// Its subscription snapshot has arrived, so its level updates are flowing.
    streaming: bool,
    /// `None` = Pending or Invalid.
    book: Option<OrderBook>,
    resync_since: Option<i64>,
}

pub struct ConnState {
    conn: ConnId,
    markets: HashMap<String, Market>,
    next_seq: HashMap<u64, u64>,
    connected: bool,
    session: Box<dyn Session>,
    pub stats: ConnStats,
}

impl ConnState {
    pub fn new(conn: ConnId, tickers: &[String], session: Box<dyn Session>) -> Self {
        Self {
            conn,
            markets: tickers
                .iter()
                .map(|t| (t.clone(), Market::default()))
                .collect(),
            next_seq: HashMap::new(),
            connected: false,
            session,
            stats: ConnStats::default(),
        }
    }

    /// Commands for a fresh connection. Every market becomes Pending.
    pub fn subscribe_cmds(&mut self) -> Vec<String> {
        self.next_seq.clear();
        self.connected = true;
        self.markets
            .values_mut()
            .for_each(|m| *m = Market::default());
        let mut tickers: Vec<String> = self.markets.keys().cloned().collect();
        tickers.sort();
        self.session.on_connect(&tickers)
    }

    pub fn disconnected(&mut self) {
        self.connected = false;
    }

    pub fn keepalive(&self) -> Message {
        self.session.keepalive()
    }

    /// Start collecting `tickers`. Returns the commands to send now (none while
    /// disconnected: the next connect subscribes everything).
    pub fn add_markets(&mut self, tickers: Vec<String>) -> Vec<String> {
        let new: Vec<String> = tickers
            .into_iter()
            .filter(|t| !self.markets.contains_key(t))
            .collect();
        for t in &new {
            self.markets.insert(t.clone(), Market::default());
        }
        if self.connected {
            self.session.subscribe(&new)
        } else {
            Vec::new()
        }
    }

    /// Drop and re-request `tickers`. Each becomes Pending; an `Invalidated` event goes
    /// out for any that had a valid book.
    pub fn resubscribe(&mut self, tickers: Vec<String>) -> Vec<Action> {
        let mut out = Vec::new();
        let known: Vec<String> = tickers
            .into_iter()
            .filter(|t| self.markets.contains_key(t))
            .collect();
        for t in &known {
            let m = self.markets.get_mut(t).expect("filtered");
            if m.book.is_some() {
                out.push(Action::Event(ConnEvent::Invalidated {
                    conn: self.conn,
                    market: t.clone(),
                }));
            }
            *m = Market::default();
        }
        if !self.connected || known.is_empty() {
            return out; // the next connect subscribes them anyway
        }
        self.stats.resubscribes += 1;
        let cmds = self
            .session
            .unsubscribe(&known)
            .into_iter()
            .chain(self.session.subscribe(&known));
        out.extend(cmds.map(Action::Send));
        out
    }

    /// Stop collecting `tickers`: a `Removed` event per market (in order with every
    /// earlier event) and the unsubscribe commands.
    pub fn remove_markets(&mut self, tickers: &[String]) -> Vec<Action> {
        let gone: Vec<String> = tickers
            .iter()
            .filter(|t| self.markets.remove(*t).is_some())
            .cloned()
            .collect();
        let mut out: Vec<Action> = gone
            .iter()
            .map(|m| {
                Action::Event(ConnEvent::Removed {
                    conn: self.conn,
                    market: m.clone(),
                })
            })
            .collect();
        if self.connected && !gone.is_empty() {
            out.extend(
                self.session
                    .unsubscribe(&gone)
                    .into_iter()
                    .map(Action::Send),
            );
        }
        out
    }

    /// True if a requested resync is overdue and the connection should be recycled.
    pub fn resync_overdue(&self, now_us: i64) -> bool {
        self.markets.values().any(|m| {
            m.resync_since
                .is_some_and(|t| now_us - t > RESYNC_TIMEOUT_US)
        })
    }

    pub fn on_frame(&mut self, text: &str, recv_us: i64) -> Vec<Action> {
        self.stats.frames += 1;
        let mut out = Vec::new();
        match self.session.decode(text) {
            Ok(decoded) => {
                for d in decoded {
                    self.on_decoded(d, recv_us, &mut out);
                }
            }
            Err(e) => {
                // A lost seq-bearing frame shows up as a gap on the next frame.
                self.stats.decode_errors += 1;
                tracing::warn!(conn = self.conn, error = %format!("{e:#}"), "undecodable frame");
            }
        }
        out
    }

    fn on_decoded(&mut self, d: Decoded, recv_us: i64, out: &mut Vec<Action>) {
        if let Some((sid, seq)) = d.sid_seq() {
            let expected = self.next_seq.insert(sid, seq + 1);
            if let Some(expected) = expected.filter(|&e| e != seq) {
                self.stats.seq_gaps += 1;
                tracing::warn!(
                    conn = self.conn,
                    sid,
                    expected,
                    seq,
                    "seq gap; resyncing sid"
                );
                self.invalidate_sid(sid, recv_us, out);
            }
        }
        match d {
            Decoded::Snapshot {
                sid_seq,
                requested,
                market,
                bids,
                asks,
                exch_us,
            } => {
                let Some(m) = self.markets.get_mut(&market) else {
                    return;
                };
                if requested && !m.streaming {
                    // Not a base for level updates until its subscription snapshot arrives.
                    self.stats.early_snapshots += 1;
                    return;
                }
                self.stats.snapshots += 1;
                let cause = match (m.streaming, requested) {
                    (false, _) => SnapCause::Subscribed,
                    (true, true) => SnapCause::Resync,
                    (true, false) => SnapCause::Exchange,
                };
                m.streaming = true;
                m.sid = sid_seq.map(|(sid, _)| sid);
                match OrderBook::from_levels(&bids, &asks) {
                    Ok(book) => {
                        m.book = Some(book.clone());
                        m.resync_since = None;
                        let exch_us = self.exch_or_fallback(exch_us, recv_us);
                        let conn = self.conn;
                        out.push(Action::Event(ConnEvent::Snapshot {
                            conn,
                            market,
                            book,
                            recv_us,
                            exch_us,
                            cause,
                        }));
                    }
                    Err(e) => {
                        tracing::warn!(conn = self.conn, %market, error = %e, "bad snapshot");
                        let report = m.book.take().is_some();
                        let sid = m.sid;
                        self.resync(sid, vec![market], report, recv_us, out);
                    }
                }
            }
            Decoded::Level {
                market,
                side,
                price,
                change,
                exch_us,
                ..
            } => {
                self.stats.deltas += 1;
                let Some(m) = self.markets.get_mut(&market) else {
                    return;
                };
                let Some(book) = m.book.as_mut() else {
                    self.stats.dropped_invalid += 1;
                    return;
                };
                let applied = match change {
                    Change::Delta(delta) => book.apply_delta(side, price, delta),
                    Change::Absolute(qty) if qty >= 0 => {
                        book.set_level(side, price, qty);
                        Ok(qty)
                    }
                    Change::Absolute(qty) => {
                        Err(crate::book::BookError::BadQuantity { price, qty })
                    }
                };
                match applied {
                    Ok(qty) => {
                        if book.is_crossed() {
                            self.stats.crossed += 1;
                        }
                        let exch_us = self.exch_or_fallback(exch_us, recv_us);
                        let conn = self.conn;
                        out.push(Action::Event(ConnEvent::Update {
                            conn,
                            market,
                            side,
                            price,
                            qty,
                            recv_us,
                            exch_us,
                        }));
                    }
                    Err(e) => {
                        self.stats.bad_deltas += 1;
                        tracing::warn!(conn = self.conn, %market, error = %e, "bad level update; resyncing market");
                        m.book = None;
                        let sid = m.sid;
                        self.resync(sid, vec![market], true, recv_us, out);
                    }
                }
            }
            Decoded::Lifecycle { market, event, .. } => {
                let conn = self.conn;
                out.push(Action::Event(ConnEvent::Lifecycle {
                    conn,
                    market,
                    event,
                }));
            }
            Decoded::Error { code, msg } => {
                *self.stats.errors.entry(code).or_default() += 1;
                tracing::warn!(conn = self.conn, code, %msg, "venue error");
            }
            Decoded::Other { .. } => {}
        }
    }

    fn exch_or_fallback(&mut self, exch_us: Option<i64>, recv_us: i64) -> i64 {
        exch_us.unwrap_or_else(|| {
            self.stats.exch_ts_fallbacks += 1;
            recv_us
        })
    }

    /// A gap on `sid`: every market on it is invalid; streaming ones are resynced, the
    /// rest are still waiting for their subscription snapshot anyway.
    fn invalidate_sid(&mut self, sid: u64, recv_us: i64, out: &mut Vec<Action>) {
        let mut tickers = Vec::new();
        for (ticker, m) in &mut self.markets {
            if m.sid == Some(sid) && m.streaming {
                if m.book.take().is_some() {
                    out.push(Action::Event(ConnEvent::Invalidated {
                        conn: self.conn,
                        market: ticker.clone(),
                    }));
                }
                tickers.push(ticker.clone());
            }
        }
        tickers.sort();
        self.resync(Some(sid), tickers, false, recv_us, out);
    }

    /// Fresh books for `tickers`: a snapshot request where the venue supports one,
    /// otherwise a resubscribe. `report` emits `Invalidated` first.
    fn resync(
        &mut self,
        sid: Option<u64>,
        tickers: Vec<String>,
        report: bool,
        recv_us: i64,
        out: &mut Vec<Action>,
    ) {
        if tickers.is_empty() {
            return;
        }
        if report {
            for t in &tickers {
                out.push(Action::Event(ConnEvent::Invalidated {
                    conn: self.conn,
                    market: t.clone(),
                }));
            }
        }
        match sid.and_then(|sid| self.session.request_snapshots(sid, &tickers)) {
            Some(cmds) => {
                for t in &tickers {
                    if let Some(m) = self.markets.get_mut(t) {
                        m.resync_since.get_or_insert(recv_us);
                    }
                }
                self.stats.resync_requests += cmds.len() as u64;
                out.extend(cmds.into_iter().map(Action::Send));
            }
            // Already reported above; resubscribe would report again.
            None => out.extend(
                self.resubscribe(tickers)
                    .into_iter()
                    .filter(|a| matches!(a, Action::Send(_))),
            ),
        }
    }
}

/// Changes to a connection's market set, from the control task.
#[derive(Debug)]
pub enum ConnCmd {
    Add(Vec<String>),
    Remove(Vec<String>),
    /// Unsubscribe and resubscribe these markets: they become Pending until a fresh
    /// subscription snapshot arrives.
    Resubscribe(Vec<String>),
    /// Drop the socket now; it reconnects as after any failure (health recycle, tests).
    Drop,
}

impl ConnState {
    fn apply(&mut self, cmd: ConnCmd) -> Vec<Action> {
        match cmd {
            ConnCmd::Add(t) => self.add_markets(t).into_iter().map(Action::Send).collect(),
            ConnCmd::Remove(t) => self.remove_markets(&t),
            ConnCmd::Resubscribe(t) => self.resubscribe(t),
            ConnCmd::Drop => Vec::new(),
        }
    }
}

/// How to start a connection task.
pub struct Spec {
    pub conn: ConnId,
    pub exchange: Arc<dyn Exchange>,
    /// Websocket URL override (e.g. a local fault proxy).
    pub ws_url: Option<String>,
    pub tickers: Vec<String>,
    pub lifecycle: bool,
    pub net: crate::net::NetOpts,
}

/// The socket loop. Returns when `stop` flips, after sending a close frame.
pub async fn run(
    spec: Spec,
    mut cmds: mpsc::UnboundedReceiver<ConnCmd>,
    events: mpsc::Sender<ConnEvent>,
    mut stop: watch::Receiver<bool>,
) -> ConnStats {
    let Spec {
        conn,
        exchange,
        ws_url,
        tickers,
        lifecycle,
        net,
    } = spec;
    let mut st = ConnState::new(conn, &tickers, exchange.session(lifecycle));
    let mut backoff = Duration::from_secs(1);
    while !*stop.borrow() {
        let request = exchange.ws_request(ws_url.as_deref());
        let connected = match request {
            Ok(req) => crate::net::connect(req, &net, conn)
                .await
                .map(|(ws, peer)| {
                    tracing::debug!(conn, %peer, "tcp peer");
                    ws
                }),
            Err(e) => Err(e),
        };
        let mut ws = match connected {
            Ok(ws) => ws,
            Err(e) => {
                // ponytail: jitter from the clock's low bits; add `rand` if it ever matters.
                let jitter = Duration::from_millis((now_us() % 1000) as u64);
                tracing::warn!(conn, error = %format!("{e:#}"), ?backoff, "connect failed");
                let sleep = tokio::time::sleep(backoff + jitter);
                tokio::pin!(sleep);
                loop {
                    tokio::select! {
                        _ = &mut sleep => break,
                        _ = stop.changed() => return st.stats,
                        Some(cmd) = cmds.recv() => {
                            for a in st.apply(cmd) {
                                if let Action::Event(e) = a {
                                    if events.send(e).await.is_err() { return st.stats }
                                }
                            }
                        }
                    }
                }
                backoff = (backoff * 2).min(Duration::from_secs(30));
                continue;
            }
        };
        backoff = Duration::from_secs(1);
        st.stats.connects += 1;
        tracing::info!(conn, markets = st.markets.len(), "connected");
        let mut ok = true;
        for cmd in st.subscribe_cmds() {
            ok &= ws.send(Message::text(cmd)).await.is_ok();
        }
        let mut ping = tokio::time::interval(PING_EVERY);
        let mut last_rx = tokio::time::Instant::now();
        while ok {
            tokio::select! {
                biased; // drain the socket before timers decide anything
                msg = ws.next() => {
                    let recv_us = now_us(); // before any decoding
                    last_rx = tokio::time::Instant::now();
                    match msg {
                        Some(Ok(Message::Text(t))) => {
                            for a in st.on_frame(t.as_str(), recv_us) {
                                match a {
                                    Action::Event(e) => if events.send(e).await.is_err() { return st.stats },
                                    Action::Send(c) => ok &= ws.send(Message::text(c)).await.is_ok(),
                                }
                            }
                        }
                        Some(Ok(Message::Close(c))) => { tracing::warn!(conn, ?c, "server closed"); ok = false }
                        Some(Ok(_)) => {}
                        Some(Err(e)) => { tracing::warn!(conn, error = %e, "read error"); ok = false }
                        None => { tracing::warn!(conn, "stream ended"); ok = false }
                    }
                }
                _ = ping.tick() => {
                    if last_rx.elapsed() > SILENT_FOR {
                        st.stats.stalls += 1;
                        tracing::warn!(conn, "nothing received for {SILENT_FOR:?}; reconnecting");
                        ok = false;
                    } else if st.resync_overdue(now_us()) {
                        tracing::warn!(conn, "resync overdue; reconnecting");
                        ok = false;
                    } else {
                        // Keeps idle sockets producing replies, so silence means dead.
                        ok &= ws.send(st.keepalive()).await.is_ok();
                    }
                }
                Some(cmd) = cmds.recv() => {
                    if matches!(cmd, ConnCmd::Drop) {
                        tracing::warn!(conn, "dropping socket (recycle or test hook)");
                        ok = false;
                    }
                    for a in st.apply(cmd) {
                        match a {
                            Action::Event(e) => if events.send(e).await.is_err() { return st.stats },
                            Action::Send(c) => ok &= ws.send(Message::text(c)).await.is_ok(),
                        }
                    }
                }
                _ = stop.changed() => {
                    let _ = tokio::time::timeout(Duration::from_secs(2), ws.close(None)).await;
                    tracing::info!(conn, "closed");
                    return st.stats;
                }
            }
        }
        st.disconnected();
        if events.send(ConnEvent::Disconnected { conn }).await.is_err() {
            break;
        }
    }
    st.stats
}
