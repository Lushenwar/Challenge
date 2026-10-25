//! One websocket connection: subscribe, check `seq` per sid, keep its own copy of every
//! book, and report facts to the arbiter. It never decides what is written.
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
use serde_json::json;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest, http::HeaderValue};

use crate::{
    book::OrderBook,
    exchange::kalshi::{
        Env, WS_PATH,
        auth::Signer,
        codec::{Decoded, decode},
    },
    model::{ConnEvent, ConnId, SnapCause, now_us},
};

/// Markets per `subscribe` / `update_subscription` command. Kalshi accepted 20,000 in
/// one command; smaller batches keep each frame modest.
const BATCH: usize = 1000;
/// A requested resync that has not arrived after this long means the subscription is
/// broken: reconnect.
const RESYNC_TIMEOUT_US: i64 = 15_000_000;
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
    pub exch_ts_fallbacks: u64,
    pub crossed: u64,
    pub stalls: u64,
    /// Kalshi `error` frames by code.
    pub errors: BTreeMap<i64, u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    Event(ConnEvent),
    /// A JSON command to send on the socket.
    Send(String),
}

#[derive(Default)]
struct Market {
    sid: Option<u64>,
    /// Its subscription snapshot has arrived, so its deltas are flowing.
    streaming: bool,
    /// `None` = Pending or Invalid.
    book: Option<OrderBook>,
    resync_since: Option<i64>,
}

pub struct ConnState {
    conn: ConnId,
    markets: HashMap<String, Market>,
    next_seq: HashMap<u64, u64>,
    next_id: u64,
    /// Also subscribe to `market_lifecycle_v2` (all markets) on this connection.
    lifecycle: bool,
    connected: bool,
    /// The order-book subscription's sid (one per connection: subscribes merge).
    ob_sid: Option<u64>,
    pub stats: ConnStats,
}

impl ConnState {
    pub fn new(conn: ConnId, tickers: &[String], lifecycle: bool) -> Self {
        Self {
            conn,
            markets: tickers
                .iter()
                .map(|t| (t.clone(), Market::default()))
                .collect(),
            next_seq: HashMap::new(),
            next_id: 0,
            lifecycle,
            connected: false,
            ob_sid: None,
            stats: ConnStats::default(),
        }
    }

    /// Commands for a fresh connection. Every market becomes Pending.
    pub fn subscribe_cmds(&mut self) -> Vec<String> {
        self.next_seq.clear();
        self.ob_sid = None;
        self.connected = true;
        self.markets
            .values_mut()
            .for_each(|m| *m = Market::default());
        let mut tickers: Vec<String> = self.markets.keys().cloned().collect();
        tickers.sort();
        let mut cmds = Vec::new();
        if self.lifecycle {
            self.next_id += 1;
            cmds.push(
                json!({"id": self.next_id, "cmd": "subscribe",
                    "params": {"channels": ["market_lifecycle_v2"]}})
                .to_string(),
            );
        }
        cmds.extend(self.orderbook_subscribe(&tickers));
        cmds
    }

    pub fn disconnected(&mut self) {
        self.connected = false;
    }

    fn orderbook_subscribe(&mut self, tickers: &[String]) -> Vec<String> {
        tickers
            .chunks(BATCH)
            .map(|chunk| {
                self.next_id += 1;
                json!({"id": self.next_id, "cmd": "subscribe",
                    "params": {"channels": ["orderbook_delta"], "market_tickers": chunk}})
                .to_string()
            })
            .collect()
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
            self.orderbook_subscribe(&new)
        } else {
            Vec::new()
        }
    }

    /// Drop and re-request `tickers` (`delete_markets` then `subscribe`). Each becomes
    /// Pending; an `Invalidated` event goes out for any that had a valid book.
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
        if let Some(sid) = self.ob_sid {
            self.next_id += 1;
            out.push(Action::Send(
                json!({"id": self.next_id, "cmd": "update_subscription",
                    "params": {"sids": [sid], "market_tickers": known, "action": "delete_markets"}})
                .to_string(),
            ));
        }
        out.extend(
            self.orderbook_subscribe(&known)
                .into_iter()
                .map(Action::Send),
        );
        out
    }

    /// Stop collecting `tickers`: a `Removed` event per market (in order with every
    /// earlier event) and a `delete_markets` command.
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
        // ponytail: before the sid is known the delete is skipped; frames for an unknown
        // market are ignored anyway, and the next reconnect will not subscribe it.
        if let (true, Some(sid)) = (self.connected, self.ob_sid) {
            for chunk in gone.chunks(BATCH) {
                self.next_id += 1;
                out.push(Action::Send(
                    json!({"id": self.next_id, "cmd": "update_subscription",
                        "params": {"sids": [sid], "market_tickers": chunk, "action": "delete_markets"}})
                    .to_string(),
                ));
            }
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
        let d = match decode(text) {
            Ok(d) => d,
            Err(e) => {
                // A lost seq-bearing frame shows up as a gap on the next frame.
                self.stats.decode_errors += 1;
                tracing::warn!(conn = self.conn, error = %format!("{e:#}"), "undecodable frame");
                return out;
            }
        };
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
                self.invalidate_sid(sid, recv_us, &mut out);
            }
        }
        match d {
            Decoded::Snapshot {
                sid,
                id,
                ticker,
                bids,
                asks,
                sending_ts_ms,
                ..
            } => {
                let Some(m) = self.markets.get_mut(&ticker) else {
                    return out;
                };
                if id.is_some() && !m.streaming {
                    // Not a base for deltas until its subscription snapshot arrives.
                    self.stats.early_snapshots += 1;
                    return out;
                }
                self.stats.snapshots += 1;
                let cause = match (m.streaming, id) {
                    (false, _) => SnapCause::Subscribed,
                    (true, Some(_)) => SnapCause::Resync,
                    (true, None) => SnapCause::Exchange,
                };
                m.streaming = true;
                m.sid = Some(sid);
                match OrderBook::from_levels(&bids, &asks) {
                    Ok(book) => {
                        m.book = Some(book.clone());
                        m.resync_since = None;
                        let exch_us = match sending_ts_ms {
                            Some(ms) => ms * 1000,
                            None => {
                                self.stats.exch_ts_fallbacks += 1;
                                recv_us
                            }
                        };
                        let (conn, market) = (self.conn, ticker);
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
                        tracing::warn!(conn = self.conn, %ticker, error = %e, "bad snapshot");
                        let was_valid = m.book.take().is_some();
                        self.resync(sid, vec![ticker], was_valid, recv_us, &mut out);
                    }
                }
            }
            Decoded::Delta {
                ticker,
                side,
                price,
                delta,
                ts_ms,
                sending_ts_ms,
                ..
            } => {
                self.stats.deltas += 1;
                let Some(m) = self.markets.get_mut(&ticker) else {
                    return out;
                };
                let Some(book) = m.book.as_mut() else {
                    self.stats.dropped_invalid += 1;
                    return out;
                };
                match book.apply_delta(side, price, delta) {
                    Ok(qty) => {
                        if book.is_crossed() {
                            self.stats.crossed += 1;
                        }
                        let exch_us = match ts_ms.or(sending_ts_ms) {
                            Some(ms) => ms * 1000,
                            None => {
                                self.stats.exch_ts_fallbacks += 1;
                                recv_us
                            }
                        };
                        let (conn, market) = (self.conn, ticker);
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
                        tracing::warn!(conn = self.conn, %ticker, error = %e, "bad delta; resyncing market");
                        m.book = None;
                        let sid = m.sid.expect("a valid book has a sid");
                        self.resync(sid, vec![ticker], true, recv_us, &mut out);
                    }
                }
            }
            Decoded::Lifecycle { ticker, event, .. } => {
                let conn = self.conn;
                out.push(Action::Event(ConnEvent::Lifecycle {
                    conn,
                    market: ticker,
                    event,
                }));
            }
            Decoded::Error { id, code, msg } => {
                *self.stats.errors.entry(code).or_default() += 1;
                tracing::warn!(conn = self.conn, ?id, code, %msg, "kalshi error");
            }
            Decoded::Subscribed { channel, sid, .. } => {
                if channel == "orderbook_delta" {
                    self.ob_sid = Some(sid);
                }
            }
            Decoded::Ok { .. } | Decoded::Unsubscribed { .. } | Decoded::Other { .. } => {}
        }
        out
    }

    /// A gap on `sid`: every market on it is invalid; streaming ones get a `get_snapshot`,
    /// the rest are still waiting for their subscription snapshot anyway.
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
        self.resync(sid, tickers, false, recv_us, out);
    }

    /// Request fresh snapshots for `tickers` on `sid`. `report` emits `Invalidated` first.
    fn resync(
        &mut self,
        sid: u64,
        tickers: Vec<String>,
        report: bool,
        recv_us: i64,
        out: &mut Vec<Action>,
    ) {
        for t in &tickers {
            if report {
                out.push(Action::Event(ConnEvent::Invalidated {
                    conn: self.conn,
                    market: t.clone(),
                }));
            }
            if let Some(m) = self.markets.get_mut(t) {
                m.resync_since.get_or_insert(recv_us);
            }
        }
        for chunk in tickers.chunks(BATCH) {
            self.next_id += 1;
            self.stats.resync_requests += 1;
            out.push(Action::Send(
                json!({"id": self.next_id, "cmd": "update_subscription",
                    "params": {"sids": [sid], "market_tickers": chunk, "action": "get_snapshot"}})
                .to_string(),
            ));
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
    pub env: Env,
    pub signer: Arc<Signer>,
    pub tickers: Vec<String>,
    pub lifecycle: bool,
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
        env,
        signer,
        tickers,
        lifecycle,
    } = spec;
    let mut st = ConnState::new(conn, &tickers, lifecycle);
    let mut backoff = Duration::from_secs(1);
    while !*stop.borrow() {
        let mut ws = match connect(env, &signer).await {
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
                        // Keeps idle sockets producing pongs, so silence means dead.
                        ok &= ws.send(Message::Ping(Vec::new().into())).await.is_ok();
                    }
                }
                Some(cmd) = cmds.recv() => {
                    if matches!(cmd, ConnCmd::Drop) {
                        tracing::warn!(conn, "dropping socket (test hook)");
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

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect(env: Env, signer: &Signer) -> anyhow::Result<Ws> {
    let mut req = env.ws().into_client_request()?;
    for (name, value) in signer.headers("GET", WS_PATH) {
        req.headers_mut()
            .insert(name, HeaderValue::from_str(&value)?);
    }
    let (ws, _) = tokio_tungstenite::connect_async(req).await?;
    Ok(ws)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Side;

    fn snap(seq: u64, id: Option<u64>, yes: &str) -> String {
        let id = id.map(|i| format!(r#""id":{i},"#)).unwrap_or_default();
        format!(
            r#"{{"type":"orderbook_snapshot",{id}"sid":2,"seq":{seq},"msg":{{"market_ticker":"T","yes_dollars_fp":[{yes}]}},"sending_ts_ms":5}}"#
        )
    }
    fn delta(seq: u64, d: &str) -> String {
        format!(
            r#"{{"type":"orderbook_delta","sid":2,"seq":{seq},"msg":{{"market_ticker":"T","price_dollars":"0.4000","delta_fp":"{d}","side":"yes","ts_ms":7}}}}"#
        )
    }
    fn events(a: Vec<Action>) -> Vec<ConnEvent> {
        a.into_iter()
            .filter_map(|a| {
                if let Action::Event(e) = a {
                    Some(e)
                } else {
                    None
                }
            })
            .collect()
    }

    #[test]
    fn pending_until_subscription_snapshot_then_streams() {
        let mut s = ConnState::new(0, &["T".into()], false);
        s.subscribe_cmds();
        assert!(s.on_frame(&delta(1, "5.00"), 10).is_empty()); // pending: dropped
        assert_eq!(s.stats.dropped_invalid, 1);
        assert!(
            s.on_frame(&snap(2, Some(9), r#"["0.4000","1.00"]"#), 11)
                .is_empty()
        ); // early reply
        let e = events(s.on_frame(&snap(3, None, r#"["0.4000","1.00"]"#), 12));
        assert!(matches!(
            e[0],
            ConnEvent::Snapshot {
                cause: SnapCause::Subscribed,
                recv_us: 12,
                exch_us: 5000,
                ..
            }
        ));
        let e = events(s.on_frame(&delta(4, "2.00"), 13));
        assert_eq!(
            e,
            vec![ConnEvent::Update {
                conn: 0,
                market: "T".into(),
                side: Side::Buy,
                price: 400_000,
                qty: 3_000_000,
                recv_us: 13,
                exch_us: 7000
            }]
        );
    }

    #[test]
    fn gap_invalidates_and_requests_snapshot() {
        let mut s = ConnState::new(0, &["T".into()], false);
        s.subscribe_cmds();
        s.on_frame(&snap(1, None, r#"["0.4000","1.00"]"#), 1);
        let a = s.on_frame(&delta(3, "1.00"), 2); // seq 2 missing
        assert_eq!(s.stats.seq_gaps, 1);
        assert!(matches!(
            &a[0],
            Action::Event(ConnEvent::Invalidated { .. })
        ));
        assert!(
            matches!(&a[1], Action::Send(c) if c.contains("get_snapshot") && c.contains("\"T\""))
        );
        assert_eq!(a.len(), 2); // the delta itself is dropped
        let e = events(s.on_frame(&snap(4, Some(2), r#"["0.4000","9.00"]"#), 3));
        assert!(matches!(
            e[0],
            ConnEvent::Snapshot {
                cause: SnapCause::Resync,
                ..
            }
        ));
        assert!(!s.resync_overdue(i64::MAX));
    }

    #[test]
    fn resubscribe_deletes_resubscribes_and_waits_for_a_fresh_snapshot() {
        let mut s = ConnState::new(0, &["T".into()], false);
        s.subscribe_cmds();
        s.on_frame(
            r#"{"type":"subscribed","id":1,"msg":{"channel":"orderbook_delta","sid":2}}"#,
            0,
        );
        s.on_frame(&snap(1, None, r#"["0.4000","1.00"]"#), 1);
        let a = s.resubscribe(vec!["T".into(), "UNKNOWN".into()]);
        assert!(matches!(
            &a[0],
            Action::Event(ConnEvent::Invalidated { .. })
        ));
        assert!(
            matches!(&a[1], Action::Send(c) if c.contains("delete_markets") && c.contains("\"T\""))
        );
        assert!(
            matches!(&a[2], Action::Send(c) if c.contains("\"subscribe\"") && !c.contains("UNKNOWN"))
        );
        assert_eq!(a.len(), 3);
        // Pending again: deltas dropped until the new subscription snapshot.
        assert!(s.on_frame(&delta(2, "1.00"), 2).is_empty());
        let e = events(s.on_frame(&snap(3, None, r#"["0.4000","4.00"]"#), 3));
        assert!(matches!(
            e[0],
            ConnEvent::Snapshot {
                cause: SnapCause::Subscribed,
                ..
            }
        ));
    }

    #[test]
    fn negative_delta_invalidates_market() {
        let mut s = ConnState::new(0, &["T".into()], false);
        s.subscribe_cmds();
        s.on_frame(&snap(1, None, r#"["0.4000","1.00"]"#), 1);
        let a = s.on_frame(&delta(2, "-2.00"), 2);
        assert!(matches!(
            &a[0],
            Action::Event(ConnEvent::Invalidated { .. })
        ));
        assert!(matches!(&a[1], Action::Send(c) if c.contains("get_snapshot")));
        assert!(s.resync_overdue(2 + RESYNC_TIMEOUT_US + 1));
    }
}
