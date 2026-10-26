//! A local Kalshi-shaped exchange for end-to-end tests: websocket (subscribe / merge /
//! `ok`, `update_subscription` add/delete/get_snapshot, per-sid seq, lifecycle channel)
//! plus REST `/markets`. It keeps the true book of every market and the full history of
//! level changes, in output terms (YES-quoted, absolute quantities), so a capture can be
//! checked against what really happened.
//!
//! One task owns all state; connections and the test talk to it through `Cmd`s.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use engineering_challenge::Side;
use futures_util::{SinkExt, StreamExt};
use rand::{Rng, SeedableRng, rngs::StdRng};
use serde_json::{Value, json};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot, watch},
};
use tokio_tungstenite::tungstenite::Message;

pub const SCALE: i64 = 1_000_000;

/// One level change, in the collector's output terms.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    pub ts_ms: i64,
    pub market: String,
    pub side: Side,
    pub price: i64,
    /// New absolute quantity (0 = level removed).
    pub qty: i64,
}

pub enum Fault {
    /// Close accepted connection `n` (accept order, from 0).
    Drop(usize),
    /// Close every connection.
    DropAll,
    /// Refuse new websocket handshakes for this long.
    Refuse(Duration),
    /// Stop sending `market`'s deltas on connection `n`.
    Silence(usize, String),
    /// The next order-book frame on connection `n` skips one seq number.
    SkipSeq(usize),
    /// Hold every later frame on connection `n` back by this long.
    Delay(usize, Duration),
    /// List a new market (REST) and announce it (lifecycle `created`).
    Add(String),
    /// Close a market: lifecycle `determined`, gone from REST, no more events.
    Remove(String),
    /// Drop a market from REST only (no lifecycle event), and stop its events.
    Delist(String),
    /// Stop / restart generating events.
    Pause(bool),
}

enum Cmd {
    Connect(
        mpsc::UnboundedSender<(Instant, Message)>,
        oneshot::Sender<usize>,
    ),
    Frame(usize, String),
    Closed(usize),
    Fault(Fault),
    History(oneshot::Sender<Vec<Event>>),
    Stats(oneshot::Sender<Value>),
}

pub struct Fake {
    pub rest_url: String,
    pub ws_url: String,
    pub markets: Vec<String>,
    cmds: mpsc::UnboundedSender<Cmd>,
}

impl Fake {
    /// Start on random local ports: `markets` markets, ~`rate` level changes per second.
    pub async fn start(markets: usize, rate: u64, seed: u64) -> Self {
        let names: Vec<String> = (0..markets).map(|i| format!("FAKE-{i:05}")).collect();
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (listing_tx, listing_rx) = watch::channel(names.clone());
        let (refuse_tx, refuse_rx) = watch::channel(None::<Instant>);
        let core = Core::new(&names, seed, listing_tx, refuse_tx);
        tokio::spawn(core.run(cmd_rx, rate));

        let rest = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let rest_url = format!("http://{}", rest.local_addr().unwrap());
        std::thread::spawn(move || serve_rest(rest, listing_rx));
        let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_url = format!("ws://{}/trade-api/ws/v2", ws.local_addr().unwrap());
        tokio::spawn(serve_ws(ws, cmd_tx.clone(), refuse_rx));
        Self {
            rest_url,
            ws_url,
            markets: names,
            cmds: cmd_tx,
        }
    }

    pub fn fault(&self, f: Fault) {
        let _ = self.cmds.send(Cmd::Fault(f));
    }

    pub async fn history(&self) -> Vec<Event> {
        let (tx, rx) = oneshot::channel();
        let _ = self.cmds.send(Cmd::History(tx));
        rx.await.unwrap()
    }

    /// Commands seen, connections accepted, etc.
    pub async fn stats(&self) -> Value {
        let (tx, rx) = oneshot::channel();
        let _ = self.cmds.send(Cmd::Stats(tx));
        rx.await.unwrap()
    }
}

/// Kalshi's two sides, each price in cents → contracts.
#[derive(Default)]
struct Book {
    yes: BTreeMap<i64, i64>,
    no: BTreeMap<i64, i64>,
}

struct Conn {
    tx: mpsc::UnboundedSender<(Instant, Message)>,
    ob_sid: Option<u64>,
    life_sid: Option<u64>,
    next_sid: u64,
    seq: HashMap<u64, u64>,
    markets: BTreeSet<String>,
    silenced: HashSet<String>,
    skip_next: bool,
    delay: Duration,
}

struct Core {
    books: BTreeMap<String, Book>,
    live: BTreeSet<String>,
    history: Vec<Event>,
    conns: BTreeMap<usize, Conn>,
    accepted: usize,
    last_ts: i64,
    rng: StdRng,
    paused: bool,
    listing: watch::Sender<Vec<String>>,
    refuse: watch::Sender<Option<Instant>>,
    commands: BTreeMap<String, u64>,
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Output mapping (must match the collector's): YES c → bid; NO c → ask at 1 − c.
fn out_level(side: &str, cents: i64) -> (Side, i64) {
    match side {
        "yes" => (Side::Buy, cents * 10_000),
        _ => (Side::Sell, SCALE - cents * 10_000),
    }
}

fn levels(side: &BTreeMap<i64, i64>) -> Value {
    side.iter()
        .map(|(c, q)| json!([format!("0.{c:02}00"), format!("{q}.00")]))
        .collect()
}

impl Core {
    fn new(
        names: &[String],
        seed: u64,
        listing: watch::Sender<Vec<String>>,
        refuse: watch::Sender<Option<Instant>>,
    ) -> Self {
        let mut c = Self {
            books: BTreeMap::new(),
            live: BTreeSet::new(),
            history: Vec::new(),
            conns: BTreeMap::new(),
            accepted: 0,
            last_ts: now_ms(),
            rng: StdRng::seed_from_u64(seed),
            paused: false,
            listing,
            refuse,
            commands: BTreeMap::new(),
        };
        for n in names {
            c.list(n.clone());
        }
        c
    }

    fn ts(&mut self) -> i64 {
        self.last_ts = now_ms().max(self.last_ts + 1);
        self.last_ts
    }

    /// A new market with a few random levels (recorded in the history).
    fn list(&mut self, market: String) {
        let ts = self.ts();
        let mut book = Book::default();
        for _ in 0..4 {
            let c = self.rng.gen_range(1..50);
            let q = self.rng.gen_range(1..300);
            book.yes.insert(c, q);
            let c = self.rng.gen_range(1..50);
            book.no.insert(c, q + 1);
        }
        for (side, map) in [("yes", &book.yes), ("no", &book.no)] {
            for (&c, &q) in map {
                let (s, p) = out_level(side, c);
                self.history.push(Event {
                    ts_ms: ts,
                    market: market.clone(),
                    side: s,
                    price: p,
                    qty: q * SCALE,
                });
            }
        }
        self.books.insert(market.clone(), book);
        self.live.insert(market);
        self.listing
            .send_replace(self.live.iter().cloned().collect());
    }

    async fn run(mut self, mut cmds: mpsc::UnboundedReceiver<Cmd>, rate: u64) {
        let mut tick = tokio::time::interval(Duration::from_millis(5));
        let mut owed = 0u64; // events per tick, in thousandths
        loop {
            tokio::select! {
                _ = tick.tick() => {
                    owed += rate * 5;
                    while owed >= 1000 {
                        owed -= 1000;
                        if !self.paused {
                            self.event();
                        }
                    }
                }
                cmd = cmds.recv() => match cmd {
                    None => return,
                    Some(Cmd::Connect(tx, reply)) => {
                        let id = self.accepted;
                        self.accepted += 1;
                        self.conns.insert(id, Conn {
                            tx, ob_sid: None, life_sid: None, next_sid: 1, seq: HashMap::new(),
                            markets: BTreeSet::new(), silenced: HashSet::new(), skip_next: false,
                            delay: Duration::ZERO,
                        });
                        let _ = reply.send(id);
                    }
                    Some(Cmd::Frame(id, text)) => self.command(id, &text),
                    Some(Cmd::Closed(id)) => { self.conns.remove(&id); }
                    Some(Cmd::Fault(f)) => self.fault(f),
                    Some(Cmd::History(tx)) => { let _ = tx.send(self.history.clone()); }
                    Some(Cmd::Stats(tx)) => {
                        let _ = tx.send(json!({"accepted": self.accepted, "open": self.conns.len(),
                            "commands": self.commands, "events": self.history.len()}));
                    }
                },
            }
        }
    }

    fn fault(&mut self, f: Fault) {
        match f {
            Fault::Drop(n) => {
                self.conns.remove(&n);
            }
            Fault::DropAll => self.conns.clear(),
            Fault::Refuse(d) => {
                self.refuse.send_replace(Some(Instant::now() + d));
            }
            Fault::Silence(n, m) => {
                if let Some(c) = self.conns.get_mut(&n) {
                    c.silenced.insert(m);
                }
            }
            Fault::SkipSeq(n) => {
                if let Some(c) = self.conns.get_mut(&n) {
                    c.skip_next = true;
                }
            }
            Fault::Delay(n, d) => {
                if let Some(c) = self.conns.get_mut(&n) {
                    c.delay = d;
                }
            }
            Fault::Add(m) => {
                self.list(m.clone());
                self.lifecycle(&m, "created");
            }
            Fault::Remove(m) => {
                self.live.remove(&m);
                self.listing
                    .send_replace(self.live.iter().cloned().collect());
                self.lifecycle(&m, "determined");
            }
            Fault::Delist(m) => {
                self.live.remove(&m);
                self.listing
                    .send_replace(self.live.iter().cloned().collect());
            }
            Fault::Pause(p) => self.paused = p,
        }
    }

    /// One random level change on a random live market, sent to its subscribers.
    fn event(&mut self) {
        if self.live.is_empty() {
            return;
        }
        let i = self.rng.gen_range(0..self.live.len());
        let market = self.live.iter().nth(i).unwrap().clone();
        let side = if self.rng.gen_bool(0.5) { "yes" } else { "no" };
        let cents = self.rng.gen_range(1..50);
        let book = self.books.get_mut(&market).unwrap();
        let map = if side == "yes" {
            &mut book.yes
        } else {
            &mut book.no
        };
        let old = map.get(&cents).copied().unwrap_or(0);
        let new = if old > 0 && self.rng.gen_bool(0.3) {
            0
        } else {
            self.rng.gen_range(1..500)
        };
        if new == old {
            return;
        }
        if new == 0 {
            map.remove(&cents);
        } else {
            map.insert(cents, new);
        }
        let ts = self.ts();
        let (s, p) = out_level(side, cents);
        self.history.push(Event {
            ts_ms: ts,
            market: market.clone(),
            side: s,
            price: p,
            qty: new * SCALE,
        });
        let delta = new - old;
        for c in self.conns.values_mut() {
            if c.markets.contains(&market) && !c.silenced.contains(&market) {
                let sid = c.ob_sid.unwrap();
                let msg = json!({"market_ticker": market, "price_dollars": format!("0.{cents:02}00"),
                    "delta_fp": format!("{delta}.00"), "side": side, "ts_ms": ts});
                send_seq(
                    c,
                    sid,
                    json!({"type": "orderbook_delta", "sid": sid, "sending_ts_ms": ts, "msg": msg}),
                );
            }
        }
    }

    fn lifecycle(&mut self, market: &str, event: &str) {
        for c in self.conns.values_mut() {
            if let Some(sid) = c.life_sid {
                send_seq(
                    c,
                    sid,
                    json!({"type": "market_lifecycle_v2", "sid": sid,
                    "msg": {"market_ticker": market, "event_type": event}}),
                );
            }
        }
    }

    fn snapshot(&self, market: &str, id: Option<u64>) -> Value {
        let book = &self.books[market];
        let mut msg = json!({"market_ticker": market, "market_id": market});
        if !book.yes.is_empty() {
            msg["yes_dollars_fp"] = levels(&book.yes);
        }
        if !book.no.is_empty() {
            msg["no_dollars_fp"] = levels(&book.no);
        }
        let mut v =
            json!({"type": "orderbook_snapshot", "sending_ts_ms": self.last_ts, "msg": msg});
        if let Some(id) = id {
            v["id"] = json!(id);
        }
        v
    }

    fn command(&mut self, id: usize, text: &str) {
        let Ok(v) = serde_json::from_str::<Value>(text) else {
            return;
        };
        let cmd_id = v["id"].as_u64();
        let p = &v["params"];
        let tickers: Vec<String> = p["market_tickers"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|t| t.as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default();
        let name = match v["cmd"].as_str() {
            Some("update_subscription") => {
                format!("update_subscription:{}", p["action"].as_str().unwrap_or(""))
            }
            Some(c) => c.to_owned(),
            None => return,
        };
        *self.commands.entry(name.clone()).or_default() += 1;
        let snaps: Vec<(String, Value)> = tickers
            .iter()
            .filter(|t| self.books.contains_key(*t))
            .map(|t| (t.clone(), self.snapshot(t, None)))
            .collect();
        let Some(c) = self.conns.get_mut(&id) else {
            return;
        };
        let channels: Vec<&str> = p["channels"]
            .as_array()
            .map(|a| a.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        match name.as_str() {
            "subscribe" if channels.contains(&"market_lifecycle_v2") => {
                let sid = c.next_sid;
                c.next_sid += 1;
                c.life_sid = Some(sid);
                send(
                    c,
                    json!({"type": "subscribed", "id": cmd_id, "msg": {"channel": "market_lifecycle_v2", "sid": sid}}),
                );
            }
            "subscribe" | "update_subscription:add_markets" => {
                let sid = match c.ob_sid {
                    Some(sid) => sid,
                    None => {
                        let sid = c.next_sid;
                        c.next_sid += 1;
                        c.ob_sid = Some(sid);
                        send(
                            c,
                            json!({"type": "subscribed", "id": cmd_id, "msg": {"channel": "orderbook_delta", "sid": sid}}),
                        );
                        sid
                    }
                };
                if !c.markets.is_empty() || name != "subscribe" {
                    // Merged into the existing subscription: an `ok` in the sid's sequence.
                    c.markets.extend(tickers.iter().cloned());
                    let all: Vec<&String> = c.markets.iter().collect();
                    send_seq(
                        c,
                        sid,
                        json!({"type": "ok", "id": cmd_id, "sid": sid, "msg": {"market_tickers": all}}),
                    );
                }
                c.markets.extend(tickers.iter().cloned());
                for (_, mut s) in snaps {
                    s["sid"] = json!(sid);
                    send_seq(c, sid, s);
                }
            }
            "update_subscription:delete_markets" => {
                for t in &tickers {
                    c.markets.remove(t);
                }
                if let Some(sid) = c.ob_sid {
                    send_seq(
                        c,
                        sid,
                        json!({"type": "ok", "id": cmd_id, "sid": sid, "msg": {}}),
                    );
                }
            }
            "update_subscription:get_snapshot" => {
                let Some(sid) = c.ob_sid else { return };
                for (t, mut s) in snaps {
                    if c.markets.contains(&t) {
                        s["sid"] = json!(sid);
                        s["id"] = json!(cmd_id);
                        send_seq(c, sid, s);
                    }
                }
            }
            _ => {}
        }
    }
}

fn send(c: &mut Conn, v: Value) {
    let _ = c.tx.send((
        Instant::now() + c.delay,
        Message::Text(v.to_string().into()),
    ));
}

/// Stamp the next seq on `sid` (skipping one if a fault asked for it) and send.
fn send_seq(c: &mut Conn, sid: u64, mut v: Value) {
    let seq = c.seq.entry(sid).or_insert(0);
    *seq += 1;
    if c.skip_next && Some(sid) == c.ob_sid {
        c.skip_next = false;
        *seq += 1;
    }
    v["seq"] = json!(*seq);
    send(c, v);
}

async fn serve_ws(
    listener: TcpListener,
    core: mpsc::UnboundedSender<Cmd>,
    refuse: watch::Receiver<Option<Instant>>,
) {
    while let Ok((tcp, _)) = listener.accept().await {
        if refuse.borrow().is_some_and(|until| Instant::now() < until) {
            drop(tcp);
            continue;
        }
        let core = core.clone();
        tokio::spawn(async move {
            let Ok(ws) = tokio_tungstenite::accept_async(tcp).await else {
                return;
            };
            let (mut sink, mut stream) = ws.split();
            let (tx, mut rx) = mpsc::unbounded_channel::<(Instant, Message)>();
            let (id_tx, id_rx) = oneshot::channel();
            let _ = core.send(Cmd::Connect(tx, id_tx));
            let Ok(id) = id_rx.await else { return };
            let writer = tokio::spawn(async move {
                // Ends when the core drops the sender (a Drop fault) or the socket fails.
                while let Some((at, msg)) = rx.recv().await {
                    tokio::time::sleep_until(at.into()).await;
                    if sink.send(msg).await.is_err() {
                        return;
                    }
                }
                let _ = sink.close().await;
            });
            loop {
                tokio::select! {
                    msg = stream.next() => match msg {
                        Some(Ok(Message::Text(t))) => { let _ = core.send(Cmd::Frame(id, t.to_string())); }
                        Some(Ok(_)) => {}
                        _ => break,
                    },
                    _ = async { while !writer.is_finished() { tokio::time::sleep(Duration::from_millis(20)).await } } => break,
                }
            }
            writer.abort();
            let _ = core.send(Cmd::Closed(id));
        });
    }
}

/// `GET /markets...` → one page listing every live market.
fn serve_rest(listener: std::net::TcpListener, listing: watch::Receiver<Vec<String>>) {
    use std::io::{Read, Write};
    for mut tcp in listener.incoming().flatten() {
        let mut buf = vec![0u8; 8192];
        let mut got = 0;
        while !buf[..got].windows(4).any(|w| w == b"\r\n\r\n") {
            match tcp.read(&mut buf[got..]) {
                Ok(0) | Err(_) => break,
                Ok(n) => got += n,
            }
        }
        let markets: Vec<Value> = listing
            .borrow()
            .iter()
            .map(|t| json!({"ticker": t, "status": "active", "volume_24h_fp": "100.00"}))
            .collect();
        let body = json!({"markets": markets, "cursor": ""}).to_string();
        let _ = write!(
            tcp,
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
    }
}
