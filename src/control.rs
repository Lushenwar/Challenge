//! Control: discovery passes and lifecycle events → planner → connection tasks.
//!
//! A market is removed only by a lifecycle event (determined / settled / deactivated) or
//! when two complete discovery passes in a row miss it. A failed pass removes nothing.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use serde::Serialize;
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};

use crate::{
    conn::{self, ConnCmd, ConnStats},
    exchange::kalshi::{
        Env,
        auth::Signer,
        codec::Lifecycle,
        discovery::{self, MarketInfo},
    },
    model::{ConnEvent, ConnId, ControlMsg},
    planner::Planner,
};

/// Gap between opening new sockets: opening many at once fails handshakes.
const STAGGER: Duration = Duration::from_millis(250);
/// A removed market is not re-added by discovery for this long (a pass that started
/// before the removal can still list it).
const TOMBSTONE: Duration = Duration::from_secs(600);

pub struct Config {
    pub env: Env,
    /// Websocket URL override (a local fault proxy); `None` = the environment's.
    pub ws_url: Option<String>,
    pub signer: Arc<Signer>,
    pub markets_per_socket: usize,
    /// Sockets each market is collected on.
    pub replicas: usize,
    /// Test hook: drop socket `.0` after `.1`.
    pub drop_conn: Option<(ConnId, Duration)>,
    pub discovery_every: Duration,
    /// Cap on collected markets (testing); `None` = every live market.
    pub max_markets: Option<usize>,
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct ControlStats {
    pub discovery_passes: u64,
    pub discovery_failures: u64,
    pub added: u64,
    pub removed_discovery: u64,
    pub removed_lifecycle: u64,
    pub sockets: usize,
}

struct Control {
    cfg: Config,
    planner: Planner,
    conns: Vec<mpsc::UnboundedSender<ConnCmd>>,
    handles: Vec<JoinHandle<ConnStats>>,
    misses: HashMap<String, u32>,
    tombstones: HashMap<String, Instant>,
    events: mpsc::Sender<ConnEvent>,
    stop: watch::Receiver<bool>,
    stats: ControlStats,
}

/// Runs until `stop` flips, then waits for every connection to close and returns the
/// stats. Dropping the last event sender here lets the arbiter finish.
pub async fn run(
    cfg: Config,
    initial: Vec<MarketInfo>,
    events: mpsc::Sender<ConnEvent>,
    mut from_arbiter: mpsc::UnboundedReceiver<ControlMsg>,
    stop: watch::Receiver<bool>,
) -> (ControlStats, Vec<ConnStats>) {
    let mut c = Control {
        planner: Planner::new(cfg.markets_per_socket, cfg.replicas),
        cfg,
        conns: Vec::new(),
        handles: Vec::new(),
        misses: HashMap::new(),
        tombstones: HashMap::new(),
        events,
        stop: stop.clone(),
        stats: ControlStats::default(),
    };
    c.add(initial).await;
    let mut stop = stop;
    let mut pass: Option<JoinHandle<anyhow::Result<Vec<MarketInfo>>>> = None;
    let next = tokio::time::sleep(c.cfg.discovery_every);
    tokio::pin!(next);
    let drop_at = c.cfg.drop_conn;
    let drop_hook = tokio::time::sleep(drop_at.map_or(Duration::MAX / 4, |(_, d)| d));
    tokio::pin!(drop_hook);
    loop {
        tokio::select! {
            _ = &mut next, if pass.is_none() => {
                let (client, base) = (reqwest::Client::new(), c.cfg.env.rest());
                pass = Some(tokio::spawn(async move { discovery::discover(&client, base).await }));
            }
            res = async { pass.as_mut().expect("guarded").await }, if pass.is_some() => {
                pass = None;
                match res.map_err(anyhow::Error::from).and_then(|r| r) {
                    Ok(markets) => c.reconcile(markets).await,
                    Err(e) => {
                        c.stats.discovery_failures += 1;
                        tracing::warn!(error = %format!("{e:#}"), "discovery pass failed; nothing removed");
                    }
                }
                next.as_mut().reset(tokio::time::Instant::now() + c.cfg.discovery_every);
            }
            Some(msg) = from_arbiter.recv() => match msg {
                ControlMsg::Resubscribe { conn, market } => {
                    // Only if it is still collected (it may have been removed since).
                    if let Some(tx) = c.conns.get(conn).filter(|_| c.planner.contains(&market)) {
                        let _ = tx.send(ConnCmd::Resubscribe(vec![market]));
                    }
                }
                ControlMsg::Recycle(conn) => {
                    if let Some(tx) = c.conns.get(conn) {
                        let _ = tx.send(ConnCmd::Drop);
                    }
                }
                ControlMsg::Lifecycle(market, event) => {
                    if matches!(event, Lifecycle::Activated | Lifecycle::Reactivated) {
                        c.tombstones.remove(&market); // discovery may add it again
                    }
                    if matches!(event, Lifecycle::Determined | Lifecycle::Settled | Lifecycle::Deactivated)
                        && c.planner.contains(&market)
                    {
                        tracing::info!(%market, ?event, "market removed (lifecycle)");
                        c.stats.removed_lifecycle += 1;
                        c.remove(&[market]);
                    }
                }
            },
            _ = &mut drop_hook, if drop_at.is_some() => {
                let (conn, _) = drop_at.expect("guarded");
                if let Some(tx) = c.conns.get(conn) {
                    let _ = tx.send(ConnCmd::Drop);
                }
                drop_hook.as_mut().reset(tokio::time::Instant::now() + Duration::MAX / 4);
            }
            _ = stop.changed() => break,
        }
    }
    if let Some(p) = pass {
        p.abort();
    }
    c.stats.sockets = c.planner.sockets();
    let mut conn_stats = Vec::new();
    for h in c.handles {
        conn_stats.push(h.await.unwrap_or_default());
    }
    (c.stats, conn_stats)
}

impl Control {
    async fn reconcile(&mut self, markets: Vec<MarketInfo>) {
        self.stats.discovery_passes += 1;
        let live: HashSet<String> = markets.iter().map(|m| m.ticker.clone()).collect();
        let mut gone = Vec::new();
        for m in self.planner.markets() {
            if live.contains(m) {
                self.misses.remove(m);
            } else {
                let n = self.misses.entry(m.clone()).or_default();
                *n += 1;
                if *n >= 2 {
                    gone.push(m.clone());
                }
            }
        }
        if !gone.is_empty() {
            gone.sort();
            tracing::info!(count = gone.len(), first = %gone[0], "markets removed (missing from 2 passes)");
            self.stats.removed_discovery += gone.len() as u64;
            self.remove(&gone);
        }
        self.tombstones.retain(|_, t| t.elapsed() < TOMBSTONE);
        let new: Vec<MarketInfo> = markets
            .into_iter()
            .filter(|m| {
                !self.planner.contains(&m.ticker) && !self.tombstones.contains_key(&m.ticker)
            })
            .collect();
        self.add(new).await;
    }

    async fn add(&mut self, mut markets: Vec<MarketInfo>) {
        if let Some(cap) = self.cfg.max_markets {
            let room = cap.saturating_sub(self.planner.len());
            markets.sort_by_key(|m| std::cmp::Reverse(m.volume_24h));
            markets.truncate(room);
        }
        if markets.is_empty() {
            return;
        }
        let first = markets[0].ticker.clone();
        let n = markets.len();
        for (conn, tickers) in self.planner.add(markets.into_iter().map(|m| m.ticker)) {
            if conn == self.conns.len() {
                self.spawn(conn, tickers).await;
            } else {
                let _ = self.conns[conn].send(ConnCmd::Add(tickers));
            }
        }
        self.stats.added += n as u64;
        tracing::info!(count = n, %first, sockets = self.planner.sockets(), "markets added");
    }

    fn remove(&mut self, markets: &[String]) {
        for m in markets {
            self.misses.remove(m);
            self.tombstones.insert(m.clone(), Instant::now());
        }
        for (conn, tickers) in self.planner.remove(markets) {
            let _ = self.conns[conn].send(ConnCmd::Remove(tickers));
        }
    }

    async fn spawn(&mut self, conn: ConnId, tickers: Vec<String>) {
        if conn > 0 {
            tokio::time::sleep(STAGGER).await;
        }
        let (tx, rx) = mpsc::unbounded_channel();
        let spec = conn::Spec {
            conn,
            ws_url: self
                .cfg
                .ws_url
                .clone()
                .unwrap_or_else(|| self.cfg.env.ws().to_owned()),
            signer: self.cfg.signer.clone(),
            tickers,
            // Two connections carry the (market-global) lifecycle feed, for redundancy.
            lifecycle: conn < 2,
        };
        self.handles.push(tokio::spawn(conn::run(
            spec,
            rx,
            self.events.clone(),
            self.stop.clone(),
        )));
        self.conns.push(tx);
    }
}
