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
    exchange::{Exchange, Lifecycle, MarketInfo},
    model::{ConnEvent, ConnId, ControlMsg},
    planner::Planner,
};

/// Gap between opening new sockets: opening many at once fails handshakes.
const STAGGER: Duration = Duration::from_millis(250);
/// A removed market is not re-added by discovery for this long (a pass that started
/// before the removal can still list it).
const TOMBSTONE: Duration = Duration::from_secs(600);

pub struct Config {
    pub exchange: Arc<dyn Exchange>,
    /// Websocket URL override (a local fault proxy); `None` = the venue's.
    pub ws_url: Option<String>,
    pub markets_per_socket: usize,
    /// Sockets each market is collected on.
    pub replicas: usize,
    /// Test hook: drop socket `.0` after `.1`.
    pub drop_conn: Option<(ConnId, Duration)>,
    pub discovery_every: Duration,
    /// Cap on collected markets (testing); `None` = every live market.
    pub max_markets: Option<usize>,
    pub net: crate::net::NetOpts,
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
                let exchange = c.cfg.exchange.clone();
                pass = Some(tokio::spawn(async move {
                    exchange.discover(&reqwest::Client::new()).await
                }));
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

/// One pass of the removal rule. `live`: every live market, if this pass knows it
/// (`None`: a capped or failed listing, which counts nothing). A collected market missing
/// from two such passes in a row is returned (sorted); seeing it again resets its count.
fn count_misses<'a>(
    misses: &mut HashMap<String, u32>,
    collected: impl Iterator<Item = &'a String>,
    live: Option<&HashSet<String>>,
) -> Vec<String> {
    let Some(live) = live else { return Vec::new() };
    let mut gone = Vec::new();
    for m in collected {
        if live.contains(m) {
            misses.remove(m);
        } else {
            let n = misses.entry(m.clone()).or_default();
            *n += 1;
            if *n >= 2 {
                gone.push(m.clone());
            }
        }
    }
    gone.sort();
    gone
}

/// Discovered markets to start collecting: not collected yet and not recently removed.
fn addable(
    markets: Vec<MarketInfo>,
    collected: impl Fn(&str) -> bool,
    tombstones: &HashMap<String, Instant>,
) -> Vec<MarketInfo> {
    markets
        .into_iter()
        .filter(|m| !collected(&m.ticker) && !tombstones.contains_key(&m.ticker))
        .collect()
}

impl Control {
    async fn reconcile(&mut self, markets: Vec<MarketInfo>) {
        self.stats.discovery_passes += 1;
        // The set removals are judged against: this pass if it lists every live market,
        // else a finished complete listing, if the venue has one.
        let live: Option<HashSet<String>> = if self.cfg.exchange.discovery_is_complete() {
            Some(markets.iter().map(|m| m.ticker.clone()).collect())
        } else {
            self.cfg.exchange.live_markets(&reqwest::Client::new())
        };
        let gone = count_misses(&mut self.misses, self.planner.markets(), live.as_ref());
        if !gone.is_empty() {
            tracing::info!(count = gone.len(), first = %gone[0], "markets removed (missing from 2 passes)");
            self.stats.removed_discovery += gone.len() as u64;
            self.remove(&gone);
        }
        self.tombstones.retain(|_, t| t.elapsed() < TOMBSTONE);
        let new = addable(markets, |t| self.planner.contains(t), &self.tombstones);
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
            exchange: self.cfg.exchange.clone(),
            ws_url: self.cfg.ws_url.clone(),
            tickers,
            // Two connections carry the (market-global) lifecycle feed, for redundancy.
            lifecycle: conn < 2,
            net: self.cfg.net.clone(),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn set(xs: &[&str]) -> HashSet<String> {
        xs.iter().map(|x| x.to_string()).collect()
    }

    fn names(xs: &[&str]) -> Vec<String> {
        xs.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn removed_only_after_two_complete_passes_in_a_row_miss_it() {
        let collected = names(&["A", "B", "C"]);
        let mut misses = HashMap::new();
        assert!(count_misses(&mut misses, collected.iter(), Some(&set(&["A"]))).is_empty());
        // B is back, C is missing a second time.
        assert_eq!(
            count_misses(&mut misses, collected.iter(), Some(&set(&["A", "B"]))),
            ["C"]
        );
        // B's earlier miss was forgotten when it reappeared: one more miss is not enough.
        assert!(
            count_misses(&mut misses, names(&["A", "B"]).iter(), Some(&set(&["A"]))).is_empty()
        );
    }

    #[test]
    fn a_failed_or_capped_listing_counts_nothing() {
        let collected = names(&["A"]);
        let mut misses = HashMap::new();
        assert!(count_misses(&mut misses, collected.iter(), Some(&set(&[]))).is_empty());
        for _ in 0..5 {
            assert!(count_misses(&mut misses, collected.iter(), None).is_empty());
        }
        assert_eq!(
            misses["A"], 1,
            "passes without a complete listing do not count"
        );
        assert_eq!(
            count_misses(&mut misses, collected.iter(), Some(&set(&[]))),
            ["A"]
        );
    }

    #[test]
    fn recently_removed_and_collected_markets_are_not_added() {
        let m = |t: &str| MarketInfo {
            ticker: t.into(),
            volume_24h: 0,
        };
        let tomb = HashMap::from([("GONE".to_owned(), Instant::now())]);
        let new = addable(vec![m("NEW"), m("HAVE"), m("GONE")], |t| t == "HAVE", &tomb);
        assert_eq!(new, vec![m("NEW")]);
    }
}
