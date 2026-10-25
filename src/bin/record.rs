//! Raw frame recorder: one JSON line per websocket frame, ground truth for replay tests.
//!
//! Lines: `{"conn":0,"recv_us":…,"frame":"<raw text>"}` for data frames,
//! `{"conn":0,"recv_us":…,"ctrl":"ping|pong|close|connected"}` for control events and
//! `{"conn":0,"sent_us":…,"cmd":"<raw text>"}` for commands we sent.

use std::{
    io::Write,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use clap::Parser;
use engineering_challenge::{
    conn,
    exchange::{self, Decoded, Exchange},
};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::{Message, handshake::client::Request};

#[derive(Parser)]
struct Args {
    #[command(flatten)]
    venue: exchange::VenueArgs,
    /// Record the N most active markets (by 24h volume).
    #[arg(long, default_value_t = 50)]
    markets: usize,
    /// Explicit tickers instead of the top-N pick.
    #[arg(long, value_delimiter = ',')]
    tickers: Vec<String>,
    /// Parallel connections, each subscribing the same markets.
    #[arg(long, default_value_t = 1)]
    conns: usize,
    #[arg(long, default_value_t = 30)]
    minutes: u64,
    /// Force one reconnect of every connection after this many seconds.
    #[arg(long)]
    reconnect_after: Option<u64>,
    /// Request fresh snapshots of every market this often (seconds), where the venue
    /// supports it; replay tests compare them with books rebuilt from updates.
    #[arg(long)]
    snapshot_every: Option<u64>,
    /// Latency lab: connection i uses `ws_urls[i % len]` (e.g. two hostnames of one
    /// venue); default the venue's URL.
    #[arg(long, value_delimiter = ',')]
    ws_urls: Vec<String>,
    /// Latency lab: connect connection i to `ips[i % len]` instead of DNS (TLS still
    /// uses the hostname).
    #[arg(long, value_delimiter = ',')]
    ips: Vec<IpAddr>,
    /// Latency lab: set TCP_NODELAY on these connections.
    #[arg(long, value_delimiter = ',')]
    nodelay_conns: Vec<usize>,
    #[arg(long)]
    out: PathBuf,
}

fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before 1970")
        .as_micros() as u64
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();
    let exchange = args.venue.build()?;

    let tickers = if args.tickers.is_empty() {
        let mut all = exchange.discover(&reqwest::Client::new()).await?;
        tracing::info!(markets = all.len(), "discovered");
        all.sort_by_key(|m| std::cmp::Reverse(m.volume_24h));
        all.into_iter()
            .take(args.markets)
            .map(|m| m.ticker)
            .collect()
    } else {
        args.tickers.clone()
    };
    tracing::info!(count = tickers.len(), first = ?tickers.first(), "recording");

    let (line_tx, mut line_rx) = mpsc::unbounded_channel::<String>();
    let mut out = std::io::BufWriter::new(
        std::fs::File::create(&args.out)
            .with_context(|| format!("creating {}", args.out.display()))?,
    );
    let writer = tokio::task::spawn_blocking(move || -> anyhow::Result<u64> {
        let mut n = 0;
        while let Some(line) = line_rx.blocking_recv() {
            writeln!(out, "{line}")?;
            n += 1;
            if n % 1000 == 0 {
                out.flush()?;
            }
        }
        out.flush()?;
        Ok(n)
    });

    let (stop_tx, stop_rx) = watch::channel(false);
    let mut tasks = Vec::new();
    for conn in 0..args.conns {
        let ip = (!args.ips.is_empty()).then(|| args.ips[conn % args.ips.len()]);
        let nodelay = args.nodelay_conns.contains(&conn);
        let url =
            (!args.ws_urls.is_empty()).then(|| args.ws_urls[conn % args.ws_urls.len()].clone());
        tasks.push(tokio::spawn(run_conn(
            conn,
            ip,
            nodelay,
            url,
            exchange.clone(),
            tickers.clone(),
            args.reconnect_after.map(Duration::from_secs),
            args.snapshot_every.map(Duration::from_secs),
            line_tx.clone(),
            stop_rx.clone(),
        )));
    }
    drop(line_tx);

    tokio::select! {
        _ = tokio::signal::ctrl_c() => tracing::info!("SIGINT, stopping"),
        _ = tokio::time::sleep(Duration::from_secs(args.minutes * 60)) => tracing::info!("duration reached"),
    }
    stop_tx.send(true)?;
    for t in tasks {
        if let Err(e) = t.await? {
            tracing::error!(error = %e, "connection task failed");
        }
    }
    let lines = writer.await??;
    tracing::info!(lines, out = %args.out.display(), "done");
    Ok(())
}

/// TCP to `ip` (or the first DNS answer), optional TCP_NODELAY, then TLS + websocket.
async fn open(
    req: Request,
    ip: Option<IpAddr>,
    nodelay: bool,
) -> anyhow::Result<(conn::Ws, SocketAddr)> {
    let host = req
        .uri()
        .host()
        .context("websocket URL has no host")?
        .to_owned();
    let port = req.uri().port_u16().unwrap_or(443);
    let addr = match ip {
        Some(ip) => SocketAddr::new(ip, port),
        None => tokio::net::lookup_host((host.as_str(), port))
            .await?
            .next()
            .context("no address")?,
    };
    let tcp = tokio::net::TcpStream::connect(addr).await?;
    tcp.set_nodelay(nodelay)?;
    let (ws, _) = tokio_tungstenite::client_async_tls(req, tcp).await?;
    Ok((ws, addr))
}

#[allow(clippy::too_many_arguments)]
async fn run_conn(
    conn: usize,
    ip: Option<IpAddr>,
    nodelay: bool,
    url: Option<String>,
    exchange: Arc<dyn Exchange>,
    tickers: Vec<String>,
    reconnect_after: Option<Duration>,
    snapshot_every: Option<Duration>,
    lines: mpsc::UnboundedSender<String>,
    mut stop: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut forced_reconnect_done = reconnect_after.is_none();
    let mut backoff = Duration::from_secs(1);
    let started = tokio::time::Instant::now();
    // Lifecycle (venue-wide) on conn 0 only: it is large and the same on every socket.
    let mut session = exchange.session(conn == 0);
    while !*stop.borrow() {
        let (mut ws, peer) = match open(exchange.ws_request(url.as_deref())?, ip, nodelay).await {
            Ok(x) => x,
            Err(e) => {
                tracing::warn!(conn, error = %e, ?backoff, "connect failed");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
                continue;
            }
        };
        backoff = Duration::from_secs(1);
        let peer = peer.to_string();
        let _ = lines.send(
            json!({"conn": conn, "recv_us": now_us(), "ctrl": "connected", "peer": peer, "nodelay": nodelay})
                .to_string(),
        );
        tracing::info!(conn, %peer, nodelay, "connected");
        for text in session.on_connect(&tickers) {
            let _ = lines.send(json!({"conn": conn, "sent_us": now_us(), "cmd": text}).to_string());
            ws.send(Message::text(text)).await?;
        }

        let mut sid = None;
        let mut snap_tick = tokio::time::interval(snapshot_every.unwrap_or(Duration::MAX / 4));
        snap_tick.tick().await; // the first tick is immediate
        let mut keepalive = tokio::time::interval(Duration::from_secs(10));
        let reconnect_at = match reconnect_after {
            Some(d) if !forced_reconnect_done => started + d,
            _ => started + Duration::from_secs(u64::MAX / 4),
        };
        loop {
            tokio::select! {
                msg = ws.next() => {
                    let recv_us = now_us();
                    let record = match msg {
                        Some(Ok(Message::Text(t))) => {
                            if sid.is_none() {
                                sid = session.decode(t.as_str()).ok().and_then(|ds| {
                                    ds.iter().find_map(|d| match d {
                                        Decoded::Snapshot { sid_seq, .. } => sid_seq.map(|(s, _)| s),
                                        _ => None,
                                    })
                                });
                            }
                            json!({"conn": conn, "recv_us": recv_us, "frame": t.as_str()})
                        }
                        Some(Ok(Message::Ping(_))) => json!({"conn": conn, "recv_us": recv_us, "ctrl": "ping"}),
                        Some(Ok(Message::Pong(_))) => json!({"conn": conn, "recv_us": recv_us, "ctrl": "pong"}),
                        Some(Ok(Message::Close(c))) => {
                            let _ = lines.send(json!({"conn": conn, "recv_us": recv_us, "ctrl": "close", "reason": format!("{c:?}")}).to_string());
                            tracing::warn!(conn, ?c, "server closed");
                            break;
                        }
                        Some(Ok(_)) => continue,
                        Some(Err(e)) => { tracing::warn!(conn, error = %e, "read error"); break; }
                        None => { tracing::warn!(conn, "stream ended"); break; }
                    };
                    let _ = lines.send(record.to_string());
                }
                _ = snap_tick.tick(), if sid.is_some() => {
                    let cmds = session.request_snapshots(sid.expect("guarded"), &tickers).unwrap_or_default();
                    for text in cmds {
                        let _ = lines.send(json!({"conn": conn, "sent_us": now_us(), "cmd": text}).to_string());
                        ws.send(Message::text(text)).await?;
                    }
                }
                _ = keepalive.tick() => ws.send(session.keepalive()).await?,
                _ = tokio::time::sleep_until(reconnect_at) => {
                    tracing::info!(conn, "forced reconnect");
                    forced_reconnect_done = true;
                    let _ = ws.close(None).await;
                    break;
                }
                _ = stop.changed() => {
                    let _ = ws.close(None).await;
                    return Ok(());
                }
            }
        }
    }
    Ok(())
}
