//! Raw Kalshi frame recorder: one JSON line per websocket frame, ground truth for replay tests.
//!
//! Lines: `{"conn":0,"recv_us":…,"frame":"<raw text>"}` for data frames,
//! `{"conn":0,"recv_us":…,"ctrl":"ping|pong|close|connected"}` for control events and
//! `{"conn":0,"sent_us":…,"cmd":"<raw text>"}` for commands we sent.

use std::{
    io::Write,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use clap::Parser;
use engineering_challenge::exchange::kalshi::{Env, WS_PATH, auth::Signer, discovery};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest, http::HeaderValue};

#[derive(Parser)]
struct Args {
    #[arg(long, value_enum, default_value = "prod")]
    env: Env,
    #[arg(long, env = "KALSHI_API_KEY_ID")]
    key_id: String,
    #[arg(long, env = "KALSHI_PRIVATE_KEY_PATH")]
    key_path: PathBuf,
    /// Record the N most active markets (by 24h volume).
    #[arg(long, default_value_t = 50)]
    markets: usize,
    /// Explicit tickers instead of the top-N pick.
    #[arg(long, value_delimiter = ',')]
    tickers: Vec<String>,
    /// Parallel connections, each subscribing the same markets.
    #[arg(long, default_value_t = 1)]
    conns: usize,
    /// Markets per subscribe command.
    #[arg(long, default_value_t = 100)]
    batch: usize,
    #[arg(long, default_value_t = 30)]
    minutes: u64,
    /// Force one reconnect of every connection after this many seconds.
    #[arg(long)]
    reconnect_after: Option<u64>,
    /// Request fresh snapshots of every market this often (seconds); replay tests
    /// compare them with books rebuilt from deltas.
    #[arg(long)]
    snapshot_every: Option<u64>,
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
    let signer = std::sync::Arc::new(Signer::from_file(args.key_id.clone(), &args.key_path)?);

    let tickers = if args.tickers.is_empty() {
        let client = reqwest::Client::new();
        let mut all = discovery::discover(&client, args.env.rest()).await?;
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
        tasks.push(tokio::spawn(run_conn(
            conn,
            args.env,
            signer.clone(),
            tickers.clone(),
            args.batch,
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

#[allow(clippy::too_many_arguments)]
async fn run_conn(
    conn: usize,
    env: Env,
    signer: std::sync::Arc<Signer>,
    tickers: Vec<String>,
    batch: usize,
    reconnect_after: Option<Duration>,
    snapshot_every: Option<Duration>,
    lines: mpsc::UnboundedSender<String>,
    mut stop: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut forced_reconnect_done = reconnect_after.is_none();
    let mut backoff = Duration::from_secs(1);
    let started = tokio::time::Instant::now();
    while !*stop.borrow() {
        let mut req = env.ws().into_client_request()?;
        for (name, value) in signer.headers("GET", WS_PATH) {
            req.headers_mut()
                .insert(name, HeaderValue::from_str(&value)?);
        }
        let mut ws = match tokio_tungstenite::connect_async(req).await {
            Ok((ws, _)) => ws,
            Err(e) => {
                tracing::warn!(conn, error = %e, ?backoff, "connect failed");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
                continue;
            }
        };
        backoff = Duration::from_secs(1);
        let _ =
            lines.send(json!({"conn": conn, "recv_us": now_us(), "ctrl": "connected"}).to_string());
        tracing::info!(conn, "connected");

        // Lifecycle (all markets, conn 0 only: it is market-global and large), order books
        // in batches, plus one deliberately bad command so the recording holds an `error`.
        let mut cmds = Vec::new();
        if conn == 0 {
            cmds.push(json!({"cmd": "subscribe", "params": {"channels": ["market_lifecycle_v2"]}}));
        }
        for chunk in tickers.chunks(batch.max(1)) {
            cmds.push(json!({"cmd": "subscribe",
                "params": {"channels": ["orderbook_delta"], "market_tickers": chunk}}));
        }
        cmds.push(json!({"cmd": "subscribe", "params": {"channels": ["no_such_channel"]}}));
        let mut next_id = 0;
        for mut cmd in cmds {
            next_id += 1;
            cmd["id"] = json!(next_id);
            let text = cmd.to_string();
            let _ = lines.send(json!({"conn": conn, "sent_us": now_us(), "cmd": text}).to_string());
            ws.send(Message::text(text)).await?;
        }
        let mut ob_sid = None;
        let mut snap_tick = tokio::time::interval(snapshot_every.unwrap_or(Duration::MAX / 4));
        snap_tick.tick().await; // the first tick is immediate

        let mut ping = tokio::time::interval(Duration::from_secs(10));
        let reconnect_at = match reconnect_after {
            Some(d) if !forced_reconnect_done => started + d,
            _ => started + Duration::from_secs(u64::MAX / 4),
        };
        loop {
            tokio::select! {
                msg = ws.next() => {
                    let recv_us = now_us();
                    let record = match msg {
                        Some(Ok(Message::Text(t))) => json!({"conn": conn, "recv_us": recv_us, "frame": t.as_str()}),
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
                    ob_sid = ob_sid.or_else(|| orderbook_sid(&record["frame"]));
                }
                _ = snap_tick.tick(), if ob_sid.is_some() => {
                    for chunk in tickers.chunks(batch.max(1)) {
                        next_id += 1;
                        let text = json!({"id": next_id, "cmd": "update_subscription",
                            "params": {"sids": [ob_sid], "market_tickers": chunk, "action": "get_snapshot"}})
                            .to_string();
                        let _ = lines.send(json!({"conn": conn, "sent_us": now_us(), "cmd": text}).to_string());
                        ws.send(Message::text(text)).await?;
                    }
                }
                _ = ping.tick() => ws.send(Message::Ping(Vec::new().into())).await?,
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

/// The sid of a `subscribed` ack for the `orderbook_delta` channel.
fn orderbook_sid(frame: &serde_json::Value) -> Option<i64> {
    let text = frame.as_str()?;
    if !text.starts_with(r#"{"type":"subscribed""#) {
        return None;
    }
    let v: serde_json::Value = serde_json::from_str(text).ok()?;
    (v["msg"]["channel"] == "orderbook_delta").then(|| v["msg"]["sid"].as_i64())?
}
