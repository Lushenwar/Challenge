//! Kalshi order-book collector: SBE deltas plus minute snapshots on stdout, logs and the
//! final JSON report on stderr.

use std::{io::Write, path::PathBuf, sync::Arc};

use anyhow::Context;
use clap::Parser;
use engineering_challenge::{
    arbiter, conn,
    exchange::kalshi::{Env, auth::Signer, discovery},
    model::now_us,
    writer,
};
use serde_json::json;
use tokio::sync::{mpsc, watch};

#[derive(Parser)]
#[command(about = "Kalshi order books → SBE on stdout")]
struct Args {
    #[arg(long, value_enum, default_value = "prod")]
    env: Env,
    #[arg(long, env = "KALSHI_API_KEY_ID")]
    key_id: String,
    #[arg(long, env = "KALSHI_PRIVATE_KEY_PATH")]
    key_path: PathBuf,
    /// Only collect the N most active markets (by 24h volume).
    #[arg(long)]
    max_markets: Option<usize>,
    #[arg(long, default_value = "/dev/stdout")]
    output: PathBuf,
    /// Also write the final report here.
    #[arg(long)]
    report: Option<PathBuf>,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    // Help/usage goes to stderr too: stdout carries only SBE.
    let args = Args::try_parse().unwrap_or_else(|e| {
        let _ = write!(std::io::stderr(), "{e}");
        std::process::exit(if e.use_stderr() { 2 } else { 0 })
    });
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(collect(args))
}

async fn collect(args: Args) -> anyhow::Result<()> {
    let started_us = now_us();
    let signer = Arc::new(Signer::from_file(args.key_id.clone(), &args.key_path)?);
    let mut markets = discovery::discover(&reqwest::Client::new(), args.env.rest())
        .await
        .context("initial discovery")?;
    tracing::info!(markets = markets.len(), "discovered");
    if let Some(n) = args.max_markets {
        markets.sort_by_key(|m| std::cmp::Reverse(m.volume_24h));
        markets.truncate(n);
    }
    let tickers: Vec<String> = markets.into_iter().map(|m| m.ticker).collect();

    let (out_tx, writer) = writer::spawn(args.output.clone())?;
    let (ev_tx, ev_rx) = mpsc::channel(100_000);
    let (stop_tx, stop_rx) = watch::channel(false);
    // ponytail: one socket for every market; Phase 5 shards across sockets.
    let conn = tokio::spawn(conn::run(
        0,
        args.env,
        signer,
        tickers.clone(),
        ev_tx,
        stop_rx,
    ));
    let arbiter = tokio::spawn(arbiter::run(ev_rx, out_tx));

    shutdown_signal().await;
    tracing::info!("shutting down");
    tokio::spawn(async {
        shutdown_signal().await;
        tracing::warn!("second signal; exiting now");
        std::process::exit(130);
    });
    stop_tx.send(true)?;
    let conn_stats = conn.await?;
    let arbiter_stats = arbiter.await?; // ends once the connection dropped its sender
    let writer_stats = tokio::task::spawn_blocking(move || writer.join())
        .await?
        .map_err(|_| anyhow::anyhow!("writer thread panicked"))??;

    let report = json!({
        "run_start_us": started_us,
        "run_end_us": now_us(),
        "markets_subscribed": tickers.len(),
        "connections": [conn_stats],
        "arbiter": arbiter_stats,
        "writer": writer_stats,
    });
    let text = serde_json::to_string_pretty(&report)?;
    writeln!(std::io::stderr(), "{text}")?;
    if let Some(path) = args.report {
        std::fs::write(path, text)?;
    }
    Ok(())
}

async fn shutdown_signal() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}
