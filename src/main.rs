//! Prediction-market order-book collector: SBE deltas plus minute snapshots on stdout,
//! logs and the final JSON report on stderr.

use std::{io::Write, path::PathBuf, time::Duration};

use anyhow::Context;
use clap::Parser;
use engineering_challenge::{arbiter, control, exchange, model::now_us, writer};
use serde_json::json;
use tokio::sync::{mpsc, watch};

#[derive(Parser)]
#[command(about = "Prediction-market order books → SBE on stdout")]
struct Args {
    #[command(flatten)]
    venue: exchange::VenueArgs,
    /// Only collect the N most active markets (by 24h volume). Default: all live markets.
    #[arg(long)]
    max_markets: Option<usize>,
    /// Markets per websocket.
    #[arg(long, default_value_t = 5000)]
    markets_per_socket: usize,
    /// Sockets each market is collected on (failover needs at least 2).
    #[arg(long, default_value_t = 2)]
    replicas: usize,
    /// Connect here instead of the venue (e.g. `ws://127.0.0.1:9000` for faultproxy).
    #[arg(long)]
    ws_url: Option<String>,
    /// Test hook: drop socket N after S seconds, as `N@S`.
    #[arg(long, value_parser = parse_drop)]
    drop_conn: Option<(usize, Duration)>,
    /// Seconds between the end of one discovery pass and the start of the next.
    #[arg(long, default_value_t = 30)]
    discovery_secs: u64,
    #[arg(long, default_value = "/dev/stdout")]
    output: PathBuf,
    /// Also write the final report here.
    #[arg(long)]
    report: Option<PathBuf>,
    /// Race N handshakes per (re)connect across the host's addresses; keep the fastest.
    #[arg(long, default_value_t = 1)]
    race: usize,
    /// Pin local source ports: socket i uses BASE + i * race (+ attempt).
    #[arg(long)]
    source_port_base: Option<u16>,
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
    // Handlers first: a signal during startup (initial discovery can take ~40 s) must not
    // kill the process by default or, when started as a background job with SIGINT
    // ignored, be lost.
    let mut signals = Signals::new()?;
    let exchange = args.venue.build()?;
    let client = reqwest::Client::new();
    let markets = tokio::select! {
        m = exchange.discover(&client) => m.context("initial discovery")?,
        _ = signals.next() => {
            tracing::info!("signal during startup; nothing collected yet, exiting");
            return Ok(());
        }
    };
    tracing::info!(
        venue = exchange.name(),
        markets = markets.len(),
        "discovered"
    );

    let (out_tx, writer) = writer::spawn(args.output.clone())?;
    let (ev_tx, ev_rx) = mpsc::channel(100_000);
    let (ctl_tx, ctl_rx) = mpsc::unbounded_channel();
    let (stop_tx, stop_rx) = watch::channel(false);
    let cfg = control::Config {
        exchange,
        ws_url: args.ws_url,
        markets_per_socket: args.markets_per_socket,
        replicas: args.replicas,
        drop_conn: args.drop_conn,
        discovery_every: Duration::from_secs(args.discovery_secs),
        max_markets: args.max_markets,
        net: engineering_challenge::net::NetOpts {
            race: args.race,
            source_port_base: args.source_port_base,
            ..Default::default()
        },
    };
    let control = tokio::spawn(control::run(cfg, markets, ev_tx, ctl_rx, stop_rx));
    let arbiter = tokio::spawn(arbiter::run(ev_rx, out_tx, ctl_tx));

    signals.next().await;
    tracing::info!("shutting down");
    tokio::spawn(async move {
        signals.next().await;
        tracing::warn!("second signal; exiting now");
        std::process::exit(130);
    });
    stop_tx.send(true)?;
    let (control_stats, conn_stats) = control.await?;
    let arbiter_stats = arbiter.await?; // ends once every connection dropped its sender
    let writer_stats = tokio::task::spawn_blocking(move || writer.join())
        .await?
        .map_err(|_| anyhow::anyhow!("writer thread panicked"))??;

    let report = json!({
        "run_start_us": started_us,
        "run_end_us": now_us(),
        "control": control_stats,
        "connections": conn_stats,
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

fn parse_drop(s: &str) -> Result<(usize, Duration), String> {
    let (n, secs) = s.split_once('@').ok_or("expected N@SECONDS")?;
    Ok((
        n.parse().map_err(|e| format!("{e}"))?,
        Duration::from_secs(secs.parse().map_err(|e| format!("{e}"))?),
    ))
}

/// SIGINT and SIGTERM, registered once at startup.
struct Signals {
    int: tokio::signal::unix::Signal,
    term: tokio::signal::unix::Signal,
}

impl Signals {
    fn new() -> anyhow::Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};
        Ok(Self {
            int: signal(SignalKind::interrupt())?,
            term: signal(SignalKind::terminate())?,
        })
    }

    async fn next(&mut self) {
        tokio::select! {
            _ = self.int.recv() => {}
            _ = self.term.recv() => {}
        }
    }
}
