//! Prediction-market order-book collector: SBE deltas plus minute snapshots on stdout,
//! logs and the final JSON report on stderr.

use std::{io::Write, path::PathBuf, time::Duration};

use anyhow::Context;
use clap::Parser;
use engineering_challenge::{arbiter, control, exchange, metrics, model::now_us, writer};
use serde_json::json;
use tokio::sync::{mpsc, watch};

#[derive(Debug, Parser)]
#[command(
    about = "Prediction-market order books → SBE on stdout",
    args_override_self = true
)]
struct Args {
    /// TOML file of settings, keyed by flag name (`markets_per_socket = 2000`).
    /// Precedence: defaults < file < environment < command line.
    #[arg(long)]
    config: Option<PathBuf>,
    /// Print the effective settings to stderr and exit.
    #[arg(long)]
    print_config: bool,
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
    /// Append a JSON line per collected market (tick size / price ranges) whenever it
    /// first appears or changes.
    #[arg(long)]
    metadata: Option<PathBuf>,
    /// Serve Prometheus metrics at http://ADDR/metrics and a health check at /healthz.
    #[arg(long)]
    metrics_listen: Option<std::net::SocketAddr>,
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
    let argv = with_config_file(std::env::args_os().collect(), |k| {
        std::env::var_os(k).is_some()
    })
    .unwrap_or_else(|e| {
        let _ = writeln!(std::io::stderr(), "error: {e:#}");
        std::process::exit(2)
    });
    let args = Args::try_parse_from(argv).unwrap_or_else(|e| {
        let _ = write!(std::io::stderr(), "{e}");
        std::process::exit(if e.use_stderr() { 2 } else { 0 })
    });
    if args.print_config {
        let _ = writeln!(std::io::stderr(), "{args:#?}");
        return Ok(());
    }
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
    // Bound before discovery: a bad address fails at once, and /healthz answers (503)
    // while starting up.
    let (view_tx, sockets_tx) = match args.metrics_listen {
        Some(addr) => {
            let listener = tokio::net::TcpListener::bind(addr)
                .await
                .with_context(|| format!("--metrics-listen {addr}"))?;
            tracing::info!(addr = %listener.local_addr()?, "metrics listening");
            let (view_tx, view_rx) = watch::channel(metrics::ArbiterView::default());
            let (sockets_tx, sockets_rx) = mpsc::unbounded_channel();
            tokio::spawn(metrics::serve(listener, view_rx, sockets_rx));
            (Some(view_tx), Some(sockets_tx))
        }
        None => (None, None),
    };
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
        metadata: match &args.metadata {
            Some(p) => Some(
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(p)
                    .with_context(|| format!("opening --metadata {}", p.display()))?,
            ),
            None => None,
        },
        metrics: sockets_tx,
        net: engineering_challenge::net::NetOpts {
            race: args.race,
            source_port_base: args.source_port_base,
            ..Default::default()
        },
    };
    let control = tokio::spawn(control::run(cfg, markets, ev_tx, ctl_rx, stop_rx));
    let arbiter = tokio::spawn(arbiter::run(ev_rx, out_tx, ctl_tx, view_tx));

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

/// Expand `--config FILE` into flags placed before the command line's own, so the
/// command line wins (`args_override_self`). A key whose flag reads an environment
/// variable that is set is skipped: the environment beats the file. Unknown keys fail.
fn with_config_file(
    argv: Vec<std::ffi::OsString>,
    env_is_set: impl Fn(&str) -> bool,
) -> anyhow::Result<Vec<std::ffi::OsString>> {
    use clap::CommandFactory;
    let pos = argv.iter().position(|a| a == "--config");
    let path = match pos {
        Some(i) => argv.get(i + 1).map(PathBuf::from),
        None => argv
            .iter()
            .find_map(|a| a.to_str()?.strip_prefix("--config=").map(PathBuf::from)),
    };
    let Some(path) = path else { return Ok(argv) };
    let text = std::fs::read_to_string(&path)
        .with_context(|| format!("reading --config {}", path.display()))?;
    let table: toml::Table = text
        .parse()
        .with_context(|| format!("parsing {}", path.display()))?;
    let cmd = Args::command();
    let mut flags = Vec::new();
    for (key, value) in table {
        let long = key.replace('_', "-");
        let arg = cmd
            .get_arguments()
            .find(|a| a.get_long() == Some(long.as_str()) && long != "config")
            .with_context(|| format!("{}: unknown setting {key:?}", path.display()))?;
        if arg
            .get_env()
            .and_then(|e| e.to_str())
            .is_some_and(&env_is_set)
        {
            continue;
        }
        match value {
            toml::Value::Boolean(true) => flags.push(format!("--{long}")),
            toml::Value::Boolean(false) => {}
            toml::Value::String(v) => flags.push(format!("--{long}={v}")),
            toml::Value::Integer(v) => flags.push(format!("--{long}={v}")),
            other => anyhow::bail!(
                "{}: {key} must be a string, integer or boolean, not {other}",
                path.display()
            ),
        }
    }
    let mut out = vec![argv[0].clone()];
    out.extend(flags.into_iter().map(Into::into));
    out.extend(argv.into_iter().skip(1));
    Ok(out)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn config(text: &str) -> (tempfile::TempDir, String) {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("c.toml");
        std::fs::write(&p, text).unwrap();
        let p = p.to_str().unwrap().to_owned();
        (dir, p)
    }

    fn parse(argv: &[&str], env: &[&str]) -> anyhow::Result<Args> {
        let argv = argv.iter().map(Into::into).collect();
        let argv = with_config_file(argv, |k| env.contains(&k))?;
        Ok(Args::try_parse_from(argv)?)
    }

    #[test]
    fn config_file_sits_between_defaults_and_the_command_line() {
        let (_d, p) = config(
            "markets_per_socket = 2000
replicas = 3
race = 2
output = \"cap.sbe\"
",
        );
        let a = parse(&["c", "--config", &p, "--replicas", "4"], &[]).unwrap();
        assert_eq!(a.markets_per_socket, 2000, "file over default");
        assert_eq!(a.replicas, 4, "command line over file");
        assert_eq!((a.race, a.output.to_str()), (2, Some("cap.sbe")));
        assert_eq!(a.discovery_secs, 30, "untouched default");
        let a = parse(&["c", &format!("--config={p}")], &[]).unwrap();
        assert_eq!(a.replicas, 3);
    }

    #[test]
    fn the_environment_beats_the_config_file() {
        let (_d, p) = config(
            "key_id = \"from-file\"
",
        );
        let a = parse(&["c", "--config", &p], &[]).unwrap();
        assert_eq!(a.venue.key_id.as_deref(), Some("from-file"));
        // The key id flag's environment variable (named by the venue layer).
        use clap::CommandFactory;
        let cmd = Args::command();
        let var = cmd
            .get_arguments()
            .find(|a| a.get_long() == Some("key-id"))
            .and_then(|a| a.get_env())
            .and_then(|e| e.to_str())
            .unwrap();
        let a = parse(&["c", "--config", &p], &[var]).unwrap();
        assert_ne!(
            a.venue.key_id.as_deref(),
            Some("from-file"),
            "skipped: the env var is set"
        );
    }

    #[test]
    fn bad_config_files_are_rejected() {
        let (_d, p) = config(
            "no_such_flag = 1
",
        );
        assert!(parse(&["c", "--config", &p], &[]).is_err());
        let (_d, p) = config(
            "replicas = [1, 2]
",
        );
        assert!(parse(&["c", "--config", &p], &[]).is_err());
        let (_d, p) = config(
            "replicas = \"many\"
",
        );
        assert!(
            parse(&["c", "--config", &p], &[]).is_err(),
            "validated by the normal parser"
        );
        assert!(parse(&["c", "--config", "/no/such/file.toml"], &[]).is_err());
    }

    #[test]
    fn parses_the_drop_hook() {
        assert_eq!(parse_drop("3@60"), Ok((3, Duration::from_secs(60))));
        for bad in ["3", "x@1", "3@", "@5", "3@-1"] {
            assert!(parse_drop(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn the_command_line_is_valid() {
        use clap::CommandFactory;
        Args::command().debug_assert(); // the clap self-check that once panicked at startup
        let a = Args::try_parse_from([
            "c",
            "--race",
            "3",
            "--source-port-base",
            "40000",
            "--drop-conn",
            "1@5",
        ])
        .unwrap();
        assert_eq!(
            (a.race, a.source_port_base, a.drop_conn),
            (3, Some(40000), Some((1, Duration::from_secs(5))))
        );
        assert_eq!(a.output, PathBuf::from("/dev/stdout"), "stdout by default");
    }
}
