//! `--metrics-listen`: Prometheus text at `/metrics`, `/healthz`. Off by default.
//!
//! The arbiter and every connection task publish copies of their counters through
//! `watch` channels; this server only reads them (no shared mutable state).

use std::fmt::Write as _;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{mpsc, watch},
};

use crate::{arbiter::ArbiterStats, conn::ConnStats};

/// What the arbiter publishes about once a second.
#[derive(Debug, Default, Clone)]
pub struct ArbiterView {
    pub stats: ArbiterStats,
    /// Collected markets (not removed).
    pub markets_live: u64,
    /// Of those, markets with no valid source right now.
    pub markets_in_gap: u64,
    pub writer_queue: usize,
}

/// Healthy while at most 1% of live markets are in a gap (and there are markets).
pub fn healthy(a: &ArbiterView) -> bool {
    a.markets_live > 0 && a.markets_in_gap * 100 <= a.markets_live
}

/// Prometheus text exposition format.
pub fn render(a: &ArbiterView, sockets: &[ConnStats]) -> String {
    let s = &a.stats;
    let mut o = String::new();
    let mut metric = |name: &str, kind: &str, help: &str, samples: &[(String, u64)]| {
        let _ = writeln!(
            o,
            "# HELP collector_{name} {help}\n# TYPE collector_{name} {kind}"
        );
        for (labels, v) in samples {
            let _ = writeln!(o, "collector_{name}{labels} {v}");
        }
    };
    let one = |v: u64| vec![(String::new(), v)];
    metric(
        "markets_live",
        "gauge",
        "Markets being collected.",
        &one(a.markets_live),
    );
    metric(
        "markets_in_gap",
        "gauge",
        "Markets with no valid source right now.",
        &one(a.markets_in_gap),
    );
    metric(
        "writer_queue",
        "gauge",
        "Messages waiting for the writer thread.",
        &one(a.writer_queue as u64),
    );
    metric(
        "updates_written_total",
        "counter",
        "Price level updates written.",
        &one(s.updates),
    );
    metric(
        "snapshots_written_total",
        "counter",
        "Snapshots written.",
        &one(s.snapshots),
    );
    metric(
        "failovers_total",
        "counter",
        "Active source switches, by reason.",
        &[
            (
                "{reason=\"lost\"}".into(),
                s.failovers.saturating_sub(s.failovers_stale),
            ),
            ("{reason=\"stale\"}".into(), s.failovers_stale),
            ("{reason=\"faster\"}".into(), s.switches_faster),
        ],
    );
    metric(
        "gaps_total",
        "counter",
        "Gaps (no valid source) that ended.",
        &one(s.gaps),
    );
    metric(
        "gap_microseconds_total",
        "counter",
        "Total length of ended gaps.",
        &one(s.gap_total_us.max(0) as u64),
    );
    metric(
        "resubscribes_total",
        "counter",
        "Health-rule resubscribes, by rule.",
        &[
            ("{rule=\"lag\"}".into(), s.resubscribes_lag),
            ("{rule=\"rate\"}".into(), s.resubscribes_rate),
        ],
    );
    metric(
        "recycles_total",
        "counter",
        "Scheduled socket reconnects.",
        &one(s.recycles),
    );
    metric(
        "writer_blocked_microseconds_total",
        "counter",
        "Time the arbiter waited on a full writer queue.",
        &one(s.writer_blocked_us.max(0) as u64),
    );
    let per = |f: fn(&ConnStats) -> u64| -> Vec<(String, u64)> {
        sockets
            .iter()
            .enumerate()
            .map(|(i, c)| (format!("{{socket=\"{i}\"}}"), f(c)))
            .collect()
    };
    metric(
        "socket_connected",
        "gauge",
        "1 while the socket is connected.",
        &per(|c| c.connected as u64),
    );
    metric(
        "socket_connects_total",
        "counter",
        "Successful connects.",
        &per(|c| c.connects),
    );
    metric(
        "socket_stalls_total",
        "counter",
        "Reconnects after 20 s of silence.",
        &per(|c| c.stalls),
    );
    metric(
        "socket_seq_gaps_total",
        "counter",
        "Sequence gaps detected.",
        &per(|c| c.seq_gaps),
    );
    metric(
        "socket_frames_total",
        "counter",
        "Frames received.",
        &per(|c| c.frames),
    );
    o
}

/// Serve until the process exits. `sockets` delivers each new connection's stats channel.
pub async fn serve(
    listener: TcpListener,
    arbiter: watch::Receiver<ArbiterView>,
    mut sockets: mpsc::UnboundedReceiver<watch::Receiver<ConnStats>>,
) {
    let mut conns: Vec<watch::Receiver<ConnStats>> = Vec::new();
    loop {
        let (mut tcp, _) = tokio::select! {
            Some(rx) = sockets.recv() => { conns.push(rx); continue }
            accepted = listener.accept() => match accepted {
                Ok(a) => a,
                Err(_) => continue,
            },
        };
        let mut buf = [0u8; 1024];
        let n = match tokio::time::timeout(std::time::Duration::from_secs(2), tcp.read(&mut buf))
            .await
        {
            Ok(Ok(n)) => n,
            _ => continue,
        };
        let path = std::str::from_utf8(&buf[..n])
            .ok()
            .and_then(|r| r.split_whitespace().nth(1))
            .unwrap_or("");
        let view = arbiter.borrow().clone();
        let (status, body) = match path {
            "/metrics" => {
                let stats: Vec<ConnStats> = conns.iter().map(|c| c.borrow().clone()).collect();
                ("200 OK", render(&view, &stats))
            }
            "/healthz" if healthy(&view) => ("200 OK", "ok\n".to_owned()),
            "/healthz" => (
                "503 Service Unavailable",
                format!(
                    "{} of {} markets in a gap\n",
                    view.markets_in_gap, view.markets_live
                ),
            ),
            _ => ("404 Not Found", "not found\n".to_owned()),
        };
        let resp = format!(
            "HTTP/1.1 {status}\r\ncontent-type: text/plain; version=0.0.4\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = tcp.write_all(resp.as_bytes()).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn view(live: u64, gap: u64) -> ArbiterView {
        ArbiterView {
            markets_live: live,
            markets_in_gap: gap,
            ..Default::default()
        }
    }

    #[test]
    fn healthy_while_at_most_one_percent_of_markets_are_in_a_gap() {
        assert!(healthy(&view(1000, 10)));
        assert!(!healthy(&view(1000, 11)));
        assert!(!healthy(&view(0, 0)), "collecting nothing is not healthy");
    }

    #[test]
    fn renders_prometheus_text() {
        let mut v = view(5, 1);
        v.stats.failovers = 7;
        v.stats.failovers_stale = 2;
        v.stats.switches_faster = 1;
        let sockets = vec![
            ConnStats {
                connected: true,
                connects: 3,
                ..Default::default()
            },
            ConnStats::default(),
        ];
        let text = render(&v, &sockets);
        for line in [
            "collector_markets_live 5",
            "collector_markets_in_gap 1",
            "collector_failovers_total{reason=\"lost\"} 5",
            "collector_failovers_total{reason=\"stale\"} 2",
            "collector_failovers_total{reason=\"faster\"} 1",
            "collector_socket_connected{socket=\"0\"} 1",
            "collector_socket_connected{socket=\"1\"} 0",
            "collector_socket_connects_total{socket=\"0\"} 3",
            "# TYPE collector_gaps_total counter",
        ] {
            assert!(
                text.lines().any(|l| l == line),
                "missing {line:?} in\n{text}"
            );
        }
        // Every sample line is `name{labels} integer`.
        for l in text.lines().filter(|l| !l.starts_with('#')) {
            let (name, v) = l.rsplit_once(' ').unwrap();
            assert!(
                name.starts_with("collector_") && v.parse::<u64>().is_ok(),
                "{l}"
            );
        }
    }
}
