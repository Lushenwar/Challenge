//! Chaos websocket proxy. The collector connects here (`--ws-url ws://127.0.0.1:9000`);
//! each client connection is relayed to the venue over its own (signed) upstream socket,
//! and faults are injected at random per connection:
//!
//! - `drop_conn`: close both sides.
//! - `drop_frame`: swallow one `orderbook_delta` (the collector sees a seq gap).
//! - `delay`: stop relaying for `--delay-ms`, then burst (exchange-side buffering).
//! - `silence`: stop relaying one market's deltas, renumbering `seq` so no gap shows
//!   (a silent subscription). Lifted when the client sends a command naming it.
//!
//! Every fault is one JSON line in `--log`, so the collector's report can be checked.

use std::{
    collections::HashMap,
    io::Write,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use clap::Parser;
use engineering_challenge::{
    conn,
    exchange::{self, Exchange},
};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::{net::TcpListener, time::Instant};
use tokio_tungstenite::tungstenite::Message;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:9000")]
    listen: String,
    #[command(flatten)]
    venue: exchange::Args,
    /// Mean seconds between faults on each connection.
    #[arg(long, default_value_t = 120)]
    fault_every: u64,
    #[arg(long, default_value_t = 3000)]
    delay_ms: u64,
    /// Fault log (JSON lines).
    #[arg(long)]
    log: PathBuf,
}

struct Shared {
    exchange: Arc<dyn Exchange>,
    fault_every: Duration,
    delay: Duration,
    log: Mutex<std::fs::File>,
}

fn now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_micros() as i64
}

impl Shared {
    fn fault(&self, conn: u64, fault: &str, detail: serde_json::Value) {
        let line = json!({"t_us": now_us(), "conn": conn, "fault": fault, "detail": detail});
        tracing::info!(%line, "fault");
        let _ = writeln!(self.log.lock().expect("log lock"), "{line}");
    }
}

/// xorshift64: enough randomness for a fault schedule, no extra dependency.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    /// Uniform in [mean/2, 3*mean/2).
    fn interval(&mut self, mean: Duration) -> Duration {
        let ms = mean.as_millis() as u64;
        Duration::from_millis(ms / 2 + self.next() % ms.max(1))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();
    let args = Args::parse();
    let shared = Arc::new(Shared {
        exchange: args.venue.build()?,
        fault_every: Duration::from_secs(args.fault_every),
        delay: Duration::from_millis(args.delay_ms),
        log: Mutex::new(std::fs::File::create(&args.log)?),
    });
    let listener = TcpListener::bind(&args.listen).await?;
    tracing::info!(listen = %args.listen, "faultproxy ready");
    let mut id = 0u64;
    loop {
        let (tcp, _) = listener.accept().await?;
        id += 1;
        let shared = shared.clone();
        tokio::spawn(async move {
            if let Err(e) = relay(id, tcp, shared).await {
                tracing::warn!(conn = id, error = %format!("{e:#}"), "relay ended");
            }
        });
    }
}

async fn relay(id: u64, tcp: tokio::net::TcpStream, s: Arc<Shared>) -> anyhow::Result<()> {
    let client = tokio_tungstenite::accept_async(tcp).await?;
    let upstream = conn::connect(s.exchange.ws_request(None)?).await?;
    let (mut cw, mut cr) = client.split();
    let (mut uw, mut ur) = upstream.split();
    let mut rng = Rng(now_us() as u64 ^ id.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let next_fault = tokio::time::sleep(rng.interval(s.fault_every));
    tokio::pin!(next_fault);
    let mut drop_next = false;
    let mut silenced: Option<String> = None;
    let mut last_ticker: Option<String> = None;
    // Per sid: deltas withheld so far; later seqs on that sid are shifted down by it.
    let mut withheld: HashMap<u64, u64> = HashMap::new();
    loop {
        tokio::select! {
            msg = ur.next() => {
                let Some(Ok(msg)) = msg else { break };
                let msg = match msg {
                    Message::Text(t) => {
                        let text = t.as_str();
                        let delta = text.starts_with(r#"{"type":"orderbook_delta""#);
                        if delta {
                            last_ticker = field_str(text, "market_ticker");
                        }
                        let sid = field_u64(text, "sid");
                        if delta && silenced.as_deref().is_some_and(|m| text.contains(m)) {
                            *withheld.entry(sid.unwrap_or(0)).or_default() += 1;
                            continue;
                        }
                        if delta && drop_next {
                            drop_next = false;
                            s.fault(id, "drop_frame", json!({"sid": sid, "seq": field_u64(text, "seq")}));
                            continue;
                        }
                        match sid.and_then(|sid| withheld.get(&sid)) {
                            Some(&n) if n > 0 => Message::text(shift_seq(text, n)),
                            _ => Message::Text(t),
                        }
                    }
                    other => other,
                };
                if cw.send(msg).await.is_err() {
                    break;
                }
            }
            msg = cr.next() => {
                let Some(Ok(msg)) = msg else { break };
                if let Message::Text(t) = &msg {
                    if silenced.as_deref().is_some_and(|m| t.as_str().contains(m)) {
                        s.fault(id, "silence_lifted", json!({"market": silenced.take()}));
                    }
                }
                if uw.send(msg).await.is_err() {
                    break;
                }
            }
            _ = &mut next_fault => {
                match rng.next() % 100 {
                    0..15 => {
                        s.fault(id, "drop_conn", json!({}));
                        break;
                    }
                    15..50 => drop_next = true, // logged when the frame is dropped
                    50..75 => {
                        s.fault(id, "delay", json!({"ms": s.delay.as_millis() as u64}));
                        tokio::time::sleep(s.delay).await;
                    }
                    _ => {
                        if silenced.is_none() {
                            if let Some(m) = last_ticker.clone() {
                                s.fault(id, "silence", json!({"market": m}));
                                silenced = Some(m);
                            }
                        }
                    }
                }
                next_fault.as_mut().reset(Instant::now() + rng.interval(s.fault_every));
            }
        }
    }
    let _ = cw.close().await;
    let _ = uw.close().await;
    Ok(())
}

/// The value of `"key":"..."` in a frame (first occurrence).
fn field_str(text: &str, key: &str) -> Option<String> {
    let pat = format!("\"{key}\":\"");
    let start = text.find(&pat)? + pat.len();
    let end = text[start..].find('"')?;
    Some(text[start..start + end].to_owned())
}

/// The value of `"key":123` in a frame (first occurrence).
fn field_u64(text: &str, key: &str) -> Option<u64> {
    let pat = format!("\"{key}\":");
    let start = text.find(&pat)? + pat.len();
    let digits: String = text[start..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    digits.parse().ok()
}

/// Rewrite the frame's `"seq":N` to `N - by`.
fn shift_seq(text: &str, by: u64) -> String {
    let pat = "\"seq\":";
    let Some(i) = text.find(pat) else {
        return text.to_owned();
    };
    let start = i + pat.len();
    let len = text[start..]
        .chars()
        .take_while(char::is_ascii_digit)
        .count();
    let Ok(seq) = text[start..start + len].parse::<u64>() else {
        return text.to_owned();
    };
    format!(
        "{}{}{}",
        &text[..start],
        seq.saturating_sub(by),
        &text[start + len..]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_parsing_and_seq_shift() {
        let f = r#"{"type":"orderbook_delta","sid":2,"seq":17,"msg":{"market_ticker":"T-1","price_dollars":"0.5"}}"#;
        assert_eq!(field_u64(f, "sid"), Some(2));
        assert_eq!(field_u64(f, "seq"), Some(17));
        assert_eq!(field_str(f, "market_ticker").as_deref(), Some("T-1"));
        let shifted = shift_seq(f, 3);
        assert!(shifted.contains(r#""seq":14,"#));
        assert_eq!(shifted.len(), f.len());
    }
}
