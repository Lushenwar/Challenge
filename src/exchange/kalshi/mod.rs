//! Kalshi: Ed25519-signed websocket, `orderbook_delta` + `market_lifecycle_v2`
//! channels, per-sid sequence numbers, `get_snapshot` resyncs.

pub mod auth;
pub mod codec;
pub mod discovery;

use futures_util::future::BoxFuture;
use serde_json::json;
use tokio_tungstenite::tungstenite::{
    Message, client::IntoClientRequest, handshake::client::Request, http::HeaderValue,
};

use crate::exchange::{Change, Decoded, Exchange, MarketInfo, Session};

pub const REST_PROD: &str = "https://api.elections.kalshi.com/trade-api/v2";
pub const REST_DEMO: &str = "https://external-api.demo.kalshi.co/trade-api/v2";
pub const WS_PROD: &str = "wss://api.elections.kalshi.com/trade-api/ws/v2";
pub const WS_DEMO: &str = "wss://external-api.demo.kalshi.co/trade-api/ws/v2";
/// Path signed in the websocket handshake.
pub const WS_PATH: &str = "/trade-api/ws/v2";

/// Markets per `subscribe` / `update_subscription` command. Kalshi accepted 20,000 in
/// one command; smaller batches keep each frame modest.
const BATCH: usize = 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Env {
    Prod,
    Demo,
}

impl Env {
    pub fn rest(self) -> &'static str {
        match self {
            Self::Prod => REST_PROD,
            Self::Demo => REST_DEMO,
        }
    }

    pub fn ws(self) -> &'static str {
        match self {
            Self::Prod => WS_PROD,
            Self::Demo => WS_DEMO,
        }
    }
}

pub struct Kalshi {
    env: Env,
    signer: auth::Signer,
    rest: String,
}

impl Kalshi {
    /// `rest`: the REST base URL used for discovery (normally `env.rest()`).
    pub fn new(env: Env, signer: auth::Signer, rest: String) -> Self {
        Self { env, signer, rest }
    }
}

impl Exchange for Kalshi {
    fn name(&self) -> &'static str {
        "kalshi"
    }

    fn ws_request(&self, url: Option<&str>) -> anyhow::Result<Request> {
        let mut req = url.unwrap_or(self.env.ws()).into_client_request()?;
        for (name, value) in self.signer.headers("GET", WS_PATH) {
            req.headers_mut()
                .insert(name, HeaderValue::from_str(&value)?);
        }
        Ok(req)
    }

    fn discover<'a>(
        &'a self,
        client: &'a reqwest::Client,
    ) -> BoxFuture<'a, anyhow::Result<Vec<MarketInfo>>> {
        Box::pin(discovery::discover(client, &self.rest))
    }

    fn session(&self, lifecycle: bool) -> Box<dyn Session> {
        Box::new(KalshiSession {
            next_id: 0,
            ob_sid: None,
            lifecycle,
        })
    }
}

pub struct KalshiSession {
    next_id: u64,
    /// The order-book subscription's sid: subscribes on one connection merge into it.
    ob_sid: Option<u64>,
    lifecycle: bool,
}

impl KalshiSession {
    fn cmd(&mut self, mut v: serde_json::Value) -> String {
        self.next_id += 1;
        v["id"] = json!(self.next_id);
        v.to_string()
    }

    fn update(&mut self, action: &str, sid: u64, markets: &[String]) -> Vec<String> {
        markets
            .chunks(BATCH)
            .map(|chunk| {
                self.cmd(json!({"cmd": "update_subscription",
                    "params": {"sids": [sid], "market_tickers": chunk, "action": action}}))
            })
            .collect()
    }
}

impl Session for KalshiSession {
    fn has_sequence_numbers(&self) -> bool {
        true
    }

    fn on_connect(&mut self, markets: &[String]) -> Vec<String> {
        self.ob_sid = None;
        let mut cmds = Vec::new();
        if self.lifecycle {
            cmds.push(
                self.cmd(
                    json!({"cmd": "subscribe", "params": {"channels": ["market_lifecycle_v2"]}}),
                ),
            );
        }
        cmds.extend(self.subscribe(markets));
        cmds
    }

    fn subscribe(&mut self, markets: &[String]) -> Vec<String> {
        markets
            .chunks(BATCH)
            .map(|chunk| {
                self.cmd(json!({"cmd": "subscribe",
                    "params": {"channels": ["orderbook_delta"], "market_tickers": chunk}}))
            })
            .collect()
    }

    fn unsubscribe(&mut self, markets: &[String]) -> Vec<String> {
        // ponytail: before the sid is known the delete is skipped; frames for a market
        // the connection no longer tracks are ignored, and a reconnect will not ask again.
        match self.ob_sid {
            Some(sid) => self.update("delete_markets", sid, markets),
            None => Vec::new(),
        }
    }

    fn request_snapshots(&mut self, sid: u64, markets: &[String]) -> Option<Vec<String>> {
        Some(self.update("get_snapshot", sid, markets))
    }

    fn decode(&mut self, frame: &str) -> anyhow::Result<Vec<Decoded>> {
        use codec::Decoded as K;
        Ok(vec![match codec::decode(frame)? {
            K::Snapshot {
                sid,
                seq,
                id,
                ticker,
                bids,
                asks,
                sending_ts_ms,
            } => Decoded::Snapshot {
                sid_seq: Some((sid, seq)),
                requested: id.is_some(),
                market: ticker,
                bids,
                asks,
                exch_us: sending_ts_ms.map(|ms| ms * 1000),
            },
            K::Delta {
                sid,
                seq,
                ticker,
                side,
                price,
                delta,
                ts_ms,
                sending_ts_ms,
            } => Decoded::Level {
                sid_seq: Some((sid, seq)),
                market: ticker,
                side,
                price,
                change: Change::Delta(delta),
                exch_us: ts_ms.or(sending_ts_ms).map(|ms| ms * 1000),
            },
            K::Lifecycle {
                sid,
                seq,
                ticker,
                event,
            } => Decoded::Lifecycle {
                sid_seq: Some((sid, seq)),
                market: ticker,
                event,
            },
            K::Error { code, msg, .. } => Decoded::Error { code, msg },
            K::Subscribed { channel, sid, .. } => {
                if channel == "orderbook_delta" {
                    self.ob_sid = Some(sid);
                }
                Decoded::Other { sid_seq: None }
            }
            other => Decoded::Other {
                sid_seq: other.sid_seq(),
            },
        }])
    }

    fn keepalive(&self) -> Message {
        Message::Ping(Vec::new().into())
    }
}
