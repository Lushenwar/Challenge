//! The venue boundary. Everything specific to one exchange (URLs, auth, discovery,
//! command JSON, frame decoding) lives under this module; the rest of the collector
//! (connections, arbiter, health, writer) only sees [`Exchange`], [`Session`] and
//! [`Decoded`].

pub mod kalshi;
pub mod polymarket;

use std::{path::PathBuf, sync::Arc};

use futures_util::future::BoxFuture;
use tokio_tungstenite::tungstenite::{Message, handshake::client::Request};

use crate::Side;

/// One live market (one order book) found by discovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketInfo {
    /// The book's id on the wire; also the output `symbol`.
    pub ticker: String,
    /// Whole contracts/shares traded in the last 24h; for ranking only.
    pub volume_24h: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Lifecycle {
    Created,
    Activated,
    /// Trading stopped: the market is removed.
    Deactivated,
    /// Trading resumed.
    Reactivated,
    Determined,
    Settled,
    Other(String),
}

/// How a level message changes the book.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Change {
    /// Signed change to the level's quantity.
    Delta(i64),
    /// The level's new total quantity (0 = removed).
    Absolute(i64),
}

/// A decoded frame, venue-neutral. Prices are already on the bid/ask book in fixed point.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decoded {
    Snapshot {
        sid_seq: Option<(u64, u64)>,
        /// Answers our own snapshot request (vs. a subscription or venue-initiated one).
        requested: bool,
        market: String,
        bids: Vec<(i64, i64)>,
        asks: Vec<(i64, i64)>,
        exch_us: Option<i64>,
    },
    Level {
        sid_seq: Option<(u64, u64)>,
        market: String,
        side: Side,
        price: i64,
        change: Change,
        exch_us: Option<i64>,
    },
    Lifecycle {
        sid_seq: Option<(u64, u64)>,
        market: String,
        event: Lifecycle,
    },
    Error {
        code: i64,
        msg: String,
    },
    /// Anything else; it may still take part in a subscription's sequence.
    Other {
        sid_seq: Option<(u64, u64)>,
    },
}

impl Decoded {
    /// `(sid, seq)` for frames that take part in a subscription's sequence.
    pub fn sid_seq(&self) -> Option<(u64, u64)> {
        match self {
            Self::Snapshot { sid_seq, .. }
            | Self::Level { sid_seq, .. }
            | Self::Lifecycle { sid_seq, .. }
            | Self::Other { sid_seq } => *sid_seq,
            Self::Error { .. } => None,
        }
    }
}

/// A venue.
pub trait Exchange: Send + Sync {
    fn name(&self) -> &'static str;
    /// The websocket handshake request (URL and auth headers); `url` overrides the URL
    /// (e.g. a local fault proxy).
    fn ws_request(&self, url: Option<&str>) -> anyhow::Result<Request>;
    /// Whether a successful [`Exchange::discover`] lists *every* live market. If not
    /// (a capped listing), a market missing from it may still be live, so discovery
    /// never removes markets.
    fn discovery_is_complete(&self) -> bool {
        true
    }
    /// For a venue whose [`Exchange::discover`] is capped: the complete set of live
    /// tickers, when a (slow, background) complete listing has finished since the last
    /// call. Markets missing from two of these are removed. `None`: nothing new.
    fn live_markets(&self, _client: &reqwest::Client) -> Option<std::collections::HashSet<String>> {
        None
    }
    /// One discovery pass; an error means the pass failed.
    fn discover<'a>(
        &'a self,
        client: &'a reqwest::Client,
    ) -> BoxFuture<'a, anyhow::Result<Vec<MarketInfo>>>;
    /// Per-connection protocol state. `lifecycle`: also subscribe to market lifecycle
    /// events on this connection, if the venue has them.
    fn session(&self, lifecycle: bool) -> Box<dyn Session>;
}

/// One connection's protocol state: builds commands, decodes frames.
pub trait Session: Send {
    /// Frames carry per-subscription sequence numbers (gaps are detectable).
    fn has_sequence_numbers(&self) -> bool;
    /// A fresh connection: everything to send to collect `markets`.
    fn on_connect(&mut self, markets: &[String]) -> Vec<String>;
    fn subscribe(&mut self, markets: &[String]) -> Vec<String>;
    fn unsubscribe(&mut self, markets: &[String]) -> Vec<String>;
    /// Ask for fresh snapshots of `markets` on `sid`. `None`: the venue cannot, so the
    /// caller resubscribes instead.
    fn request_snapshots(&mut self, sid: u64, markets: &[String]) -> Option<Vec<String>>;
    fn decode(&mut self, frame: &str) -> anyhow::Result<Vec<Decoded>>;
    /// Sent every 10s to keep the socket (and our liveness check) busy.
    fn keepalive(&self) -> Message;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Venue {
    Kalshi,
    Polymarket,
}

/// Venue choice and credentials, shared by every binary.
#[derive(Debug, Clone, clap::Args)]
pub struct VenueArgs {
    #[arg(long, value_enum, default_value = "kalshi")]
    pub exchange: Venue,
    #[arg(long, value_enum, default_value = "prod")]
    pub env: kalshi::Env,
    #[arg(long, env = "KALSHI_API_KEY_ID")]
    pub key_id: Option<String>,
    #[arg(long, env = "KALSHI_PRIVATE_KEY_PATH")]
    pub key_path: Option<PathBuf>,
    /// REST base URL override for discovery (tests, fake exchanges).
    #[arg(long)]
    pub rest_url: Option<String>,
}

impl VenueArgs {
    pub fn build(&self) -> anyhow::Result<Arc<dyn Exchange>> {
        Ok(match self.exchange {
            Venue::Kalshi => {
                let (Some(id), Some(path)) = (&self.key_id, &self.key_path) else {
                    anyhow::bail!(
                        "Kalshi needs --key-id and --key-path (or KALSHI_API_KEY_ID / KALSHI_PRIVATE_KEY_PATH)"
                    );
                };
                let signer = kalshi::auth::Signer::from_file(id.clone(), path)?;
                let rest = self
                    .rest_url
                    .clone()
                    .unwrap_or_else(|| self.env.rest().to_owned());
                Arc::new(kalshi::Kalshi::new(self.env, signer, rest))
            }
            Venue::Polymarket => Arc::new(polymarket::Polymarket::new(
                self.rest_url
                    .clone()
                    .unwrap_or_else(|| polymarket::GAMMA.to_owned()),
            )),
        })
    }
}
