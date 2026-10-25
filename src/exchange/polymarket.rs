//! Polymarket public CLOB market channel. No auth, no sequence numbers: recovery is
//! resubscribe. `book` = full snapshot of one asset (outcome token); `price_change` =
//! the new absolute size at a level. Each asset id is its own book and output symbol.

use anyhow::{Context, bail};
use futures_util::future::BoxFuture;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::{
    Message, client::IntoClientRequest, handshake::client::Request,
};

use crate::{
    Side,
    exchange::{Change, Decoded, Exchange, MarketInfo, Session},
    fixed::{SCALE, parse_fixed},
};

pub const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
pub const GAMMA: &str = "https://gamma-api.polymarket.com";
/// Gamma returns at most 100 markets per page.
const PAGE: usize = 100;
/// Gamma's offset pagination refuses offsets past ~2,000 (HTTP 422, seen 2026-10-25).
// ponytail: discovery covers the ~2,000 most active markets (~4k books); page through
// `/markets/keyset` to cover every market.
const MAX_OFFSET: usize = 1_900;
const BATCH: usize = 500;

pub struct Polymarket {
    /// Gamma REST base URL (normally [`GAMMA`]).
    pub gamma: String,
}

impl Exchange for Polymarket {
    fn name(&self) -> &'static str {
        "polymarket"
    }

    fn ws_request(&self, url: Option<&str>) -> anyhow::Result<Request> {
        Ok(url.unwrap_or(WS_URL).into_client_request()?)
    }

    /// Capped at the most active markets (see `MAX_OFFSET`): markets drift in and out of
    /// the window while still live.
    // ponytail: with no lifecycle feed, a closed market stays subscribed (and quiet).
    fn discovery_is_complete(&self) -> bool {
        false
    }

    fn discover<'a>(
        &'a self,
        client: &'a reqwest::Client,
    ) -> BoxFuture<'a, anyhow::Result<Vec<MarketInfo>>> {
        Box::pin(discover(client, &self.gamma))
    }

    fn session(&self, _lifecycle: bool) -> Box<dyn Session> {
        Box::new(PolySession {
            initial_sent: false,
        })
    }
}

async fn discover(client: &reqwest::Client, gamma: &str) -> anyhow::Result<Vec<MarketInfo>> {
    let mut out = Vec::new();
    let mut offset = 0;
    while offset <= MAX_OFFSET {
        let url = format!(
            "{gamma}/markets?active=true&closed=false&order=volume24hr&ascending=false&limit={PAGE}&offset={offset}"
        );
        let resp = client.get(&url).send().await?;
        if !resp.status().is_success() {
            bail!("gamma page failed: HTTP {}", resp.status());
        }
        let page: Vec<RawMarket> = resp.json().await.context("decoding gamma page")?;
        if page.is_empty() {
            break;
        }
        offset += page.len();
        out.extend(page.into_iter().flat_map(RawMarket::into_books));
    }
    Ok(out)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawMarket {
    #[serde(default)]
    clob_token_ids: Option<String>,
    #[serde(default)]
    enable_order_book: Option<bool>,
    #[serde(default)]
    accepting_orders: Option<bool>,
    #[serde(default)]
    volume24hr: Option<serde_json::Number>,
}

impl RawMarket {
    /// One book per outcome token of a tradeable market.
    fn into_books(self) -> Vec<MarketInfo> {
        if self.enable_order_book != Some(true) || self.accepting_orders == Some(false) {
            return Vec::new();
        }
        let tokens: Vec<String> = self
            .clob_token_ids
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();
        // Whole units only; a decimal volume is cut at the point (ranking only).
        let volume_24h = self
            .volume24hr
            .map(|n| n.to_string())
            .and_then(|s| s.split('.').next().and_then(|w| w.parse().ok()))
            .unwrap_or(0);
        tokens
            .into_iter()
            .map(|ticker| MarketInfo { ticker, volume_24h })
            .collect()
    }
}

pub struct PolySession {
    /// The first subscribe on a connection uses `type: market`; later ones `operation`.
    initial_sent: bool,
}

impl Session for PolySession {
    fn has_sequence_numbers(&self) -> bool {
        false
    }

    fn on_connect(&mut self, markets: &[String]) -> Vec<String> {
        self.initial_sent = false;
        self.subscribe(markets)
    }

    fn subscribe(&mut self, markets: &[String]) -> Vec<String> {
        markets
            .chunks(BATCH)
            .map(|chunk| {
                if self.initial_sent {
                    json!({"assets_ids": chunk, "operation": "subscribe"}).to_string()
                } else {
                    self.initial_sent = true;
                    json!({"assets_ids": chunk, "type": "market"}).to_string()
                }
            })
            .collect()
    }

    fn unsubscribe(&mut self, markets: &[String]) -> Vec<String> {
        markets
            .chunks(BATCH)
            .map(|chunk| json!({"assets_ids": chunk, "operation": "unsubscribe"}).to_string())
            .collect()
    }

    fn request_snapshots(&mut self, _sid: u64, _markets: &[String]) -> Option<Vec<String>> {
        None
    }

    fn decode(&mut self, frame: &str) -> anyhow::Result<Vec<Decoded>> {
        decode(frame)
    }

    fn keepalive(&self) -> Message {
        Message::text("PING")
    }
}

pub fn decode(frame: &str) -> anyhow::Result<Vec<Decoded>> {
    if frame == "PONG" {
        return Ok(Vec::new());
    }
    let v: Value = serde_json::from_str(frame).context("malformed frame")?;
    let events = match v {
        Value::Array(a) => a,
        other => vec![other],
    };
    let mut out = Vec::new();
    for e in events {
        match e["event_type"].as_str().unwrap_or("") {
            "book" => {
                let book: Book = serde_json::from_value(e).context("book")?;
                out.push(Decoded::Snapshot {
                    sid_seq: None,
                    requested: false,
                    exch_us: ms_to_us(&book.timestamp),
                    bids: levels(&book.bids)?,
                    asks: levels(&book.asks)?,
                    market: book.asset_id,
                });
            }
            "price_change" => {
                let pc: PriceChanges = serde_json::from_value(e).context("price_change")?;
                let exch_us = ms_to_us(&pc.timestamp);
                for c in pc.price_changes {
                    let side = match c.side.as_str() {
                        "BUY" => Side::Buy,
                        "SELL" => Side::Sell,
                        other => bail!("unknown side {other:?}"),
                    };
                    out.push(Decoded::Level {
                        sid_seq: None,
                        market: c.asset_id,
                        side,
                        price: price(&c.price)?,
                        change: Change::Absolute(parse_fixed(&c.size)?),
                        exch_us,
                    });
                }
            }
            _ => out.push(Decoded::Other { sid_seq: None }),
        }
    }
    Ok(out)
}

#[derive(Deserialize)]
struct Level {
    price: String,
    size: String,
}

#[derive(Deserialize)]
struct Book {
    asset_id: String,
    #[serde(default)]
    timestamp: String,
    bids: Vec<Level>,
    asks: Vec<Level>,
}

#[derive(Deserialize)]
struct PriceChange {
    asset_id: String,
    price: String,
    size: String,
    side: String,
}

#[derive(Deserialize)]
struct PriceChanges {
    #[serde(default)]
    timestamp: String,
    price_changes: Vec<PriceChange>,
}

fn price(s: &str) -> anyhow::Result<i64> {
    let p = parse_fixed(s)?;
    anyhow::ensure!((0..=SCALE).contains(&p), "price {s} outside [0, 1]");
    Ok(p)
}

/// Book levels, dropping zero sizes (absent level).
fn levels(raw: &[Level]) -> anyhow::Result<Vec<(i64, i64)>> {
    let mut out = Vec::with_capacity(raw.len());
    for l in raw {
        let size = parse_fixed(&l.size)?;
        if size > 0 {
            out.push((price(&l.price)?, size));
        }
    }
    Ok(out)
}

fn ms_to_us(ms: &str) -> Option<i64> {
    ms.parse::<i64>().ok().map(|ms| ms * 1000)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Shapes as documented for the market channel.
    const BOOK: &str = r#"{"event_type":"book","asset_id":"65818619657568813474341868652308942079804919287380422192892211131408793125422","market":"0xbd31","bids":[{"price":"0.48","size":"30"},{"price":"0.49","size":"20"},{"price":"0.47","size":"0"}],"asks":[{"price":"0.52","size":"25"}],"timestamp":"1757908892351","hash":"0xabc"}"#;
    const CHANGE: &str = r#"{"market":"0x5f65","price_changes":[{"asset_id":"A1","price":"0.5","size":"200","side":"BUY","hash":"h","best_bid":"0.5","best_ask":"1"},{"asset_id":"A2","price":"0.5","size":"0","side":"SELL","hash":"h","best_bid":"0","best_ask":"0.5"}],"timestamp":"1757908892351","event_type":"price_change"}"#;

    #[test]
    fn book_is_a_snapshot_without_zero_levels() {
        let d = decode(BOOK).unwrap();
        let Decoded::Snapshot {
            bids,
            asks,
            exch_us,
            sid_seq,
            ..
        } = &d[0]
        else {
            panic!()
        };
        assert_eq!(bids, &vec![(480_000, 30_000_000), (490_000, 20_000_000)]);
        assert_eq!(asks, &vec![(520_000, 25_000_000)]);
        assert_eq!(*exch_us, Some(1_757_908_892_351_000));
        assert_eq!(*sid_seq, None);
    }

    #[test]
    fn price_change_is_an_absolute_size_per_asset() {
        let d = decode(CHANGE).unwrap();
        assert_eq!(d.len(), 2);
        assert!(
            matches!(&d[0], Decoded::Level { market, side: Side::Buy, price: 500_000, change: Change::Absolute(200_000_000), .. } if market == "A1")
        );
        assert!(matches!(
            &d[1],
            Decoded::Level {
                side: Side::Sell,
                change: Change::Absolute(0),
                ..
            }
        ));
    }

    #[test]
    fn arrays_pong_and_other_events() {
        assert!(decode("PONG").unwrap().is_empty());
        let arr = format!("[{BOOK},{{\"event_type\":\"last_trade_price\"}}]");
        let d = decode(&arr).unwrap();
        assert!(matches!(d[0], Decoded::Snapshot { .. }));
        assert_eq!(d[1], Decoded::Other { sid_seq: None });
        assert!(decode(r#"{"event_type":"book","asset_id":"x","bids":[{"price":"1.5","size":"1"}],"asks":[]}"#).is_err());
    }

    #[test]
    fn discovery_expands_tradeable_markets_into_token_books() {
        let raw: RawMarket = serde_json::from_str(
            r#"{"clobTokenIds":"[\"T1\",\"T2\"]","enableOrderBook":true,"acceptingOrders":true,"volume24hr":1234.56}"#,
        )
        .unwrap();
        let books = raw.into_books();
        assert_eq!(books.len(), 2);
        assert_eq!(
            books[0],
            MarketInfo {
                ticker: "T1".into(),
                volume_24h: 1234
            }
        );
        let closed: RawMarket =
            serde_json::from_str(r#"{"clobTokenIds":"[\"T3\"]","enableOrderBook":false}"#).unwrap();
        assert!(closed.into_books().is_empty());
    }

    #[test]
    fn subscribe_then_operation_commands() {
        let mut s = PolySession {
            initial_sent: false,
        };
        let first = s.on_connect(&["A".into()]);
        assert!(first[0].contains(r#""type":"market""#));
        let more = s.subscribe(&["B".into()]);
        assert!(more[0].contains(r#""operation":"subscribe""#));
        assert!(s.unsubscribe(&["A".into()])[0].contains("unsubscribe"));
        assert!(s.request_snapshots(0, &[]).is_none());
    }
}
