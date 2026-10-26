//! Polymarket public CLOB market channel. No auth, no sequence numbers: recovery is
//! resubscribe. `book` = full snapshot of one asset (outcome token); `price_change` =
//! the new absolute size at a level. Each asset id is its own book and output symbol.

use anyhow::{Context, bail};
use std::collections::HashSet;

use futures_util::{FutureExt as _, future::BoxFuture};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::task::JoinHandle;
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
/// Gamma returns at most 100 markets per page (offset and keyset listings alike).
const PAGE: usize = 100;
/// Offset pagination refuses offsets past ~2,000 (HTTP 422): the volume-ranked listing
/// that decides what is collected covers the ~2,000 most active markets (~4k books).
// ponytail: collects the most active markets only; the complete listing is 243k markets
// / 486k books and takes 24 min to page, so it is used for removals, not subscriptions.
const MAX_OFFSET: usize = 1_900;
const BATCH: usize = 500;

pub struct Polymarket {
    /// Gamma REST base URL (normally [`GAMMA`]).
    gamma: String,
    /// The background complete (keyset) listing in progress, if any.
    full_pass: std::sync::Mutex<Option<JoinHandle<anyhow::Result<HashSet<String>>>>>,
}

impl Polymarket {
    pub fn new(gamma: String) -> Self {
        Self {
            gamma,
            full_pass: Default::default(),
        }
    }
}

impl Exchange for Polymarket {
    fn name(&self) -> &'static str {
        "polymarket"
    }

    fn ws_request(&self, url: Option<&str>) -> anyhow::Result<Request> {
        Ok(url.unwrap_or(WS_URL).into_client_request()?)
    }

    /// The volume-ranked listing is capped; removals come from [`Self::live_markets`].
    fn discovery_is_complete(&self) -> bool {
        false
    }

    /// Runs the complete keyset listing in the background (~24 min); returns its result
    /// once per finished pass and starts the next one.
    fn live_markets(&self, client: &reqwest::Client) -> Option<HashSet<String>> {
        let mut slot = self.full_pass.lock().expect("full_pass lock poisoned");
        if slot.as_ref().is_some_and(|h| !h.is_finished()) {
            return None;
        }
        let done = slot.take().and_then(|h| h.now_or_never());
        let (client, gamma) = (client.clone(), self.gamma.clone());
        *slot = Some(tokio::spawn(async move {
            let all = keyset(&client, &gamma).await?;
            tracing::info!(books = all.len(), "complete market listing finished");
            Ok(all.into_iter().map(|m| m.ticker).collect())
        }));
        match done? {
            Ok(Ok(set)) => Some(set),
            Ok(Err(e)) => {
                tracing::warn!(error = %format!("{e:#}"), "complete market listing failed; nothing removed");
                None
            }
            Err(e) => {
                tracing::warn!(error = %e, "complete market listing task failed");
                None
            }
        }
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

/// The ~2,000 most active markets (24 h volume), via offset pages.
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

/// Every open market via `/markets/keyset`, continued with `after_cursor`. Any failed
/// page or repeated cursor fails the whole pass (so nothing is removed on a partial list).
async fn keyset(client: &reqwest::Client, gamma: &str) -> anyhow::Result<Vec<MarketInfo>> {
    let mut out = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let mut cursor: Option<String> = None;
    loop {
        let mut url = format!("{gamma}/markets/keyset?active=true&closed=false&limit={PAGE}");
        if let Some(c) = &cursor {
            url.push_str("&after_cursor=");
            url.push_str(c);
        }
        let resp = client.get(&url).send().await?;
        if !resp.status().is_success() {
            bail!("gamma page failed: HTTP {}", resp.status());
        }
        let page: KeysetPage = resp.json().await.context("decoding gamma page")?;
        let done = page.markets.is_empty();
        out.extend(page.markets.into_iter().flat_map(RawMarket::into_books));
        match page.next_cursor.filter(|c| !c.is_empty()) {
            Some(c) if !done => {
                if !seen.insert(c.clone()) {
                    bail!("gamma repeated a cursor; aborting the pass");
                }
                cursor = Some(c);
            }
            _ => break,
        }
    }
    Ok(out)
}

#[derive(Deserialize)]
struct KeysetPage {
    #[serde(default)]
    markets: Vec<RawMarket>,
    next_cursor: Option<String>,
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

    fn page(tokens: &[&str], next: Option<&str>) -> (u16, String) {
        let markets: Vec<String> = tokens
            .iter()
            .map(|t| format!(r#"{{"clobTokenIds":"[\"{t}\"]","enableOrderBook":true}}"#))
            .collect();
        let next = next.map_or("null".to_owned(), |c| format!("\"{c}\""));
        (
            200,
            format!(
                r#"{{"markets":[{}],"next_cursor":{next}}}"#,
                markets.join(",")
            ),
        )
    }

    fn run(pages: Vec<(u16, String)>) -> (anyhow::Result<Vec<MarketInfo>>, Vec<String>) {
        let (url, rx) = crate::exchange::stub_http::serve(pages);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();
        let res = rt.block_on(keyset(&client, &url));
        (res, rx.try_iter().collect())
    }

    #[test]
    fn keyset_discovery_follows_after_cursor_to_the_end() {
        let (res, reqs) = run(vec![
            page(&["A", "B"], Some("c1")),
            page(&["C"], Some("c2")),
            page(&[], None),
        ]);
        let tickers: Vec<String> = res.unwrap().into_iter().map(|m| m.ticker).collect();
        assert_eq!(tickers, ["A", "B", "C"]);
        assert!(!reqs[0].contains("after_cursor"));
        assert!(reqs[1].contains("after_cursor=c1") && reqs[2].contains("after_cursor=c2"));
    }

    #[test]
    fn live_markets_runs_the_complete_listing_in_the_background() {
        let (url, _requests) =
            crate::exchange::stub_http::serve(vec![page(&["A"], Some("c1")), page(&["B"], None)]);
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let pm = Polymarket::new(url);
        let client = reqwest::Client::new();
        rt.block_on(async {
            assert!(
                pm.live_markets(&client).is_none(),
                "first call only starts the listing"
            );
            for _ in 0..100 {
                if pm
                    .full_pass
                    .lock()
                    .unwrap()
                    .as_ref()
                    .is_some_and(|h| h.is_finished())
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            let set = pm.live_markets(&client).expect("finished listing");
            assert_eq!(set, HashSet::from(["A".to_owned(), "B".to_owned()]));
        });
    }

    #[test]
    fn keyset_discovery_aborts_on_failed_page_or_repeated_cursor() {
        let (res, _) = run(vec![page(&["A"], Some("c1")), (500, "{}".into())]);
        assert!(res.is_err(), "a failed page fails the pass");
        let (res, _) = run(vec![page(&["A"], Some("c1")), page(&["B"], Some("c1"))]);
        assert!(res.is_err(), "a repeated cursor fails the pass");
    }

    #[test]
    fn subscriptions_are_batched() {
        let mut s = PolySession {
            initial_sent: false,
        };
        let ms: Vec<String> = (0..2 * BATCH + 1).map(|i| format!("T{i}")).collect();
        let cmds = s.on_connect(&ms);
        assert_eq!(cmds.len(), 3);
        assert!(
            cmds[0].contains(r#""type":"market""#),
            "the first message opens the channel"
        );
        assert!(
            cmds[1..]
                .iter()
                .all(|c| c.contains(r#""operation":"subscribe""#))
        );
        // A reconnect starts over with `type: market`.
        assert!(s.on_connect(&ms[..1])[0].contains(r#""type":"market""#));
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
