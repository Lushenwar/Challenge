//! Discovery: page through `GET /markets?status=open&mve_filter=exclude`.
//!
//! A pass is all-or-nothing: a failed page or a repeated cursor aborts it, so
//! callers never mistake a partial list for the full market set.

use std::{collections::HashSet, time::Duration};

use anyhow::{Context, bail};
use serde::Deserialize;
use serde_json::Value;

const PAGE_LIMIT: u32 = 1000;
const PAGE_RETRIES: u32 = 5;

pub use crate::exchange::MarketInfo;

#[derive(Deserialize)]
struct Page {
    #[serde(default)]
    cursor: String,
    markets: Vec<RawMarket>,
}

#[derive(Deserialize)]
struct RawMarket {
    ticker: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    market_type: String,
    #[serde(default)]
    mve_collection_ticker: Option<String>,
    #[serde(default)]
    mve_selected_legs: Option<Value>,
    #[serde(default)]
    volume_24h_fp: String,
}

impl RawMarket {
    fn into_market(self) -> Option<MarketInfo> {
        let is_mve = self.market_type == "mve"
            || self.mve_collection_ticker.is_some_and(|t| !t.is_empty())
            || self
                .mve_selected_legs
                .is_some_and(|l| !l.is_null() && l.as_array().is_none_or(|a| !a.is_empty()));
        let live = matches!(self.status.as_str(), "open" | "active");
        (live && !is_mve).then(|| MarketInfo {
            volume_24h: self
                .volume_24h_fp
                .split('.')
                .next()
                .and_then(|w| w.parse().ok())
                .unwrap_or(0),
            ticker: self.ticker,
        })
    }
}

/// One complete discovery pass. Errors if any page fails after retries.
pub async fn discover(
    client: &reqwest::Client,
    rest_base: &str,
) -> anyhow::Result<Vec<MarketInfo>> {
    let mut markets = Vec::new();
    let mut seen_cursors = HashSet::new();
    let mut cursor = String::new();
    loop {
        let page = fetch_page(client, rest_base, &cursor).await?;
        markets.extend(page.markets.into_iter().filter_map(RawMarket::into_market));
        if page.cursor.is_empty() {
            return Ok(markets);
        }
        if !seen_cursors.insert(page.cursor.clone()) {
            bail!("discovery cursor repeated; aborting pass");
        }
        cursor = page.cursor;
    }
}

async fn fetch_page(
    client: &reqwest::Client,
    rest_base: &str,
    cursor: &str,
) -> anyhow::Result<Page> {
    let url = format!("{rest_base}/markets");
    let limit = PAGE_LIMIT.to_string();
    let mut query = vec![
        ("status", "open"),
        ("mve_filter", "exclude"),
        ("limit", &limit),
    ];
    if !cursor.is_empty() {
        query.push(("cursor", cursor));
    }
    let mut backoff = Duration::from_millis(500);
    for attempt in 1..=PAGE_RETRIES {
        let result = client.get(&url).query(&query).send().await;
        match result {
            Ok(resp) if resp.status().is_success() => {
                return resp.json().await.context("decoding /markets page");
            }
            Ok(resp) if resp.status().as_u16() == 429 || resp.status().is_server_error() => {
                tracing::warn!(status = %resp.status(), attempt, "discovery page retry");
            }
            Ok(resp) => bail!("discovery page failed: HTTP {}", resp.status()),
            Err(e) => tracing::warn!(error = %e, attempt, "discovery page retry"),
        }
        tokio::time::sleep(backoff).await;
        backoff *= 2;
    }
    bail!("discovery page failed after {PAGE_RETRIES} attempts")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(json: &str) -> RawMarket {
        serde_json::from_str(json).unwrap()
    }

    fn page(tickers: &[&str], cursor: &str) -> (u16, String) {
        let markets: Vec<String> = tickers
            .iter()
            .map(|t| format!(r#"{{"ticker":"{t}","status":"active"}}"#))
            .collect();
        (
            200,
            format!(
                r#"{{"markets":[{}],"cursor":"{cursor}"}}"#,
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
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        let res = rt.block_on(discover(&client, &url));
        (res, rx.try_iter().collect())
    }

    fn tickers(r: anyhow::Result<Vec<MarketInfo>>) -> Vec<String> {
        r.unwrap().into_iter().map(|m| m.ticker).collect()
    }

    #[test]
    fn follows_the_cursor_to_an_empty_one() {
        let (res, reqs) = run(vec![page(&["A", "B"], "c1"), page(&["C"], "")]);
        assert_eq!(tickers(res), ["A", "B", "C"]);
        assert!(reqs[0].contains("status=open") && reqs[0].contains("mve_filter=exclude"));
        assert!(!reqs[0].contains("cursor=") && reqs[1].contains("cursor=c1"));
    }

    /// A pass that cannot be completed must fail as a whole, or the caller would take a
    /// partial list for the full market set and remove live markets.
    #[test]
    fn a_repeated_cursor_fails_the_pass() {
        let (res, _) = run(vec![
            page(&["A"], "c1"),
            page(&["B"], "c1"),
            page(&["C"], ""),
        ]);
        assert!(res.is_err());
    }

    #[test]
    fn rate_limited_pages_are_retried() {
        let (res, reqs) = run(vec![(429, "{}".into()), page(&["A"], "")]);
        assert_eq!(tickers(res), ["A"]);
        assert_eq!(reqs.len(), 2);
    }

    #[test]
    fn a_client_error_fails_the_pass_without_retrying() {
        let (res, reqs) = run(vec![
            page(&["A"], "c1"),
            (404, "{}".into()),
            page(&["B"], ""),
        ]);
        assert!(res.is_err());
        assert_eq!(reqs.len(), 2);
    }

    #[test]
    fn keeps_live_binary_markets_and_drops_mve() {
        let m = raw(
            r#"{"ticker":"A","status":"active","market_type":"binary","volume_24h_fp":"12.50"}"#,
        );
        assert_eq!(
            m.into_market(),
            Some(MarketInfo {
                ticker: "A".into(),
                volume_24h: 12
            })
        );
        for j in [
            r#"{"ticker":"B","status":"active","market_type":"mve"}"#,
            r#"{"ticker":"C","status":"active","mve_collection_ticker":"KXMVE"}"#,
            r#"{"ticker":"D","status":"active","mve_selected_legs":[{"market":"X"}]}"#,
            r#"{"ticker":"E","status":"closed"}"#,
        ] {
            assert_eq!(raw(j).into_market(), None, "{j}");
        }
        let empty_legs = raw(
            r#"{"ticker":"F","status":"open","mve_collection_ticker":"","mve_selected_legs":[]}"#,
        );
        assert!(empty_legs.into_market().is_some());
    }
}
