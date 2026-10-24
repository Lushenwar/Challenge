//! Pure decoder: one Kalshi websocket text frame → [`Decoded`]. No I/O, no clocks.
//!
//! Prices come out already mapped to the YES-quoted book: YES levels are bids at p,
//! NO levels are asks at `SCALE - q` (see [`map_side`], the only place this happens).

use anyhow::{Context, bail};
use serde::Deserialize;

use crate::{
    Side,
    fixed::{SCALE, parse_fixed},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decoded {
    Snapshot {
        sid: u64,
        seq: u64,
        /// Set when this answers our `get_snapshot` command.
        id: Option<u64>,
        ticker: String,
        bids: Vec<(i64, i64)>,
        asks: Vec<(i64, i64)>,
        sending_ts_ms: Option<i64>,
    },
    Delta {
        sid: u64,
        seq: u64,
        ticker: String,
        side: Side,
        price: i64,
        delta: i64,
        ts_ms: Option<i64>,
        sending_ts_ms: Option<i64>,
    },
    Lifecycle {
        sid: u64,
        seq: u64,
        ticker: String,
        event: Lifecycle,
    },
    Subscribed {
        id: Option<u64>,
        channel: String,
        sid: u64,
    },
    /// Ack for a merged subscribe / `update_subscription`. Carries a `seq` on the
    /// order-book sid, so it counts in that sid's sequence.
    Ok {
        id: Option<u64>,
        sid: Option<u64>,
        seq: Option<u64>,
        tickers: Vec<String>,
    },
    Unsubscribed {
        id: Option<u64>,
        sid: Option<u64>,
    },
    Error {
        id: Option<u64>,
        code: i64,
        msg: String,
    },
    /// A frame type we do not use (e.g. `event_lifecycle`). It may still carry a
    /// `seq` on a sid we track, so the sequence check must see it.
    Other {
        kind: String,
        sid_seq: Option<(u64, u64)>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Lifecycle {
    Created,
    Activated,
    /// `deactivated` with `is_deactivated: true`. Removes the market.
    Deactivated,
    /// `deactivated` with `is_deactivated: false`: trading resumed.
    Reactivated,
    Determined,
    Settled,
    Other(String),
}

impl Decoded {
    /// `(sid, seq)` for frames that take part in a subscription's sequence.
    pub fn sid_seq(&self) -> Option<(u64, u64)> {
        match *self {
            Self::Snapshot { sid, seq, .. }
            | Self::Delta { sid, seq, .. }
            | Self::Lifecycle { sid, seq, .. } => Some((sid, seq)),
            Self::Ok {
                sid: Some(sid),
                seq: Some(seq),
                ..
            } => Some((sid, seq)),
            Self::Other { sid_seq, .. } => sid_seq,
            _ => None,
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Wire {
    OrderbookSnapshot {
        sid: u64,
        seq: u64,
        id: Option<u64>,
        sending_ts_ms: Option<i64>,
        msg: SnapshotMsg,
    },
    OrderbookDelta {
        sid: u64,
        seq: u64,
        sending_ts_ms: Option<i64>,
        msg: DeltaMsg,
    },
    #[serde(rename = "market_lifecycle_v2")]
    Lifecycle {
        sid: u64,
        seq: u64,
        msg: LifecycleMsg,
    },
    Subscribed {
        id: Option<u64>,
        msg: SubscribedMsg,
    },
    Ok {
        id: Option<u64>,
        sid: Option<u64>,
        seq: Option<u64>,
        #[serde(default)]
        msg: OkMsg,
    },
    Unsubscribed {
        id: Option<u64>,
        sid: Option<u64>,
    },
    Error {
        id: Option<u64>,
        msg: ErrorMsg,
    },
}

#[derive(Deserialize)]
struct SnapshotMsg {
    market_ticker: String,
    // ponytail: JSON null is read as an absent (empty) side; Kalshi omits empty sides.
    yes_dollars_fp: Option<Vec<(String, String)>>,
    no_dollars_fp: Option<Vec<(String, String)>>,
}

#[derive(Deserialize)]
struct DeltaMsg {
    market_ticker: String,
    price_dollars: String,
    delta_fp: String,
    side: String,
    ts_ms: Option<i64>,
}

#[derive(Deserialize)]
struct LifecycleMsg {
    market_ticker: String,
    event_type: String,
    is_deactivated: Option<bool>,
}

#[derive(Deserialize)]
struct SubscribedMsg {
    channel: String,
    sid: u64,
}

#[derive(Deserialize, Default)]
struct OkMsg {
    #[serde(default)]
    market_tickers: Vec<String>,
}

#[derive(Deserialize)]
struct ErrorMsg {
    code: i64,
    #[serde(default)]
    msg: String,
}

/// Map a Kalshi side + price (both sides quote what that side's buyer pays) onto the
/// YES-quoted book. Used by both snapshot and delta paths.
pub fn map_side(side: &str, price: i64) -> anyhow::Result<(Side, i64)> {
    anyhow::ensure!((0..=SCALE).contains(&price), "price {price} outside [0, 1]");
    match side {
        "yes" => Ok((Side::Buy, price)),
        "no" => Ok((Side::Sell, SCALE - price)),
        other => bail!("unknown side {other:?}"),
    }
}

fn levels(side: &str, raw: Option<Vec<(String, String)>>) -> anyhow::Result<Vec<(i64, i64)>> {
    raw.unwrap_or_default()
        .iter()
        .map(|(p, q)| {
            let (_, price) = map_side(
                side,
                parse_fixed(p).with_context(|| format!("price {p:?}"))?,
            )?;
            Ok((
                price,
                parse_fixed(q).with_context(|| format!("quantity {q:?}"))?,
            ))
        })
        .collect()
}

pub fn decode(frame: &str) -> anyhow::Result<Decoded> {
    let wire: Wire = match serde_json::from_str(frame) {
        Ok(w) => w,
        Err(e) => {
            // Unknown `type` values are not errors; anything else malformed is.
            #[derive(Deserialize)]
            struct Kind {
                #[serde(rename = "type")]
                kind: String,
                sid: Option<u64>,
                seq: Option<u64>,
            }
            return match serde_json::from_str::<Kind>(frame) {
                Ok(k) if !KNOWN.contains(&k.kind.as_str()) => Ok(Decoded::Other {
                    kind: k.kind,
                    sid_seq: k.sid.zip(k.seq),
                }),
                _ => Err(e).context("malformed Kalshi frame"),
            };
        }
    };
    Ok(match wire {
        Wire::OrderbookSnapshot {
            sid,
            seq,
            id,
            sending_ts_ms,
            msg,
        } => {
            let mut asks = levels("no", msg.no_dollars_fp)?;
            asks.sort_unstable(); // mapped NO prices come out reversed
            Decoded::Snapshot {
                sid,
                seq,
                id,
                bids: levels("yes", msg.yes_dollars_fp)?,
                asks,
                ticker: msg.market_ticker,
                sending_ts_ms,
            }
        }
        Wire::OrderbookDelta {
            sid,
            seq,
            sending_ts_ms,
            msg,
        } => {
            let (side, price) = map_side(&msg.side, parse_fixed(&msg.price_dollars)?)?;
            Decoded::Delta {
                sid,
                seq,
                side,
                price,
                delta: parse_fixed(&msg.delta_fp)?,
                ticker: msg.market_ticker,
                ts_ms: msg.ts_ms,
                sending_ts_ms,
            }
        }
        Wire::Lifecycle { sid, seq, msg } => Decoded::Lifecycle {
            sid,
            seq,
            event: match msg.event_type.as_str() {
                "created" => Lifecycle::Created,
                "activated" => Lifecycle::Activated,
                "deactivated" if msg.is_deactivated == Some(false) => Lifecycle::Reactivated,
                "deactivated" => Lifecycle::Deactivated,
                "determined" => Lifecycle::Determined,
                "settled" => Lifecycle::Settled,
                _ => Lifecycle::Other(msg.event_type),
            },
            ticker: msg.market_ticker,
        },
        Wire::Subscribed { id, msg } => Decoded::Subscribed {
            id,
            channel: msg.channel,
            sid: msg.sid,
        },
        Wire::Ok { id, sid, seq, msg } => Decoded::Ok {
            id,
            sid,
            seq,
            tickers: msg.market_tickers,
        },
        Wire::Unsubscribed { id, sid } => Decoded::Unsubscribed { id, sid },
        Wire::Error { id, msg } => Decoded::Error {
            id,
            code: msg.code,
            msg: msg.msg,
        },
    })
}

const KNOWN: &[&str] = &[
    "orderbook_snapshot",
    "orderbook_delta",
    "market_lifecycle_v2",
    "subscribed",
    "ok",
    "unsubscribed",
    "error",
];

#[cfg(test)]
mod tests {
    use super::*;

    // Real frames from a 2026-10-24 prod recording (snapshot trimmed to a few levels).
    const SNAPSHOT: &str = r#"{"type":"orderbook_snapshot","sid":2,"seq":3,"msg":{"market_ticker":"KXUFCFIGHT-26OCT24YANDVA-DVA","market_id":"01a132af-9f7f-7697-9c18-61cfead59483","yes_dollars_fp":[["0.0100","26794.95"],["0.0200","737.00"]],"no_dollars_fp":[["0.5000","10.00"],["0.7000","3.50"]]}}"#;
    const DELTA: &str = r#"{"type":"orderbook_delta","sid":2,"seq":6,"msg":{"market_ticker":"KXUFCFIGHT-26OCT24YANDVA-DVA","market_id":"01a132af-9f7f-7697-9c18-61cfead59483","price_dollars":"0.2100","delta_fp":"-628.00","side":"no","ts":"2026-10-24T20:06:59.842Z","ts_ms":1792872419842},"sending_ts_ms":1792872419849}"#;

    #[test]
    fn snapshot_maps_no_to_asks_ascending() {
        let Decoded::Snapshot {
            bids,
            asks,
            sid,
            seq,
            id,
            ..
        } = decode(SNAPSHOT).unwrap()
        else {
            panic!()
        };
        assert_eq!((sid, seq, id), (2, 3, None));
        assert_eq!(bids, vec![(10_000, 26_794_950_000), (20_000, 737_000_000)]);
        assert_eq!(asks, vec![(300_000, 3_500_000), (500_000, 10_000_000)]);
    }

    #[test]
    fn absent_side_is_empty() {
        let f = r#"{"type":"orderbook_snapshot","sid":1,"seq":1,"msg":{"market_ticker":"T"}}"#;
        let Decoded::Snapshot { bids, asks, .. } = decode(f).unwrap() else {
            panic!()
        };
        assert!(bids.is_empty() && asks.is_empty());
    }

    #[test]
    fn no_delta_is_a_yes_ask() {
        let d = decode(DELTA).unwrap();
        assert_eq!(
            d,
            Decoded::Delta {
                sid: 2,
                seq: 6,
                ticker: "KXUFCFIGHT-26OCT24YANDVA-DVA".into(),
                side: Side::Sell,
                price: 790_000,
                delta: -628_000_000,
                ts_ms: Some(1_792_872_419_842),
                sending_ts_ms: Some(1_792_872_419_849),
            }
        );
        assert_eq!(d.sid_seq(), Some((2, 6)));
    }

    #[test]
    fn control_frames() {
        let ok = r#"{"type":"ok","id":3,"sid":2,"seq":1,"msg":{"market_tickers":["A","B"]},"sending_ts_ms":1}"#;
        let ok = decode(ok).unwrap();
        assert_eq!(ok.sid_seq(), Some((2, 1)));
        assert!(matches!(ok, Decoded::Ok { id: Some(3), ref tickers, .. } if tickers.len() == 2));
        assert_eq!(
            decode(r#"{"type":"error","id":3,"msg":{"code":8,"msg":"Unknown channel name"}}"#)
                .unwrap(),
            Decoded::Error {
                id: Some(3),
                code: 8,
                msg: "Unknown channel name".into()
            }
        );
        assert_eq!(
            decode(
                r#"{"type":"subscribed","id":1,"msg":{"channel":"market_lifecycle_v2","sid":1}}"#
            )
            .unwrap(),
            Decoded::Subscribed {
                id: Some(1),
                channel: "market_lifecycle_v2".into(),
                sid: 1
            }
        );
        let other = decode(r#"{"type":"event_lifecycle","sid":1,"seq":7,"msg":{}}"#).unwrap();
        assert_eq!(other.sid_seq(), Some((1, 7)));
        assert_eq!(
            decode(r#"{"type":"brand_new","x":1}"#).unwrap(),
            Decoded::Other {
                kind: "brand_new".into(),
                sid_seq: None
            }
        );
    }

    #[test]
    fn lifecycle_events() {
        let f = |e: &str, extra: &str| {
            let s = format!(
                r#"{{"type":"market_lifecycle_v2","sid":1,"seq":9,"msg":{{"market_ticker":"T","event_type":"{e}"{extra}}}}}"#
            );
            match decode(&s).unwrap() {
                Decoded::Lifecycle { event, .. } => event,
                d => panic!("{d:?}"),
            }
        };
        assert_eq!(f("settled", ""), Lifecycle::Settled);
        assert_eq!(
            f("deactivated", r#","is_deactivated":true"#),
            Lifecycle::Deactivated
        );
        assert_eq!(
            f("deactivated", r#","is_deactivated":false"#),
            Lifecycle::Reactivated
        );
        assert_eq!(
            f("metadata_updated", r#","floor_strike":80453.13"#),
            Lifecycle::Other("metadata_updated".into())
        );
    }

    #[test]
    fn rejects_malformed_known_frames() {
        for f in [
            r#"{"type":"orderbook_delta","sid":1,"seq":1,"msg":{"market_ticker":"T","price_dollars":"0.5","delta_fp":"1","side":"maybe"}}"#,
            r#"{"type":"orderbook_delta","sid":1,"seq":1,"msg":{"market_ticker":"T","price_dollars":"1.5","delta_fp":"1","side":"no"}}"#,
            r#"{"type":"orderbook_snapshot","sid":1,"seq":1,"msg":{"market_ticker":"T","yes_dollars_fp":[["0.40"]]}}"#,
            r#"{"type":"orderbook_snapshot","sid":1,"seq":1,"msg":{"market_ticker":"T","yes_dollars_fp":[["0.4","1e3"]]}}"#,
            r#"{"type":"orderbook_delta","sid":1,"msg":{}}"#,
            "not json",
        ] {
            assert!(decode(f).is_err(), "{f}");
        }
    }
}
