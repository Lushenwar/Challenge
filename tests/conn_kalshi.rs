//! Connection state machine driven by Kalshi-shaped frames.

use engineering_challenge::{
    Side,
    conn::{Action, ConnState, RESYNC_TIMEOUT_US},
    exchange::{Exchange, kalshi},
    model::{ConnEvent, SnapCause},
};

fn state() -> ConnState {
    // A throwaway key: sessions only sign handshakes, which these tests never do.
    use ed25519_dalek::{SigningKey, pkcs8::EncodePrivateKey};
    let pem = SigningKey::from_bytes(&[1; 32])
        .to_pkcs8_pem(Default::default())
        .unwrap();
    let signer = kalshi::auth::Signer::new("test".into(), &pem).unwrap();
    let k = kalshi::Kalshi::new(kalshi::Env::Prod, signer, String::new());
    let mut s = ConnState::new(0, &["T".into()], k.session(false));
    s.subscribe_cmds();
    s
}

fn snap(seq: u64, id: Option<u64>, yes: &str) -> String {
    let id = id.map(|i| format!(r#""id":{i},"#)).unwrap_or_default();
    format!(
        r#"{{"type":"orderbook_snapshot",{id}"sid":2,"seq":{seq},"msg":{{"market_ticker":"T","yes_dollars_fp":[{yes}]}},"sending_ts_ms":5}}"#
    )
}

fn delta(seq: u64, d: &str) -> String {
    format!(
        r#"{{"type":"orderbook_delta","sid":2,"seq":{seq},"msg":{{"market_ticker":"T","price_dollars":"0.4000","delta_fp":"{d}","side":"yes","ts_ms":7}}}}"#
    )
}

fn events(a: Vec<Action>) -> Vec<ConnEvent> {
    a.into_iter()
        .filter_map(|a| {
            if let Action::Event(e) = a {
                Some(e)
            } else {
                None
            }
        })
        .collect()
}

#[test]
fn pending_until_subscription_snapshot_then_streams() {
    let mut s = state();
    assert!(s.on_frame(&delta(1, "5.00"), 10).is_empty()); // pending: dropped
    assert_eq!(s.stats.dropped_invalid, 1);
    assert!(
        s.on_frame(&snap(2, Some(9), r#"["0.4000","1.00"]"#), 11)
            .is_empty()
    ); // early reply
    let e = events(s.on_frame(&snap(3, None, r#"["0.4000","1.00"]"#), 12));
    assert!(matches!(
        e[0],
        ConnEvent::Snapshot {
            cause: SnapCause::Subscribed,
            recv_us: 12,
            exch_us: 5000,
            ..
        }
    ));
    let e = events(s.on_frame(&delta(4, "2.00"), 13));
    assert_eq!(
        e,
        vec![ConnEvent::Update {
            conn: 0,
            market: "T".into(),
            side: Side::Buy,
            price: 400_000,
            qty: 3_000_000,
            recv_us: 13,
            exch_us: 7000
        }]
    );
}

#[test]
fn gap_invalidates_and_requests_snapshot() {
    let mut s = state();
    s.on_frame(&snap(1, None, r#"["0.4000","1.00"]"#), 1);
    let a = s.on_frame(&delta(3, "1.00"), 2); // seq 2 missing
    assert_eq!(s.stats.seq_gaps, 1);
    assert!(matches!(
        &a[0],
        Action::Event(ConnEvent::Invalidated { .. })
    ));
    assert!(matches!(&a[1], Action::Send(c) if c.contains("get_snapshot") && c.contains("\"T\"")));
    assert_eq!(a.len(), 2); // the delta itself is dropped
    let e = events(s.on_frame(&snap(4, Some(2), r#"["0.4000","9.00"]"#), 3));
    assert!(matches!(
        e[0],
        ConnEvent::Snapshot {
            cause: SnapCause::Resync,
            ..
        }
    ));
    assert!(!s.resync_overdue(i64::MAX));
}

#[test]
fn resubscribe_deletes_resubscribes_and_waits_for_a_fresh_snapshot() {
    let mut s = state();
    s.on_frame(
        r#"{"type":"subscribed","id":1,"msg":{"channel":"orderbook_delta","sid":2}}"#,
        0,
    );
    s.on_frame(&snap(1, None, r#"["0.4000","1.00"]"#), 1);
    let a = s.resubscribe(vec!["T".into(), "UNKNOWN".into()]);
    assert!(matches!(
        &a[0],
        Action::Event(ConnEvent::Invalidated { .. })
    ));
    assert!(
        matches!(&a[1], Action::Send(c) if c.contains("delete_markets") && c.contains("\"T\""))
    );
    assert!(
        matches!(&a[2], Action::Send(c) if c.contains("\"subscribe\"") && !c.contains("UNKNOWN"))
    );
    assert_eq!(a.len(), 3);
    // Pending again: deltas dropped until the new subscription snapshot.
    assert!(s.on_frame(&delta(2, "1.00"), 2).is_empty());
    let e = events(s.on_frame(&snap(3, None, r#"["0.4000","4.00"]"#), 3));
    assert!(matches!(
        e[0],
        ConnEvent::Snapshot {
            cause: SnapCause::Subscribed,
            ..
        }
    ));
}

#[test]
fn negative_delta_invalidates_market() {
    let mut s = state();
    s.on_frame(&snap(1, None, r#"["0.4000","1.00"]"#), 1);
    let a = s.on_frame(&delta(2, "-2.00"), 2);
    assert!(matches!(
        &a[0],
        Action::Event(ConnEvent::Invalidated { .. })
    ));
    assert!(matches!(&a[1], Action::Send(c) if c.contains("get_snapshot")));
    assert!(s.resync_overdue(2 + RESYNC_TIMEOUT_US + 1));
}

fn valid() -> ConnState {
    let mut s = state();
    s.on_frame(&snap(1, None, r#"["0.4000","1.00"]"#), 1);
    s
}

#[test]
fn a_reconnect_makes_every_market_pending_again() {
    let mut s = valid();
    s.disconnected();
    let cmds = s.subscribe_cmds(); // the next connection
    assert!(
        cmds.iter()
            .any(|c| c.contains("\"subscribe\"") && c.contains("\"T\""))
    );
    // seq restarts with the new subscription; deltas wait for its snapshot.
    assert!(s.on_frame(&delta(1, "1.00"), 5).is_empty());
    assert_eq!(s.stats.seq_gaps, 0);
    let e = events(s.on_frame(&snap(2, None, r#"["0.4000","2.00"]"#), 6));
    assert!(matches!(
        e[0],
        ConnEvent::Snapshot {
            cause: SnapCause::Subscribed,
            ..
        }
    ));
}

#[test]
fn error_frames_are_counted_by_code() {
    let mut s = valid();
    let a = s.on_frame(
        r#"{"type":"error","id":4,"msg":{"code":8,"msg":"Unknown channel name"}}"#,
        2,
    );
    assert!(events(a).is_empty());
    assert_eq!(s.stats.errors.get(&8), Some(&1));
}

#[test]
fn ok_and_unknown_frames_on_the_sid_take_part_in_its_sequence() {
    let mut s = valid();
    s.on_frame(
        r#"{"type":"ok","id":2,"sid":2,"seq":2,"msg":{"market_tickers":["T"]}}"#,
        2,
    );
    s.on_frame(
        r#"{"type":"event_lifecycle","sid":2,"seq":3,"msg":{"event_ticker":"E"}}"#,
        3,
    );
    assert_eq!(events(s.on_frame(&delta(4, "1.00"), 4)).len(), 1);
    assert_eq!(
        s.stats.seq_gaps, 0,
        "seq 2 and 3 were consumed by the ok / unknown frames"
    );
}

#[test]
fn a_delta_without_exchange_time_falls_back_to_receive_time_and_is_counted() {
    let mut s = valid();
    let f = r#"{"type":"orderbook_delta","sid":2,"seq":2,"msg":{"market_ticker":"T","price_dollars":"0.4000","delta_fp":"1.00","side":"yes"}}"#;
    let e = events(s.on_frame(f, 77));
    assert!(matches!(
        e[0],
        ConnEvent::Update {
            recv_us: 77,
            exch_us: 77,
            ..
        }
    ));
    assert_eq!(s.stats.exch_ts_fallbacks, 1);
}

#[test]
fn a_removed_market_says_so_and_its_frames_are_ignored() {
    let mut s = valid();
    s.on_frame(
        r#"{"type":"subscribed","id":1,"msg":{"channel":"orderbook_delta","sid":2}}"#,
        1,
    );
    let a = s.remove_markets(&["T".into()]);
    assert!(matches!(&a[0], Action::Event(ConnEvent::Removed { .. })));
    assert!(matches!(&a[1], Action::Send(c) if c.contains("delete_markets")));
    assert!(events(s.on_frame(&delta(2, "1.00"), 2)).is_empty());
    assert!(
        s.remove_markets(&["T".into()]).is_empty(),
        "removing twice does nothing"
    );
}

#[test]
fn a_market_added_mid_stream_waits_for_its_own_snapshot() {
    let mut s = valid();
    let cmds = s.add_markets(vec!["U".into(), "T".into()]);
    assert_eq!(cmds.len(), 1);
    assert!(
        cmds[0].contains("\"U\"") && !cmds[0].contains("\"T\""),
        "only the new market"
    );
    let du = r#"{"type":"orderbook_delta","sid":2,"seq":2,"msg":{"market_ticker":"U","price_dollars":"0.1000","delta_fp":"1.00","side":"no","ts_ms":8}}"#;
    assert!(events(s.on_frame(du, 2)).is_empty(), "U is pending");
    assert_eq!(
        events(s.on_frame(&delta(3, "1.00"), 3)).len(),
        1,
        "T still streams"
    );
}

#[test]
fn garbage_frames_are_counted_not_fatal() {
    let mut s = valid();
    assert!(s.on_frame("not json", 2).is_empty());
    assert!(
        s.on_frame(r#"{"type":"orderbook_delta","sid":2}"#, 3)
            .is_empty()
    );
    assert_eq!(s.stats.decode_errors, 2);
    assert_eq!(
        events(s.on_frame(&delta(2, "1.00"), 4)).len(),
        1,
        "the stream goes on"
    );
}

#[test]
fn an_unsolicited_snapshot_mid_stream_is_an_exchange_reset() {
    let mut s = valid();
    // Kalshi clears the book at market close with an empty snapshot.
    let f = r#"{"type":"orderbook_snapshot","sid":2,"seq":2,"msg":{"market_ticker":"T"},"sending_ts_ms":9}"#;
    let e = events(s.on_frame(f, 2));
    match &e[0] {
        ConnEvent::Snapshot {
            cause: SnapCause::Exchange,
            book,
            ..
        } => {
            assert_eq!(book.levels(), (vec![], vec![]));
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_crossed_book_is_counted_not_rejected() {
    let mut s = state();
    // YES bid 0.60 and NO bid 0.50 (= YES ask 0.50): crossed.
    let f = r#"{"type":"orderbook_snapshot","sid":2,"seq":1,"msg":{"market_ticker":"T","yes_dollars_fp":[["0.6000","1.00"]],"no_dollars_fp":[["0.5000","1.00"]]},"sending_ts_ms":5}"#;
    assert_eq!(events(s.on_frame(f, 1)).len(), 1);
    let e = events(s.on_frame(&delta(2, "1.00"), 2));
    assert_eq!(e.len(), 1, "a crossed book still streams");
    assert!(s.stats.crossed >= 1);
}

#[test]
fn a_seq_gap_invalidates_every_market_on_the_sid() {
    let mut s = valid();
    s.add_markets(vec!["U".into()]);
    let su = r#"{"type":"orderbook_snapshot","sid":2,"seq":2,"msg":{"market_ticker":"U","yes_dollars_fp":[["0.1000","1.00"]]},"sending_ts_ms":6}"#;
    s.on_frame(su, 2);
    let a = s.on_frame(&delta(9, "1.00"), 3);
    let invalid: Vec<&str> = a
        .iter()
        .filter_map(|a| match a {
            Action::Event(ConnEvent::Invalidated { market, .. }) => Some(market.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(invalid.len(), 2, "both T and U: {a:?}");
}
