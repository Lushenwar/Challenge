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
