//! Connection state machine driven by Polymarket-shaped frames: no sequence numbers,
//! `book` = snapshot, `price_change` = absolute size at a level.

use engineering_challenge::{
    Side,
    conn::{Action, ConnState},
    exchange::{Exchange, polymarket},
    model::{ConnEvent, SnapCause},
};

fn state() -> ConnState {
    let pm = polymarket::Polymarket::new(String::new());
    let mut s = ConnState::new(0, &["A1".into()], pm.session(false));
    s.subscribe_cmds();
    s
}

fn book(bid_size: &str) -> String {
    format!(
        r#"{{"event_type":"book","asset_id":"A1","market":"0x1","bids":[{{"price":"0.48","size":"{bid_size}"}}],"asks":[{{"price":"0.52","size":"25"}}],"timestamp":"1757908892351"}}"#
    )
}

fn change(size: &str) -> String {
    format!(
        r#"{{"event_type":"price_change","market":"0x1","price_changes":[{{"asset_id":"A1","price":"0.48","size":"{size}","side":"BUY"}}],"timestamp":"1757908892352"}}"#
    )
}

fn updates(a: Vec<Action>) -> Vec<(Side, i64, i64)> {
    a.into_iter()
        .filter_map(|a| match a {
            Action::Event(ConnEvent::Update {
                side, price, qty, ..
            }) => Some((side, price, qty)),
            _ => None,
        })
        .collect()
}

#[test]
fn changes_before_the_book_are_dropped_then_sizes_replace_levels() {
    let mut s = state();
    assert!(
        updates(s.on_frame(&change("5"), 1)).is_empty(),
        "no book yet"
    );
    assert_eq!(s.stats.dropped_invalid, 1);
    let a = s.on_frame(&book("30"), 2);
    assert!(matches!(
        &a[0],
        Action::Event(ConnEvent::Snapshot {
            cause: SnapCause::Subscribed,
            exch_us: 1_757_908_892_351_000,
            ..
        })
    ));
    // Absolute sizes: 200 then 50 leaves 50 (a delta reading would give 280).
    assert_eq!(
        updates(s.on_frame(&change("200"), 3)),
        [(Side::Buy, 480_000, 200_000_000)]
    );
    assert_eq!(
        updates(s.on_frame(&change("50"), 4)),
        [(Side::Buy, 480_000, 50_000_000)]
    );
    assert_eq!(s.stats.seq_gaps, 0, "no sequence numbers to check");
}

#[test]
fn a_zero_size_removes_the_level_and_another_book_resets() {
    let mut s = state();
    s.on_frame(&book("30"), 1);
    assert_eq!(
        updates(s.on_frame(&change("0"), 2)),
        [(Side::Buy, 480_000, 0)]
    );
    // A later `book` for the same asset is the exchange resetting it mid-stream.
    let a = s.on_frame(&book("7"), 3);
    match &a[0] {
        Action::Event(ConnEvent::Snapshot {
            cause: SnapCause::Exchange,
            book,
            ..
        }) => {
            assert_eq!(book.levels().0, vec![(480_000, 7_000_000)]);
        }
        other => panic!("{other:?}"),
    }
}
