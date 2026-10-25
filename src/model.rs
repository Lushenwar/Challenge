//! Messages between components: connection tasks → arbiter → writer.

use std::time::{SystemTime, UNIX_EPOCH};

use crate::{
    Side,
    book::OrderBook,
    exchange::kalshi::codec::Lifecycle,
    util::dtos::challenge_orderbook::{PriceLevelUpdateDomain, SnapshotDomain},
};

pub type ConnId = usize;

/// Microseconds since the Unix epoch, local wall clock.
pub fn now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before 1970")
        .as_micros() as i64
}

/// Why a snapshot reached the output; logged so `verify` can explain each one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapCause {
    /// First snapshot of a market on this connection (subscribe or reconnect).
    Subscribed,
    /// Reply to our `get_snapshot` after a gap or a bad delta.
    Resync,
    /// Sent by the exchange on its own mid-stream (e.g. book cleared at close).
    Exchange,
    Periodic,
    Shutdown,
    /// Terminal empty snapshot: the market stopped trading.
    Removed,
    /// The active source was lost; this is the new source's book.
    Failover,
}

/// Facts a connection reports. It never decides what is written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnEvent {
    Snapshot {
        conn: ConnId,
        market: String,
        book: OrderBook,
        recv_us: i64,
        exch_us: i64,
        cause: SnapCause,
    },
    /// `qty` is the new absolute quantity at the level (0 = removed).
    Update {
        conn: ConnId,
        market: String,
        side: Side,
        price: i64,
        qty: i64,
        recv_us: i64,
        exch_us: i64,
    },
    /// This connection's book for `market` can no longer be trusted.
    Invalidated { conn: ConnId, market: String },
    /// Every market on the connection is invalid until it resubscribes.
    Disconnected { conn: ConnId },
    Lifecycle {
        conn: ConnId,
        market: String,
        event: Lifecycle,
    },
    /// The connection stopped collecting `market` (control removed it). Comes after
    /// every earlier event for that market on this connection.
    Removed { conn: ConnId, market: String },
}

#[derive(Debug, PartialEq)]
pub enum OutMsg {
    Snapshot(SnapshotDomain),
    Update(PriceLevelUpdateDomain),
}
