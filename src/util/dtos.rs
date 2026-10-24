//! SBE helper types, such as [`SbeEncode`].

use chrono::{DateTime, Utc};

use crate::{InvalidSide, Side};

macro_rules! schemas {
    ($($schema:ident),* $(,)?) => {$(
        pub mod $schema {
            #![allow(
                dead_code,
                unused_variables,
                unused_mut,
                unused_assignments,
                unused_comparisons,
                unused_unsafe,
                clippy::all
            )]
            include!(concat!(env!("OUT_DIR"), "/", stringify!($schema), ".rs"));
        }

        impl $schema::TryFromSbe<$schema::UTCTimestampMicrosEncoding> for DateTime<Utc> {
            type Error = &'static str;

            fn try_from_sbe(wire: $schema::UTCTimestampMicrosEncoding) -> Result<Self, Self::Error> {
                i64::try_from(wire.time())
                    .ok()
                    .and_then(DateTime::from_timestamp_micros)
                    .ok_or("UTC microseconds outside chrono's range")
            }
        }

        impl $schema::TryToSbe<$schema::UTCTimestampMicrosEncoding> for DateTime<Utc> {
            type Error = &'static str;

            fn try_to_sbe(&self) -> Result<$schema::UTCTimestampMicrosEncoding, Self::Error> {
                u64::try_from(self.timestamp_micros())
                    .map($schema::UTCTimestampMicrosEncoding::new)
                    .map_err(|_| "UTC timestamp predates the Unix epoch")
            }
        }
    )*};
}

/// This trait allows us to encode multiple SBE schemas using the same function.
pub trait SbeEncode {
    /// Returns the exact encoded length, including the SBE message header.
    fn encoded_length_with_header(&self) -> anyhow::Result<usize>;

    /// Encodes the message, including its header, into `buf` and returns the bytes written.
    /// The buffer must be at least `encoded_length_with_header()` bytes long.
    /// An encoding error may leave the buffer partially written.
    fn encode(&self, buf: &mut [u8]) -> anyhow::Result<usize>;

    fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        let mut buf = vec![0u8; self.encoded_length_with_header()?];
        let len = self.encode(&mut buf)?;
        debug_assert_eq!(len, buf.len());
        Ok(buf)
    }
}

macro_rules! sbe_encode {
    ($($message:ty),* $(,)?) => {$(
        impl SbeEncode for $message {
            #[inline]
            fn encoded_length_with_header(&self) -> anyhow::Result<usize> {
                Ok(<$message>::encoded_length_with_header(self)?)
            }

            #[inline]
            fn encode(&self, buf: &mut [u8]) -> anyhow::Result<usize> {
                Ok(<$message>::encode(self, buf)?)
            }
        }
    )*};
}

schemas!(challenge_orderbook);

impl challenge_orderbook::TryFromSbe<challenge_orderbook::SideEncoding> for Side {
    type Error = InvalidSide;

    fn try_from_sbe(wire: challenge_orderbook::SideEncoding) -> Result<Self, Self::Error> {
        Self::try_from(wire.value())
    }
}

impl challenge_orderbook::TryToSbe<challenge_orderbook::SideEncoding> for Side {
    type Error = std::convert::Infallible;

    fn try_to_sbe(&self) -> Result<challenge_orderbook::SideEncoding, Self::Error> {
        Ok(challenge_orderbook::SideEncoding::new(*self as u8))
    }
}

sbe_encode!(
    challenge_orderbook::SnapshotDomain,
    challenge_orderbook::PriceLevelUpdateDomain,
);
