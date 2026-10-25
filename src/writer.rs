//! Writer thread: the only owner of the output file.
//!
//! [`ChunkWriter`] produces exactly the bytes the starter `AlignedSbeWriter` would (same
//! encoder, same 64 MiB chunk layout, same zero padding; a differential test proves it),
//! with one difference in *when* bytes reach the file: every second it also writes the
//! bytes added to the current chunk since the last write, unpadded. The finished capture
//! is byte-for-byte the starter format; while running, data is on disk within ~1 s
//! instead of once per 64 MiB. The starter reader only loads complete chunks, so a capture
//! cut off mid-chunk (process killed) needs [`pad_capture`] before it reads the tail.

use std::{
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use crossbeam::channel::{Receiver, RecvTimeoutError, Sender, bounded};
use serde::Serialize;

use crate::{model::OutMsg, util::dtos::SbeEncode};

/// Must equal the starter `AlignedSbeReader`'s fixed 64 MiB read chunk.
pub const CHUNK: usize = 64 << 20;
/// Every SBE schema here uses the standard eight-byte message header; a chunk always keeps
/// room for one empty header (the reader's end-of-chunk sentinel).
const HEADER_LENGTH: usize = 8;
/// Holds a whole minute-boundary burst (one snapshot per market, ~160k at the largest venue) so the
/// arbiter, and through it the sockets, do not stall while the writer drains it.
const CAPACITY: usize = 500_000;
const WRITE_EVERY: Duration = Duration::from_secs(1);

/// The starter `AlignedSbeWriter`'s chunk format, writing completed bytes promptly.
pub struct ChunkWriter<W: Write> {
    out: W,
    buf: Vec<u8>,
    chunk: usize,
    /// Bytes of `buf` already written to `out`.
    written: usize,
}

impl ChunkWriter<File> {
    pub fn create(path: impl AsRef<Path>, chunk: usize) -> anyhow::Result<Self> {
        Self::new(File::create(path)?, chunk)
    }
}

impl<W: Write> ChunkWriter<W> {
    pub fn new(out: W, chunk: usize) -> anyhow::Result<Self> {
        anyhow::ensure!(
            chunk > HEADER_LENGTH,
            "chunk must leave room for SBE data and an SBE header"
        );
        Ok(Self {
            out,
            buf: Vec::with_capacity(chunk),
            chunk,
            written: 0,
        })
    }

    /// Append a message; rotates to a new chunk (padding the old one) if it does not fit.
    pub fn write_msg<T: SbeEncode + ?Sized>(&mut self, message: &T) -> anyhow::Result<()> {
        let len = message.encoded_length_with_header()?;
        let payload_limit = self.chunk - HEADER_LENGTH;
        anyhow::ensure!(len <= payload_limit, "SBE message does not fit in a chunk");
        if len > payload_limit - self.buf.len() {
            self.finish_chunk()?;
        }
        let start = self.buf.len();
        self.buf.resize(start + len, 0);
        let result = message.encode(&mut self.buf[start..]).and_then(|written| {
            anyhow::ensure!(
                written == len,
                "SBE encoded length mismatch: expected {len} bytes, wrote {written}"
            );
            Ok(())
        });
        if result.is_err() {
            self.buf.truncate(start);
        }
        result
    }

    /// Write the bytes added since the last write, without padding.
    pub fn write_pending(&mut self) -> anyhow::Result<()> {
        if self.written < self.buf.len() {
            self.out.write_all(&self.buf[self.written..])?;
            self.out.flush()?;
            self.written = self.buf.len();
        }
        Ok(())
    }

    /// Pad the current chunk to full size and write the rest of it (starter `flush`).
    pub fn finish_chunk(&mut self) -> anyhow::Result<()> {
        if !self.buf.is_empty() {
            self.buf.resize(self.chunk, 0);
            self.out.write_all(&self.buf[self.written..])?;
            self.out.flush()?;
            self.buf.clear();
            self.written = 0;
        }
        Ok(())
    }

    pub fn into_inner(self) -> W {
        self.out
    }
}

/// Zero-pad a capture whose last chunk was cut off (the writer was killed) to a whole
/// number of chunks, so the starter reader reads every message in it. Returns the
/// number of bytes added.
pub fn pad_capture(path: &Path, chunk: usize) -> anyhow::Result<u64> {
    let file = std::fs::OpenOptions::new().write(true).open(path)?;
    let len = file.metadata()?.len();
    let rem = len % chunk as u64;
    if rem == 0 {
        return Ok(0);
    }
    let add = chunk as u64 - rem;
    file.set_len(len + add)?; // the new bytes read as zeros: the end-of-chunk sentinel
    Ok(add)
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct WriterStats {
    pub snapshots: u64,
    pub updates: u64,
    pub max_queue: usize,
    /// Partial (unpadded) writes made so data reaches the file within ~1 s.
    pub partial_writes: u64,
}

/// Create the output (fails fast if it cannot be opened) and start the thread. The
/// thread pads the last chunk and closes the file once every sender is dropped.
pub fn spawn(
    path: PathBuf,
) -> anyhow::Result<(Sender<OutMsg>, JoinHandle<anyhow::Result<WriterStats>>)> {
    let w = ChunkWriter::create(&path, CHUNK)?;
    let (tx, rx) = bounded(CAPACITY);
    let handle = std::thread::Builder::new()
        .name("writer".into())
        .spawn(move || write_loop(w, rx))?;
    Ok((tx, handle))
}

fn write_loop(mut w: ChunkWriter<File>, rx: Receiver<OutMsg>) -> anyhow::Result<WriterStats> {
    let mut stats = WriterStats::default();
    let mut last_write = Instant::now();
    loop {
        match rx.recv_timeout(Duration::from_millis(100)) {
            Ok(OutMsg::Snapshot(s)) => {
                w.write_msg(&s)?;
                stats.snapshots += 1;
            }
            Ok(OutMsg::Update(u)) => {
                w.write_msg(&u)?;
                stats.updates += 1;
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
        stats.max_queue = stats.max_queue.max(rx.len());
        if last_write.elapsed() >= WRITE_EVERY {
            if w.written < w.buf.len() {
                stats.partial_writes += 1;
            }
            w.write_pending()?;
            last_write = Instant::now();
        }
    }
    w.finish_chunk()?;
    drop(w); // closes the file; no fsync (pipes reject it)
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Side,
        util::{
            AlignedSbeReader, AlignedSbeWriter, OrderbookSchema,
            dtos::challenge_orderbook::{
                AnyMessage, PriceLevelUpdateDomain, SnapshotAsksEntryDomain,
                SnapshotBidsEntryDomain, SnapshotDomain,
            },
        },
    };
    use chrono::DateTime;
    use proptest::prelude::*;

    fn snapshot(symbol: &str, levels: usize) -> OutMsg {
        let t = DateTime::from_timestamp_micros(1_700_000_000_000_000).unwrap();
        OutMsg::Snapshot(SnapshotDomain {
            received_at: t,
            exchange_timestamp: t,
            bids: (0..levels as i64)
                .map(|i| SnapshotBidsEntryDomain {
                    price_level: 400_000 - i,
                    quantity: i + 1,
                })
                .collect(),
            asks: (0..levels as i64)
                .map(|i| SnapshotAsksEntryDomain {
                    price_level: 600_000 + i,
                    quantity: i + 2,
                })
                .collect(),
            symbol: symbol.to_owned(),
        })
    }

    fn update(symbol: &str, qty: i64) -> OutMsg {
        let t = DateTime::from_timestamp_micros(1_700_000_000_000_001).unwrap();
        OutMsg::Update(PriceLevelUpdateDomain {
            backtest_timestamp: t,
            exchange_timestamp: t,
            price_level: 500_000,
            quantity: qty,
            side: Side::Sell,
            symbol: symbol.to_owned(),
        })
    }

    /// The same messages through the starter writer and ours, ours with partial writes
    /// interleaved: identical bytes.
    fn both(msgs: &[OutMsg], chunk: usize, partial_every: usize) -> (Vec<u8>, Vec<u8>) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("starter.sbe");
        let mut a = AlignedSbeWriter::create(&path, chunk).unwrap();
        let mut b = ChunkWriter::new(Vec::new(), chunk).unwrap();
        for (i, m) in msgs.iter().enumerate() {
            let (ra, rb) = match m {
                OutMsg::Snapshot(s) => (a.write_msg(s), b.write_msg(s)),
                OutMsg::Update(u) => (a.write_msg(u), b.write_msg(u)),
            };
            assert_eq!(
                ra.is_ok(),
                rb.is_ok(),
                "message {i}: both accept or both reject"
            );
            if partial_every > 0 && i % partial_every == 0 {
                b.write_pending().unwrap();
            }
        }
        a.flush().unwrap();
        drop(a);
        b.finish_chunk().unwrap();
        (std::fs::read(&path).unwrap(), b.into_inner())
    }

    proptest! {
        #[test]
        fn byte_identical_to_the_starter_writer(
            shape in prop::collection::vec((any::<bool>(), 0usize..40, 1usize..12), 1..120),
            partial_every in 0usize..7,
        ) {
            // A small chunk so rollovers and oversize rejection happen often.
            let msgs: Vec<OutMsg> = shape
                .iter()
                .enumerate()
                .map(|(i, &(snap, levels, sym))| {
                    let symbol = "S".repeat(sym);
                    if snap { snapshot(&symbol, levels) } else { update(&symbol, i as i64) }
                })
                .collect();
            let (starter, ours) = both(&msgs, 1024, partial_every);
            prop_assert_eq!(starter, ours);
        }
    }

    #[test]
    fn a_killed_capture_is_readable_after_padding() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cut.sbe");
        let mut w = ChunkWriter::create(&path, CHUNK).unwrap();
        let msgs: Vec<OutMsg> = (0..50)
            .map(|i| {
                if i % 10 == 0 {
                    snapshot("T", 5)
                } else {
                    update("T", i)
                }
            })
            .collect();
        for m in &msgs {
            match m {
                OutMsg::Snapshot(s) => w.write_msg(s).unwrap(),
                OutMsg::Update(u) => w.write_msg(u).unwrap(),
            }
        }
        w.write_pending().unwrap();
        drop(w); // "killed": the chunk is never padded
        let len = std::fs::metadata(&path).unwrap().len();
        assert!(
            len > 0 && len < CHUNK as u64,
            "bytes reached the file before the chunk filled"
        );
        assert_eq!(pad_capture(&path, CHUNK).unwrap(), CHUNK as u64 - len);
        assert_eq!(pad_capture(&path, CHUNK).unwrap(), 0); // idempotent
        let mut r = AlignedSbeReader::<OrderbookSchema>::open(&path).unwrap();
        let mut n = 0;
        while let Some(m) = r.read_msg().unwrap() {
            assert!(matches!(
                m,
                AnyMessage::Snapshot(_) | AnyMessage::PriceLevelUpdate(_)
            ));
            n += 1;
        }
        assert_eq!(n, msgs.len());
    }
}
