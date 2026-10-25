//! Writer thread: the only owner of the output file, via the starter `AlignedSbeWriter`.

use std::{path::PathBuf, thread::JoinHandle};

use crossbeam::channel::{Receiver, Sender, bounded};
use serde::Serialize;

use crate::{model::OutMsg, util::AlignedSbeWriter};

/// Must equal the starter `AlignedSbeReader`'s fixed 64 MiB read chunk, or the official
/// reader cannot decode the capture. `flush()` pads to a whole chunk, so we only flush
/// when a chunk fills (the helper does that) and once at shutdown.
// ponytail: output becomes visible in 64 MiB blocks; a time-based flush would cost up to
// 64 MiB of padding each time.
pub const CHUNK: usize = 64 << 20;
/// Holds a whole minute-boundary burst (one snapshot per market, ~160k at the largest venue) so the
/// arbiter, and through it the sockets, do not stall while the writer drains it.
const CAPACITY: usize = 500_000;

#[derive(Debug, Default, Clone, Serialize)]
pub struct WriterStats {
    pub snapshots: u64,
    pub updates: u64,
    pub max_queue: usize,
}

/// Create the output (fails fast if it cannot be opened) and start the thread. The
/// thread flushes and closes the file once every sender is dropped.
pub fn spawn(
    path: PathBuf,
) -> anyhow::Result<(Sender<OutMsg>, JoinHandle<anyhow::Result<WriterStats>>)> {
    let w = AlignedSbeWriter::create(&path, CHUNK)?;
    let (tx, rx) = bounded(CAPACITY);
    let handle = std::thread::Builder::new()
        .name("writer".into())
        .spawn(move || write_loop(w, rx))?;
    Ok((tx, handle))
}

fn write_loop(mut w: AlignedSbeWriter, rx: Receiver<OutMsg>) -> anyhow::Result<WriterStats> {
    let mut stats = WriterStats::default();
    for msg in &rx {
        match msg {
            OutMsg::Snapshot(s) => {
                w.write_msg(&s)?;
                stats.snapshots += 1;
            }
            OutMsg::Update(u) => {
                w.write_msg(&u)?;
                stats.updates += 1;
            }
        }
        stats.max_queue = stats.max_queue.max(rx.len());
    }
    w.flush()?;
    drop(w); // closes the file; no fsync (pipes reject it, and the helper cannot)
    Ok(stats)
}
