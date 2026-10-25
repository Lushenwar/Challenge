# Kalshi order-book collector

A single Rust binary that connects to [Kalshi](https://docs.kalshi.com), subscribes to
**every live non-MVE market** (~160,000 on 2026-10-24), keeps a correct order book per
market, and writes **order-book deltas plus a full snapshot of every book every minute**
to **stdout** in the supplied FIX SBE format. It keeps discovering new markets and drops
markets that stop trading, spreads markets over many websockets, keeps every market on
two sockets so a dead or silent connection loses nothing, and shuts down cleanly on
SIGINT/SIGTERM.

It ships with `verify`, which proves from a capture alone that the deltas and the
snapshots agree, and with the tools used to test it against live Kalshi: a raw frame
recorder and a fault-injecting websocket proxy. Polymarket's public CLOB is supported
through the same generic exchange layer.

Built for the [Voluntas engineering challenge](https://voluntas.io/engineering-challenge):
the base task and all four extensions (redundant sockets, subscription monitoring,
generic exchange layer, latency experiments).

---

## Contents

- [Quick start](#quick-start)
- [Output](#output)
- [How it works](#how-it-works)
- [Evidence](#evidence)
- [Tools](#tools)
- [Configuration](#configuration)
- [Testing](#testing)
- [Kalshi wire facts learned live](#kalshi-wire-facts-learned-live)
- [Known limits](#known-limits)
- [Repository layout](#repository-layout)

---

## Quick start

Requirements: Linux (or WSL2) and stable Rust via `rustup` (CI builds with the latest
stable; the crate uses edition 2024).

```bash
cargo build --release

# Kalshi needs an API key: Ed25519 (PKCS#8 PEM) or RSA (PKCS#8 or PKCS#1 PEM); the type
# is detected from the file. Credentials come from the environment:
export KALSHI_API_KEY_ID=<key id>
export KALSHI_PRIVATE_KEY_PATH=~/.kalshi/key.pem

# Every live Kalshi market, 2 sockets per market; SBE on stdout, logs + report on stderr.
./target/release/engineering-challenge > capture.sbe 2> capture.log
# ... Ctrl-C (SIGINT) to stop: websockets close, final snapshots are written, the file
# is flushed and closed, the JSON report is printed to stderr, exit 0.

# Prove the capture is self-consistent:
./target/release/verify capture.sbe --events capture.log
```

Smaller runs: `--max-markets 50` (the 50 most active markets). Polymarket (no key):
`--exchange polymarket --markets-per-socket 500`.

stdout carries **only** SBE bytes (clippy denies `print!`/`println!`; CI checks that even
`--help` writes nothing to stdout). Point `--output <path>` at a file instead of stdout if
you prefer.

## Output

Messages are written with the starter `AlignedSbeWriter` and decode with the starter
`AlignedSbeReader<OrderbookSchema>`, unmodified (CI fails if anything under `src/util/`
or `sbe_schemas/` differs from the tagged starter commit).

| Message | When | Fields |
|---|---|---|
| `Snapshot` | first book of a market; every minute at `:00` for every market; on failover / resync / exchange-initiated reset; empty book when a market is removed; at shutdown | `received_at` (local receive time, or generation time for periodic ones), `exchange_timestamp` (latest exchange event reflected), bids descending, asks ascending, `symbol` = Kalshi ticker |
| `PriceLevelUpdate` | each level change from the active source | `backtest_timestamp` = local receive time taken before decoding, `exchange_timestamp` = Kalshi `ts_ms` (else `sending_ts_ms`), price, **absolute** new quantity (0 = level removed), side, symbol |

Prices and quantities are fixed point (`1_000_000` = $1 / 1 contract), parsed exactly from
Kalshi's decimal strings; no floating point on the data path (CI greps for it). Kalshi
quotes YES and NO bids; a NO bid at `q` is written as a YES **ask** at `1 − q` (one mapping
function for snapshots and deltas).

**Chunking.** The starter reader only reads 64 MiB chunks (its smaller-chunk constructor
is private), so the writer uses 64 MiB chunks: anything else is unreadable by the official
reader. The writer (`src/writer.rs`) produces byte-for-byte what the starter
`AlignedSbeWriter` would (a property test compares the two), but every second it also
writes the bytes added to the current chunk, unpadded; a chunk is padded only when it is
full and at shutdown. So data is on disk within ~1 s and a crash loses at most a second.
The starter reader loads complete chunks only: a capture cut off by `kill -9` is made
readable with `verify capture.sbe --repair`, which zero-pads the last chunk.

## How it works

```
 Kalshi REST /markets ──► control ──► planner ──► connection tasks (≈60 websockets)
 (discovery, 30 s)        (adds,       (each        decode, check seq per sid, keep
 lifecycle events ───────► removals)    market on    their own copy of every book
                                        2 sockets)            │ events
                                                              ▼
                                           arbiter: one active source per market,
                                           failover with a snapshot, minute snapshots,
                                           health rules
                                                              │ messages
                                                              ▼
                                           writer thread ──► stdout (SBE)
```

- **Connections** (`src/conn.rs`) own their sockets and books. A market is *Pending* until
  its own subscription snapshot arrives (Kalshi starts a market's deltas only then), then
  *Valid*. A `seq` gap on a subscription invalidates every market on it and requests fresh
  snapshots (`get_snapshot`); a delta that would make a level negative invalidates that
  market. A socket is dead when *nothing* arrives for 20 s (pings every 10 s keep idle
  sockets talking). Reconnects back off with jitter.
- **Planner** (`src/planner.rs`): 5,000 markets per socket, each market on 2 different
  sockets, least-loaded first, backups mixed by hash so two markets that share one socket
  rarely share their backup too. Sticky: a live market never moves.
- **Arbiter** (`src/arbiter.rs`) mirrors every source's book, writes only the **active**
  source, and when that source is lost (invalidated, disconnected, stale) switches to the
  freshest valid replica, writing that replica's book as a snapshot **before** any of its
  deltas. So the output always equals the active source's book, and a single socket
  failure costs no data. If no replica is valid the market goes silent and the gap is
  logged and counted.
- **Health** (`src/health.rs`, Extension 2):
  1. *Lag*: a replica that has applied newer deltas than the active source for 2 s takes
     over; the stale socket resubscribes that market (unless the whole socket lagged,
     which it recovers from by itself).
  2. *Rate collapse*: a market whose message rate on a socket falls below 10% of its own
     baseline is resubscribed there (never the active source while a backup is valid).
  3. *Silence*: no frames for 20 s reconnects the socket.
  4. *Recycle*: every socket is reconnected every 30–45 min (jittered), only when every
     market on it has another valid replica.
  5. *Faster replica* (from the latency lab): a replica that is consistently first with
     the same updates becomes the active source.
- **Discovery** (`src/control.rs`): a market is removed only on a lifecycle event
  (determined / settled / deactivated) or after **two complete** discovery passes miss it.
  A failed or partial pass removes nothing. Removal writes an empty terminal snapshot.
- **Writer** (`src/writer.rs`): its own thread behind a 500k-message queue (a whole
  minute-boundary burst fits); if it ever fills, the arbiter blocks rather than drop output.
- **Exchange layer** (`src/exchange/`, Extension 3): everything venue-specific (URLs,
  auth, discovery, command JSON, decoding) is behind the `Exchange` and `Session` traits;
  `grep -ri kalshi src/ --exclude-dir=exchange` finds nothing (checked in CI). Kalshi and
  Polymarket are the two implementations.

## Evidence

All runs on live Kalshi prod unless noted; every capture checked with `verify`.

| Run | Result |
|---|---|
| Replay of 4 recordings, ~4.3M real deltas, incl. a 30-min 60-market run | rebuilt books equal every mid-stream exchange snapshot; 0 crossed books |
| 1 hour, all 162,896 markets, 1 socket per market | 110M deltas, 3,677 markets added and 4,487 removed live, 0 seq gaps; verify: 9.5M periodic snapshots checked, 0 unexplained mismatches |
| 10 min, all markets, 2 replicas, one socket killed mid-run | 4,543 failovers, **0 gaps**; verify: 0 unexplained mismatches, 0 interval violations |
| 2 hours through the fault proxy, 20,000 markets, a fault every ~2 min per socket (connection drops, single-frame drops, 3 s delays, silenced markets) | verify exit 0: 1.98M periodic snapshots checked, 0 unexplained mismatches, 0 interval violations. 152 dropped frames = 152 seq gaps detected; 61 dropped connections + 22 recycles = 83 reconnects. 1,673 gaps from overlapping faults on both replicas: median 0.1 s, max 1.6 s |
| 30 min Polymarket, 4,170 books, 2 replicas | verify exit 0, 0 unexplained mismatches, 0 interval violations |
| Final soak: 6 hours, all markets (175,165 seen), 2 replicas, 63 sockets | SIGINT exit 0; 736M deltas, **0 seq gaps**, 0 decode errors, 0 crossed books; 25,627 markets removed as they settled; 470 scheduled socket recycles; 2,000+ short gaps (98.8% under 5 s, longest ~29 s, around 6 Kalshi "subscription buffer overflow" errors). verify: exit 0, 411M messages, 52.9M periodic snapshots checked, 0 unexplained mismatches, 0 interval violations |

The same fault rate on all ~157k markets saturated the test machine's 28 MB/s link (every
fault re-downloads ~5,000 snapshots), so that run was cut to the 20,000 most active
markets; the per-market fault rate is unchanged.

## Tools

| Binary | What it does |
|---|---|
| `engineering-challenge` | the collector |
| `verify <capture> --events <stderr log>` | decodes the capture with the starter reader, rebuilds every book from the updates, and requires every later snapshot to match. A mismatch is accepted only when the collector logged a non-periodic cause for that snapshot (subscribe, resync, failover, exchange reset, removal). Also: no update before a symbol's first snapshot, ≤ 61 s between a live symbol's snapshots (longer only inside a logged gap), timestamp regressions reported. JSON summary; exit 0 only if nothing is unexplained. |
| `record --markets N --conns M --minutes T --out f.jsonl` | raw frame recorder: one JSON line per websocket frame with its local receive time; the source of the test fixtures and the latency lab |
| `faultproxy --log faults.jsonl` | websocket proxy the collector connects to (`--ws-url ws://127.0.0.1:9000`); injects connection drops, single-frame drops, delays and silent subscriptions (with `seq` renumbered so no gap shows) and logs each one |

## Configuration

| Flag | Default | Meaning |
|---|---|---|
| `--exchange kalshi\|polymarket` | kalshi | venue |
| `--env prod\|demo` | prod | Kalshi environment |
| `--key-id`, `--key-path` | `KALSHI_API_KEY_ID`, `KALSHI_PRIVATE_KEY_PATH` | Kalshi credentials |
| `--max-markets N` | all | collect only the N most active markets |
| `--replicas N` | 2 | sockets per market |
| `--markets-per-socket N` | 5000 | |
| `--discovery-secs S` | 30 | pause between discovery passes |
| `--output PATH` | `/dev/stdout` | |
| `--report PATH` | none | also write the final JSON report here |
| `--ws-url URL` | venue's | connect elsewhere (the fault proxy) |
| `--drop-conn N@S` | none | test hook: drop socket N after S seconds |

`RUST_LOG` controls stderr verbosity (default `info`).

## Testing

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```

- Unit tests (fixed-point parsing incl. property tests, order book, codecs, connection
  state machine, planner, arbiter failover / removal / health rules) are pure: no network,
  injected time.
- `tests/replay_kalshi.rs` replays a committed 1 MB live recording
  (`fixtures/kalshi/prod-2026-10-24.jsonl`) through the decoder and books; every
  mid-stream exchange snapshot must equal the rebuilt book.
- `tests/replay_collector.rs` drives the same recording through connection → arbiter →
  starter writer, then runs the real `verify` on the result: a clean capture verifies, the
  same capture with one delta removed fails, and both recorded connections used as
  replicas fail over correctly.
- CI (`.github/workflows/ci.yml`) also checks the starter files are unchanged, no `f64`
  outside the starter code, no venue names outside `src/exchange/`, empty stdout.

## Kalshi wire facts learned live

Things the docs do not say, found by recording prod traffic:

- **No `clock` field** in snapshots or deltas, so replicas are compared on delta event
  times (`ts_ms`).
- A second `subscribe` to `orderbook_delta` on one connection **merges** into the existing
  subscription (one sid per connection) and is answered with `ok`. `ok` frames, and the
  lifecycle channel's `event_lifecycle` frames, consume a `seq`.
- A market's deltas start only at **its own** subscription snapshot; a `get_snapshot`
  reply that arrives earlier is not a base for deltas.
- When a market closes, Kalshi sends an **unsolicited empty snapshot** (book cleared).
- `get_snapshot` is answered by an `orderbook_snapshot` echoing the command id, inside the
  subscription's `seq` stream.
- One subscription accepted 20,000 markets; 300 commands in a burst were accepted; 150
  concurrent sockets per key worked, but opening them all at once fails many handshakes,
  so sockets are opened 250 ms apart.
- The server pings only idle connections. A "no pong within 10 s" rule falsely kills busy
  sockets: pongs queue behind a socket's initial snapshots (~15 s on a 28 MB/s link).
- Error `25` "Subscription buffer overflow": Kalshi dropped a subscription's backlog
  because the socket fell behind (6 times in 6 hours); error `7` "Unknown subscription
  ID" answers commands that still named a subscription from before a reconnect.
- `api.elections.kalshi.com` is served through CloudFront and is **4–5 ms faster** (median)
  than the direct `external-api-ws.kalshi.com`. See [docs/latency.md](docs/latency.md).

## Known limits

- **Output latency**: bytes reach the file within ~1 s, but the unmodified starter reader
  sees a chunk only once it is complete (64 MiB, or at shutdown / `--repair`).
- **Timestamps** are the host's wall clock; on the test machine Kalshi's `sending_ts_ms`
  ran ~4 s ahead of it. A handful of microsecond-scale regressions occur at minute
  boundaries (a delta received just before `:00` but written just after the periodic
  snapshot); `verify` reports them.
- **Gaps** happen only when both replicas of a market are down at once (counted in the
  report). Under heavy fault injection they cluster after overlapping faults.
- **Polymarket**: discovery covers the ~2,000 most active markets (Gamma refuses offsets
  past ~2,000; keyset paging is the upgrade path), so it never removes markets by
  discovery; Polymarket closes sockets that are not drained fast enough.
- Bandwidth: all markets with 2 replicas is ~15 MB/s of websocket traffic.

## Repository layout

```
src/main.rs           collector: CLI, wiring, signals, report
src/conn.rs           connection state machine + socket loop
src/arbiter.rs        source selection, failover, snapshots, health actions
src/health.rs         rate EWMA, recycle schedule, thresholds
src/planner.rs        market → socket placement
src/control.rs        discovery loop, lifecycle, connection tasks
src/writer.rs         SBE writer thread
src/book.rs, fixed.rs order book; exact decimals
src/exchange/         venue layer: mod.rs (traits), kalshi/, polymarket.rs
src/bin/              verify, record, faultproxy
src/util/, sbe_schemas/, src/side.rs   supplied starter code, unmodified
tests/                replay and integration tests
fixtures/kalshi/      a 1 MB live prod recording
scripts/latency.py    latency analysis
docs/latency.md       latency findings (Extension 4)
```

The SBE schema and helpers (`src/util/`, `sbe_schemas/`) are the challenge's supplied
starter code: a custom YAML schema compiled to FIX SBE XML and then to Rust at build
time.
