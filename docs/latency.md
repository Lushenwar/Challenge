# Latency lab (Extension 4)

What makes Kalshi order-book updates reach the collector sooner, measured on live prod
traffic. Correctness comes first in this collector; these are second-order effects, in
milliseconds.

## Method

`record` opens N websocket connections that subscribe to the **same** markets and logs
every frame with its local receive time (taken before any decoding). Every
`orderbook_delta` is then matched across connections by
`(market, ts_ms, side, price, delta)`. For each matched update, a connection's **lag** is
its receive time minus the earliest connection's. Because every connection in one
recording sees the same exchange, network and host conditions, comparing them is fair
even when the host is busy.

`scripts/latency.py` does the matching and reports, per connection: how often it was
first, lag p50/p90/p99, the exchange-to-local latency (`recv - ts_ms`, which includes the
local clock's offset from Kalshi's, so only differences between connections mean
anything), and the connection ranking in four equal time windows (do fast connections
stay fast?).

```bash
# E1: 4 connections, same 15 most active markets, TCP_NODELAY on conns 0 and 2
record --markets 15 --conns 4 --minutes 10 --nodelay-conns 0,2 --out e1.jsonl
# E2: CloudFront hostname vs the direct one, alternating per connection
record --markets 15 --conns 5 --minutes 10 \
  --ws-urls wss://api.elections.kalshi.com/trade-api/ws/v2,wss://external-api-ws.kalshi.com/trade-api/ws/v2 --out e2.jsonl
# E3: 8 connections pinned two per CloudFront edge IP (TLS SNI keeps the hostname)
record --markets 15 --conns 8 --minutes 10 --ips 65.8.70.91,65.8.70.101,65.8.70.108,65.8.70.109 --out e3.jsonl
python3 scripts/latency.py e1.jsonl --labels ... > docs/latency/e1-parallel-nodelay.json
```

Results: [`docs/latency/`](latency/) (`*.json` = analysis output, `*-peers.txt` = the
server address each connection reached). The raw recordings (6–14 MB each) stay local.

Setup: Sunday 2026-10-25, 09:40–10:12 UTC (a quiet time: 4–6k matched updates per run), a
Windows laptop running the collector in WSL2, home connection measured at ~28 MB/s, with a
full 157k-market chaos run sharing the same link and CPU. Absolute lags are therefore
pessimistic; the comparisons inside each recording are not affected.

## DNS: two front doors

| Hostname | Resolves to |
|---|---|
| `api.elections.kalshi.com` (the collector's default) | 4 AWS CloudFront edge IPs, `65.8.70.{91,101,108,109}` |
| `external-api-ws.kalshi.com` | ~15 AWS us-east-2 (Ohio) addresses, no CDN in front |

Both accept the same signed websocket handshake.

## Results

**E2: CloudFront vs direct.** The CloudFront connections were first on **97%** of
updates; the two direct connections trailed by a median **4.0–5.1 ms** (p99 18–20 ms). The
ranking was identical in all four time windows.

| Connection | First | Lag p50 | Lag p90 | Lag p99 |
|---|---|---|---|---|
| c4 CloudFront | 46.2% | 27 µs | 1.9 ms | 6.9 ms |
| c0 CloudFront (+ lifecycle feed) | 37.0% | 48 µs | 2.1 ms | 8.1 ms |
| c2 CloudFront | 14.0% | 1.9 ms | 6.0 ms | 13.7 ms |
| c3 direct | 1.6% | 4.0 ms | 6.5 ms | 17.6 ms |
| c1 direct | 1.1% | 5.1 ms | 7.5 ms | 19.6 ms |

**E3: which CloudFront IP.** It does not matter; the individual connection does. The two
connections to `.101` were the fastest (p50 92 µs) and the slowest (p50 1.6 ms) of all
eight, and every IP shows the same split. The order was stable across the windows (the
top three never left the top three).

**E1: parallel connections and TCP_NODELAY.** Same picture: one connection was
consistently slowest (p50 679 µs vs ~60 µs for the others, last in all four windows).
`TCP_NODELAY` made **no difference**: the two connections with it set finished first and
last. That is expected: Nagle's algorithm delays what *we* send, and the collector only
sends commands and pings.

**Polymarket (from the Phase 9 runs, not a controlled experiment).** Replicas of the same
asset diverge by more than 2 s far more often than on Kalshi (2,195 lag failovers over
4,170 books in 30 minutes), and Polymarket closes a socket that is not drained fast enough
(`slow consumer: send buffer full`, 4 times on a host that was also running a full Kalshi
chaos run).

## What was tried and made no difference, or was not tried

- `TCP_NODELAY`: no effect (E1), as theory predicts for a receive-mostly client.
- Choosing a CloudFront edge IP: no effect beyond per-connection variance (E3).
- An earlier hypothesis that carrying the venue-wide lifecycle feed slows a connection
  (E1's slowest connection carried it) did **not** hold: in E2 the lifecycle connection
  was the second fastest.
- Not measured: source-port choice (the collector supports it, see below; its run was
  cut short), connecting right after Kalshi's scheduled maintenance (none happened during
  this session), and kernel receive timestamps (`SO_TIMESTAMPNS`).
  The per-connection variance seen here (~1 ms) would have to be separated from those
  effects with the same matched-update method.

## Fed back into the collector

1. **Keep the CloudFront hostname** (`api.elections.kalshi.com`). The direct hostname is
   rejected with data: 4–5 ms slower at the median.
2. **Prefer the faster replica.** Connection speed is stable over minutes, so each market's
   arbiter now counts, per replica, the updates it applied before any other valid replica
   had them. Once a minute, a replica that won at least 80% of at least 20 contested
   updates (and is not behind) becomes the active source, with a snapshot first (cause
   `Faster`). With two replicas per market this picks the better connection of the pair:
   in E3 the spread between a market's two connections was typically ~1 ms at the median.
   Thresholds: `FASTER_MIN_WINS` and `FASTER_PCT` in `src/health.rs`.
3. Pinning IPs is left out: no measurable benefit. `TCP_NODELAY` is now set on every
   collector socket anyway (it costs nothing and our pings/commands are tiny writes).

## Through the collector (C1–C3)

The collector itself now opens its sockets through `src/net.rs`: `TCP_NODELAY` always,
`--race N` (N parallel TCP+TLS+websocket handshakes per connect, spread over the host's
addresses; the first to finish is kept, the rest dropped), and `--source-port-base P`
(pin each socket's local port, falling back to any port if it is taken). Unit tests on a
local server cover all three.

Each run: 10 min, the 2,000 most active markets, 2 replicas, 500 markets per socket
(8 sockets), then `verify`. `verify` now reports `receive_minus_exchange_us` percentiles
over every written update, and the report counts, per socket, the updates it delivered
before every other valid replica (`first_by_conn`). Raw results:
[`latency/collector-*.json`](latency/).

| run | args | verify | p1 / p50 / p99 receive − exchange (ms) | first-share per socket (min–max) |
|---|---|---|---|---|
| C1 baseline (CloudFront) | — | exit 0, 2.77M updates | 2181.8 / 2203.3 / 4992.2 | 7.4–22.0% |
| C2 direct host | `--ws-url wss://external-api-ws…` | exit 0, 3.11M | 2212.9 / 2246.4 / 2618.1 | 5.0–19.5% |
| C3 race 4 | `--race 4` | exit 0, 3.19M | 2233.0 / 2293.4 / 3672.1 | 5.4–21.0% |
| C4 source ports | `--source-port-base 41000` | run cut off after 3 min by memory pressure: no result | | |

What this shows, and what it does not:
- **Cross-run latency comparisons are not valid with this method.** The ~2.2 s offset is
  mostly this machine's clock running behind Kalshi's, and it drifted between runs: the
  median rose 2203 → 2246 → 2293 ms over ~35 min regardless of the setting (C2 was the
  setting E2 already showed to be slower *within one recording*, by 4–5 ms; here the
  drift is ~10× that). Absolute `ts_ms` latencies can only rank settings that run in the
  same recording, as E1–E3 do.
- Within each run, some sockets are first far more often than others (7–22%), but the
  share also depends on how many busy markets each socket carries, so it is not a pure
  speed ranking; the matched-update method of E1–E3 is the fair one.
- Racing (C3) made no difference detectable here. A fair test of racing (and of source
  ports) needs both settings in one recording: e.g. `record` with half the connections
  racing. Not done.
- Correctness was unaffected: all three captures verify with 0 unexplained mismatches.
