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
- Not tried: source-port choice, connecting right after Kalshi's scheduled maintenance
  (none happened during this session), and kernel receive timestamps (`SO_TIMESTAMPNS`).
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
3. Pinning IPs and `TCP_NODELAY` are left out: no measurable benefit.
