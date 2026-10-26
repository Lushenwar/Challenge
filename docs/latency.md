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
# E4: racing, in one recording: conns 1,3,5,7 race 4 handshakes, 0,2,4,6 do not; all
#     reconnect once after 5 min, so each connection connects twice
record --markets 15 --conns 8 --minutes 10 --race-conns 1,3,5,7 --race 4 --reconnect-after 300 --out e4.jsonl
# E5: source ports: one edge IP, conn i on local port 42000+i; twice, 90 s apart
#     (so the 4-tuples leave TIME_WAIT and the same ports are reused)
record --markets 15 --conns 8 --minutes 10 --ips 65.8.70.109 --source-port-base 42000 --out e5a.jsonl
record --markets 15 --conns 8 --minutes 10 --ips 65.8.70.109 --source-port-base 42000 --out e5b.jsonl
python3 scripts/ports_compare.py e5a.json e5b.json e5a-peers.txt e5b-peers.txt
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
- Racing handshakes (E4): no gain on average (below).
- Not tried: connecting right after Kalshi's scheduled maintenance (none happened during
  this session) and kernel receive timestamps (`SO_TIMESTAMPNS`).

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
- Racing (C3) made no difference detectable here; E4 and E5 below test racing and source
  ports fairly, with both settings in one recording.

## E4: racing handshakes (one recording)

8 connections to the same 15 markets, 285,690 updates matched across all of them
(Sunday evening, NFL markets busy). Connections 1, 3, 5, 7 race 4 TCP+TLS+websocket
handshakes over the hostname's addresses and keep the first to finish; 0, 2, 4, 6 connect
once. Each connected twice (a forced reconnect at 5 min). Raw:
[`latency/e4-race.json`](latency/e4-race.json), peers and local ports in
[`latency/e4-race-peers.txt`](latency/e4-race-peers.txt).

| | connections | median lag behind the first copy (µs) | first (%) |
|---|---|---|---|
| raced | 1, 3, 5, 7 | 941, 1093, 826, 394 | 9.6, 7.6, 9.9, 20.3 |
| not raced | 0, 2, 4, 6 | 688, 554, 640, 1003 | 15.1, 15.8, 14.5, 7.4 |

**Racing does not make a connection faster on average** (raced median of medians
~880 µs vs ~660 µs). The winning handshake is the one whose TCP+TLS setup was quickest,
which says little about the path the data then takes. What stands out instead is
connection 7: first in all four time windows, before and after its reconnect (which
landed on a different edge IP, `.108`). Speed belongs to the individual connection,
as in E1/E3.

## E5: source ports (two recordings, same ports)

All 8 connections to one edge IP (`65.8.70.109`), connection i on local port 42000 + i,
recorded twice 90 s apart; the peers files confirm every connection got its pinned port
both times. Raw: [`latency/e5a-ports.json`](latency/e5a-ports.json),
[`latency/e5b-ports.json`](latency/e5b-ports.json), comparison
[`latency/e5-ports-compare.json`](latency/e5-ports-compare.json).

| port | 42000 | 42001 | 42002 | 42003 | 42004 | 42005 | 42006 | 42007 |
|---|---|---|---|---|---|---|---|---|
| median lag, run A (µs) | 151 | 113 | 1540 | 1628 | 1894 | 225 | 184 | 149 |
| median lag, run B (µs) | 159 | 225 | 508 | 273 | 2172 | 281 | 139 | 180 |

- The lags are **bimodal**: most connections sit 0.1–0.3 ms behind the first copy, a few
  1.5–2 ms. With destination IP and port fixed, the only thing that differs between
  connections is the source port, which feeds the flow hash that picks a path through
  load-balanced links.
- **Partly repeatable.** Port 42004 was the slowest both times (1.9 and 2.2 ms); 42002
  and 42003 were slow in A but fast in B. Spearman rank correlation of the two orderings:
  ρ = 0.69 (8 ports; borderline, roughly p ≈ 0.03 one-sided). So the source port does
  select the path, but not deterministically across reconnects (other hash inputs or
  path changes intervene).
- **Decision: rejected for the collector, with data.** Pinning a "known fast" port
  would not reliably stay fast, and picking ports needs a probing phase of minutes per
  socket. The collector's existing *faster replica* rule gets the same benefit at no
  cost: each market has two connections, and the one that keeps winning becomes the
  active source. `--source-port-base` stays available for experiments.
- Correctness was unaffected: all three captures verify with 0 unexplained mismatches.
