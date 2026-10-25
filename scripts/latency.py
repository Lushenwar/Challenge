#!/usr/bin/env python3
"""Latency analysis for a multi-connection recording made with `record --conns N`.

Every order-book delta is matched across connections by (market, exchange timestamp,
side, price, change). For each matched update, each connection's lag is its receive time
minus the earliest connection's receive time. Reports per connection: how often it was
first, median / p90 / p99 lag, and whether the ranking holds across time windows (do fast
connections stay fast?). Also the exchange-to-local latency (recv - exchange ts), which
includes the local clock's offset from the exchange's.

Usage: latency.py recording.jsonl [--labels a,b,c] [--windows 4] > result.json
"""
import argparse, collections, json, statistics, sys


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p))] if xs else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("recording")
    ap.add_argument("--labels", default="", help="names for conn 0..N-1 (e.g. IPs)")
    ap.add_argument("--windows", type=int, default=4)
    a = ap.parse_args()
    labels = a.labels.split(",") if a.labels else []

    arrivals = collections.defaultdict(dict)  # key -> conn -> recv_us
    exch_lag = collections.defaultdict(list)  # conn -> recv_us - exch_us
    for line in open(a.recording):
        r = json.loads(line)
        fr = r.get("frame")
        if not fr or not fr.startswith('{"type":"orderbook_delta"'):
            continue
        f = json.loads(fr)
        m = f["msg"]
        ts = m.get("ts_ms")
        if ts is None:
            continue
        key = (m["market_ticker"], ts, m["side"], m["price_dollars"], m["delta_fp"])
        conn = r["conn"]
        # The first copy per connection (a reconnect can replay nothing; keep earliest).
        arrivals[key].setdefault(conn, r["recv_us"])
        exch_lag[conn].append(r["recv_us"] - ts * 1000)

    conns = sorted({c for d in arrivals.values() for c in d})
    full = [d for d in arrivals.values() if len(d) == len(conns)]
    full.sort(key=lambda d: min(d.values()))
    lag = collections.defaultdict(list)
    first = collections.Counter()
    for d in full:
        t0 = min(d.values())
        for c, t in d.items():
            lag[c].append(t - t0)
        first[min(d, key=d.get)] += 1

    # Ranking stability: per time window, the order of connections by median lag.
    n = len(full)
    windows = []
    for w in range(a.windows):
        chunk = full[w * n // a.windows:(w + 1) * n // a.windows]
        med = {c: statistics.median([d[c] - min(d.values()) for d in chunk]) for c in conns} if chunk else {}
        windows.append(sorted(conns, key=lambda c: med.get(c, 0)))

    name = lambda c: labels[c] if c < len(labels) else str(c)
    out = {
        "recording": a.recording,
        "matched_updates": n,
        "connections": {
            name(c): {
                "first_pct": round(100 * first[c] / n, 1) if n else None,
                "lag_us_p50": pct(lag[c], 0.5),
                "lag_us_p90": pct(lag[c], 0.9),
                "lag_us_p99": pct(lag[c], 0.99),
                "exch_to_local_us_p50": pct(exch_lag[c], 0.5),
            }
            for c in conns
        },
        "rank_by_window": [[name(c) for c in w] for w in windows],
    }
    json.dump(out, sys.stdout, indent=1)
    print()


if __name__ == "__main__":
    main()
