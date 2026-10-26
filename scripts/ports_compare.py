"""E5: does a source port keep its speed? Compares two `latency.py` results of recordings
made with the same pinned ports (connection i = port BASE + i) and prints, per port, the
median lag in each, plus the Spearman rank correlation of the two orderings.
Usage: ports_compare.py A.json B.json A-peers.txt B-peers.txt"""
import json, sys


def ranks(xs):
    order = sorted(range(len(xs)), key=lambda i: xs[i])
    r = [0] * len(xs)
    for pos, i in enumerate(order):
        r[i] = pos
    return r


def ports(peers):
    out = {}
    for line in open(peers):
        conn, _peer, _, local, *_ = line.split()
        out[int(conn)] = int(local.rsplit(":", 1)[1])
    return out


a, b = (json.load(open(p)) for p in sys.argv[1:3])
pa, pb = ports(sys.argv[3]), ports(sys.argv[4])
conns = sorted(int(c) for c in a["connections"])
la = [a["connections"][str(c)]["lag_us_p50"] for c in conns]
lb = [b["connections"][str(c)]["lag_us_p50"] for c in conns]
ra, rb = ranks(la), ranks(lb)
n = len(conns)
rho = 1 - 6 * sum((x - y) ** 2 for x, y in zip(ra, rb)) / (n * (n * n - 1))
res = {
    "ports_pinned_as_asked": all(pa[c] == pb[c] for c in conns),
    "per_connection": [
        {"conn": c, "port_a": pa[c], "port_b": pb[c], "lag_p50_a_us": x, "lag_p50_b_us": y}
        for c, x, y in zip(conns, la, lb)
    ],
    "spearman_rho": round(rho, 2),
}
json.dump(res, sys.stdout, indent=1)
print()
