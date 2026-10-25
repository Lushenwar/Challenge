//! Market → socket assignment. Pure. Each market goes on `replicas` different sockets,
//! least-loaded first, never more than `max_per_socket` per socket. Ties break on a hash
//! of (market, replica, socket), so two markets that share one socket rarely share their
//! backup socket too. Assignments are sticky: a live market never moves to even out load.

use std::{
    collections::{BTreeSet, HashMap},
    hash::{DefaultHasher, Hash, Hasher},
};

use crate::model::ConnId;

pub struct Planner {
    max_per_socket: usize,
    replicas: usize,
    sockets: Vec<BTreeSet<String>>,
    assigned: HashMap<String, Vec<ConnId>>,
}

impl Planner {
    pub fn new(max_per_socket: usize, replicas: usize) -> Self {
        assert!(max_per_socket > 0 && replicas > 0);
        Self {
            max_per_socket,
            replicas,
            sockets: Vec::new(),
            assigned: HashMap::new(),
        }
    }

    pub fn sockets(&self) -> usize {
        self.sockets.len()
    }

    pub fn contains(&self, market: &str) -> bool {
        self.assigned.contains_key(market)
    }

    pub fn len(&self) -> usize {
        self.assigned.len()
    }

    pub fn is_empty(&self) -> bool {
        self.assigned.is_empty()
    }

    pub fn markets(&self) -> impl Iterator<Item = &String> {
        self.assigned.keys()
    }

    /// Place markets not yet assigned. Returns the additions per socket, in socket order;
    /// a socket id `>= sockets()` before the call is a new socket to open.
    pub fn add(&mut self, markets: impl IntoIterator<Item = String>) -> Vec<(ConnId, Vec<String>)> {
        let new: Vec<String> = markets
            .into_iter()
            .filter(|m| !self.assigned.contains_key(m))
            .collect();
        // Open enough sockets for the whole batch first, so placement can spread it.
        let free: usize = self
            .sockets
            .iter()
            .map(|s| self.max_per_socket - s.len())
            .sum();
        let need = new.len() * self.replicas;
        let mut open = need.saturating_sub(free).div_ceil(self.max_per_socket);
        while self.sockets.len() + open < self.replicas {
            open += 1;
        }
        self.sockets.extend((0..open).map(|_| BTreeSet::new()));

        let mut added: HashMap<ConnId, Vec<String>> = HashMap::new();
        for m in new {
            let mut chosen = Vec::with_capacity(self.replicas);
            for r in 0..self.replicas {
                let c = (0..self.sockets.len())
                    .filter(|&c| {
                        self.sockets[c].len() < self.max_per_socket && !chosen.contains(&c)
                    })
                    .min_by_key(|&c| (self.sockets[c].len(), tiebreak(&m, r, c)))
                    .unwrap_or_else(|| {
                        // Free room exists only on sockets this market already uses.
                        self.sockets.push(BTreeSet::new());
                        self.sockets.len() - 1
                    });
                self.sockets[c].insert(m.clone());
                added.entry(c).or_default().push(m.clone());
                chosen.push(c);
            }
            self.assigned.insert(m, chosen);
        }
        sorted(added)
    }

    /// Forget markets. Returns the removals per socket.
    pub fn remove<'a>(
        &mut self,
        markets: impl IntoIterator<Item = &'a String>,
    ) -> Vec<(ConnId, Vec<String>)> {
        let mut removed: HashMap<ConnId, Vec<String>> = HashMap::new();
        for m in markets {
            for c in self.assigned.remove(m).unwrap_or_default() {
                self.sockets[c].remove(m);
                removed.entry(c).or_default().push(m.clone());
            }
        }
        sorted(removed)
    }
}

fn tiebreak(market: &str, replica: usize, socket: ConnId) -> u64 {
    let mut h = DefaultHasher::new();
    (market, replica, socket).hash(&mut h);
    h.finish()
}

fn sorted(map: HashMap<ConnId, Vec<String>>) -> Vec<(ConnId, Vec<String>)> {
    let mut v: Vec<_> = map.into_iter().collect();
    v.sort();
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn names(r: std::ops::Range<usize>) -> Vec<String> {
        r.map(|i| format!("M{i:04}")).collect()
    }

    #[test]
    fn replicas_on_distinct_sockets_within_capacity() {
        let mut p = Planner::new(100, 2);
        let plan = p.add(names(0..1000));
        assert_eq!(p.sockets(), 20);
        let mut per_market: HashMap<&String, Vec<ConnId>> = HashMap::new();
        for (c, ms) in &plan {
            assert!(ms.len() <= 100);
            for m in ms {
                per_market.entry(m).or_default().push(*c);
            }
        }
        assert!(
            per_market
                .values()
                .all(|cs| cs.len() == 2 && cs[0] != cs[1])
        );
    }

    #[test]
    fn backups_are_mixed() {
        // Markets that share a primary socket should spread their backups widely.
        let mut p = Planner::new(100, 2);
        p.add(names(0..1000));
        let mut pairs: HashMap<ConnId, HashSet<ConnId>> = HashMap::new();
        for cs in p.assigned.values() {
            pairs.entry(cs[0]).or_default().insert(cs[1]);
        }
        let min_spread = pairs.values().map(HashSet::len).min().unwrap();
        assert!(
            min_spread >= 5,
            "a socket's markets all back up to {min_spread} sockets"
        );
    }

    proptest::proptest! {
        /// Random batches of adds and removes: every market is on exactly `replicas`
        /// distinct sockets, no socket is over capacity, the socket sets agree with the
        /// assignments, and a market that stays never moves.
        #[test]
        fn invariants_hold_under_random_adds_and_removes(
            cap in 1usize..20,
            replicas in 1usize..4,
            ops in proptest::collection::vec((proptest::bool::ANY, 0usize..60, 1usize..15), 1..40),
        ) {
            let mut p = Planner::new(cap, replicas);
            for (add, start, n) in ops {
                let batch = names(start..start + n);
                let before = p.assigned.clone();
                if add {
                    p.add(batch.clone());
                } else {
                    p.remove(&batch);
                }
                for (m, cs) in &p.assigned {
                    let distinct: HashSet<_> = cs.iter().collect();
                    proptest::prop_assert_eq!(distinct.len(), replicas, "{} on {:?}", m, cs);
                    for &c in cs {
                        proptest::prop_assert!(p.sockets[c].contains(m));
                    }
                    if let Some(old) = before.get(m) {
                        proptest::prop_assert_eq!(old, cs, "{} moved", m);
                    }
                }
                for s in &p.sockets {
                    proptest::prop_assert!(s.len() <= cap);
                    proptest::prop_assert!(s.iter().all(|m| p.assigned.contains_key(m)));
                }
                for m in &batch {
                    proptest::prop_assert_eq!(p.contains(m), add);
                }
            }
        }
    }

    #[test]
    fn sticky_idempotent_and_reuses_freed_room() {
        let mut p = Planner::new(2, 2);
        p.add(names(0..2));
        assert_eq!(p.sockets(), 2);
        assert!(p.add(names(0..2)).is_empty());
        let removed = p.remove(&names(0..1));
        assert_eq!(removed.len(), 2); // both replicas
        assert!(p.remove(&names(0..1)).is_empty());
        p.add(names(5..6));
        assert_eq!(p.sockets(), 2); // freed room reused, no new socket
        assert!(p.contains("M0005") && !p.contains("M0000"));
        // Room left only on one socket: the second replica needs a new socket.
        let mut q = Planner::new(2, 2);
        q.add(names(0..1));
        q.add(names(1..2));
        q.remove(&names(0..1));
        q.add(names(2..4)); // fills both
        q.remove(&names(1..2));
        q.add(names(9..10));
        assert!(q.assigned["M0009"][0] != q.assigned["M0009"][1]);
    }
}
