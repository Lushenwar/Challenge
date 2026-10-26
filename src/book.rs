//! One market's order book in fixed point. Pure: no I/O, no clocks.

use std::collections::BTreeMap;

use crate::{Side, fixed::SCALE};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BookError {
    #[error("price {0} outside [0, {SCALE}]")]
    BadPrice(i64),
    #[error("non-positive quantity {qty} at {price}")]
    BadQuantity { price: i64, qty: i64 },
    #[error("duplicate level at {0}")]
    Duplicate(i64),
    #[error("delta {delta} at {price} drives level {current} negative")]
    Negative {
        price: i64,
        current: i64,
        delta: i64,
    },
    #[error("quantity overflow at {0}")]
    Overflow(i64),
}

/// Levels as `(price, quantity)`.
pub type Levels = Vec<(i64, i64)>;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OrderBook {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
}

impl OrderBook {
    /// Build a book from a full snapshot. Rejects duplicate prices, prices outside
    /// [0, SCALE] and non-positive quantities.
    pub fn from_levels(bids: &[(i64, i64)], asks: &[(i64, i64)]) -> Result<Self, BookError> {
        fn side(levels: &[(i64, i64)]) -> Result<BTreeMap<i64, i64>, BookError> {
            let mut m = BTreeMap::new();
            for &(price, qty) in levels {
                check_price(price)?;
                if qty <= 0 {
                    return Err(BookError::BadQuantity { price, qty });
                }
                if m.insert(price, qty).is_some() {
                    return Err(BookError::Duplicate(price));
                }
            }
            Ok(m)
        }
        Ok(Self {
            bids: side(bids)?,
            asks: side(asks)?,
        })
    }

    /// Apply a signed delta; returns the new absolute quantity (0 = level removed).
    /// On error the book is unchanged, and the caller must treat it as invalid.
    pub fn apply_delta(&mut self, side: Side, price: i64, delta: i64) -> Result<i64, BookError> {
        check_price(price)?;
        let levels = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        let current = levels.get(&price).copied().unwrap_or(0);
        let new = current
            .checked_add(delta)
            .ok_or(BookError::Overflow(price))?;
        if new < 0 {
            return Err(BookError::Negative {
                price,
                current,
                delta,
            });
        }
        if new == 0 {
            levels.remove(&price);
        } else {
            levels.insert(price, new);
        }
        Ok(new)
    }

    /// Set an absolute quantity (0 removes the level). Used to mirror emitted output.
    pub fn set_level(&mut self, side: Side, price: i64, qty: i64) {
        let levels = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        if qty == 0 {
            levels.remove(&price);
        } else {
            levels.insert(price, qty);
        }
    }

    /// Bids descending by price, asks ascending: the order used in `Snapshot`.
    pub fn levels(&self) -> (Levels, Levels) {
        (
            self.bids.iter().rev().map(|(&p, &q)| (p, q)).collect(),
            self.asks.iter().map(|(&p, &q)| (p, q)).collect(),
        )
    }

    /// Best bid >= best ask. Logged and counted by callers, never rejected.
    pub fn is_crossed(&self) -> bool {
        match (self.bids.keys().next_back(), self.asks.keys().next()) {
            (Some(b), Some(a)) => b >= a,
            _ => false,
        }
    }
}

fn check_price(price: i64) -> Result<(), BookError> {
    if (0..=SCALE).contains(&price) {
        Ok(())
    } else {
        Err(BookError::BadPrice(price))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn snapshot_then_deltas() {
        let mut b =
            OrderBook::from_levels(&[(400_000, 10), (300_000, 5)], &[(600_000, 7)]).unwrap();
        assert_eq!(b.apply_delta(Side::Buy, 400_000, -4), Ok(6));
        assert_eq!(b.apply_delta(Side::Sell, 600_000, -7), Ok(0));
        assert_eq!(b.apply_delta(Side::Sell, 650_000, 3), Ok(3));
        assert_eq!(
            b.levels(),
            (vec![(400_000, 6), (300_000, 5)], vec![(650_000, 3)])
        );
    }

    #[test]
    fn negative_result_is_an_error_and_leaves_book_unchanged() {
        let mut b = OrderBook::from_levels(&[(400_000, 10)], &[]).unwrap();
        let before = b.clone();
        assert!(matches!(
            b.apply_delta(Side::Buy, 400_000, -11),
            Err(BookError::Negative { .. })
        ));
        assert!(matches!(
            b.apply_delta(Side::Sell, 500_000, -1),
            Err(BookError::Negative { .. })
        ));
        assert_eq!(b, before);
    }

    #[test]
    fn rejects_bad_snapshots() {
        assert_eq!(
            OrderBook::from_levels(&[(1, 1), (1, 2)], &[]),
            Err(BookError::Duplicate(1))
        );
        assert_eq!(
            OrderBook::from_levels(&[], &[(SCALE + 1, 1)]),
            Err(BookError::BadPrice(SCALE + 1))
        );
        assert_eq!(
            OrderBook::from_levels(&[(5, 0)], &[]),
            Err(BookError::BadQuantity { price: 5, qty: 0 })
        );
    }

    #[test]
    fn set_level_is_absolute_and_zero_removes() {
        let mut b = OrderBook::from_levels(&[(400_000, 10)], &[(600_000, 7)]).unwrap();
        b.set_level(Side::Buy, 400_000, 3); // replaces, does not add
        b.set_level(Side::Sell, 600_000, 0);
        b.set_level(Side::Sell, 700_000, 2);
        b.set_level(Side::Buy, 100_000, 0); // removing an absent level is a no-op
        assert_eq!(b.levels(), (vec![(400_000, 3)], vec![(700_000, 2)]));
    }

    #[test]
    fn crossed_vs_locked() {
        let locked = OrderBook::from_levels(&[(500_000, 1)], &[(500_000, 1)]).unwrap();
        assert!(locked.is_crossed()); // best bid >= best ask
        let ok = OrderBook::from_levels(&[(400_000, 1)], &[(500_000, 1)]).unwrap();
        assert!(!ok.is_crossed());
    }

    proptest! {
        /// Random delta sequences never leave a negative or zero level behind, and every
        /// rejected delta was exactly one that would have gone negative.
        #[test]
        fn never_negative(ops in prop::collection::vec((any::<bool>(), 0i64..20, -50i64..50), 0..300)) {
            let mut b = OrderBook::default();
            let mut model = std::collections::HashMap::<(bool, i64), i64>::new();
            for (buy, price, delta) in ops {
                let side = if buy { Side::Buy } else { Side::Sell };
                let cur = model.get(&(buy, price)).copied().unwrap_or(0);
                match b.apply_delta(side, price, delta) {
                    Ok(q) => { prop_assert_eq!(q, cur + delta); model.insert((buy, price), q); }
                    Err(_) => prop_assert!(cur + delta < 0),
                }
            }
            let (bids, asks) = b.levels();
            prop_assert!(bids.iter().chain(&asks).all(|&(_, q)| q > 0));
        }
    }
}
