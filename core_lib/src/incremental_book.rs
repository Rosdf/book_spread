use crate::heapless_linear_map::HeaplessLinearMap;
use crate::positive_f64::PositiveF64;
use std::cmp::Ordering;
use std::collections::{BTreeMap, Bound};
use std::hint::cold_path;
use std::marker::PhantomData;

/// Counts of where price updates landed: `(shallow, deep)`.
///
/// Process-wide, and `(0, 0)` unless the `book_stats` feature is on.
///
/// # What this is for
///
/// The `cold_path()` on the deep branch of `BookSide::update` is an assumption, not a
/// measurement. Binance's `@depth` diffs carry levels across the whole book, so for some
/// venue and symbol pairs the "deeper than the linear window" branch may well be the common
/// one - in which case that hint is pessimising the hot loop rather than helping it, and the
/// answer is venue- and symbol-dependent. This is how to find out against live traffic
/// before anyone changes it, rather than reasoning about it.
///
/// Off by default because it is an atomic increment on the per-level path, which is exactly
/// the path in question.
pub fn depth_stats() -> (u64, u64) {
    #[cfg(feature = "book_stats")]
    {
        use std::sync::atomic::Ordering as AtomicOrdering;
        // `Relaxed`: these are counters and nothing else. No data is published through them,
        // and no other read or write is ordered against them, so the only guarantee needed is
        // that each increment is not lost - which `fetch_add` gives at any ordering.
        (
            stats::SHALLOW.load(AtomicOrdering::Relaxed),
            stats::DEEP.load(AtomicOrdering::Relaxed),
        )
    }
    #[cfg(not(feature = "book_stats"))]
    {
        (0, 0)
    }
}

#[cfg(feature = "book_stats")]
mod stats {
    use std::sync::atomic::AtomicU64;

    pub(super) static SHALLOW: AtomicU64 = AtomicU64::new(0);
    pub(super) static DEEP: AtomicU64 = AtomicU64::new(0);
}

/// Records where one price update landed. Compiles to nothing without `book_stats`.
#[inline(always)]
fn record_depth(deep: bool) {
    #[cfg(feature = "book_stats")]
    {
        use std::sync::atomic::Ordering as AtomicOrdering;
        let counter = if deep { &stats::DEEP } else { &stats::SHALLOW };
        // `Relaxed`: see `depth_stats`.
        counter.fetch_add(1, AtomicOrdering::Relaxed);
    }
    #[cfg(not(feature = "book_stats"))]
    {
        let _ = deep;
    }
}

#[derive(Debug)]
struct Bid;
#[derive(Debug)]
struct Ask;

#[derive(Debug)]
struct LevelPrice<T> {
    value: PositiveF64,
    side: PhantomData<fn() -> T>,
}

impl<T> LevelPrice<T> {
    fn new(value: PositiveF64) -> Self {
        Self {
            value,
            side: PhantomData,
        }
    }
}

#[derive(Debug)]
struct BookSide<T> {
    first_levels: HeaplessLinearMap<LevelPrice<T>, PositiveF64, 20>,
    deap_levels: BTreeMap<LevelPrice<T>, PositiveF64>,
}

#[derive(Debug, Copy, Clone)]
pub struct Level {
    price: PositiveF64,
    size: PositiveF64,
}

impl Level {
    pub fn new(price: PositiveF64, size: PositiveF64) -> Self {
        Self { price, size }
    }

    pub fn price(&self) -> PositiveF64 {
        self.price
    }

    pub fn size(&self) -> PositiveF64 {
        self.size
    }
}

impl<T> Clone for BookSide<T> {
    fn clone(&self) -> Self {
        Self {
            first_levels: self.first_levels.clone(),
            deap_levels: self.deap_levels.clone(),
        }
    }

    fn clone_from(&mut self, source: &Self) {
        self.first_levels.clone_from(&source.first_levels);
        self.deap_levels.clone_from(&source.deap_levels);
    }
}

impl<T> PartialEq for LevelPrice<T> {
    fn eq(&self, other: &Self) -> bool {
        self.value.eq(&other.value)
    }
}

impl<T> Eq for LevelPrice<T> {}

impl<T> Clone for LevelPrice<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for LevelPrice<T> {}

impl<T> PartialOrd for LevelPrice<T>
where
    Self: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for LevelPrice<Ask> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value.cmp(&other.value)
    }
}

impl Ord for LevelPrice<Bid> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value.cmp(&other.value).reverse()
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum UpdateResult {
    Close,
    Deap,
    Both,
}

impl UpdateResult {
    pub fn merge(self, other: UpdateResult) -> UpdateResult {
        match self {
            Self::Close => match other {
                UpdateResult::Close => Self::Close,
                UpdateResult::Deap => Self::Both,
                UpdateResult::Both => Self::Both,
            },
            Self::Deap => match other {
                UpdateResult::Close => Self::Both,
                UpdateResult::Deap => Self::Deap,
                UpdateResult::Both => Self::Both,
            },
            Self::Both => Self::Both,
        }
    }
}

#[derive(Debug, Clone)]
pub struct IncrementalBook {
    asks: BookSide<Ask>,
    bids: BookSide<Bid>,
}

impl<T> BookSide<T>
where
    LevelPrice<T>: Ord,
{
    const fn new() -> Self {
        Self {
            first_levels: HeaplessLinearMap::new(),
            deap_levels: BTreeMap::new(),
        }
    }

    fn update(&mut self, price: LevelPrice<T>, size: PositiveF64) -> UpdateResult {
        match self.first_levels.insert(price, size) {
            Ok(Some((evicted_price, evicted_size))) => {
                record_depth(false);
                let mut cursor = self.deap_levels.lower_bound_mut(Bound::Unbounded);
                // SAFETY:
                // all elements in map has greater value then all values in linear map
                unsafe {
                    cursor.insert_before_unchecked(evicted_price, evicted_size);
                }
                UpdateResult::Both
            }
            Ok(None) => {
                record_depth(false);
                UpdateResult::Close
            }
            Err(_) => {
                // Assumed rare rather than measured to be. Turn on `book_stats` and read
                // `depth_stats` against a live feed before trusting this hint - see that
                // function for why the answer may well be the other way round.
                cold_path();
                record_depth(true);
                self.deap_levels.insert(price, size);
                UpdateResult::Deap
            }
        }
    }

    fn clear(&mut self) {
        self.first_levels.clear();
        self.deap_levels.clear();
    }

    fn remove(&mut self, price: LevelPrice<T>) -> Option<UpdateResult> {
        let Some(last_key) = self.first_levels.last().map(|x| *x.0) else {
            cold_path();
            return None;
        };

        if price <= last_key {
            if self.first_levels.remove(&price).is_none() {
                cold_path();
                return None;
            }
            if let Some(first) = self.deap_levels.pop_first() {
                // SAFETY:
                // 1. deap_levels have greater values then all first_levels
                // 2. we just popped 1 elem from first levels
                unsafe {
                    self.first_levels.insert_last_unchecked(first.0, first.1);
                }
                return Some(UpdateResult::Both);
            }

            Some(UpdateResult::Close)
        } else if self.deap_levels.remove(&price).is_none() {
            cold_path();
            None
        } else {
            Some(UpdateResult::Deap)
        }
    }
}

impl IncrementalBook {
    pub const fn new() -> Self {
        Self {
            asks: BookSide::new(),
            bids: BookSide::new(),
        }
    }

    /// Drops every level on both sides, leaving the book as if freshly constructed.
    ///
    /// Lets a connector reuse the book it already owns when a feed gap forces a resync,
    /// instead of building a new one.
    pub fn clear(&mut self) {
        self.asks.clear();
        self.bids.clear();
    }

    pub fn update_ask(&mut self, price: PositiveF64, size: PositiveF64) -> UpdateResult {
        Self::update_side(&mut self.asks, price, size)
    }

    pub fn update_bid(&mut self, price: PositiveF64, size: PositiveF64) -> UpdateResult {
        Self::update_side(&mut self.bids, price, size)
    }

    pub fn remove_ask(&mut self, price: PositiveF64) -> Option<UpdateResult> {
        Self::remove_from_side(&mut self.asks, price)
    }

    pub fn remove_bid(&mut self, price: PositiveF64) -> Option<UpdateResult> {
        Self::remove_from_side(&mut self.bids, price)
    }

    pub fn first_asks(&self) -> impl ExactSizeIterator<Item = Level> {
        Self::first_on_side(&self.asks)
    }

    pub fn first_bids(&self) -> impl ExactSizeIterator<Item = Level> {
        Self::first_on_side(&self.bids)
    }

    #[inline(always)]
    fn first_on_side<T>(side: &BookSide<T>) -> impl ExactSizeIterator<Item = Level> {
        side.first_levels
            .iter()
            .map(|(price, size)| Level::new(price.value, *size))
    }

    #[inline(always)]
    fn update_side<T>(side: &mut BookSide<T>, price: PositiveF64, size: PositiveF64) -> UpdateResult
    where
        LevelPrice<T>: Ord,
    {
        let sided = LevelPrice::<T>::new(price);
        side.update(sided, size)
    }

    #[inline(always)]
    fn remove_from_side<T>(side: &mut BookSide<T>, price: PositiveF64) -> Option<UpdateResult>
    where
        LevelPrice<T>: Ord,
    {
        let sided = LevelPrice::<T>::new(price);
        side.remove(sided)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn price(v: f64) -> PositiveF64 {
        PositiveF64::new(v).unwrap()
    }

    fn level(v: f64) -> LevelPrice<Ask> {
        LevelPrice::new(price(v))
    }

    /// Fills `first_levels` to its capacity of 20 with prices `1..=20`,
    /// using each price as its own size so entries are identifiable by value.
    fn full_ask_side() -> BookSide<Ask> {
        let mut side = BookSide::<Ask>::new();
        for p in 1..=20 {
            side.update(level(f64::from(p)), price(f64::from(p)));
        }
        side
    }

    #[test]
    fn overflow_goes_straight_to_deep_book() {
        let mut side = full_ask_side();

        // Worse than everything already in first_levels -> Err branch.
        side.update(level(21.0), price(21.0));

        assert_eq!(side.first_levels.len(), 20);
        assert_eq!(side.first_levels.last(), Some((&level(20.0), &price(20.0))));
        assert_eq!(side.deap_levels.len(), 1);
        assert_eq!(side.deap_levels.get(&level(21.0)), Some(&price(21.0)));
    }

    #[test]
    fn eviction_demotes_the_actual_evicted_level_not_the_new_one() {
        let mut side = full_ask_side();
        side.update(level(21.0), price(21.0));

        // Better than everything currently in first_levels -> evicts the
        // current worst entry (price 20).
        side.update(level(0.5), price(999.0));

        // The evicted level (price 20, its original size) must be the one
        // demoted into the deep book, not the newly-inserted price/value.
        assert_eq!(side.deap_levels.len(), 2);
        assert_eq!(side.deap_levels.get(&level(20.0)), Some(&price(20.0)));
        assert_eq!(side.deap_levels.get(&level(21.0)), Some(&price(21.0)));
        assert!(!side.deap_levels.contains_key(&level(0.5)));

        // first_levels must hold the new price, and no longer the evicted one.
        assert_eq!(side.first_levels.len(), 20);
        assert_eq!(side.first_levels.last(), Some((&level(19.0), &price(19.0))));
    }

    #[test]
    fn removed_level_is_replenished_from_the_deep_book() {
        let mut side = full_ask_side();
        side.update(level(21.0), price(21.0));
        side.update(level(0.5), price(999.0));

        // Removing the new best level should promote the deep book's best
        // (price 20, the level that was evicted above) back into first_levels.
        side.remove(level(0.5));

        assert_eq!(side.first_levels.len(), 20);
        assert_eq!(side.first_levels.last(), Some((&level(20.0), &price(20.0))));
        assert_eq!(side.deap_levels.len(), 1);
        assert_eq!(side.deap_levels.get(&level(21.0)), Some(&price(21.0)));
    }

    #[test]
    fn update_into_available_room_returns_close() {
        let mut side = BookSide::<Ask>::new();
        assert_eq!(side.update(level(1.0), price(1.0)), UpdateResult::Close);
    }

    #[test]
    fn update_existing_first_level_entry_returns_close() {
        let mut side = full_ask_side();
        assert_eq!(side.update(level(5.0), price(500.0)), UpdateResult::Close);
        assert_eq!(side.first_levels.len(), 20);
    }

    #[test]
    fn update_worse_than_full_book_returns_deap() {
        let mut side = full_ask_side();
        assert_eq!(side.update(level(21.0), price(21.0)), UpdateResult::Deap);
    }

    #[test]
    fn update_existing_deep_book_entry_returns_deap() {
        let mut side = full_ask_side();
        side.update(level(21.0), price(21.0));

        // 21 already lives in deap_levels; updating it again should stay there.
        assert_eq!(side.update(level(21.0), price(2100.0)), UpdateResult::Deap);
        assert_eq!(side.deap_levels.get(&level(21.0)), Some(&price(2100.0)));
    }

    #[test]
    fn update_causing_eviction_returns_both() {
        let mut side = full_ask_side();
        side.update(level(21.0), price(21.0));
        assert_eq!(side.update(level(0.5), price(999.0)), UpdateResult::Both);
    }

    #[test]
    fn clear_empties_both_tiers() {
        let mut side = full_ask_side();
        side.update(level(21.0), price(21.0));

        side.clear();

        assert_eq!(side.first_levels.len(), 0);
        assert!(side.deap_levels.is_empty());
    }

    #[test]
    fn cleared_book_behaves_like_a_fresh_one() {
        let mut reused = IncrementalBook::new();
        reused.update_ask(price(10.0), price(1.0));
        reused.update_bid(price(9.0), price(1.0));
        reused.clear();

        let mut fresh = IncrementalBook::new();
        for book in [&mut reused, &mut fresh] {
            book.update_ask(price(100.0), price(2.0));
            book.update_bid(price(99.0), price(3.0));
        }

        let levels = |b: &IncrementalBook| {
            let asks: Vec<_> = b.first_asks().map(|l| (l.price(), l.size())).collect();
            let bids: Vec<_> = b.first_bids().map(|l| (l.price(), l.size())).collect();
            (asks, bids)
        };
        assert_eq!(levels(&reused), levels(&fresh));
    }

    #[test]
    fn remove_from_empty_side_returns_none() {
        let mut side = BookSide::<Ask>::new();
        assert_eq!(side.remove(level(1.0)), None);
    }

    #[test]
    fn remove_missing_price_within_first_levels_range_returns_none() {
        let mut side = full_ask_side();
        assert_eq!(side.remove(level(5.5)), None);
        assert_eq!(side.first_levels.len(), 20);
    }

    #[test]
    fn remove_first_levels_entry_without_deep_book_returns_close() {
        let mut side = full_ask_side();
        assert_eq!(side.remove(level(5.0)), Some(UpdateResult::Close));
        assert_eq!(side.first_levels.len(), 19);
    }

    #[test]
    fn remove_first_levels_entry_promotes_from_deep_book_returns_both() {
        let mut side = full_ask_side();
        side.update(level(21.0), price(21.0));
        side.update(level(0.5), price(999.0));
        assert_eq!(side.remove(level(0.5)), Some(UpdateResult::Both));
    }

    #[test]
    fn remove_deep_book_entry_returns_deap() {
        let mut side = full_ask_side();
        side.update(level(21.0), price(21.0));
        assert_eq!(side.remove(level(21.0)), Some(UpdateResult::Deap));
        assert!(side.deap_levels.is_empty());
    }

    #[test]
    fn remove_missing_price_within_deep_book_range_returns_none() {
        let mut side = full_ask_side();
        side.update(level(21.0), price(21.0));

        // 22 is worse than everything (including the deep book's only entry,
        // 21) but was never actually inserted anywhere.
        assert_eq!(side.remove(level(22.0)), None);
        assert_eq!(side.deap_levels.len(), 1);
        assert!(side.deap_levels.contains_key(&level(21.0)));
    }
}
