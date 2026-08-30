use crate::heapless_linear_map::{HeaplessLinearMap, Position};
use crate::positive_f64::PositiveF64;
use std::cmp::Ordering;
use std::collections::VecDeque;
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

/// Levels the window tier holds - the sorted array a price update is scanned against.
///
/// Wide, because with the scan, the shift and the boundary check all costing `O(pos)` rather
/// than `O(WINDOW)`, the only thing depth buys back is how often a spill or a refill has to
/// run at all: the band either side of the boundary is `(WINDOW - PUBLISHED_DEPTH) / 2`
/// operations wide, so a symbol whose inserts and removals roughly balance never touches the
/// deep tier.
/// The cost is 16 bytes a level in the window itself - 648 bytes a side at this depth,
/// against 328 at half of it - which a symbol pays whether or not it ever fills them.
const WINDOW: usize = 40;

/// The published depth [`IncrementalBook`] tunes its sides for.
///
/// The book has no business knowing how deep anyone publishes it - that is
/// `venue::levels::worth_publishing`'s business, and it asserts this agrees with what it
/// shows. This is only the floor the window keeps ready underneath such a reader.
pub const PUBLISHED_DEPTH: usize = 10;

/// What a spill leaves in the window, and what a refill tops it back up to.
///
/// Halfway between the two is what buys the hysteresis: a window that spilled down to
/// `TARGET` has `WINDOW - TARGET` insertions of headroom before it spills again, and one that
/// refilled up to it has `TARGET - PUBLISHED_DEPTH` removals before it refills again. Pick
/// `TARGET = WINDOW` and every insertion at a full window spills; pick
/// `TARGET = PUBLISHED_DEPTH` and every removal at the boundary refills.
const TARGET: usize = usize::midpoint(WINDOW, PUBLISHED_DEPTH);

/// Operations either side of the boundary that stay inside the window entirely.
const BAND: usize = WINDOW - TARGET;

const _: () = assert!(
    PUBLISHED_DEPTH <= TARGET && TARGET < WINDOW && BAND > 0,
    "a spill has to leave the published prefix intact, and leave room to insert into"
);

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
    /// The best levels, sorted ascending in this side's own order. Never fewer than
    /// [`PUBLISHED_DEPTH`] of them while [`Self::deap_levels`] is non-empty, which is what
    /// keeps a refill off the publishing path.
    first_levels: HeaplessLinearMap<LevelPrice<T>, PositiveF64, WINDOW>,
    /// Everything worse than the window, sorted ascending in this side's own order, so the
    /// best of them is at the front - the end the window spills into and refills from.
    ///
    /// An array of pairs rather than the window's split arrays: this is the cold tier,
    /// reached by binary search rather than by the scan that wants keys packed together.
    deap_levels: VecDeque<(LevelPrice<T>, PositiveF64)>,
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

/// The shallowest window index one update disturbed, or `None` when nothing above the deep
/// tier moved.
///
/// A newtype over `Option<u8>` rather than an `enum { Deep, Shallow(u8) }`: the two carry the
/// same information, but the enum is a payload-carrying variant beside an empty one, which is
/// what the workspace's `variant_size_differences` is set to complain about.
///
/// An index rather than a coarser "the window changed": what the book publishes is a prefix
/// of the window, so the only question anyone downstream asks of this is whether the change
/// was shallow enough to show - see `worth_publishing`, which is where that depth is known.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct UpdateResult(Option<u8>);

impl UpdateResult {
    /// Only the deep tier moved.
    pub const fn deep() -> Self {
        Self(None)
    }

    /// The window changed, starting at `pos`.
    ///
    /// # Panics
    ///
    /// When `pos` is not a window index - the window is [`WINDOW`] deep and this carries an
    /// index to it in a byte.
    #[must_use]
    pub fn shallow(pos: usize) -> Self {
        const _: () = assert!(
            WINDOW <= u8::MAX as usize,
            "a window index has to fit the byte this carries it in"
        );

        Self(Some(
            u8::try_from(pos).expect("a window index fits a byte, asserted above"),
        ))
    }

    /// The shallowest window index this touched, or `None` for a deep-only change.
    pub const fn shallowest(self) -> Option<u8> {
        self.0
    }

    /// Where two updates together first disturbed the window: the shallower of the two.
    ///
    /// Not [`Option::min`], which orders `None` below every `Some` - here `None` means "no
    /// window index at all", which is deeper than any of them.
    #[must_use]
    pub fn merge(self, other: Self) -> Self {
        match (self.0, other.0) {
            (Some(mine), Some(theirs)) => Self(Some(mine.min(theirs))),
            (Some(mine), None) => Self(Some(mine)),
            (None, theirs) => Self(theirs),
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
            deap_levels: VecDeque::new(),
        }
    }

    /// Files `size` under `price`, in whichever tier the price belongs to.
    ///
    /// The routing decision is explicit rather than falling out of a full window, because
    /// the hysteresis below means a non-empty deep tier no longer implies a full one.
    fn update(&mut self, price: LevelPrice<T>, size: PositiveF64) -> UpdateResult {
        if !self.belongs_in_window(price) {
            // Assumed rare rather than measured to be. Turn on `book_stats` and read
            // `depth_stats` against a live feed before trusting this hint - see that
            // function for why the answer may well be the other way round.
            cold_path();
            record_depth(true);
            self.deep_update(price, size);
            return UpdateResult::deep();
        }

        record_depth(false);

        let pos = match self.first_levels.locate(&price) {
            // A price the window already carries: its size is overwritten where it lies.
            // Nothing grows, so nothing has to spill - which is the whole reason this
            // returns before the check below rather than after it.
            Position::Occupied(pos) => {
                // SAFETY:
                // `Occupied(pos)` is only reported for a position a key was found at.
                unsafe {
                    *self.first_levels.value_mut_unchecked(pos) = size;
                }
                return UpdateResult::shallow(pos);
            }
            Position::Vacant(pos) => pos,
        };

        if !self.first_levels.is_full() {
            // SAFETY:
            // `pos` is where `locate` says `price` belongs, and the window is not full.
            unsafe {
                self.first_levels.insert_at_unchecked(pos, price, size);
            }
            return UpdateResult::shallow(pos);
        }

        self.spill();

        // `pos` was measured against the window as it was, so a price that sat in the half
        // just spilled belongs with it - in the deep tier, not at a position the window no
        // longer has. The window is at `TARGET` from here on, so this cannot spill twice.
        // The spill itself cannot make the answer shallower than the insertion that
        // caused it: it only ever moves levels at `TARGET` and worse, and `TARGET` is
        // never shallower than the published depth. So the batching stays out of the
        // publish decision, which is the one place it could have leaked into.
        if pos >= TARGET {
            self.deep_update(price, size);
            return UpdateResult::deep();
        }

        // SAFETY:
        // `pos < TARGET`, so it is still within the window, and the spill left it
        // `WINDOW - TARGET` slots short of full.
        unsafe {
            self.first_levels.insert_at_unchecked(pos, price, size);
        }

        UpdateResult::shallow(pos)
    }

    /// Whether `price` is one of the best levels this side carries.
    ///
    /// Better than the window's worst, always. Worse than it, only when the window has room
    /// and the price is better than everything already deep - the case that lets a fresh
    /// book or a snapshot replay fill the window at all, instead of putting one level in it
    /// and the rest in the deep tier.
    fn belongs_in_window(&self, price: LevelPrice<T>) -> bool {
        match self.first_levels.worst() {
            None => true,
            Some((worst, _)) if price <= *worst => true,
            Some(_) => {
                !self.first_levels.is_full()
                    && self
                        .deap_levels
                        .front()
                        .is_none_or(|(deep, _)| price < *deep)
            }
        }
    }

    /// Drops every level in both tiers, keeping what they are built out of.
    ///
    /// The window resets to right-aligned-and-empty, which costs a store; the deep tier
    /// keeps its heap allocation, so a symbol that has resynced once never pays to grow it
    /// again. Both are what let `Slot::reset` reuse the book it already owns.
    fn clear(&mut self) {
        self.first_levels.clear();
        self.deap_levels.clear();
    }

    /// Drops `price` from whichever tier holds it, or reports that neither does.
    fn remove(&mut self, price: LevelPrice<T>) -> Option<UpdateResult> {
        let Some((worst, _)) = self.first_levels.worst() else {
            // An empty window means an empty book: the deep tier is only ever grown past
            // a window that is holding levels, and only ever drained back into one.
            cold_path();
            return None;
        };

        if price > *worst {
            let Ok(idx) = self.deep_position(price) else {
                cold_path();
                return None;
            };
            self.deap_levels.remove(idx);
            return Some(UpdateResult::deep());
        }

        let Position::Occupied(pos) = self.first_levels.locate(&price) else {
            cold_path();
            return None;
        };

        // SAFETY:
        // `Occupied(pos)` is only reported for a position a key was found at.
        unsafe {
            self.first_levels.remove_at_unchecked(pos);
        }

        // `pos` and not something deeper: removing at `pos` shifts every level worse than
        // it up by one, so `pos` really is the shallowest index that changed. A refill
        // appends at the worst end, no shallower than `PUBLISHED_DEPTH`, so it cannot beat it.
        if self.first_levels.len() < PUBLISHED_DEPTH {
            self.refill();
        }

        Some(UpdateResult::shallow(pos))
    }

    /// Moves the window's worst levels into the deep tier, leaving [`TARGET`] behind.
    fn spill(&mut self) {
        debug_assert!(self.first_levels.is_full(), "a spill is what makes room");

        // Worst first, so each level in turn is better than everything already deep and
        // the front is where it goes. That ordering is a property of where these came
        // from - the window's worst end - not something the deque checks.
        for level in self.first_levels.drain_worst(BAND).rev() {
            self.deap_levels.push_front(level);
        }
    }

    /// Tops the window back up to [`TARGET`] from the front of the deep tier.
    fn refill(&mut self) {
        let wanted = TARGET - self.first_levels.len();
        let count = usize::min(wanted, self.deap_levels.len());

        if count == 0 {
            return;
        }

        // SAFETY:
        // 1. `count <= TARGET - len <= WINDOW - len`, so the window has the room;
        // 2. every level in the deep tier is worse than every level in the window;
        // 3. the deque is sorted, so `drain` hands its front over best-first.
        unsafe {
            self.first_levels
                .extend_worst_unchecked(self.deap_levels.drain(..count));
        }
    }

    /// Files `size` under `price` in the deep tier, in place if it is already there.
    fn deep_update(&mut self, price: LevelPrice<T>, size: PositiveF64) {
        match self.deep_position(price) {
            Ok(idx) => self.deap_levels[idx].1 = size,
            Err(idx) => self.deap_levels.insert(idx, (price, size)),
        }
    }

    /// Where `price` sits in the deep tier, or where it would go - the deque is sorted, so
    /// this is a binary search rather than the window's scan.
    fn deep_position(&self, price: LevelPrice<T>) -> Result<usize, usize> {
        self.deap_levels
            .binary_search_by(|(deep_price, _)| deep_price.cmp(&price))
    }
}

impl Default for IncrementalBook {
    fn default() -> Self {
        Self::new()
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
    /// instead of building a new one - the window's storage is inline, and the deep tier
    /// keeps the allocation it had grown, so the next seed refills both without asking the
    /// allocator for anything.
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
    fn update_side<T>(
        target: &mut BookSide<T>,
        price: PositiveF64,
        size: PositiveF64,
    ) -> UpdateResult
    where
        LevelPrice<T>: Ord,
    {
        let sided = LevelPrice::<T>::new(price);
        target.update(sided, size)
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

    /// The sides `IncrementalBook` itself is built from.
    type AskSide = BookSide<Ask>;

    /// Removals a *full* window absorbs before one of them refills it.
    const SLACK: usize = WINDOW - PUBLISHED_DEPTH;

    fn price(v: f64) -> PositiveF64 {
        PositiveF64::new(v).unwrap()
    }

    fn level(v: f64) -> LevelPrice<Ask> {
        LevelPrice::new(price(v))
    }

    /// The `n`th level of a book priced `1.0, 2.0, ..`, so the tests can name levels
    /// relative to [`WINDOW`] rather than restating its value.
    fn nth(n: usize) -> f64 {
        f64::from(u16::try_from(n).expect("a book this deep is a test, not a market"))
    }

    /// The size filed under `price` in the deep tier, or `None` when it holds no such level.
    fn deep_size(side: &AskSide, price: LevelPrice<Ask>) -> Option<PositiveF64> {
        let idx = side.deep_position(price).ok()?;
        Some(side.deap_levels[idx].1)
    }

    /// The window's levels, best first.
    fn window_levels(side: &AskSide) -> Vec<(f64, f64)> {
        side.first_levels
            .iter()
            .map(|(window_price, size)| (window_price.value.get(), size.get()))
            .collect()
    }

    /// The deep tier's levels, best first - the order it is kept in.
    fn deep_levels(side: &AskSide) -> Vec<(f64, f64)> {
        side.deap_levels
            .iter()
            .map(|(deep_price, size)| (deep_price.value.get(), size.get()))
            .collect()
    }

    /// The levels `first..=last` at their own sizes, which is how every side here is built.
    fn levels(first: usize, last: usize) -> Vec<(f64, f64)> {
        (first..=last).map(|p| (nth(p), nth(p))).collect()
    }

    /// Fills the window to capacity with prices `1..=WINDOW`, using each price as its own
    /// size so entries are identifiable by value.
    fn full_ask_side() -> AskSide {
        let mut side = AskSide::new();
        for p in 1..=WINDOW {
            side.update(level(nth(p)), price(nth(p)));
        }
        side
    }

    /// A side whose window is full and whose deep tier holds `deep` levels, priced past it.
    fn side_with_deep(deep: usize) -> AskSide {
        let mut side = full_ask_side();
        for p in WINDOW + 1..=WINDOW + deep {
            side.update(level(nth(p)), price(nth(p)));
        }
        side
    }

    /// Strips the window down to `TARGET`, which is where a refill leaves it.
    fn drain_to_target(side: &mut AskSide) {
        for p in 1..=SLACK + 1 {
            side.remove(level(nth(p)));
        }
    }

    #[test]
    fn overflow_goes_straight_to_deep_book() {
        let mut side = full_ask_side();

        // Worse than everything in a window that is already full.
        assert_eq!(
            side.update(level(nth(WINDOW + 1)), price(nth(WINDOW + 1))),
            UpdateResult::deep()
        );

        assert_eq!(side.first_levels.len(), WINDOW);
        assert_eq!(
            side.first_levels.worst(),
            Some((&level(nth(WINDOW)), &price(nth(WINDOW))))
        );
        assert_eq!(deep_levels(&side), levels(WINDOW + 1, WINDOW + 1));
    }

    #[test]
    fn a_spill_demotes_the_window_s_worst_levels_not_the_new_one() {
        let mut side = side_with_deep(1);

        // Better than everything in the window, which is full - so the worst `BAND` levels
        // spill, and the new one goes in at the front.
        assert_eq!(
            side.update(level(0.5), price(999.0)),
            UpdateResult::shallow(0)
        );

        // The levels demoted are the window's worst, at their own sizes, in front of the
        // one already deep.
        assert_eq!(deep_levels(&side), levels(TARGET + 1, WINDOW + 1));
        assert!(deep_size(&side, level(0.5)).is_none());

        // And the window is left at `TARGET` plus the level that caused the spill.
        assert_eq!(side.first_levels.len(), TARGET + 1);
        assert_eq!(
            side.first_levels.worst(),
            Some((&level(nth(TARGET)), &price(nth(TARGET))))
        );
        assert_eq!(window_levels(&side)[0], (0.5, 999.0));
    }

    #[test]
    fn a_price_in_the_spilled_half_follows_it_into_the_deep_tier() {
        let mut side = full_ask_side();

        // Half a level past `TARGET`, so it sits inside the half about to be spilled: it
        // belongs with the levels around it, not at a position the window no longer has.
        let straddling = nth(TARGET) + 0.5;
        assert_eq!(
            side.update(level(straddling), price(straddling)),
            UpdateResult::deep(),
            "nothing a publisher can see moved"
        );

        assert_eq!(side.first_levels.len(), TARGET);
        assert_eq!(
            side.first_levels.worst(),
            Some((&level(nth(TARGET)), &price(nth(TARGET))))
        );

        let mut expected = levels(TARGET + 1, WINDOW);
        expected.insert(0, (straddling, straddling));
        assert_eq!(deep_levels(&side), expected);
    }

    #[test]
    fn an_update_to_a_price_the_full_window_already_holds_does_not_spill() {
        let mut side = full_ask_side();

        assert_eq!(
            side.update(level(5.0), price(500.0)),
            UpdateResult::shallow(4)
        );

        assert_eq!(
            side.first_levels.len(),
            WINDOW,
            "nothing grew, so nothing spilled"
        );
        assert!(side.deap_levels.is_empty());
        assert_eq!(window_levels(&side)[4], (5.0, 500.0));
    }

    #[test]
    fn seeding_fills_the_window_before_anything_reaches_the_deep_tier() {
        // What a snapshot replay looks like: far more levels than either tier alone holds.
        let deep = 100;
        let mut ascending = AskSide::new();
        for p in 1..=deep {
            ascending.update(level(nth(p)), price(nth(p)));
        }
        assert_eq!(window_levels(&ascending), levels(1, WINDOW));
        assert_eq!(deep_levels(&ascending), levels(WINDOW + 1, deep));

        // The same levels arriving worst-first, which is the order that has to spill its
        // way down rather than fill: the best `PUBLISHED_DEPTH` of them still end up published.
        let mut descending = AskSide::new();
        for p in (1..=deep).rev() {
            descending.update(level(nth(p)), price(nth(p)));
        }
        assert_eq!(window_levels(&descending)[..PUBLISHED_DEPTH], levels(1, PUBLISHED_DEPTH));
        assert_eq!(
            descending.first_levels.len() + descending.deap_levels.len(),
            deep
        );
    }

    #[test]
    fn removed_level_is_replenished_from_the_deep_book() {
        let mut side = side_with_deep(2 * BAND);

        // Removals inside the hysteresis band leave both tiers where they are.
        for p in 1..=SLACK {
            assert_eq!(
                side.remove(level(nth(p))),
                Some(UpdateResult::shallow(0)),
                "removing level {p} should not have refilled"
            );
        }
        assert_eq!(side.first_levels.len(), PUBLISHED_DEPTH);
        assert_eq!(side.deap_levels.len(), 2 * BAND);

        // The one that takes the window below the published depth refills it to `TARGET`,
        // best first.
        assert_eq!(
            side.remove(level(nth(SLACK + 1))),
            Some(UpdateResult::shallow(0))
        );
        assert_eq!(side.first_levels.len(), TARGET);
        assert_eq!(
            window_levels(&side),
            levels(SLACK + 2, WINDOW + TARGET - PUBLISHED_DEPTH + 1)
        );
        assert_eq!(
            deep_levels(&side),
            levels(WINDOW + TARGET - PUBLISHED_DEPTH + 2, WINDOW + 2 * BAND)
        );
    }

    #[test]
    fn a_refill_takes_what_the_deep_tier_has_when_that_is_less_than_it_wants() {
        let mut side = side_with_deep(2);

        for p in 1..=SLACK {
            assert_eq!(side.remove(level(nth(p))), Some(UpdateResult::shallow(0)));
        }
        assert_eq!(
            side.remove(level(nth(SLACK + 1))),
            Some(UpdateResult::shallow(0))
        );

        assert_eq!(
            side.first_levels.len(),
            PUBLISHED_DEPTH + 1,
            "the levels that were left, plus the two the deep tier had"
        );
        assert_eq!(
            side.first_levels.worst(),
            Some((&level(nth(WINDOW + 2)), &price(nth(WINDOW + 2))))
        );
        assert!(side.deap_levels.is_empty());

        // With the deep tier exhausted, the window is free to shrink past the threshold.
        assert_eq!(
            side.remove(level(nth(SLACK + 2))),
            Some(UpdateResult::shallow(0))
        );
        assert_eq!(side.first_levels.len(), PUBLISHED_DEPTH);
    }

    #[test]
    fn a_price_between_the_two_tiers_goes_into_a_window_that_has_room() {
        let mut side = side_with_deep(2 * BAND);
        drain_to_target(&mut side);

        // The window is at `TARGET`, so it has room, and this price is worse than its
        // worst level but better than everything deep: it belongs in the window, not
        // behind levels it beats.
        let worst = side.first_levels.worst().expect("the window is at TARGET").0;
        let between = worst.value.get() + 0.5;
        let deep_before = side.deap_levels.len();
        assert_eq!(side.first_levels.len(), TARGET);
        assert_eq!(
            side.update(level(between), price(between)),
            UpdateResult::shallow(TARGET)
        );
        assert_eq!(side.first_levels.len(), TARGET + 1);
        assert_eq!(
            side.first_levels.worst(),
            Some((&level(between), &price(between)))
        );
        assert_eq!(
            side.deap_levels.len(),
            deep_before,
            "and the deep tier is untouched"
        );

        // A price past the deep tier's front still goes deep, room or no room.
        let front = side.deap_levels.front().expect("levels are still deep").0;
        let past = front.value.get() + 0.5;
        assert_eq!(side.update(level(past), price(past)), UpdateResult::deep());
        assert_eq!(side.first_levels.len(), TARGET + 1);
        assert_eq!(side.deap_levels.len(), deep_before + 1);
    }

    #[test]
    fn update_into_available_room_reports_the_position_it_took() {
        let mut side = AskSide::new();
        assert_eq!(
            side.update(level(1.0), price(1.0)),
            UpdateResult::shallow(0)
        );
        assert_eq!(
            side.update(level(2.0), price(2.0)),
            UpdateResult::shallow(1)
        );
        assert_eq!(
            side.update(level(0.5), price(0.5)),
            UpdateResult::shallow(0),
            "a new best level disturbs everything from the front"
        );
    }

    #[test]
    fn update_worse_than_full_book_is_deep() {
        let mut side = full_ask_side();
        assert_eq!(
            side.update(level(nth(WINDOW + 1)), price(nth(WINDOW + 1))),
            UpdateResult::deep()
        );
    }

    #[test]
    fn update_existing_deep_book_entry_stays_deep() {
        let mut side = side_with_deep(1);

        // The level already lives in the deep tier; updating it again should stay there.
        let deep = nth(WINDOW + 1);
        assert_eq!(
            side.update(level(deep), price(2100.0)),
            UpdateResult::deep()
        );
        assert_eq!(deep_size(&side, level(deep)), Some(price(2100.0)));
    }

    #[test]
    fn clear_empties_both_tiers() {
        let mut side = side_with_deep(1);

        side.clear();

        assert_eq!(side.first_levels.len(), 0);
        assert!(side.deap_levels.is_empty());
    }

    #[test]
    fn clear_keeps_what_the_next_seed_will_need() {
        let mut side = side_with_deep(2 * BAND);
        let deep_capacity = side.deap_levels.capacity();
        assert!(deep_capacity > 0, "the deep tier grew while it was filled");

        side.clear();

        assert!(side.first_levels.is_empty());
        assert!(side.deap_levels.is_empty());
        assert_eq!(
            side.deap_levels.capacity(),
            deep_capacity,
            "a resync must not make the deep tier grow its allocation again"
        );

        // And the window is immediately fillable, right-aligned as a fresh one is.
        for p in 1..=WINDOW {
            side.update(level(nth(p)), price(nth(p)));
        }
        assert_eq!(window_levels(&side), levels(1, WINDOW));
        assert_eq!(side.deap_levels.capacity(), deep_capacity);
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

        let published = |b: &IncrementalBook| {
            let asks: Vec<_> = b.first_asks().map(|l| (l.price(), l.size())).collect();
            let bids: Vec<_> = b.first_bids().map(|l| (l.price(), l.size())).collect();
            (asks, bids)
        };
        assert_eq!(published(&reused), published(&fresh));
    }

    #[test]
    fn deep_tier_stays_sorted_however_levels_reach_it() {
        let mut side = full_ask_side();

        // Out of order, and worse than the window, so each goes through the binary search.
        for p in [WINDOW + 5, WINDOW + 1, WINDOW + 3] {
            side.update(level(nth(p)), price(nth(p)));
        }
        assert_eq!(
            deep_levels(&side),
            vec![
                (nth(WINDOW + 1), nth(WINDOW + 1)),
                (nth(WINDOW + 3), nth(WINDOW + 3)),
                (nth(WINDOW + 5), nth(WINDOW + 5)),
            ]
        );

        // A better price than anything in the window spills its worst levels, which land
        // in front of everything already deep, in their own order.
        side.update(level(0.5), price(0.5));
        assert_eq!(deep_levels(&side)[..BAND], levels(TARGET + 1, WINDOW));
        assert_eq!(deep_levels(&side).len(), 3 + BAND);

        // An update to a deep level is an assignment in place, not a second entry.
        let deep = nth(WINDOW + 3);
        side.update(level(deep), price(2300.0));
        assert_eq!(deep_size(&side, level(deep)), Some(price(2300.0)));
        assert_eq!(deep_levels(&side).len(), 3 + BAND);

        // And a removal from the middle closes the gap without disturbing the order.
        assert_eq!(side.remove(level(deep)), Some(UpdateResult::deep()));
        assert_eq!(
            deep_levels(&side)[BAND..],
            [
                (nth(WINDOW + 1), nth(WINDOW + 1)),
                (nth(WINDOW + 5), nth(WINDOW + 5)),
            ]
        );
    }

    #[test]
    fn deep_levels_are_promoted_best_first() {
        let mut side = side_with_deep(2 * BAND);
        drain_to_target(&mut side);

        // The refill took the deep tier's front, in order, and appended it to the window's
        // own worst end - so the window is still sorted across the seam.
        assert_eq!(
            window_levels(&side),
            levels(SLACK + 2, WINDOW + TARGET - PUBLISHED_DEPTH + 1)
        );
    }

    #[test]
    fn remove_from_empty_side_returns_none() {
        let mut side = AskSide::new();
        assert_eq!(side.remove(level(1.0)), None);
    }

    #[test]
    fn remove_missing_price_within_first_levels_range_returns_none() {
        let mut side = full_ask_side();
        assert_eq!(side.remove(level(5.5)), None);
        assert_eq!(side.first_levels.len(), WINDOW);
    }

    #[test]
    fn remove_reports_the_position_the_level_had() {
        let mut side = full_ask_side();
        assert_eq!(side.remove(level(5.0)), Some(UpdateResult::shallow(4)));
        assert_eq!(side.first_levels.len(), WINDOW - 1);

        // Everything worse than it shifted up one, so the same price is a position lower.
        assert_eq!(side.remove(level(6.0)), Some(UpdateResult::shallow(4)));
    }

    #[test]
    fn remove_within_the_hysteresis_band_leaves_the_deep_tier_alone() {
        let mut side = side_with_deep(1);
        assert_eq!(side.remove(level(1.0)), Some(UpdateResult::shallow(0)));
        assert_eq!(side.first_levels.len(), WINDOW - 1);
        assert_eq!(
            side.deap_levels.len(),
            1,
            "no refill this far from the threshold"
        );
    }

    #[test]
    fn remove_deep_book_entry_is_deep() {
        let mut side = side_with_deep(1);
        assert_eq!(
            side.remove(level(nth(WINDOW + 1))),
            Some(UpdateResult::deep())
        );
        assert!(side.deap_levels.is_empty());
    }

    #[test]
    fn remove_missing_price_within_deep_book_range_returns_none() {
        let mut side = side_with_deep(1);

        // Worse than everything, including the deep tier's only entry, but never inserted.
        assert_eq!(side.remove(level(nth(WINDOW + 2))), None);
        assert_eq!(side.deap_levels.len(), 1);
        assert!(deep_size(&side, level(nth(WINDOW + 1))).is_some());
    }

    #[test]
    fn merging_takes_the_shallower_of_two_results() {
        let shallow = UpdateResult::shallow(3);
        let shallower = UpdateResult::shallow(1);
        let deep = UpdateResult::deep();

        assert_eq!(shallow.merge(shallower), shallower);
        assert_eq!(shallower.merge(shallow), shallower);

        // `None` is deeper than any index, which is the opposite of how `Option` orders it.
        assert_eq!(shallow.merge(deep), shallow);
        assert_eq!(deep.merge(shallow), shallow);
        assert_eq!(deep.merge(deep), deep);
        assert_eq!(shallowest(deep.merge(shallow)), Some(3));
    }

    fn shallowest(result: UpdateResult) -> Option<u8> {
        result.shallowest()
    }
}

