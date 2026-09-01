//! One book out of several venues' books.
//!
//! A [`SmallBook`] carries no identity - it is the top of one connector's book and nothing
//! else - so merging is where a level first learns which venue quoted it. That is the whole
//! reason [`MergedLevel`] exists beside `core_lib`'s [`BookLevel`]: the wire format stamps a
//! venue on every level, and once two venues' books are interleaved the levels on one side no
//! longer share one.
//!
//! Both the one-venue and the many-venue paths are live: a catalogue instrument names one pair
//! per venue, and the registry resolves every one of them. The one-venue case ([`tagged`]) is a
//! specialisation of the k-way merge below, not a separate story - with nothing to interleave
//! there is nothing to compare, so it is a straight walk instead of a merge.

use core_lib::Venue;
use core_lib::positive_f64::PositiveF64;
use std::cmp::Ordering;
use std::ops::Deref;
use core_lib::incremental_book::Level as BookLevel;
use core_lib::small_book::SmallBook;

/// A level with the venue that quoted it attached.
#[derive(Debug, Clone, Copy)]
pub(super) struct MergedLevel {
    size: PositiveF64,
    price: PositiveF64,
    venue: Venue,
}

impl MergedLevel {
    pub(super) fn new(price: PositiveF64, size: PositiveF64, venue: Venue) -> Self {
        Self { size, price, venue }
    }

    pub(super) fn size(&self) -> PositiveF64 {
        self.size
    }

    pub(super) fn price(&self) -> PositiveF64 {
        self.price
    }

    pub(super) fn venue(&self) -> Venue {
        self.venue
    }
}

/// The head of each venue's remaining levels on one side, in the order the caller gave them.
type Heads<'a> = heapless::Vec<(Venue, &'a [BookLevel]), { Venue::COUNT }>;

/// The books to merge, and the source of one iterator per side.
///
/// Holds nothing but the borrow: a side is produced level by level as the encoder asks for it,
/// so no merged side ever exists as a value. There is nothing to allocate or reuse across
/// publishes - a level goes straight from the reader's slot to the frame.
pub(super) struct BookMerger<'a, T> {
    books: &'a [(Venue, T)],
}

impl<'a, T: Deref<Target=SmallBook>> BookMerger<'a, T> {
    pub(super) fn new(books: &'a [(Venue, T)]) -> Self {
        Self { books }
    }

    /// Best first: the ascending merge across every venue's asks.
    pub(super) fn asks(&self) -> MergedSide<'a> {
        MergedSide::new(self.books, SmallBook::asks, Ordering::Less)
    }

    /// Best first: the descending merge across every venue's bids.
    pub(super) fn bids(&self) -> MergedSide<'a> {
        MergedSide::new(self.books, SmallBook::bids, Ordering::Greater)
    }
}

/// One side's k-way merge, best level first.
///
/// Bounded by what a single venue's book can hold: the merged book is no deeper than one
/// venue's, because a client asked for the top of the book and interleaving venues makes those
/// ten levels better, not more numerous.
///
/// Two things fall out of the walk and are relied on elsewhere:
///
/// * A venue with no levels contributes none. An empty book is a connector's resync signal, so
///   a resyncing venue simply drops out of the merge while the others keep quoting, and both
///   merged sides come out empty only when every venue's book is empty - which is what keeps
///   `SmallBook::is_empty`'s meaning intact on the wire.
/// * Two venues quoting the same price stay two levels, each tagged with its own venue, the
///   earlier entry in `books` first. Both count against the depth.
///
/// More than [`SmallBook::LEVELS`] levels available across the venues means the best ten
/// overall, not ten per venue.
pub(super) struct MergedSide<'a> {
    heads: Heads<'a>,
    better: Ordering,
    /// Exactly what `next` will still yield.
    remaining: usize,
}

impl<'a> MergedSide<'a> {
    fn new<T: Deref<Target=SmallBook>>(
        books: &'a [(Venue, T)],
        side: fn(&SmallBook) -> &[BookLevel],
        better: Ordering,
    ) -> Self {
        let mut heads = Heads::new();
        for (venue, book) in books {
            // A catalogue instrument names at most one pair per venue, so `books` is never
            // longer than the venue table; a caller that broke that would silently lose a
            // venue here, which is worth an assertion rather than a truncation.
            heads
                .push((*venue, side(book)))
                .expect("a book carries at most one pair per venue");
        }

        let remaining = usize::min(
            SmallBook::LEVELS,
            heads.iter().map(|(_, levels)| levels.len()).sum(),
        );

        Self {
            heads,
            better,
            remaining,
        }
    }
}

impl Iterator for MergedSide<'_> {
    type Item = MergedLevel;

    fn next(&mut self) -> Option<MergedLevel> {
        if self.remaining == 0 {
            return None;
        }

        let best = best_head(&self.heads, self.better)
            .expect("`remaining` counts levels the heads still hold, so one of them has a head");
        let (venue, levels) = &mut self.heads[best];
        let level = levels[0];
        *levels = &levels[1..];
        self.remaining -= 1;
        Some(MergedLevel::new(level.price(), level.size(), *venue))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for MergedSide<'_> {}

/// The index of the run whose head is best, or `None` once every run is spent.
fn best_head(heads: &Heads<'_>, better: Ordering) -> Option<usize> {
    let mut best: Option<(usize, PositiveF64)> = None;

    for (idx, (_, levels)) in heads.iter().enumerate() {
        let Some(head) = levels.first() else {
            continue;
        };
        if best.is_none_or(|(_, price)| head.price().cmp(&price) == better) {
            best = Some((idx, head.price()));
        }
    }

    best.map(|(idx, _)| idx)
}

/// The single-venue path: with nothing to interleave there is nothing to compare, so this is a
/// straight walk of levels the `SmallBook` already ordered, tagged with the one venue that
/// quoted them.
///
/// No cap is needed here: `SmallBook::refill` already bounds each side at `SmallBook::LEVELS`,
/// so `levels` is never longer than that to begin with.
pub(super) fn tagged(
    venue: Venue,
    levels: &[BookLevel],
) -> impl ExactSizeIterator<Item = MergedLevel> + '_ {
    levels
        .iter()
        .map(move |level| MergedLevel::new(level.price(), level.size(), venue))
}

#[cfg(test)]
mod test {
    use super::{BookMerger, MergedLevel};
    use crate::test_util::book;
    use crate::venue::Venue;
    use core_lib::small_book::SmallBook;

    const FIRST: Venue = Venue::BinanceSpot;
    const SECOND: Venue = Venue::Bitstamp;

    /// A `SmallBook` the only way there is to build one: refilled from an `IncrementalBook`.
    fn small(asks: &[(f64, f64)], bids: &[(f64, f64)]) -> SmallBook {
        let mut built = SmallBook::default();
        built.refill(&book(asks, bids));
        built
    }

    fn empty() -> SmallBook {
        SmallBook::default()
    }

    /// A full venue's worth of asks, two apart, starting at `first`.
    fn ladder(first: f64) -> Vec<(f64, f64)> {
        let mut levels = Vec::with_capacity(SmallBook::LEVELS);
        let mut price = first;
        for _ in 0..SmallBook::LEVELS {
            levels.push((price, 1.0));
            price += 2.0;
        }
        levels
    }

    /// `(price, size, venue)` for each level, which is everything a level carries.
    fn seen(levels: impl Iterator<Item = MergedLevel>) -> Vec<(f64, f64, Venue)> {
        levels
            .map(|level| (level.price().get(), level.size().get(), level.venue()))
            .collect()
    }

    #[test]
    fn one_venue_passes_through_best_first() {
        let only = small(&[(100.5, 1.25), (101.0, 2.0)], &[(99.5, 2.0), (99.0, 4.0)]);
        let books = [(SECOND, &only)];
        let merger = BookMerger::new(&books);

        assert_eq!(
            seen(merger.asks()),
            vec![(100.5, 1.25, SECOND), (101.0, 2.0, SECOND)],
            "asks stay ascending and carry the venue they were merged under"
        );
        assert_eq!(
            seen(merger.bids()),
            vec![(99.5, 2.0, SECOND), (99.0, 4.0, SECOND)],
            "bids stay descending"
        );
    }

    #[test]
    fn two_venues_interleave_by_price() {
        let left = small(&[(100.0, 1.0), (102.0, 1.0)], &[(99.0, 1.0), (97.0, 1.0)]);
        let right = small(&[(101.0, 2.0), (103.0, 2.0)], &[(98.0, 2.0), (96.0, 2.0)]);
        let books = [(FIRST, &left), (SECOND, &right)];
        let merger = BookMerger::new(&books);

        assert_eq!(
            seen(merger.asks()),
            vec![
                (100.0, 1.0, FIRST),
                (101.0, 2.0, SECOND),
                (102.0, 1.0, FIRST),
                (103.0, 2.0, SECOND),
            ],
            "the merged asks ascend across both venues"
        );
        assert_eq!(
            seen(merger.bids()),
            vec![
                (99.0, 1.0, FIRST),
                (98.0, 2.0, SECOND),
                (97.0, 1.0, FIRST),
                (96.0, 2.0, SECOND),
            ],
            "the merged bids descend across both venues"
        );
    }

    /// Aggregating them into one level would lose the venue, which is the one thing a level
    /// carries beyond its price and size.
    #[test]
    fn an_equal_price_from_two_venues_stays_two_levels() {
        let left = small(&[(100.0, 1.0)], &[(99.0, 1.0)]);
        let right = small(&[(100.0, 2.0)], &[(99.0, 2.0)]);
        let books = [(FIRST, &left), (SECOND, &right)];
        let merger = BookMerger::new(&books);

        assert_eq!(
            seen(merger.asks()),
            vec![(100.0, 1.0, FIRST), (100.0, 2.0, SECOND)],
            "a tie goes to the earlier venue, so the bytes are the same on every run"
        );
        assert_eq!(
            seen(merger.bids()),
            vec![(99.0, 1.0, FIRST), (99.0, 2.0, SECOND)],
        );
    }

    /// The resync signal is per venue: one connector rebuilding its book must not blank the
    /// merged book the others are still quoting.
    #[test]
    fn a_resyncing_venue_contributes_nothing() {
        let quoting = small(&[(100.0, 1.0)], &[(99.0, 1.0)]);
        let empty_book = empty();
        let books = [(FIRST, &empty_book), (SECOND, &quoting)];
        let merger = BookMerger::new(&books);

        assert_eq!(seen(merger.asks()), vec![(100.0, 1.0, SECOND)]);
        assert_eq!(seen(merger.bids()), vec![(99.0, 1.0, SECOND)]);
    }

    #[test]
    fn every_venue_empty_leaves_both_sides_empty() {
        let left = empty();
        let right = empty();
        let books = [(FIRST, &left), (SECOND, &right)];
        let merger = BookMerger::new(&books);

        assert!(
            seen(merger.asks()).is_empty() && seen(merger.bids()).is_empty(),
            "no venue has a book, so the merged book is the resync signal too"
        );
    }

    #[test]
    fn the_merged_book_is_no_deeper_than_one_venue() {
        // Two runs of ten that interleave exactly: evens on one venue, odds on the other, so
        // the best ten span both and the tenth is the second venue's.
        let evens: Vec<(f64, f64)> = ladder(100.0);
        let odds: Vec<(f64, f64)> = ladder(101.0);
        let left = small(&evens, &[]);
        let right = small(&odds, &[]);
        let books = [(FIRST, &left), (SECOND, &right)];
        let merger = BookMerger::new(&books);

        let asks = seen(merger.asks());
        assert_eq!(
            asks.len(),
            SmallBook::LEVELS,
            "twenty levels in, ten out - the best ten overall, not ten per venue"
        );
        assert_eq!(
            asks.first().copied(),
            Some((100.0, 1.0, FIRST)),
            "and they are the best ten"
        );
        assert_eq!(asks.last().copied(), Some((109.0, 1.0, SECOND)));
    }

    /// `MergedSide::len` is what `BookEncoder` sizes its buffer from, so it must equal exactly
    /// what `next` goes on to yield - not an estimate.
    #[test]
    fn len_matches_what_the_iterator_actually_yields() {
        let shallow = small(&[(100.0, 1.0)], &[]);
        let deep = small(&ladder(200.0), &[]);
        let other_deep = small(&ladder(201.0), &[]);

        let shallow_books = [(FIRST, &shallow)];
        let shallow_merger = BookMerger::new(&shallow_books);
        let mut asks = shallow_merger.asks();
        assert_eq!(asks.len(), 1);
        let yielded = asks.by_ref().count();
        assert_eq!(yielded, 1);

        let deep_books = [(FIRST, &deep)];
        let deep_merger = BookMerger::new(&deep_books);
        let mut asks = deep_merger.asks();
        assert_eq!(asks.len(), SmallBook::LEVELS);
        let yielded = asks.by_ref().count();
        assert_eq!(yielded, SmallBook::LEVELS);

        // Capped: twenty levels available across the two venues, but never more than
        // `SmallBook::LEVELS` reported or yielded.
        let capped_books = [(FIRST, &deep), (SECOND, &other_deep)];
        let capped_merger = BookMerger::new(&capped_books);
        let mut asks = capped_merger.asks();
        assert_eq!(asks.len(), SmallBook::LEVELS);
        let yielded = asks.by_ref().count();
        assert_eq!(yielded, SmallBook::LEVELS);
    }
}
