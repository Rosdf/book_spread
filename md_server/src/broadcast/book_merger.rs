//! One book out of several venues' books.
//!
//! A [`SmallBook`] carries no identity - it is the top of one connector's book and nothing
//! else - so merging is where a level first learns which venue quoted it. That is the whole
//! reason [`MergedLevel`] exists beside `core_lib`'s [`BookLevel`]: the wire format stamps a
//! venue on every level, and once two venues' books are interleaved the levels on one side no
//! longer share one.
//!
//! Only one venue is served per book today - the registry resolves a catalogue instrument's
//! first pair and a broadcaster owns one reader - so [`MergedBook::refill`] is called with a
//! single book and takes its fast path. The k-way merge below is what the second pair will
//! need, and is written and tested now so that stage is additive.

use core_lib::Venue;
use core_lib::positive_f64::PositiveF64;
use std::cmp::Ordering;

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

/// One side of a merged book, bounded by what a single venue's book can hold.
///
/// The merged book is no deeper than one venue's: a client asked for the top of the book, and
/// interleaving venues makes those ten levels better, not more numerous.
type MergedSide = heapless::Vec<MergedLevel, { SmallBook::LEVELS }>;

/// The head of each venue's remaining levels on one side, in the order the caller gave them.
type Heads<'a> = heapless::Vec<(Venue, &'a [BookLevel]), { Venue::COUNT }>;

/// Both merged sides, reused across publishes.
///
/// Owned by the broadcaster and refilled in place rather than returned by value: a side is
/// ten levels of 24 bytes, and the point of the encoder's buffer pool is that a book costs no
/// allocation at all.
#[derive(Debug, Default)]
pub(super) struct MergedBook {
    asks: MergedSide,
    bids: MergedSide,
}

impl MergedBook {
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Best first, as [`SmallBook`] hands them over.
    pub(super) fn asks(&self) -> &[MergedLevel] {
        &self.asks
    }

    pub(super) fn bids(&self) -> &[MergedLevel] {
        &self.bids
    }

    /// Overwrites both sides with the best levels across `books`, best first.
    ///
    /// Each input side must already be best first, which is what a [`SmallBook`] is:
    /// `IncrementalBook` keeps its shallow levels sorted in side order - ascending for asks,
    /// descending for bids - and `SmallBook::refill` copies the top of that out. This walks
    /// those runs rather than sorting, so a merged side costs one comparison per venue per
    /// level.
    ///
    /// Two things fall out of the walk and are relied on above:
    ///
    /// * A venue with no levels contributes none. An empty book is a connector's resync
    ///   signal, so a resyncing venue simply drops out of the merge while the others keep
    ///   quoting, and both merged sides come out empty only when every venue's book is empty -
    ///   which is what keeps `SmallBook::is_empty`'s meaning intact on the wire.
    /// * Two venues quoting the same price stay two levels, each tagged with its own venue,
    ///   the earlier entry in `books` first. Both count against the depth.
    ///
    /// More than [`SmallBook::LEVELS`] levels available across the venues means the best ten
    /// overall, not ten per venue.
    pub(super) fn refill(&mut self, books: &[(Venue, &SmallBook)]) {
        self.asks.clear();
        self.bids.clear();

        // The only case reachable today, and worth its own path: with nothing to interleave
        // there is nothing to compare, so this is a straight copy of at most ten levels.
        if let [(venue, book)] = books {
            copy_side(&mut self.asks, *venue, book.asks());
            copy_side(&mut self.bids, *venue, book.bids());
            return;
        }

        merge_side(&mut self.asks, books, SmallBook::asks, Ordering::Less);
        merge_side(&mut self.bids, books, SmallBook::bids, Ordering::Greater);
    }
}

/// The single-venue path: `levels` is already the answer, minus the venue tag.
fn copy_side(out: &mut MergedSide, venue: Venue, levels: &[BookLevel]) {
    out.extend(
        levels
            .iter()
            .take(out.capacity())
            .map(|level| MergedLevel::new(level.price(), level.size(), venue)),
    );
}

/// Walks every venue's run of `side` at once, taking the best head each time round.
///
/// `better` is the [`Ordering`] a head's price must have against the best so far to displace
/// it: `Less` for asks, `Greater` for bids. It is a strict comparison, which is what makes a
/// tie go to the earlier venue in `books`.
fn merge_side(
    out: &mut MergedSide,
    books: &[(Venue, &SmallBook)],
    side: fn(&SmallBook) -> &[BookLevel],
    better: Ordering,
) {
    let mut heads = Heads::new();
    for (venue, book) in books {
        // A catalogue instrument names at most one pair per venue, so `books` is never longer
        // than the venue table; a caller that broke that would silently lose a venue here,
        // which is worth an assertion rather than a truncation.
        heads
            .push((*venue, side(book)))
            .expect("a book carries at most one pair per venue");
    }

    while !out.is_full() {
        let Some(best) = best_head(&heads, better) else {
            break;
        };

        let (venue, levels) = &mut heads[best];
        let level = levels[0];
        *levels = &levels[1..];
        out.push(MergedLevel::new(level.price(), level.size(), *venue))
            .map_err(|_| ())
            .expect("the loop guard checked there is room for one more");
    }
}

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

#[cfg(test)]
mod test {
    use super::{MergedBook, MergedLevel};
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
    fn seen(levels: &[MergedLevel]) -> Vec<(f64, f64, Venue)> {
        levels
            .iter()
            .map(|level| (level.price().get(), level.size().get(), level.venue()))
            .collect()
    }

    #[test]
    fn one_venue_passes_through_best_first() {
        let only = small(&[(100.5, 1.25), (101.0, 2.0)], &[(99.5, 2.0), (99.0, 4.0)]);
        let mut merged = MergedBook::new();

        merged.refill(&[(SECOND, &only)]);

        assert_eq!(
            seen(merged.asks()),
            vec![(100.5, 1.25, SECOND), (101.0, 2.0, SECOND)],
            "asks stay ascending and carry the venue they were merged under"
        );
        assert_eq!(
            seen(merged.bids()),
            vec![(99.5, 2.0, SECOND), (99.0, 4.0, SECOND)],
            "bids stay descending"
        );
    }

    #[test]
    fn two_venues_interleave_by_price() {
        let left = small(&[(100.0, 1.0), (102.0, 1.0)], &[(99.0, 1.0), (97.0, 1.0)]);
        let right = small(&[(101.0, 2.0), (103.0, 2.0)], &[(98.0, 2.0), (96.0, 2.0)]);
        let mut merged = MergedBook::new();

        merged.refill(&[(FIRST, &left), (SECOND, &right)]);

        assert_eq!(
            seen(merged.asks()),
            vec![
                (100.0, 1.0, FIRST),
                (101.0, 2.0, SECOND),
                (102.0, 1.0, FIRST),
                (103.0, 2.0, SECOND),
            ],
            "the merged asks ascend across both venues"
        );
        assert_eq!(
            seen(merged.bids()),
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
        let mut merged = MergedBook::new();

        merged.refill(&[(FIRST, &left), (SECOND, &right)]);

        assert_eq!(
            seen(merged.asks()),
            vec![(100.0, 1.0, FIRST), (100.0, 2.0, SECOND)],
            "a tie goes to the earlier venue, so the bytes are the same on every run"
        );
        assert_eq!(
            seen(merged.bids()),
            vec![(99.0, 1.0, FIRST), (99.0, 2.0, SECOND)],
        );
    }

    /// The resync signal is per venue: one connector rebuilding its book must not blank the
    /// merged book the others are still quoting.
    #[test]
    fn a_resyncing_venue_contributes_nothing() {
        let quoting = small(&[(100.0, 1.0)], &[(99.0, 1.0)]);
        let mut merged = MergedBook::new();

        merged.refill(&[(FIRST, &empty()), (SECOND, &quoting)]);

        assert_eq!(seen(merged.asks()), vec![(100.0, 1.0, SECOND)]);
        assert_eq!(seen(merged.bids()), vec![(99.0, 1.0, SECOND)]);
    }

    #[test]
    fn every_venue_empty_leaves_both_sides_empty() {
        let mut merged = MergedBook::new();

        merged.refill(&[(FIRST, &empty()), (SECOND, &empty())]);

        assert!(
            merged.asks().is_empty() && merged.bids().is_empty(),
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
        let mut merged = MergedBook::new();

        merged.refill(&[(FIRST, &left), (SECOND, &right)]);

        assert_eq!(
            merged.asks().len(),
            SmallBook::LEVELS,
            "twenty levels in, ten out - the best ten overall, not ten per venue"
        );
        assert_eq!(
            seen(merged.asks()).first().copied(),
            Some((100.0, 1.0, FIRST)),
            "and they are the best ten"
        );
        assert_eq!(
            seen(merged.asks()).last().copied(),
            Some((109.0, 1.0, SECOND)),
        );
    }

    /// `refill` is called on the same buffer for every book a broadcaster publishes, so a
    /// shallower book must not leave the previous one's tail behind.
    #[test]
    fn refilling_fully_overwrites_the_previous_book() {
        let deep = small(&[(100.0, 1.0), (101.0, 1.0), (102.0, 1.0)], &[(99.0, 1.0)]);
        let shallow = small(&[(200.0, 5.0)], &[]);
        let mut merged = MergedBook::new();

        merged.refill(&[(FIRST, &deep)]);
        merged.refill(&[(SECOND, &shallow)]);

        assert_eq!(seen(merged.asks()), vec![(200.0, 5.0, SECOND)]);
        assert!(
            merged.bids().is_empty(),
            "the previous book's bids must not survive a refill"
        );
    }
}
