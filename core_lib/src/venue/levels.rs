//! Price levels, as every venue this connector talks to spells them on the wire.
//!
//! `binance_spot` and `bitstamp` arrived at byte-identical code for this: a decimal string
//! decoded straight to `f64`, a `[["price","qty"], ...]` array visited pair by pair, and the
//! "quantity zero means delete" rule mapped onto [`IncrementalBook`], which has no such rule of
//! its own. None of that is venue-specific - it is what a JSON order-book feed looks like - so
//! it lives here once. In particular [`apply_level`]'s `PositiveF64` invariant is stated in one
//! place rather than argued for separately in each venue crate.
//!
//! What is *not* here is where the levels go. Binance knows the target book before it parses a
//! level and applies each one directly; Bitstamp names the channel only after the data and has
//! to stage them first. [`LevelSink`] is that seam: [`LevelsSeed`] does the parsing for both,
//! and each venue supplies the sink. [`BookSink`] covers the "straight into a known book" half,
//! which both venues use for their REST snapshot.

use crate::incremental_book::{IncrementalBook, PUBLISHED_DEPTH, UpdateResult};
use crate::positive_f64::PositiveF64;
use crate::small_book::SmallBook;
use serde::Deserialize;
use serde::de::{DeserializeSeed, Deserializer, SeqAccess, Visitor};
use std::fmt::{self, Formatter};

/// A price or quantity string that would not parse as a number.
///
/// Raised from inside a `serde` visitor, where the only channel back to the caller is
/// `serde::de::Error::custom`, which takes something `Display` - so this never propagates as
/// itself, it ends up as the message inside a `simd_json::Error`. It still gets its own type so
/// the condition is written once and tests can match on the wording.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("malformed decimal {0:?}")]
pub struct MalformedDecimal(Box<str>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Bid,
    Ask,
}

/// Applies one price level, mapping the venues' shared "quantity zero means delete" rule onto
/// [`IncrementalBook`].
///
/// `qty` is inspected *before* it is turned into a [`PositiveF64`], because on the delete path
/// there is no size to represent and constructing one would be both wasted work and a value the
/// book should never see.
///
/// Returns `None` when nothing changed - a delete for a level that was not present, which is
/// routine rather than an error.
#[inline]
pub fn apply_level(
    book: &mut IncrementalBook,
    side: Side,
    price: f64,
    qty: f64,
) -> Option<UpdateResult> {
    debug_assert!(
        PositiveF64::new(price).is_some(),
        "venue sent a non-positive or NaN price: {price}"
    );
    // SAFETY: order book prices on every venue here are positive finite decimal strings, so
    // parsing one yields a sign-positive non-NaN `f64` - exactly `PositiveF64`'s invariant.
    // Checked by the assertion above in debug builds.
    let checked_price = unsafe { PositiveF64::new_unchecked(price) };

    if qty == 0.0 {
        return match side {
            Side::Bid => book.remove_bid(checked_price),
            Side::Ask => book.remove_ask(checked_price),
        };
    }

    debug_assert!(
        PositiveF64::new(qty).is_some(),
        "venue sent a non-positive or NaN quantity: {qty}"
    );
    // SAFETY: as above for the price; the zero case returned already, so `qty` is strictly
    // positive here.
    let checked_qty = unsafe { PositiveF64::new_unchecked(qty) };

    Some(match side {
        Side::Bid => book.update_bid(checked_price, checked_qty),
        Side::Ask => book.update_ask(checked_price, checked_qty),
    })
}

/// Merges `next` into `acc` using [`UpdateResult::merge`].
#[inline]
pub fn merge(acc: &mut Option<UpdateResult>, next: Option<UpdateResult>) {
    if let Some(result) = next {
        *acc = Some(acc.map_or(result, |a| a.merge(result)));
    }
}

const _: () = assert!(
    SmallBook::LEVELS == PUBLISHED_DEPTH,
    "the book tunes its window around the depth published here; they have to agree"
);

/// Whether a merged result touched anything a [`SmallBook`] can show.
///
/// The book knows how deep its window is; how much of that window anybody looks at is this
/// module's business, and a change deeper than [`SmallBook::LEVELS`] is a change nothing
/// downstream would be able to see.
#[inline]
pub fn worth_publishing(merged: Option<UpdateResult>) -> bool {
    merged
        .and_then(UpdateResult::shallowest)
        .is_some_and(|idx| usize::from(idx) < SmallBook::LEVELS)
}

/// A decimal string such as `"0.01000000"`, decoded straight to `f64`.
///
/// The visitor parses the number out of a transient `&str`, which deliberately avoids depending
/// on the deserializer's ability to hand back *borrowed* strings - not guaranteed for escaped
/// input.
#[derive(Debug, Clone, Copy)]
pub struct Decimal(f64);

impl Decimal {
    pub const fn get(self) -> f64 {
        self.0
    }
}

impl<'de> Deserialize<'de> for Decimal {
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        struct DecimalVisitor;

        impl Visitor<'_> for DecimalVisitor {
            type Value = Decimal;

            fn expecting(&self, f: &mut Formatter<'_>) -> fmt::Result {
                f.write_str("a decimal number encoded as a JSON string")
            }

            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Decimal, E> {
                v.parse()
                    .map(Decimal)
                    .map_err(|_| E::custom(MalformedDecimal(v.into())))
            }
        }

        de.deserialize_str(DecimalVisitor)
    }
}

/// Where one array's price levels go as they are parsed.
///
/// The one thing that genuinely differs between the venues, and between a live diff and a
/// buffered one: a known book, or a venue's staging arena.
pub trait LevelSink {
    fn push_level(&mut self, price: f64, qty: f64);
}

/// Decodes `[["price","qty"], ...]`, handing each pair to `sink` at the moment it is parsed.
///
/// Nothing is collected: there is no `Vec` of levels here, no borrowed slice held across a
/// call, and no allocation per array - whatever the sink does with a pair is all that happens.
#[derive(Debug)]
pub struct LevelsSeed<'s, S> {
    sink: &'s mut S,
}

impl<'s, S: LevelSink> LevelsSeed<'s, S> {
    pub const fn new(sink: &'s mut S) -> Self {
        Self { sink }
    }
}

impl<'de, S: LevelSink> DeserializeSeed<'de> for LevelsSeed<'_, S> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, de: D) -> Result<(), D::Error> {
        de.deserialize_seq(self)
    }
}

impl<'de, S: LevelSink> Visitor<'de> for LevelsSeed<'_, S> {
    type Value = ();

    fn expecting(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("an array of [price, quantity] pairs")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        while let Some([price, qty]) = seq.next_element::<[Decimal; 2]>()? {
            self.sink.push_level(price.get(), qty.get());
        }
        Ok(())
    }
}

/// A [`LevelSink`] that applies each level straight into a book, accumulating what the whole
/// array did to the top of it.
///
/// For every path where the target book is known before the levels are parsed: both venues'
/// REST snapshots, and Binance's live diffs - whose envelope names the stream first.
#[derive(Debug)]
pub struct BookSink<'b, 'm> {
    book: &'b mut IncrementalBook,
    side: Side,
    merged: &'m mut Option<UpdateResult>,
}

impl<'b, 'm> BookSink<'b, 'm> {
    pub const fn new(
        book: &'b mut IncrementalBook,
        side: Side,
        merged: &'m mut Option<UpdateResult>,
    ) -> Self {
        Self { book, side, merged }
    }
}

impl LevelSink for BookSink<'_, '_> {
    fn push_level(&mut self, price: f64, qty: f64) {
        let result = apply_level(self.book, self.side, price, qty);
        merge(self.merged, result);
    }
}

#[cfg(test)]
mod test {
    use super::{Side, apply_level};
    use crate::incremental_book::{IncrementalBook, UpdateResult};

    #[test]
    fn zero_quantity_deletes_instead_of_inserting_a_zero_size_level() {
        let mut book = IncrementalBook::new();

        assert_eq!(apply_level(&mut book, Side::Bid, 100.0, 0.0), None);
        assert_eq!(book.first_bids().len(), 0);

        apply_level(&mut book, Side::Bid, 100.0, 5.0);
        assert_eq!(book.first_bids().len(), 1);

        assert_eq!(
            apply_level(&mut book, Side::Bid, 100.0, 0.0),
            Some(UpdateResult::shallow(0))
        );
        assert_eq!(book.first_bids().len(), 0);
    }
}
