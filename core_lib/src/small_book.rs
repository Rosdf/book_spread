use crate::incremental_book::{IncrementalBook, Level};

const DEPTH: usize = 10;

#[derive(Debug, Clone, Default)]
pub struct SmallBook {
    asks: heapless::Vec<Level, DEPTH>,
    bids: heapless::Vec<Level, DEPTH>,
}

impl SmallBook {
    pub fn asks(&self) -> &[Level] {
        &self.asks
    }

    pub fn bids(&self) -> &[Level] {
        &self.bids
    }

    /// True when neither side holds a level, which is what a connector publishes while it
    /// is resyncing so readers can tell "no book" from a stale one.
    pub fn is_empty(&self) -> bool {
        self.asks.is_empty() && self.bids.is_empty()
    }

    /// Overwrites both sides with the top of `book`, best level first.
    ///
    /// In place rather than by value, and there is no `From<&IncrementalBook>` beside it, so
    /// that there is only one way to fill a book: a `SmallBook` is around 336 bytes, and
    /// building one on the stack to hand to
    /// [`Publisher::update`](crate::shared_buffer::Publisher::update) copies all of it twice -
    /// once into the temporary and once into the claimed slot. Written through
    /// [`Publisher::update_with`](crate::shared_buffer::Publisher::update_with) instead, the
    /// levels go straight from `book` into the slot.
    ///
    /// Both sides are cleared first, which is what satisfies `update_with`'s "fully
    /// overwrite" contract: the slot handed to it holds a book from several publishes ago,
    /// not the previous one.
    pub fn refill(&mut self, book: &IncrementalBook) {
        self.asks.clear();
        self.bids.clear();
        self.asks.extend(book.first_asks().take(DEPTH));
        self.bids.extend(book.first_bids().take(DEPTH));
    }

    /// Empties both sides in place: the resync signal, written straight into a slot.
    pub fn clear(&mut self) {
        self.asks.clear();
        self.bids.clear();
    }
}
