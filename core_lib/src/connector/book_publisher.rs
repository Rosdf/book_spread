//! Single-producer / single-consumer book channel.
//!
//! [`BookPublisher`] writes books into a [`Publisher`] slot buffer and notifies
//! the reader; [`BookReader`] reads the newest book and can park until a newer
//! one arrives. Both are just the two halves of a [`shared_buffer`] pair paired
//! with the two halves of an [`atomic_waker`] pair - the buffering lives in one,
//! the parking and the shared lifetime in the other, and nothing else is added
//! here.
//!
//! [`shared_buffer`]: crate::shared_buffer
//! [`atomic_waker`]: crate::atomic_waker
//!
//! # Shutdown
//!
//! [`BookPublisher::drop`] publishes an empty [`SmallBook`], which is the
//! established in-band signal (see [`SmallBook::is_empty`]). The notifier's own
//! drop, which runs immediately afterwards, then reports the shutdown: the
//! reader is handed `Some(())` for the sentinel and `None` from the following
//! [`BookReader::wait_update`] onwards, so it never parks forever.

use crate::atomic_waker::{self, Notifier, Waiter};
use crate::incremental_book::IncrementalBook;
use crate::shared_buffer::{Publisher, Reader};
use crate::small_book::SmallBook;
use std::ops::Deref;

#[derive(Debug)]
pub struct BookPublisher {
    buffer: Publisher<SmallBook>,
    notifier: Notifier,
}

impl BookPublisher {
    /// Publishes the top of `book` and wakes the reader.
    ///
    /// Written straight into the claimed slot rather than built and moved in: see
    /// [`SmallBook::refill`], which is also what satisfies `update_with`'s overwrite
    /// contract.
    pub fn publish(&mut self, book: &IncrementalBook) {
        self.buffer.update_with(|slot| slot.refill(book));
        self.notifier.notify();
    }

    /// Publishes an empty book, so a reader observably sees "no book" while a symbol resyncs
    /// rather than continuing to act on arbitrarily stale levels.
    pub fn publish_empty(&mut self) {
        self.buffer.update_with(SmallBook::clear);
        self.notifier.notify();
    }
}

impl Drop for BookPublisher {
    fn drop(&mut self) {
        // The empty book is the in-band "no book" signal readers already
        // understand, and this also delivers the final notification. The
        // notifier's own `Drop`, which runs straight after this, releases the
        // shared state.
        self.publish_empty();
    }
}

#[derive(Debug)]
pub struct BookReader {
    buffer: Reader<SmallBook>,
    waiter: Waiter,
}

impl BookReader {
    pub fn get_last(&mut self) -> impl Deref<Target = SmallBook> {
        self.buffer.get()
    }

    /// Waits until a book is published.
    ///
    /// Resolves to `Some(())` on an update - immediately, if one arrived since
    /// the last call - and to `None` once the publisher is gone and no update is
    /// left to report. It never parks forever.
    ///
    /// Cancel-safe; see [`Waiter::wait`].
    pub async fn wait_update(&mut self) -> Option<()> {
        self.waiter.wait().await
    }
}

/// The two halves of one symbol's book channel: the publisher a connector writes through,
/// and the single reader its broadcaster owns.
///
/// # Panics
///
/// Never in practice. `Publisher::reader` is fallible because a buffer can run out of slots
/// to hand readers, and this one is built with a slot for exactly the one reader taken here.
#[must_use]
pub fn make_book_publisher_pair() -> (BookPublisher, BookReader) {
    let mut publisher = Publisher::new(1);
    let reader = publisher
        .reader()
        .expect("a buffer built for one reader always yields that reader");

    let (notifier, waiter) = atomic_waker::pair();

    (
        BookPublisher {
            buffer: publisher,
            notifier,
        },
        BookReader {
            buffer: reader,
            waiter,
        },
    )
}
