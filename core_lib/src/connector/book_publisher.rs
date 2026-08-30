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

    /// Takes this reader apart into the slot buffer and the update notification.
    ///
    /// For a consumer that follows several books at once and so cannot simply await
    /// [`Self::wait_update`]: once the publisher is gone that call resolves to `None`
    /// *immediately, and on every call thereafter* - the departure is a flag in the shared
    /// state, not a one-shot - so a `select!` that keeps a branch on a finished reader spins
    /// instead of parking. Dropping that reader's [`Waiter`] is how such a consumer stops
    /// polling it, and it releases this half's share of the notification state there and then.
    ///
    /// The [`Reader`] is unaffected and still hands back the last book published, which for a
    /// departed publisher is the empty one [`BookPublisher::drop`] writes - so a finished
    /// stream reads as "no book" rather than as arbitrarily stale levels.
    #[must_use]
    pub fn split(self) -> (Reader<SmallBook>, Waiter) {
        (self.buffer, self.waiter)
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

#[cfg(all(test, not(loom)))]
mod test {
    use super::make_book_publisher_pair;
    use crate::incremental_book::IncrementalBook;
    use crate::positive_f64::PositiveF64;
    use std::future::Future as _;
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};

    fn book(price: f64, size: f64) -> IncrementalBook {
        let mut built = IncrementalBook::new();
        built.update_ask(
            PositiveF64::new(price).expect("test prices are positive"),
            PositiveF64::new(size).expect("test sizes are positive"),
        );
        built
    }

    /// Splitting changes nothing about what either half sees: the buffer still receives what
    /// the publisher writes, and the notification still reports it.
    #[test]
    fn both_halves_of_a_split_reader_still_follow_the_publisher() {
        let (mut publisher, reader) = make_book_publisher_pair();
        let (mut books, mut updates) = reader.split();

        publisher.publish(&book(100.5, 1.25));

        let mut cx = Context::from_waker(Waker::noop());
        assert_eq!(
            pin!(updates.wait()).poll(&mut cx),
            Poll::Ready(Some(())),
            "the notification half still hears the publish"
        );
        assert_eq!(
            books.get().asks()[0].price().get(),
            100.5,
            "and the buffer half still holds the book"
        );
    }

    /// The point of splitting: a consumer following several books drops the notification half
    /// of a stream that has ended, and goes on reading its last book - the empty one the
    /// publisher's own `Drop` writes.
    #[test]
    fn the_notification_half_can_be_dropped_without_disturbing_the_buffer() {
        let (mut publisher, reader) = make_book_publisher_pair();
        let (mut books, mut updates) = reader.split();

        publisher.publish(&book(100.5, 1.25));
        drop(publisher);

        let mut cx = Context::from_waker(Waker::noop());
        assert_eq!(
            pin!(updates.wait()).poll(&mut cx),
            Poll::Ready(Some(())),
            "the parting empty book is reported as an update"
        );
        assert_eq!(
            pin!(updates.wait()).poll(&mut cx),
            Poll::Ready(None),
            "and the departure itself follows it"
        );
        assert_eq!(
            pin!(updates.wait()).poll(&mut cx),
            Poll::Ready(None),
            "a departed publisher is reported on every call, not once - which is why a \
             consumer polling several readers has to stop polling this one"
        );

        drop(updates);
        assert!(
            books.get().is_empty(),
            "the buffer outlives the notification and still reads as the resync signal"
        );
    }
}
