//! The symbols carried by one connection.
//!
//! Two type parameters, both a venue's own: the "ready" half of the slot state machine
//! ([`VenueSpec::Ready`] - Binance has `Seeded`/`Live`, Bitstamp only `Live`) and the arena its
//! bootstrapping half buffers parsed diffs into ([`VenueSpec::Pending`]). [`Bootstrap`] used to be
//! concrete because every venue buffered the same thing - raw bytes - which is no longer true;
//! see [`crate::venue::pending`] for why that changed.
//!
//! Generic over those two concrete types directly rather than over the venue itself - unlike
//! [`crate::venue::connection::Connection`]/[`crate::venue::connection::Handler`], nothing here
//! needs any *other* piece of a venue (its config, its pacer, ...) at the same time, so there is
//! no reason to name the whole `VenueSpec` trait. Every type below is consequently unbounded: no
//! `Ready: VenueSpec`, no `P: PendingDiffs`, since a bare type parameter used only as a plain field
//! never needs one. The single exception is [`Slot`]'s inherent impl, which needs
//! `P: PendingDiffs` because [`Slot::reset`] both empties an existing arena and builds a fresh
//! one for a slot that was not already bootstrapping - an ordinary "this function calls it"
//! bound, not a trait-shaped one.
//!
//! [`VenueSpec::Ready`]: crate::venue::spec::VenueSpec::Ready
//! [`VenueSpec::Pending`]: crate::venue::spec::VenueSpec::Pending

use crate::connector::book_publisher::BookPublisher;
use crate::incremental_book::IncrementalBook;
use crate::instrument::Instrument;
use crate::map::{InternalHashMap, new_internal_map};
use crate::shared_string::SharedString;
use crate::venue::pending::PendingDiffs;
use hashbrown::hash_map::Entry;
use std::time::Instant;

/// State held while a symbol waits for its REST snapshot.
///
/// Boxed inside [`SlotState::Bootstrapping`] because this is by far the largest variant and
/// the rarely-occupied one - a slot spends almost all its life `Ready` - so boxing keeps the
/// enum from paying this size on every slot.
#[derive(Debug, Default)]
pub struct Bootstrap<P> {
    /// The diffs seen so far, parsed on arrival into the venue's own arena.
    pub pending: P,
    /// The cursor (Binance's `U`, Bitstamp's `microtimestamp`) of the first buffered frame,
    /// which the snapshot must reach.
    pub first_cursor: Option<u64>,
    /// The in-flight fetch, if any. `Some` exactly while a request is outstanding, so it
    /// doubles as the guard that keeps a retry from racing a second fetch.
    pub abort: Option<tokio::task::AbortHandle>,
    /// How many *extra* snapshots this bootstrap attempt has asked for, after one came back
    /// that did not reach the buffered diffs. Bounded by the connection, so a venue whose
    /// snapshot never catches up gives up and resyncs rather than fetching forever.
    pub refetches: u32,
}

#[derive(Debug)]
pub enum SlotState<Ready, P> {
    Bootstrapping(Box<Bootstrap<P>>),
    Ready(Ready),
}

impl<Ready, P> SlotState<Ready, P> {
    pub fn bootstrapping(pending: P) -> Self {
        Self::Bootstrapping(Box::new(Bootstrap {
            pending,
            first_cursor: None,
            abort: None,
            refetches: 0,
        }))
    }
}

/// One instrument's book, publish sink, and bootstrap state, as carried on a connection.
///
/// `wire_name` is the name this venue's frames echo back (a Binance stream name, a Bitstamp
/// channel name); it is also [`SlotTable`]'s key - see that type's doc for why.
#[derive(Debug)]
pub struct Slot<Ready, P> {
    pub instrument: Instrument,
    pub wire_name: SharedString,
    pub book: IncrementalBook,
    pub publisher: BookPublisher,
    pub state: SlotState<Ready, P>,
    /// Names the current bootstrap attempt. Stamped onto every fetch spawned for this slot, so
    /// a result from a superseded attempt - one from before an unsubscribe/resubscribe or a
    /// reset raced it - can be told apart from the one currently in flight.
    pub generation: u64,
    /// When a frame last arrived for this symbol. Fed by the per-symbol idle watchdog: a
    /// symbol with no frame for too long is resynced, without touching the socket or its
    /// neighbours.
    pub last_frame: Instant,
}

impl<Ready, P: PendingDiffs> Slot<Ready, P> {
    /// Aborts this slot's outstanding fetch, if any. Best-effort by nature - see the
    /// generation stamp, which is what makes a result that outran the abort harmless.
    pub fn abort_fetch(&mut self) {
        if let SlotState::Bootstrapping(boot) = &mut self.state
            && let Some(handle) = boot.abort.take()
        {
            handle.abort();
        }
    }

    /// Throws the book away, tells readers it is gone, and returns to buffering under a fresh
    /// generation.
    ///
    /// Reuses the book and - when this slot was already bootstrapping - the pending arena it
    /// already owns, rather than building new ones: see `IncrementalBook::clear` and
    /// [`PendingDiffs::clear`], both of which drop the contents while keeping the allocation.
    /// The *contents* can never carry over, since `generation` changing means anything already
    /// buffered belongs to a superseded attempt.
    pub fn reset(&mut self, generation: u64) {
        self.abort_fetch();
        self.book.clear();
        self.publisher.publish_empty();
        match &mut self.state {
            SlotState::Bootstrapping(boot) => {
                boot.pending.clear();
                boot.first_cursor = None;
                boot.refetches = 0;
                // `abort` was already taken by `abort_fetch` above.
            }
            SlotState::Ready(_) => self.state = SlotState::bootstrapping(P::default()),
        }
        self.generation = generation;
        self.last_frame = Instant::now();
    }
}

/// The symbols carried by one connection, keyed by wire name rather than by [`Instrument`].
///
/// A wire frame names a *stream/channel* (e.g. `btcusdt@depth@100ms`), the same string this
/// connection subscribed with, so keying on it lets a decoder resolve a frame straight off the
/// bytes it already has - via [`std::borrow::Borrow<str>`] - with nothing built or looked up
/// twice.
///
/// Dedup is connector-wide, not per-socket: the supervisor's routing table keeps an instrument
/// from being routed to two connections in the first place, so this table's own check is a
/// defensive backstop rather than the primary guard.
///
/// Hashed with `FxHasher` rather than the standard `SipHash-1-3`: this lookup is on the
/// per-frame path - every decoded frame resolves its symbol through it - and the keys are
/// venue symbols the connector chose, never client input, so the denial-of-service
/// resistance the random seed buys is not doing any work here. The registry's and router's maps deliberately keep
/// the default hasher: they are per-subscription, not per-frame, and the registry's keys *do*
/// come from clients.
#[derive(Debug)]
pub struct SlotTable<Ready, P> {
    by_wire_name: InternalHashMap<SharedString, Slot<Ready, P>>,
}

impl<Ready, P> Default for SlotTable<Ready, P> {
    fn default() -> Self {
        Self {
            by_wire_name: new_internal_map(),
        }
    }
}

impl<Ready, P> SlotTable<Ready, P> {
    /// How many symbols this connection carries. Named rather than `len`, since there is no
    /// `is_empty` to pair it with: nothing asks that question, and the count is only ever a
    /// log field.
    pub fn symbol_count(&self) -> usize {
        self.by_wire_name.len()
    }

    /// Adds a symbol.
    ///
    /// On a duplicate, hands `slot` back (boxed - `Slot` is large enough that
    /// `Result<(), Slot<Ready, P>>` would pay for it on every call, not just the rare
    /// rejection) so the caller can answer the reply channel and drop the rejected publisher
    /// rather than leak it.
    ///
    /// # Errors
    /// The boxed `slot` back, unchanged, if `slot.wire_name` is already present.
    pub fn insert(&mut self, slot: Slot<Ready, P>) -> Result<(), Box<Slot<Ready, P>>> {
        match self.by_wire_name.entry(slot.wire_name.clone()) {
            Entry::Occupied(_) => Err(Box::new(slot)),
            Entry::Vacant(vac) => {
                vac.insert(slot);
                Ok(())
            }
        }
    }

    pub fn get_mut(&mut self, wire_name: &str) -> Option<&mut Slot<Ready, P>> {
        self.by_wire_name.get_mut(wire_name)
    }

    /// Removes a symbol's slot, if this connection carries it. Dropping the returned slot
    /// drops its `BookPublisher`, which is what tells the reader the stream is gone.
    pub fn remove(&mut self, wire_name: &str) -> Option<Slot<Ready, P>> {
        self.by_wire_name.remove(wire_name)
    }

    pub fn iter_mut(&mut self) -> impl ExactSizeIterator<Item = &mut Slot<Ready, P>> {
        self.by_wire_name.values_mut()
    }

    /// Every subscribed wire name, for the reconnect resubscribe.
    pub fn wire_names(&self) -> impl ExactSizeIterator<Item = &SharedString> {
        self.by_wire_name.keys()
    }
}
