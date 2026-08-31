//! The connector doubles the end-to-end test drives a whole server with.
//!
//! Everything here is used by `tests/end_to_end.rs`, which is why it is `pub` and gated on
//! the `test-util` feature as well as `cfg(test)`: that target builds this module from
//! outside the crate. Doubles that only unit tests need live beside whatever they stand in
//! for instead - the in-memory socket and listener in [`crate::transport`], the mock client
//! in [`crate::client`], the registry harness in [`crate::registry`].
//!
//! `make_book_publisher_pair` is public, so a fake source can hand out a real [`BookReader`]
//! and keep the matching [`BookPublisher`] for the test to drive - which is the whole reason
//! [`BookSource`] exists as a trait.
use crate::catalogue::Catalogue;
use crate::venue::{BookSource, Connectors, Venue};
use md_wire::grpc::CatalogueIdx;
use core_lib::connector::book_publisher::{BookPublisher, BookReader, make_book_publisher_pair};
use core_lib::incremental_book::IncrementalBook;
use core_lib::instrument::{Instrument, InstrumentId};
use core_lib::map::InternalHashMap;
use core_lib::positive_f64::PositiveF64;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use tokio::net::TcpListener;

#[derive(Debug, Default)]
struct SourceState {
    /// The publisher half of every live subscription.
    live: InternalHashMap<&'static str, BookPublisher>,
    subscribed: Vec<&'static str>,
    unsubscribed: Vec<&'static str>,
    /// When set, every subscribe is answered with this rejection instead of a reader.
    reject: Option<String>,
    /// When set, `unsubscribe` panics rather than releasing the symbol.
    panic_on_unsubscribe: bool,
}

/// One venue's connector with the venue taken out of it.
#[derive(Debug, Default)]
pub struct FakeSource {
    state: Mutex<SourceState>,
}

impl FakeSource {
    /// A source that turns every subscribe down, the way a venue does for a symbol it does
    /// not list.
    pub fn rejecting(why: &str) -> Self {
        Self {
            state: Mutex::new(SourceState {
                reject: Some(why.to_owned()),
                ..SourceState::default()
            }),
        }
    }

    /// A source whose teardown blows up, so a test can watch what a panicking event handler
    /// does to the registry task around it. Nothing a venue does, and that is the point: it
    /// stands in for a bug anywhere inside a critical section.
    pub fn panicking_unsubscribe() -> Self {
        Self {
            state: Mutex::new(SourceState {
                panic_on_unsubscribe: true,
                ..SourceState::default()
            }),
        }
    }

    /// Every symbol `subscribe` was called with, in order. Duplicates are the point of
    /// looking: the registry's job is to make sure there are none.
    pub fn subscribed(&self) -> Vec<&'static str> {
        self.lock().subscribed.clone()
    }

    /// Every symbol `unsubscribe` was called with, in order.
    pub fn unsubscribed(&self) -> Vec<&'static str> {
        self.lock().unsubscribed.clone()
    }

    /// Publishes `book` on `symbol`'s stream.
    pub fn publish(&self, symbol: &str, book: &IncrementalBook) {
        self.with_publisher(symbol, |publisher| publisher.publish(book));
    }

    /// Publishes the empty book, which is how a connector says it is resyncing.
    pub fn publish_empty(&self, symbol: &str) {
        self.with_publisher(symbol, BookPublisher::publish_empty);
    }

    /// Drops `symbol`'s publisher: what a connector shutting down, or a venue delisting the
    /// symbol, looks like from the reader's side.
    pub fn drop_stream(&self, symbol: &str) {
        self.lock().live.remove(symbol);
    }

    fn with_publisher(&self, symbol: &str, f: impl FnOnce(&mut BookPublisher)) {
        let mut state = self.lock();
        let publisher = state
            .live
            .get_mut(symbol)
            .expect("the test published on a symbol nothing is subscribed to");
        f(publisher);
        drop(state);
    }

    /// Records the subscribe and opens a book channel for it, keeping the publisher half.
    fn open(&self, symbol: &'static str) -> anyhow::Result<BookReader> {
        let mut state = self.lock();
        state.subscribed.push(symbol);
        if let Some(why) = state.reject.clone() {
            return Err(anyhow::anyhow!(why));
        }
        let (publisher, reader) = make_book_publisher_pair();
        state.live.insert(symbol, publisher);
        drop(state);
        Ok(reader)
    }

    fn lock(&self) -> MutexGuard<'_, SourceState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl BookSource for FakeSource {
    fn subscribe(&self, instrument_id: InstrumentId) -> oneshot::Receiver<anyhow::Result<BookReader>> {
        let (reply, result) = oneshot::channel();
        let _ = reply.send(self.open(Instrument::by_id(instrument_id).name()));
        result
    }

    fn unsubscribe(&self, instrument_id: InstrumentId) {
        let symbol = Instrument::by_id(instrument_id).name();
        let mut state = self.lock();
        assert!(
            !state.panic_on_unsubscribe,
            "scripted panic while releasing {symbol}"
        );
        // A real unsubscribe drops the symbol's publisher, which is what ends the reader's
        // stream. The fake does the same so teardown looks identical from above.
        state.live.remove(symbol);
        state.unsubscribed.push(symbol);
    }
}

/// Both venues, each backed by a [`FakeSource`].
#[derive(Debug)]
pub struct FakeConnectors {
    binance_spot: Arc<FakeSource>,
    bitstamp: Arc<FakeSource>,
}

impl FakeConnectors {
    /// Both sources are handed in as `Arc`s so the test keeps a handle on them after the
    /// connectors have been given away to a [`Registry`] - which a test of a merged book
    /// needs on both sides, since it publishes a different book on each venue.
    pub fn new(binance_spot: Arc<FakeSource>, bitstamp: Arc<FakeSource>) -> Self {
        Self {
            binance_spot,
            bitstamp,
        }
    }
}

impl Connectors for FakeConnectors {
    type Source = FakeSource;

    fn source(&self, venue: Venue) -> &FakeSource {
        match venue {
            Venue::BinanceSpot => &self.binance_spot,
            Venue::Bitstamp => &self.bitstamp,
        }
    }

    async fn shutdown(self) {}
}

/// A book with the given `(price, size)` levels on each side.
pub fn book(asks: &[(f64, f64)], bids: &[(f64, f64)]) -> IncrementalBook {
    let mut built = IncrementalBook::new();
    for &(price, size) in asks {
        built.update_ask(positive(price), positive(size));
    }
    for &(price, size) in bids {
        built.update_bid(positive(price), positive(size));
    }
    built
}

fn positive(value: f64) -> PositiveF64 {
    PositiveF64::new(value).expect("test prices and sizes are positive")
}

/// The catalogue a test serves, built pair by pair.
///
/// A wrapper rather than the crate's own `Catalogue`, which is private: what a test needs is
/// a way to say "index 3 is `binance_spot/BTCUSDT`" and then name index 3 on the wire. The venue
/// table is every venue this build carries, at its position in `Venue::ALL`, so
/// [`TestCatalogue::venue_idx`] is what a test asserts a level's `venue_idx` against without
/// either side spelling the numbering out.
#[derive(Debug, Default)]
pub struct TestCatalogue {
    instruments: Vec<(u32, Vec<(Venue, String)>)>,
}

impl TestCatalogue {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Carries `symbol` on `venue` at instrument index `idx`.
    #[must_use]
    pub fn with(mut self, idx: u32, venue: Venue, symbol: &str) -> Self {
        self.instruments
            .push((idx, vec![(venue, symbol.to_owned())]));
        self
    }

    /// Carries one instrument spelled differently on each of `pairs`. Every one of them is
    /// subscribed, and their books are merged into the one book this instrument streams.
    #[must_use]
    pub fn with_pairs(mut self, idx: u32, pairs: &[(Venue, &str)]) -> Self {
        self.instruments.push((
            idx,
            pairs
                .iter()
                .map(|&(venue, symbol)| (venue, symbol.to_owned()))
                .collect(),
        ));
        self
    }

    /// The index a subscribe names for the instrument at `idx`. Trivial today - it is `idx` -
    /// and here so a test says what it means rather than casting.
    #[must_use]
    pub fn instrument_idx(idx: u32) -> CatalogueIdx {
        CatalogueIdx::new(idx)
    }

    fn build(&self) -> Catalogue {
        let borrowed: Vec<(u32, Vec<(Venue, &str)>)> = self
            .instruments
            .iter()
            .map(|(idx, pairs)| {
                (
                    *idx,
                    pairs
                        .iter()
                        .map(|(venue, symbol)| (*venue, symbol.as_str()))
                        .collect(),
                )
            })
            .collect();
        let entries: Vec<(u32, &[(Venue, &str)])> = borrowed
            .iter()
            .map(|(idx, pairs)| (*idx, pairs.as_slice()))
            .collect();
        Catalogue::for_test(&entries)
    }
}

/// Serves the book feed on `listener` until `stop` resolves, then stops the connectors.
///
/// # Shutdown ordering
///
/// Three steps, and the order is the whole of it.
///
/// 1. `stop` resolves, and the accept loop is told to stop. It stops accepting and then waits
///    for every handshake still in flight - each of which holds a `RegistryTx` of its own,
///    and each of which is bounded by the handshake timeout, so this cannot hang.
/// 2. [`RegistryHandle::shutdown`] sends the registry its `ShutDown`, which clears its
///    entries. Clearing the entries drops the sending half of every broadcaster's join
///    channel, which each broadcaster's `recv` reports as `None` and takes as "stop". Nothing
///    has to be drained: a broadcaster drops its sessions on the way out, and dropping a
///    broadcaster ends every one of its streams with a status on the way out.
/// 3. The same call then drops the last `RegistryTx` outside the registry task, so the task's
///    own `recv` reports `None` the moment the last broadcaster has dropped its copy - and
///    hands the connectors back on its way out. They are reclaimed rather than dropped
///    because `ConnectorHandle::shutdown` consumes the handle.
///
/// # Errors
///
/// Never actually returns `Err` today; the `Result` is `anyhow`'s so a future transport error
/// has somewhere to go without another signature change.
pub async fn serve(
    listener: TcpListener,
    connectors: FakeConnectors,
    catalogue: &TestCatalogue,
    stop: impl Future<Output = ()> + Send,
) -> anyhow::Result<()> {
    crate::server::serve(listener, connectors, catalogue.build(), stop).await
}

pub const fn venue_idx(venue: Venue) -> u32 {
    crate::encode::venue_idx(venue).get()
}
