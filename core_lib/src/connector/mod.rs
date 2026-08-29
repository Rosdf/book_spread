use crate::connector::book_publisher::BookReader;
use crate::connector::events::{ConnectorEvent, ConnectorRx, ConnectorTx, create_event_channel};
use crate::instrument::{Instrument, InstrumentId};
use crate::venue::ConnectorConfig;
use all_venues::Venue;
use serde::Deserialize;
use std::marker::PhantomData;

pub mod book_publisher;
pub mod events;

#[derive(Debug)]
pub struct ConnectorHandle {
    tx: ConnectorTx,
    task: tokio::task::JoinHandle<()>,
}

mod sealed {
    pub trait Sealed {}
}

/// Proof that the holder is a connector spawned for one particular venue, and the only way to
/// register a new [`Instrument`] on it.
///
/// Sealed: the only implementor is a zero-sized guard type declared inside
/// [`crate::connector::ConnectorHandle::new`], so no code outside it can name the type, let
/// alone forge one for a venue it does not own. Because [`Self::venue`] comes off the guard
/// rather than an argument, a venue's decoder cannot register an instrument on another venue by
/// mistake.
pub trait InstrumentRegistrar: Send + Sync + Sized + sealed::Sealed {
    /// The venue this guard was spawned for.
    fn venue(&self) -> Venue;

    /// Interns `name` on [`Self::venue`], returning the existing handle if it is already
    /// registered.
    fn register(&self, name: &str) -> Instrument {
        Instrument::register(name, self)
    }

    /// As [`lookup`], scoped to [`Self::venue`].
    fn lookup(&self, name: &str) -> Option<Instrument> {
        Instrument::lookup(self.venue(), name)
    }
}

pub trait Connector {
    /// The venue this connector talks to. What lets [`ConnectorHandle::new`] hand `run` a
    /// [`InstrumentRegistrar`] scoped to exactly this venue, with no way for the connector to name
    /// another.
    const VENUE: Venue;

    /// The venue's own extras - its endpoints and wire-format knobs. Not the whole connector
    /// config: `run` receives those wrapped in a [`ConnectorConfig`], which carries the shared
    /// `CoreConfig` alongside them.
    type Config: for<'de> Deserialize<'de> + Send + 'static;

    fn run(
        rx: ConnectorRx,
        config: ConnectorConfig<Self::Config>,
        registrar: impl InstrumentRegistrar + 'static,
    ) -> impl Future<Output = ()> + Send + 'static;
}

impl ConnectorHandle {
    /// Spawns `C`'s connector and returns the handle to drive it.
    ///
    /// `config` pairs the shared tuning with `C`'s own extras. `ConnectorConfig::default()`
    /// is the usual call: its `Default` runs through the venue's `Defaults` impl, so both
    /// halves come out with that venue's chosen values rather than a generic baseline.
    pub fn new<C: Connector + 'static>(config: ConnectorConfig<C::Config>) -> Self {
        /// A sealed, zero-sized proof that the holder is `C`'s own connector. Declared inside
        /// this function so nothing outside it can name the type, let alone forge one for a
        /// venue it does not own - see [`InstrumentRegistrar`]'s doc.
        // `PhantomData<fn() -> C>` rather than `PhantomData<C>`: it is `Send` regardless of
        // `C`, since it never actually stores one - a plain `PhantomData<C>` would need
        // `C: Send` too.
        struct Guard<C>(PhantomData<fn() -> C>);

        impl<C> sealed::Sealed for Guard<C> {}
        impl<C: Connector> InstrumentRegistrar for Guard<C> {
            fn venue(&self) -> Venue {
                C::VENUE
            }
        }

        let (rx, tx) = create_event_channel();
        let task = tokio::task::spawn(async move {
            C::run(rx, config, Guard::<C>(PhantomData)).await;
        });

        Self { tx, task }
    }

    pub fn subscribe(
        &self,
        instrument: InstrumentId,
    ) -> oneshot::Receiver<anyhow::Result<BookReader>> {
        self.tx.subscribe(instrument)
    }

    /// Tears down one instrument's stream. An instrument this connector never carried is
    /// logged by the connector and otherwise ignored - the teardown is already observable
    /// in-band through the book channel, so there is nothing for this call to report.
    pub fn unsubscribe(&self, instrument: InstrumentId) {
        self.tx.unsubscribe(instrument);
    }

    pub async fn shutdown(self) {
        let (tx, rx) = oneshot::channel();
        self.tx.send(ConnectorEvent::ShutDown(tx));
        if rx.await.is_err() {
            tracing::error!("Connector failed to shut down, aborting task");
            self.task.abort();
        }

        let _ = self.task.await;
    }
}

/// An [`InstrumentRegistrar`] over any venue a test names, for test code that needs more than
/// one venue registered at once.
#[cfg(any(test, feature = "test_util"))]
#[derive(Debug)]
pub struct VenueGuard(Venue);

#[cfg(any(test, feature = "test_util"))]
impl VenueGuard {
    pub fn new(venue: Venue) -> Self {
        Self(venue)
    }
}

#[cfg(any(test, feature = "test_util"))]
impl sealed::Sealed for VenueGuard {}

#[cfg(any(test, feature = "test_util"))]
impl InstrumentRegistrar for VenueGuard {
    fn venue(&self) -> Venue {
        self.0
    }
}
