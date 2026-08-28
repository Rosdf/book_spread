//! Venue identity, and the slice of a connector the fan-out layer actually uses.

use binance_spot::BinanceSpot;
use bitstamp::Bitstamp;
use core_lib::connector::ConnectorHandle;
use core_lib::connector::book_publisher::BookReader;
use core_lib::venue::ConnectorConfig;
use std::fmt::Debug;
use std::future::Future;

/// The venues this server carries.
///
/// The names live here rather than in `core_lib`: `core_lib`'s `Venue::wire_name` names a
/// *symbol* on a venue, and nothing upstream names the venue itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Venue {
    BinanceSpot,
    Bitstamp,
}

impl Venue {
    /// Every venue, in the order a listing should present them.
    pub(crate) const ALL: [Self; 2] = [Self::BinanceSpot, Self::Bitstamp];

    /// The name a client puts in `SubscribeBookRequest::venue`, and the one echoed back on
    /// every `BookUpdate`.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::BinanceSpot => "binance_spot",
            Self::Bitstamp => "bitstamp",
        }
    }

    /// Case-insensitive. `None` for a venue this server does not carry.
    pub(crate) fn parse(raw: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|venue| venue.as_str().eq_ignore_ascii_case(raw))
    }
}

/// What a broadcaster needs from a connector.
///
/// A trait so the broadcaster and the registry can be exercised against
/// `make_book_publisher_pair()` with no socket involved; [`ConnectorHandle`] is the only
/// implementation that talks to a venue.
pub(crate) trait BookSource: Debug + Send + Sync + 'static {
    /// Asks for `symbol`'s book stream. The reply carries the venue's own rejection when the
    /// symbol is not tradable there, or when it is already subscribed.
    fn subscribe(&self, symbol: Box<str>) -> oneshot::Receiver<anyhow::Result<BookReader>>;

    /// Tears one symbol's stream down. Nothing to report: the teardown is already visible
    /// in-band, as the book channel ending.
    fn unsubscribe(&self, symbol: Box<str>);
}

#[expect(
    clippy::use_self,
    reason = "naming the concrete type is what shows these forward to the inherent methods rather than to themselves"
)]
impl BookSource for ConnectorHandle {
    fn subscribe(&self, symbol: Box<str>) -> oneshot::Receiver<anyhow::Result<BookReader>> {
        ConnectorHandle::subscribe(self, symbol)
    }

    fn unsubscribe(&self, symbol: Box<str>) {
        ConnectorHandle::unsubscribe(self, symbol);
    }
}

/// Every venue's connector, addressable by [`Venue`].
///
/// Every type that carries this trait reaches its callers by generic parameter rather than
/// behind a trait object - there is exactly one implementation in a running server, and one
/// more in the tests, so nothing is gained by erasing which.
pub(crate) trait Connectors: Debug + Send + Sync + 'static {
    /// What every one of this set's connectors is. A single type, not a per-venue one: the
    /// venues differ in their configuration, not in the shape of their handle.
    type Source: BookSource;

    /// The connector carrying `venue`.
    fn source(&self, venue: Venue) -> &Self::Source;

    /// Stops every connector and waits for it.
    ///
    /// Takes `self` because [`ConnectorHandle::shutdown`] consumes the handle.
    fn shutdown(self) -> impl Future<Output = ()> + Send;
}

/// The real connectors, one spawned task per venue.
#[derive(Debug)]
pub(crate) struct LiveConnectors {
    binance_spot: ConnectorHandle,
    bitstamp: ConnectorHandle,
}

impl LiveConnectors {
    /// Spawns both connectors on that venue's own default configuration.
    ///
    /// Neither is subscribed to anything yet: symbols are subscribed on demand, by the first
    /// client that asks for one.
    pub(crate) fn spawn() -> Self {
        Self {
            binance_spot: ConnectorHandle::new::<BinanceSpot>(ConnectorConfig::default()),
            bitstamp: ConnectorHandle::new::<Bitstamp>(ConnectorConfig::default()),
        }
    }
}

impl Connectors for LiveConnectors {
    type Source = ConnectorHandle;

    fn source(&self, venue: Venue) -> &ConnectorHandle {
        match venue {
            Venue::BinanceSpot => &self.binance_spot,
            Venue::Bitstamp => &self.bitstamp,
        }
    }

    async fn shutdown(self) {
        self.binance_spot.shutdown().await;
        self.bitstamp.shutdown().await;
    }
}
