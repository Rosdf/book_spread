//! Binance spot order book connector.
//!
//! Maintains an [`core_lib::incremental_book::IncrementalBook`] per symbol from Binance's
//! keyless depth stream, and publishes a [`core_lib::small_book::SmallBook`] through a
//! [`core_lib::connector::book_publisher::BookPublisher`] whenever the top of book moves,
//! waking the reader that subscribing handed back.
//!
//! The connection loop, slot table, supervisor and REST fetch are all generic and live in
//! [`core_lib::venue`] - this crate supplies only what is genuinely Binance-specific: its wire
//! shapes and sequencing rules (`decode.rs`), its control-frame pacing (`pacer.rs`), and its
//! config extras (`subscription.rs`), wired together by the `impl VenueSpec for BinanceSpot` below.
//!
//! # Why JSON rather than SBE
//!
//! Binance's SBE market-data streams require an Ed25519 API key in the `X-MBX-APIKEY`
//! header, so they cannot be used without an account. The JSON streams on
//! `wss://data-stream.binance.vision` carry the same depth data and need no credentials of
//! any kind, which is what this connector uses.
//!
//! # Shape
//!
//! [`BinanceSpot`] implements [`Connector`], so it is driven through a
//! [`ConnectorHandle`]: each [`Subscribe`] names a symbol and comes with a reply channel that
//! yields the [`BookReader`] for it, or the reason it was rejected. Symbols are packed onto
//! shared connections - Binance allows 1024 streams per socket - and each symbol bootstraps
//! and resyncs independently, so a sequence gap on one leaves the others streaming.
//!
//! [`ConnectorHandle`]: core_lib::connector::ConnectorHandle
//! [`Subscribe`]: core_lib::connector::events::Subscribe
//! [`BookReader`]: core_lib::connector::book_publisher::BookReader
//!
//! ```no_run
//! # async fn example() -> anyhow::Result<()> {
//! use binance_spot::BinanceSpot;
//! use core_lib::Venue;
//! use core_lib::connector::ConnectorHandle;
//! use core_lib::venue::ConnectorConfig;
//!
//! // The venue's own `Config` is only its extras; `ConnectorConfig` pairs it with the
//! // shared `CoreConfig`, and its `Default` picks up this venue's overrides for both.
//! let handle = ConnectorHandle::new::<BinanceSpot>(ConnectorConfig::default());
//!
//! // `subscribe` takes an already-interned `Instrument` rather than a raw name: the connector
//! // registers every symbol it lists as tradable, so a caller resolves one from the global
//! // registry - here, after giving the connector's first listing refresh time to land.
//! let btcusdt = core_lib::instrument::lookup(Venue::BinanceSpot, "BTCUSDT")
//!     .expect("BTCUSDT is listed");
//! let mut reader = handle.subscribe(btcusdt).await??;
//!
//! while reader.wait_update().await.is_some() {
//!     let book = reader.get_last();
//!     let _ = (book.bids(), book.asks());
//! }
//!
//! handle.shutdown().await;
//! # Ok(())
//! # }
//! ```

mod decode;
mod pacer;
mod subscription;
mod symbol;

// `anyhow` is a dev-dependency used only by this module's doctest, which the
// `unused_crate_dependencies` lint cannot see into.
#[cfg(test)]
use anyhow as _;

use bytes::Bytes;
use core_lib::Venue;
use core_lib::connector::events::ConnectorRx;
use core_lib::connector::{Connector, InstrumentRegistrar};
use core_lib::instrument::{Instrument, InstrumentId};
use core_lib::map::InternalHashSet;
use core_lib::shared_string::SharedString;
use core_lib::venue::{ConnectorConfig, FrameAction, FrameCtx, Slot};

pub use subscription::Config;

/// The Binance spot connector, both as a [`Connector`] to hand to
/// [`ConnectorHandle::new`](core_lib::connector::ConnectorHandle::new) and as the
/// [`core_lib::venue::VenueSpec`] that supplies Binance's wire shapes to the generic connection
/// machinery.
#[derive(Debug)]
pub struct BinanceSpot;

impl Connector for BinanceSpot {
    const VENUE: Venue = Venue::BinanceSpot;

    type Config = Config;

    fn run(
        rx: ConnectorRx,
        config: ConnectorConfig<Self::Config>,
        registrar: impl InstrumentRegistrar + 'static,
    ) -> impl Future<Output = ()> + Send + 'static {
        core_lib::venue::supervisor::run::<Self, reqwest::Client, core_lib::net::TungsteniteConnector>(
            rx,
            config,
            reqwest::Client::new(),
            core_lib::net::TungsteniteConnector,
            registrar,
        )
    }
}

impl core_lib::venue::VenueSpec for BinanceSpot {
    type Config = Config;
    type Ready = decode::Ready;
    type Stage = ();
    type Pending = decode::Buffered;
    type ReplayError = decode::BootstrapError;
    type SymbolsError = decode::SymbolsError;
    type Pacer = pacer::BatchPacer;

    fn stream_url(cfg: &Self::Config) -> String {
        format!("{}/stream", cfg.stream_endpoint())
    }

    fn symbols_url(cfg: &Self::Config) -> String {
        format!("{}/api/v3/exchangeInfo", cfg.rest_endpoint())
    }

    fn parse_symbols<R: InstrumentRegistrar>(
        body: Bytes,
        reg: &R,
    ) -> Result<InternalHashSet<InstrumentId>, Self::SymbolsError> {
        decode::parse_symbols(body, reg)
    }

    /// The REST API wants the name uppercased; `instrument.name()` already is, since that is
    /// exactly the casing `exchangeInfo` handed back when this instrument was registered.
    fn snapshot_url(cfg: &Self::Config, instrument: Instrument) -> String {
        format!(
            "{}/api/v3/depth?symbol={}&limit={}",
            cfg.rest_endpoint(),
            instrument.name(),
            cfg.snapshot_limit(),
        )
    }

    fn wire_name(cfg: &Self::Config, instrument: Instrument) -> SharedString {
        symbol::stream_name(instrument, cfg.depth_speed())
    }

    fn on_frame<'t>(
        ctx: FrameCtx<'t, '_, '_, Self::Ready, Self::Stage, Self::Pending>,
        bytes: Bytes,
    ) -> FrameAction<'t, Self::Ready, Self::Pending> {
        decode::on_frame(ctx, bytes)
    }

    fn seed_and_replay(
        slot: &mut Slot<Self::Ready, Self::Pending>,
        pending: &Self::Pending,
        first_buffered: Option<u64>,
        body: Bytes,
        dec: &mut core_lib::venue::Decoder<Self::Stage>,
    ) -> Result<Self::Ready, Self::ReplayError> {
        decode::seed_and_replay(slot, pending, first_buffered, body, dec)
    }
}
