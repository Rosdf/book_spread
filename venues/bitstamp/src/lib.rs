//! Bitstamp spot order book connector.
//!
//! Maintains an [`core_lib::incremental_book::IncrementalBook`] per symbol from Bitstamp's
//! keyless `diff_order_book` stream, and publishes a [`core_lib::small_book::SmallBook`]
//! through a [`core_lib::connector::book_publisher::BookPublisher`] whenever the top of book
//! moves - the same shape as `binance_spot`, down to the [`Connector`] impl, so a consumer
//! reads both venues through the identical [`ConnectorHandle`]/[`BookReader`] surface.
//!
//! The connection loop, slot table, supervisor and REST fetch are all generic and live in
//! [`core_lib::venue`] - this crate supplies only what is genuinely Bitstamp-specific: its
//! wire shapes and sequencing rules (`decode.rs`), its control-frame pacing (`pacer.rs`), and
//! its config extras (`subscription.rs`), wired together by the `impl Venue for Bitstamp`
//! below.
//!
//! # Why the decode differs from Binance
//!
//! Binance's combined-stream envelope puts the demux key first - `{"stream": "...", "data":
//! {...}}` - so its decoder resolves the target book before parsing a single price level and
//! applies each one directly, with no intermediate model. Bitstamp's envelope is `{"data":
//! {...}, "channel": "...", "event": "data"}`: the levels arrive *before* the channel name
//! that says which book they belong to. That forces a real two-phase decode - see
//! `decode.rs`'s module doc - which is allocation-free in steady state but is not
//! allocation-*free of an intermediate model* the way Binance's is.
//!
//! Bitstamp also carries no sequence numbers, only a `microtimestamp` per frame - there is no
//! Binance-style `U`/`u` pair to detect a dropped frame with. `decode.rs`'s slot machine has
//! one ready state rather than two as a result, and the per-symbol idle resync in
//! `core_lib::venue::connection` is the mitigation both venues share.
//!
//! [`ConnectorHandle`]: core_lib::connector::ConnectorHandle
//! [`BookReader`]: core_lib::connector::book_publisher::BookReader
//!
//! ```no_run
//! # async fn example() -> anyhow::Result<()> {
//! use bitstamp::Bitstamp;
//! use core_lib::connector::ConnectorHandle;
//! use core_lib::venue::ConnectorConfig;
//!
//! // The venue's own `Config` is only its extras; `ConnectorConfig` pairs it with the
//! // shared `CoreConfig`, and its `Default` picks up this venue's overrides for both.
//! let handle = ConnectorHandle::new::<Bitstamp>(ConnectorConfig::default());
//!
//! let mut reader = handle.subscribe("btcusd".into()).await??;
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
use core_lib::connector::Connector;
use core_lib::connector::events::ConnectorRx;
use core_lib::venue::{ConnectorConfig, FrameAction, FrameCtx, Slot, Symbol};
use std::collections::HashSet;

pub use subscription::Config;

/// The Bitstamp connector, both as a [`Connector`] to hand to
/// [`ConnectorHandle::new`](core_lib::connector::ConnectorHandle::new) and as the
/// [`core_lib::venue::Venue`] that supplies Bitstamp's wire shapes to the generic connection
/// machinery.
#[derive(Debug)]
pub struct Bitstamp;

impl Connector for Bitstamp {
    type Config = Config;

    fn run(rx: ConnectorRx, config: ConnectorConfig<Self::Config>) -> impl Future<Output = ()> + Send + 'static {
        core_lib::venue::supervisor::run::<Self, reqwest::Client, core_lib::net::TungsteniteConnector>(
            rx,
            config,
            reqwest::Client::new(),
            core_lib::net::TungsteniteConnector,
        )
    }
}

impl core_lib::venue::Venue for Bitstamp {
    type Config = Config;
    type Ready = decode::Ready;
    type Stage = decode::LevelStage;
    type Pending = decode::Buffered;
    type ReplayError = decode::BootstrapError;
    type SymbolsError = decode::SymbolsError;
    type Pacer = pacer::QueuePacer;

    fn stream_url(cfg: &Self::Config) -> String {
        cfg.stream_endpoint().to_owned()
    }

    fn symbols_url(cfg: &Self::Config) -> String {
        format!("{}/api/v2/trading-pairs-info/", cfg.rest_endpoint())
    }

    fn parse_symbols(body: Bytes) -> Result<HashSet<Symbol>, Self::SymbolsError> {
        decode::parse_symbols(body)
    }

    fn snapshot_url(cfg: &Self::Config, symbol: &mut Symbol) -> String {
        format!("{}/api/v2/order_book/{}/", cfg.rest_endpoint(), symbol.as_str())
    }

    fn wire_name(_cfg: &Self::Config, symbol: &Symbol) -> Box<str> {
        symbol::channel_name(symbol)
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
