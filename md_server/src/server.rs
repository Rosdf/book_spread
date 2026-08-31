//! Wiring and shutdown ordering.
//!
//! One listener, one registry, one handshaker. A connection is accepted, handshaken onto a
//! broadcaster by [`crate::framed`], and from then on written to by that broadcaster directly
//! - see [`crate::broadcast`].

use crate::catalogue::Catalogue;
use crate::catalogue::source::{CatalogueSource as _, FileCatalogue};
use crate::client::Handshaker;
use crate::config::ServerConfig;
use crate::grpc::H2Handshaker;
use crate::registry::RegistryHandle;
use crate::transport::Listener;
use crate::venue::{Connectors, LiveConnectors};
use core_lib::venue::ConnectorConfig;
use std::future::Future;
use tokio::net::TcpListener;

/// Loads the catalogue, binds the configured address, and serves the book feed until ctrl-c.
///
/// The catalogue is loaded *before* the listener is bound, because a failure to load it is a
/// failure to start: there is nothing to fall back to, and a server with an empty catalogue
/// can serve nobody.
///
/// # Errors
///
/// Fails if the catalogue cannot be loaded, or if the configured address cannot be bound.
pub async fn run(
    config: ServerConfig,
    binance_spot: ConnectorConfig<binance_spot::Config>,
    bitstamp: ConnectorConfig<bitstamp::Config>,
) -> anyhow::Result<()> {
    let catalogue = FileCatalogue::new(config.catalogue().path()).load().await?;
    tracing::info!(
        instruments = catalogue.instruments().len(),
        "loaded the catalogue"
    );

    let listener = TcpListener::bind(config.addr()).await?;
    tracing::info!(addr = %listener.local_addr()?, "serving md.v1.MarketData");

    serve(
        listener,
        LiveConnectors::spawn(binance_spot, bitstamp),
        catalogue,
        async {
            if let Err(err) = tokio::signal::ctrl_c().await {
                tracing::error!(%err, "failed to listen for ctrl-c");
            }
        },
    )
    .await
}

/// Serves the book feed on `listener` until `stop` resolves, then stops the connectors.
///
/// `catalogue` is taken by value: it is loaded once and never republished, so there is no
/// catalogue task to order against the shutdown below and nothing here holds a subscription
/// to it.
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
///    has to be drained: a broadcaster ends every one of its streams with a status on the way
///    out, under one deadline for all of them, and drops what will not read it.
/// 3. The same call then drops the last `RegistryTx` outside the registry task, so the task's
///    own `recv` reports `None` the moment the last broadcaster has dropped its copy - and
///    hands the connectors back on its way out. They are reclaimed rather than dropped
///    because `ConnectorHandle::shutdown` consumes the handle.
///
/// # Errors
///
/// Never actually returns `Err` today; the `Result` is `anyhow`'s so a future transport error
/// has somewhere to go without another signature change.
pub(crate) async fn serve<V: Connectors, L: Listener>(
    listener: L,
    connectors: V,
    catalogue: Catalogue,
    stop: impl Future<Output = ()> + Send,
) -> anyhow::Result<()>
where
    H2Handshaker: Handshaker<L::Stream>,
{
    // The one place both halves of the catalogue are in scope at once: the venue table is read
    // by the encoder here, and each instrument's pairs carry the venue index the registry
    // stamps on a broadcaster's books - so the response is encoded first and the entries are
    // then moved into the registry.
    let encoded = crate::catalogue::encode::encode(&catalogue);
    let registry = RegistryHandle::spawn(connectors, catalogue.into_instruments());

    let (stop_accepting, stopped) = oneshot::channel();
    let accepting = tokio::spawn(crate::framed::accept(
        registry.tx(),
        listener,
        Box::leak(Box::new(H2Handshaker::new())),
        encoded,
        stopped,
    ));

    stop.await;
    let _ = stop_accepting.send(());
    if let Err(err) = accepting.await {
        tracing::error!(%err, "the accept loop panicked");
    }

    if let Some(reclaimed) = registry.shutdown().await {
        reclaimed.shutdown().await;
    } else {
        tracing::error!("the registry task did not finish; connectors left running");
    }

    report_depth_stats();
    Ok(())
}

/// Reports where price updates landed over the life of the process, when anything was
/// counting.
///
/// Silent unless `core_lib`'s `book_stats` feature is on, which is the only build that counts
/// anything. It answers one open question - whether the `cold_path()` hint on
/// `IncrementalBook`'s deep branch matches what venues actually send - and it is reported at
/// shutdown because the counters are process-wide and only interesting over a long run.
fn report_depth_stats() {
    let (shallow, deep) = core_lib::incremental_book::depth_stats();
    let total = shallow + deep;
    if total == 0 {
        return;
    }

    #[expect(
        clippy::cast_precision_loss,
        reason = "a ratio for a log line; the counts would have to reach 2^53 updates to lose a digit that matters"
    )]
    let deep_share = deep as f64 / total as f64;
    tracing::info!(
        shallow,
        deep,
        deep_share,
        "price updates by depth; a high deep share means the cold_path hint on the deep branch is pessimising the hot loop"
    );
}
