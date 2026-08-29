//! Routes subscribe/unsubscribe requests to connections, spawning a new one when the current
//! lanes are full.
//!
//! Generic over `V: VenueSpec`; thin glue over [`Router`] that is identical for every venue once
//! the connection task itself is generic.
//!
//! Every subscribe is checked against the venue's own symbol listing first - see
//! [`crate::venue::universe`] - so an instrument no longer tradable is refused before a lane is
//! chosen, rather than discovered from a control-frame rejection that leaves a slot
//! bootstrapping forever. An instrument that has never been listed at all cannot reach here in
//! the first place: it can only exist as an [`Instrument`] because some past listing registered
//! it, so there is no "the listing has not arrived yet" state for a subscribe to be held in -
//! unlike a raw, unvalidated symbol string, which is why this no longer needs a wait queue.

use crate::connector::InstrumentRegistrar;
use crate::connector::events::{ConnectorEvent, ConnectorRx, Subscribe, Unsubscribe};
use crate::instrument::{Instrument, InstrumentId};
use crate::map::{InternalHashSet, new_internal_set};
use crate::net::{RestClient, WsConnector};
use crate::venue::connection::{self, LaneCommand};
use crate::venue::router::{LaneId, Router};
use crate::venue::spec::{SnapshotFetchError, VenueSpec};
use crate::venue::{ConnectorConfig, universe};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinSet;

/// Per-connection channel capacity. Subscriptions are rare, so this only has to absorb a burst
/// arriving while a connection is mid-reconnect.
const SUBS_CHANNEL: usize = 32;

/// Depth of the listing channel. One refresh at a time, and the supervisor takes each as soon
/// as it comes back to its `select!`, so this only has to hold the one in flight.
const UNIVERSE_CHANNEL: usize = 1;

/// Total budget for every connection to finish its close handshake. Past this the stragglers
/// are aborted so `shutdown` still answers promptly.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

/// Runs a venue's connector: reads [`Subscribe`]/[`Unsubscribe`] requests off `rx` and routes
/// each to a connection.
///
/// Returns once `rx` yields [`ConnectorEvent::ShutDown`] or is closed; either way every
/// connection is closed and joined before this returns, so no connection outlives the
/// supervisor.
///
/// Symbol identity is connector-wide: [`Router`] rejects a second subscribe of a symbol already
/// live on some lane, so one symbol can never end up with two independent books.
pub async fn run<V, R, W>(
    mut rx: ConnectorRx,
    cfg: ConnectorConfig<V::Config>,
    client: R,
    ws: W,
    registrar: impl InstrumentRegistrar + 'static,
) where
    V: VenueSpec,
    R: RestClient,
    W: WsConnector,
    V::ReplayError: From<SnapshotFetchError<R::Builder>>,
{
    let core = cfg.core();
    let capacity = core.max_symbols_per_connection().max(1);
    let ctx: ConnCtx<V, R, W> = ConnCtx {
        permits: Arc::new(Semaphore::new(core.max_concurrent_snapshots().max(1))),
        client,
        ws,
        cfg,
    };

    let mut router = Router::new(capacity);
    let mut tasks: JoinSet<()> = JoinSet::new();

    let (universe_tx, mut universe_rx) = mpsc::channel(UNIVERSE_CHANNEL);
    let refresher = tokio::spawn(universe::refresh_loop::<V, R>(
        ctx.cfg.clone(),
        ctx.client.clone(),
        registrar,
        universe_tx,
    ));

    // The venue's listing. Empty until the first refresh lands, which is the same fail-closed
    // answer a `None` gave: nothing is routed until something has actually been listed.
    let mut listed = new_internal_set();

    loop {
        tokio::select! {
            // `Some(..)`, so a refresh task that has gone away simply disables this arm
            // rather than completing instantly forever.
            Some(fresh) = universe_rx.recv() => {
                retire_unlisted(&fresh, &mut router).await;
                listed = fresh;
            }

            received = rx.recv() => {
                let Some(event) = received else {
                    tracing::info!(
                        connections = router.lane_count(),
                        "subscription queue closed, shutting down connections"
                    );
                    refresher.abort();
                    stop_connections(router, tasks).await;
                    return;
                };

                match event {
                    ConnectorEvent::Subscribe(sub) => {
                        handle_subscribe(sub, &listed, &mut router, &mut tasks, &ctx).await;
                    }
                    ConnectorEvent::Unsubscribe(unsub) => {
                        handle_unsubscribe(unsub, &mut router).await;
                    }
                    ConnectorEvent::ShutDown(ack) => {
                        refresher.abort();
                        stop_connections(router, tasks).await;
                        let _ = ack.send(());
                        return;
                    }
                }
            }

            Some(finished) = tasks.join_next(), if !tasks.is_empty() => {
                if let Err(err) = finished {
                    tracing::error!(%err, "connection task panicked");
                }
                // A lane task that has returned dropped its receiver, so the sender the
                // router still holds now reports closed. That is the signal rather than the
                // task's identity: it works for a panic exactly as for a clean return, and
                // finds nothing for a lane already removed by `reap_idle`.
                let orphaned = router.purge_closed();
                if !orphaned.is_empty() {
                    tracing::error!(symbols = orphaned.len(), "connection gone, symbols dropped");
                }
            }
        }
    }
}

/// Tears down every subscribed instrument the venue no longer lists as tradable.
///
/// Dropping the slot on the connection drops its `BookPublisher`, which is what tells the
/// reader the stream has ended - the same signal an explicit unsubscribe gives, since from a
/// reader's point of view it is the same thing.
async fn retire_unlisted(listed: &InternalHashSet<InstrumentId>, router: &mut Router<LaneCommand>) {
    let gone: Box<[_]> = router
        .symbols()
        .filter(|instrument_id| !listed.contains(instrument_id))
        .collect();

    for instrument_id in gone {
        let Some(lane_id) = router.take(instrument_id) else {
            continue;
        };
        let instrument = Instrument::by_id(instrument_id);
        tracing::warn!(%instrument, "no longer listed as tradable, dropping subscription");
        if let Some(tx) = router.tx(lane_id) {
            let _ = tx.send(LaneCommand::Unsubscribe { instrument_id }).await;
        }
    }
}

/// What every connection task is spawned with. Grouped so a spawn site takes one argument
/// instead of three.
///
/// `R`/`W` are unbounded here - `client` and `ws` are plain fields, never projected - unlike
/// `V`, whose `cfg: ConnectorConfig<V::Config>` reaches through a projection and so needs
/// `V: VenueSpec` stated right on this declaration; every function below that actually calls into
/// `R`/`W`'s traits states its own bound instead.
struct ConnCtx<V: VenueSpec, R, W> {
    cfg: ConnectorConfig<V::Config>,
    client: R,
    ws: W,
    permits: Arc<Semaphore>,
}

async fn handle_subscribe<V, R, W>(
    sub: Subscribe,
    listed: &InternalHashSet<InstrumentId>,
    router: &mut Router<LaneCommand>,
    tasks: &mut JoinSet<()>,
    ctx: &ConnCtx<V, R, W>,
) where
    V: VenueSpec,
    R: RestClient,
    W: WsConnector,
    V::ReplayError: From<SnapshotFetchError<R::Builder>>,
{
    let (instrument_id, reply) = sub.into_parts();

    // Fail closed: a listing that does not name this instrument as currently tradable is
    // refused, even though the instrument itself is known - it may simply have been delisted
    // since the listing that first registered it.
    if !listed.contains(&instrument_id) {
        let instrument = Instrument::by_id(instrument_id);
        tracing::error!(%instrument, "not listed as tradable on this venue, rejecting subscription");
        let _ = reply.send(Err(anyhow::anyhow!(
            "{instrument} is not listed as tradable on this venue",
        )));
        return;
    }

    if router.contains(instrument_id) {
        let instrument = Instrument::by_id(instrument_id);
        tracing::error!(%instrument, "already subscribed on this connector");
        let _ = reply.send(Err(anyhow::anyhow!("{instrument} is already subscribed")));
        return;
    }

    let lane_id = router.lane_with_room().unwrap_or_else(|| {
        let id = spawn_lane::<V, R, W>(router, tasks, ctx);
        tracing::info!(connections = router.lane_count(), "opening connection");
        id
    });

    send_subscribe::<V, R, W>(instrument_id, reply, lane_id, router, tasks, ctx).await;
}

/// Sends one `Subscribe` command to `lane_id`. On a dead lane - one whose task exited between
/// the liveness check and the send - the command comes back through the `SendError`, so the
/// lane is purged and the command is retried once on a freshly opened one; a second failure
/// gives up and answers the request's reply channel with `Err`.
async fn send_subscribe<V, R, W>(
    instrument_id: InstrumentId,
    reply: oneshot::Sender<anyhow::Result<crate::connector::book_publisher::BookReader>>,
    mut lane_id: LaneId,
    router: &mut Router<LaneCommand>,
    tasks: &mut JoinSet<()>,
    ctx: &ConnCtx<V, R, W>,
) where
    V: VenueSpec,
    R: RestClient,
    W: WsConnector,
    V::ReplayError: From<SnapshotFetchError<R::Builder>>,
{
    let mut cmd = LaneCommand::Subscribe {
        instrument_id,
        reply,
    };

    for attempt in 0..2 {
        // `None` is a lane the router has already dropped since `lane_id` was chosen - the
        // same situation as a failed send, and handled the same way.
        if let Some(tx) = router.tx(lane_id) {
            match tx.send(cmd).await {
                Ok(()) => {
                    router.bind(instrument_id, lane_id);
                    return;
                }
                Err(mpsc::error::SendError(returned)) => cmd = returned,
            }
        } else {
            tracing::debug!("lane already gone before the subscribe could be sent");
        }

        let orphaned = router.drop_lane(lane_id);
        if !orphaned.is_empty() {
            tracing::error!(symbols = orphaned.len(), "connection gone, symbols dropped");
        }
        if attempt == 1 {
            break;
        }
        lane_id = spawn_lane::<V, R, W>(router, tasks, ctx);
    }

    let LaneCommand::Subscribe {
        instrument_id,
        reply,
    } = cmd
    else {
        unreachable!("only Subscribe is ever routed here")
    };
    let instrument = Instrument::by_id(instrument_id);
    tracing::error!(%instrument, "connection task gone, dropping subscription");
    let _ = reply.send(Err(anyhow::anyhow!(
        "connection task carrying {instrument} is gone"
    )));
}

/// Opens a new connection task, registers it in `tasks`, and returns the lane the router
/// created for it.
fn spawn_lane<V, R, W>(
    router: &mut Router<LaneCommand>,
    tasks: &mut JoinSet<()>,
    ctx: &ConnCtx<V, R, W>,
) -> LaneId
where
    V: VenueSpec,
    R: RestClient,
    W: WsConnector,
    V::ReplayError: From<SnapshotFetchError<R::Builder>>,
{
    let (tx, conn_rx) = mpsc::channel(SUBS_CHANNEL);
    tasks.spawn(connection::run::<V, R, W>(
        conn_rx,
        ctx.cfg.clone(),
        ctx.client.clone(),
        ctx.ws.clone(),
        Arc::clone(&ctx.permits),
    ));
    router.insert_lane(tx)
}

async fn handle_unsubscribe(unsub: Unsubscribe, router: &mut Router<LaneCommand>) {
    let instrument_id = unsub.into_instrument();

    let Some(lane_id) = router.take(instrument_id) else {
        let instrument = Instrument::by_id(instrument_id);
        tracing::debug!(%instrument, "unsubscribe for a symbol that is not subscribed");
        return;
    };

    match router.tx(lane_id) {
        // The lane is already gone; nothing left to tell it. The next `join_next` reap will
        // notice and purge whatever else it was carrying.
        None => {
            let instrument = Instrument::by_id(instrument_id);
            tracing::debug!(%instrument, "lane already gone, nothing to unsubscribe");
        }
        Some(tx) => {
            let _ = tx.send(LaneCommand::Unsubscribe { instrument_id }).await;
        }
    }

    router.reap_idle();
}

/// Closes every connection and waits for it, within [`SHUTDOWN_GRACE`] overall.
///
/// Dropping every lane's sender is the signal: each connection sees its queue close, sends a
/// close frame and returns.
async fn stop_connections(router: Router<LaneCommand>, mut tasks: JoinSet<()>) {
    let deadline = tokio::time::Instant::now() + SHUTDOWN_GRACE;
    // Dropping `router` here drops every lane's `tx`, which is what tells the connections to
    // stop.
    drop(router);

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, tasks.join_next()).await {
            Ok(Some(_)) => {}
            Ok(None) => return,
            Err(_) => break,
        }
    }

    if !tasks.is_empty() {
        tracing::warn!("connections did not close in time, aborting");
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }
}

#[cfg(test)]
mod test {
    use super::{ConnCtx, handle_subscribe, retire_unlisted, run};
    use crate::connector::VenueGuard;
    use crate::connector::book_publisher::BookReader;
    use crate::connector::events::{ConnectorEvent, Subscribe, create_event_channel};
    use crate::instrument::InstrumentId;
    use crate::map::{InternalHashSet, new_internal_set};
    use crate::venue::connection::LaneCommand;
    use crate::venue::router::Router;
    use crate::venue::test_util::{
        Incoming, ScriptedWs, StubRest, TestConfig, TestVenue, test_instrument_for,
    };
    use crate::venue::{ConnectorConfig, CoreConfig};
    use all_venues::Venue;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::{Semaphore, mpsc};

    fn listing(names: &[&str]) -> InternalHashSet<InstrumentId> {
        let mut set = new_internal_set();
        for name in names {
            set.insert(test_instrument_for(Venue::BinanceSpot, name).id());
        }
        set
    }

    /// A context nothing in these tests reaches: every path exercised here answers the reply
    /// channel before a lane is ever chosen.
    fn ctx() -> ConnCtx<TestVenue, StubRest, ScriptedWs> {
        ConnCtx {
            cfg: ConnectorConfig::new(CoreConfig::default(), TestConfig),
            client: StubRest::always("100"),
            ws: ScriptedWs::new(Vec::new()),
            permits: Arc::new(Semaphore::new(1)),
        }
    }

    async fn rejection(listed: &InternalHashSet<InstrumentId>, name: &str) -> String {
        let (sub, reply) = Subscribe::new(test_instrument_for(Venue::BinanceSpot, name).id());
        let mut router: Router<LaneCommand> = Router::new(10);
        let mut tasks = tokio::task::JoinSet::new();

        handle_subscribe(sub, listed, &mut router, &mut tasks, &ctx()).await;

        assert_eq!(
            router.lane_count(),
            0,
            "a rejected subscribe must not open a connection"
        );
        reply
            .await
            .expect("every path answers the reply channel")
            .expect_err("expected a rejection")
            .to_string()
    }

    #[tokio::test]
    async fn an_instrument_the_venue_does_not_currently_list_is_rejected_before_a_lane_is_chosen() {
        let listed = listing(&["btcusd"]);
        let why = rejection(&listed, "ethusd").await;
        assert!(why.contains("not listed as tradable"), "{why}");
    }

    /// Fail closed: with nothing listed there is nothing to check an instrument against, so
    /// `handle_subscribe` refuses rather than routing on trust.
    #[tokio::test]
    async fn a_subscribe_with_nothing_listed_at_all_is_refused() {
        let why = rejection(&new_internal_set(), "btcusd").await;
        assert!(why.contains("not listed as tradable"), "{why}");
    }

    #[tokio::test]
    async fn a_symbol_dropped_from_a_refresh_is_torn_down() {
        let (tx, mut lane_rx) = mpsc::channel(4);
        let mut router: Router<LaneCommand> = Router::new(10);
        let lane = router.insert_lane(tx);
        let btcusd = test_instrument_for(Venue::BinanceSpot, "btcusd");
        let lunausd = test_instrument_for(Venue::BinanceSpot, "lunausd");
        router.bind(btcusd.id(), lane);
        router.bind(lunausd.id(), lane);

        retire_unlisted(&listing(&["btcusd"]), &mut router).await;

        assert!(router.contains(btcusd.id()), "a still-listed symbol stays");
        assert!(!router.contains(lunausd.id()));

        let sent = lane_rx
            .recv()
            .await
            .expect("the lane must be told to drop it");
        let LaneCommand::Unsubscribe {
            instrument_id: dropped,
        } = sent
        else {
            panic!("expected an unsubscribe, got {sent:?}");
        };
        assert_eq!(dropped, lunausd.id());
    }

    /// The wait queue this used to exercise is gone: an instrument can only reach
    /// `handle_subscribe` as an already-registered [`Instrument`], so a listed one is accepted
    /// immediately rather than eventually, once a listing lands, drained from a queue.
    #[tokio::test]
    async fn a_listed_instrument_is_accepted_immediately() {
        let btcusd = test_instrument_for(Venue::BinanceSpot, "btcusd");
        let listed = listing(&["btcusd"]);
        let (sub, reply) = Subscribe::new(btcusd.id());
        let mut router: Router<LaneCommand> = Router::new(10);
        let mut tasks = tokio::task::JoinSet::new();

        handle_subscribe(sub, &listed, &mut router, &mut tasks, &ctx()).await;

        assert_eq!(router.lane_count(), 1, "a listed instrument opens a lane");
        let reader: BookReader = reply.await.unwrap().expect("btcusd is listed");
        drop(reader);
    }

    #[tokio::test(start_paused = true)]
    async fn an_unlisted_symbol_is_rejected_end_to_end() {
        let client = StubRest::always("100").with_route("listing", "btcusd");
        let ws = ScriptedWs::with_fallback(Vec::new(), vec![Incoming::Parks]);
        let (rx, tx) = create_event_channel();

        let supervisor = tokio::spawn(run::<TestVenue, StubRest, ScriptedWs>(
            rx,
            ConnectorConfig::new(CoreConfig::default(), TestConfig),
            client,
            ws,
            VenueGuard::new(Venue::BinanceSpot),
        ));

        // Interned so the event can carry a real `Instrument`, but never listed by the venue
        // above - the same shape a delisted instrument would have.
        let dogeusd = test_instrument_for(Venue::BinanceSpot, "dogeusd");
        let (event, reply) = Subscribe::new(dogeusd.id());
        tx.send(ConnectorEvent::Subscribe(event));

        let why = tokio::time::timeout(Duration::from_secs(30), reply)
            .await
            .expect("the request must be answered")
            .unwrap()
            .expect_err("dogeusd is not listed")
            .to_string();
        assert!(why.contains("not listed as tradable"), "{why}");

        let (ack, acked) = oneshot::channel();
        tx.send(ConnectorEvent::ShutDown(ack));
        let _ = tokio::time::timeout(Duration::from_secs(120), acked).await;
        let _ = tokio::time::timeout(Duration::from_secs(120), supervisor).await;
    }
}
