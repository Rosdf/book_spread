//! Routes subscribe/unsubscribe requests to connections, spawning a new one when the current
//! lanes are full.
//!
//! Generic over `V: Venue`; thin glue over [`Router`] that is identical for every venue once
//! the connection task itself is generic.
//!
//! Every subscribe is checked against the venue's own symbol listing first - see
//! [`crate::venue::universe`] - so an unknown or halted symbol is refused before a lane is
//! chosen, rather than discovered from a control-frame rejection that leaves a slot
//! bootstrapping forever. The listing is *fail closed*: nothing is routed until one has been
//! fetched. Requests that arrive before that first listing are held rather than refused, since
//! refusing them would answer "not tradable" for symbols that in fact are - the connector has
//! simply not looked yet.

use crate::connector::events::{ConnectorEvent, ConnectorRx, Subscribe, Unsubscribe};
use crate::net::{RestClient, WsConnector};
use crate::venue::connection::{self, LaneCommand};
use crate::venue::router::{LaneId, Router};
use crate::venue::spec::{SnapshotFetchError, Venue};
use crate::venue::symbol::Symbol;
use crate::venue::{universe, ConnectorConfig};
use std::collections::{HashSet, VecDeque};
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
pub async fn run<V, R, W>(mut rx: ConnectorRx, cfg: ConnectorConfig<V::Config>, client: R, ws: W)
where
    V: Venue,
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

    let mut router: Router<Symbol, LaneCommand> = Router::new(capacity);
    let mut tasks: JoinSet<()> = JoinSet::new();

    let (universe_tx, mut universe_rx) = mpsc::channel(UNIVERSE_CHANNEL);
    let refresher = tokio::spawn(universe::refresh_loop::<V, R>(
        ctx.cfg.clone(),
        ctx.client.clone(),
        universe_tx,
    ));

    // The venue's listing, `None` until the first refresh lands.
    let mut universe: Option<HashSet<Symbol>> = None;
    // Subscribes that arrived before the first listing did, answered in arrival order the
    // moment one is in hand. Bounded only by how many a caller sends in that window.
    let mut waiting: VecDeque<Subscribe> = VecDeque::new();

    loop {
        tokio::select! {
            // `Some(..)`, so a refresh task that has gone away simply disables this arm
            // rather than completing instantly forever.
            Some(listed) = universe_rx.recv() => {
                let first = universe.is_none();
                retire_unlisted(&listed, &mut router).await;
                universe = Some(listed);
                if first {
                    while let Some(sub) = waiting.pop_front() {
                        handle_subscribe(sub, universe.as_ref(), &mut router, &mut tasks, &ctx).await;
                    }
                }
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
                    // Held rather than routed while the listing is still on its way: see the
                    // module doc. Dropping these on shutdown answers each caller's reply
                    // channel with `Err`, which is what a dropped `oneshot::Sender` means.
                    ConnectorEvent::Subscribe(sub) if universe.is_none() => waiting.push_back(sub),
                    ConnectorEvent::Subscribe(sub) => {
                        handle_subscribe(sub, universe.as_ref(), &mut router, &mut tasks, &ctx).await;
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

/// Tears down every subscribed symbol the venue no longer lists as tradable.
///
/// Dropping the slot on the connection drops its `BookPublisher`, which is what tells the
/// reader the stream has ended - the same signal an explicit unsubscribe gives, since from a
/// reader's point of view it is the same thing.
async fn retire_unlisted(listed: &HashSet<Symbol>, router: &mut Router<Symbol, LaneCommand>) {
    let gone: Vec<Symbol> = router
        .symbols()
        .filter(|symbol| !listed.contains(*symbol))
        .cloned()
        .collect();

    for symbol in gone {
        let Some(lane_id) = router.take(&symbol) else {
            continue;
        };
        tracing::warn!(%symbol, "no longer listed as tradable, dropping subscription");
        if let Some(tx) = router.tx(lane_id) {
            let _ = tx.send(LaneCommand::Unsubscribe { symbol }).await;
        }
    }
}

/// What every connection task is spawned with. Grouped so a spawn site takes one argument
/// instead of three.
///
/// `R`/`W` are unbounded here - `client` and `ws` are plain fields, never projected - unlike
/// `V`, whose `cfg: ConnectorConfig<V::Config>` reaches through a projection and so needs
/// `V: Venue` stated right on this declaration; every function below that actually calls into
/// `R`/`W`'s traits states its own bound instead.
struct ConnCtx<V: Venue, R, W> {
    cfg: ConnectorConfig<V::Config>,
    client: R,
    ws: W,
    permits: Arc<Semaphore>,
}

async fn handle_subscribe<V, R, W>(
    sub: Subscribe,
    universe: Option<&HashSet<Symbol>>,
    router: &mut Router<Symbol, LaneCommand>,
    tasks: &mut JoinSet<()>,
    ctx: &ConnCtx<V, R, W>,
) where
    V: Venue,
    R: RestClient,
    W: WsConnector,
    V::ReplayError: From<SnapshotFetchError<R::Builder>>,
{
    let (requested, reply) = sub.into_parts();

    let symbol = match Symbol::new(requested) {
        Ok(symbol) => symbol,
        Err(err) => {
            tracing::error!(symbol = err.as_str(), %err, "rejecting subscription");
            let _ = reply.send(Err(err.into()));
            return;
        }
    };

    // Fail closed on both counts: no listing at all, or a listing that does not name this
    // symbol as tradable. The caller of a held request cannot reach here with `None` (see
    // `run`), so this is the backstop rather than the usual path.
    if !universe.is_some_and(|listed| listed.contains(&symbol)) {
        tracing::error!(%symbol, "not listed as tradable on this venue, rejecting subscription");
        let _ = reply.send(Err(anyhow::anyhow!(
            "{symbol} is not listed as tradable on this venue"
        )));
        return;
    }

    if router.contains(&symbol) {
        tracing::error!(%symbol, "already subscribed on this connector");
        let _ = reply.send(Err(anyhow::anyhow!("{symbol} is already subscribed")));
        return;
    }

    let lane_id = router.lane_with_room().unwrap_or_else(|| {
        let id = spawn_lane::<V, R, W>(router, tasks, ctx);
        tracing::info!(connections = router.lane_count(), "opening connection");
        id
    });

    send_subscribe::<V, R, W>(symbol, reply, lane_id, router, tasks, ctx).await;
}

/// Sends one `Subscribe` command to `lane_id`. On a dead lane - one whose task exited between
/// the liveness check and the send - the command comes back through the `SendError`, so the
/// lane is purged and the command is retried once on a freshly opened one; a second failure
/// gives up and answers the request's reply channel with `Err`.
async fn send_subscribe<V, R, W>(
    symbol: Symbol,
    reply: oneshot::Sender<anyhow::Result<crate::connector::book_publisher::BookReader>>,
    mut lane_id: LaneId,
    router: &mut Router<Symbol, LaneCommand>,
    tasks: &mut JoinSet<()>,
    ctx: &ConnCtx<V, R, W>,
) where
    V: Venue,
    R: RestClient,
    W: WsConnector,
    V::ReplayError: From<SnapshotFetchError<R::Builder>>,
{
    let mut cmd = LaneCommand::Subscribe {
        symbol: symbol.clone(),
        reply,
    };

    for attempt in 0..2 {
        // `None` is a lane the router has already dropped since `lane_id` was chosen - the
        // same situation as a failed send, and handled the same way.
        if let Some(tx) = router.tx(lane_id) {
            match tx.send(cmd).await {
                Ok(()) => {
                    router.bind(symbol, lane_id);
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

    let LaneCommand::Subscribe { symbol, reply } = cmd else {
        unreachable!("only Subscribe is ever routed here")
    };
    tracing::error!(%symbol, "connection task gone, dropping subscription");
    let _ = reply.send(Err(anyhow::anyhow!(
        "connection task carrying {symbol} is gone"
    )));
}

/// Opens a new connection task, registers it in `tasks`, and returns the lane the router
/// created for it.
fn spawn_lane<V, R, W>(
    router: &mut Router<Symbol, LaneCommand>,
    tasks: &mut JoinSet<()>,
    ctx: &ConnCtx<V, R, W>,
) -> LaneId
where
    V: Venue,
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

async fn handle_unsubscribe(unsub: Unsubscribe, router: &mut Router<Symbol, LaneCommand>) {
    let requested = unsub.into_symbol();
    let symbol = match Symbol::new(requested) {
        Ok(symbol) => symbol,
        Err(err) => {
            tracing::debug!(symbol = err.as_str(), %err, "ignoring unsubscribe");
            return;
        }
    };

    let Some(lane_id) = router.take(&symbol) else {
        tracing::debug!(%symbol, "unsubscribe for a symbol that is not subscribed");
        return;
    };

    match router.tx(lane_id) {
        // The lane is already gone; nothing left to tell it. The next `join_next` reap will
        // notice and purge whatever else it was carrying.
        None => tracing::debug!(%symbol, "lane already gone, nothing to unsubscribe"),
        Some(tx) => {
            let _ = tx.send(LaneCommand::Unsubscribe { symbol }).await;
        }
    }

    router.reap_idle();
}

/// Closes every connection and waits for it, within [`SHUTDOWN_GRACE`] overall.
///
/// Dropping every lane's sender is the signal: each connection sees its queue close, sends a
/// close frame and returns.
async fn stop_connections(router: Router<Symbol, LaneCommand>, mut tasks: JoinSet<()>) {
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
    use crate::connector::book_publisher::BookReader;
    use crate::connector::events::{ConnectorEvent, Subscribe, create_event_channel};
    use crate::venue::connection::LaneCommand;
    use crate::venue::router::Router;
    use crate::venue::symbol::Symbol;
    use crate::venue::test_util::{Incoming, ScriptedWs, StubRest, TestConfig, TestVenue};
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::{Semaphore, mpsc};
    use crate::venue::{ConnectorConfig, CoreConfig};

    fn symbol(name: &str) -> Symbol {
        Symbol::new(name.into()).unwrap()
    }

    fn listing(names: &[&str]) -> HashSet<Symbol> {
        names.iter().map(|name| symbol(name)).collect()
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

    async fn rejection(universe: Option<&HashSet<Symbol>>, name: &str) -> String {
        let (sub, reply) = Subscribe::new(name.into());
        let mut router: Router<Symbol, LaneCommand> = Router::new(10);
        let mut tasks = tokio::task::JoinSet::new();

        handle_subscribe(sub, universe, &mut router, &mut tasks, &ctx()).await;

        assert_eq!(router.lane_count(), 0, "a rejected subscribe must not open a connection");
        reply
            .await
            .expect("every path answers the reply channel")
            .expect_err("expected a rejection")
            .to_string()
    }

    #[tokio::test]
    async fn a_symbol_the_venue_does_not_list_is_rejected_before_a_lane_is_chosen() {
        let listed = listing(&["btcusd"]);
        let why = rejection(Some(&listed), "ethusd").await;
        assert!(why.contains("not listed as tradable"), "{why}");
    }

    /// Fail closed: with no listing in hand there is nothing to check a symbol against, so
    /// `handle_subscribe` refuses rather than routing on trust. `run` never actually calls it
    /// in that state - it holds the request until a listing lands - so this is the backstop.
    #[tokio::test]
    async fn a_subscribe_with_no_listing_at_all_is_refused() {
        let why = rejection(None, "btcusd").await;
        assert!(why.contains("not listed as tradable"), "{why}");
    }

    #[tokio::test]
    async fn an_invalid_symbol_is_still_rejected_on_its_own_terms() {
        let listed = listing(&["btcusd"]);
        let why = rejection(Some(&listed), "btc-usd").await;
        assert!(why.contains("invalid symbol"), "{why}");
    }

    #[tokio::test]
    async fn a_symbol_dropped_from_a_refresh_is_torn_down() {
        let (tx, mut lane_rx) = mpsc::channel(4);
        let mut router: Router<Symbol, LaneCommand> = Router::new(10);
        let lane = router.insert_lane(tx);
        router.bind(symbol("btcusd"), lane);
        router.bind(symbol("lunausd"), lane);

        retire_unlisted(&listing(&["btcusd"]), &mut router).await;

        assert!(router.contains(&symbol("btcusd")), "a still-listed symbol stays");
        assert!(!router.contains(&symbol("lunausd")));

        let sent = lane_rx.recv().await.expect("the lane must be told to drop it");
        let LaneCommand::Unsubscribe { symbol: dropped } = sent else {
            panic!("expected an unsubscribe, got {sent:?}");
        };
        assert_eq!(dropped, symbol("lunausd"));
    }

    /// The listing is fetched asynchronously, so a subscribe sent the instant the connector
    /// starts routinely beats it. Refusing those would answer "not tradable" for symbols that
    /// in fact are, so they wait instead.
    #[tokio::test(start_paused = true)]
    async fn a_subscribe_that_beats_the_first_listing_is_served_once_it_lands() {
        let client = StubRest::always("100").with_route("listing", "btcusd,ethusd");
        let ws = ScriptedWs::with_fallback(Vec::new(), vec![Incoming::Parks]);
        let (rx, tx) = create_event_channel();

        let supervisor = tokio::spawn(run::<TestVenue, StubRest, ScriptedWs>(
            rx,
            ConnectorConfig::new(CoreConfig::default(), TestConfig),
            client,
            ws,
        ));

        let (event, reply) = Subscribe::new("btcusd".into());
        tx.send(ConnectorEvent::Subscribe(event));

        let reader: BookReader = tokio::time::timeout(Duration::from_secs(30), reply)
            .await
            .expect("the held request must be answered once the listing lands")
            .unwrap()
            .expect("btcusd is listed");
        drop(reader);

        let (ack, acked) = oneshot::channel();
        tx.send(ConnectorEvent::ShutDown(ack));
        let _ = tokio::time::timeout(Duration::from_secs(120), acked).await;
        let _ = tokio::time::timeout(Duration::from_secs(120), supervisor).await;
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
        ));

        let (event, reply) = Subscribe::new("dogeusd".into());
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
