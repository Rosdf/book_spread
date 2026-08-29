//! One WebSocket connection serving many symbols of one [`VenueSpec`].
//!
//! Generic over `V: VenueSpec` (wire shapes and sequencing), `R: RestClient` (the bootstrap
//! snapshot fetch) and `W: WsConnector` (the socket) - the transport generics [`VenueSpec`] itself
//! deliberately stays free of. Symbols are added to and dropped from a live socket with
//! control frames paced by `V::Pacer`; each carries its own book and its own bootstrap state,
//! stamped with a `generation` (see [`Slot::generation`]) so a fetch spawned for one bootstrap
//! attempt can never land on a slot a later attempt built.
//!
//! A session cannot take the lane down with it. The whole session future is run under
//! `catch_unwind`, so a panic anywhere in decoding, pacing or bookkeeping becomes a reconnect
//! rather than the end of the task. `tokio::spawn` already contained such a panic at the task
//! level - it never reached the process - but the task itself died, and with it every symbol
//! on that socket: a [`BookReader`] is bound to one [`BookPublisher`], so every reader on the
//! lane was dead permanently rather than merely resynced. What this adds is that the lane, and
//! therefore every reader on it, survives.
//!
//! [`BookPublisher`]: crate::connector::book_publisher::BookPublisher
//!
//! A bootstrap that fails has two possible recoveries and picks between them by asking the
//! venue's own error which it needs - see [`Retry`]. A snapshot that did not reach the diffs
//! already buffered is retried *against those same diffs*, leaving `first_cursor` where it is;
//! only a failure that discredits the buffer restarts the attempt. Collapsing the two - which
//! is what restarting unconditionally did - re-arms `first_cursor` from the next diff to
//! arrive, so the bar each snapshot has to clear rises every attempt and a venue whose snapshot
//! advances more slowly than its diff stream never bootstraps that symbol at all.
//!
//! Two independent watchdogs run alongside the frame-handling loop: a per-symbol idle resync,
//! config-driven, that resets one slot without touching the socket or its neighbours; and a
//! connection-level stall timeout, hardcoded because it is a property of the transport rather
//! than a venue tuning knob, that ends the session outright when nothing at all - not even a
//! ping - has arrived for [`SOCKET_STALL_TIMEOUT`]. The stall path needs no new recovery: it
//! only feeds `run`'s existing reconnect loop, which already resets every slot under a fresh
//! generation and resubscribes.

use crate::connector::book_publisher::{BookReader, make_book_publisher_pair};
use crate::incremental_book::IncrementalBook;
use crate::instrument::{Instrument, InstrumentId};
use crate::net::{RestClient, WsConnector};
use crate::panic::panic_message;
use crate::shared_string::SharedString;
use crate::venue::backoff::Backoff;
use crate::venue::pending::PendingDiffs as _;
use crate::venue::session::{SessionEnd, SessionError, SessionErrorImpl, close, ws_err};
use crate::venue::spec::{
    BootstrapRetry as _, ControlPacer as _, Decoder, FrameAction, FrameCtx, Generations, Method,
    Retry, SnapshotFetchError, SnapshotResult, VenueSpec,
};
use crate::venue::table::{Slot, SlotState, SlotTable};
use crate::venue::{ConnectorConfig, rest};
use bytes::Bytes;
use futures_util::{FutureExt as _, StreamExt as _};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Semaphore, mpsc};
use tokio_tungstenite::tungstenite::Message;

/// A session lasting at least this long is treated as healthy, so the next failure starts
/// backing off from scratch rather than from wherever the last streak left off.
const HEALTHY_SESSION: Duration = Duration::from_secs(60);

/// No frame of any kind - data, control, ping or pong - for this long means the socket is dead
/// even though the OS has not noticed. Hardcoded rather than configurable: it is a property of
/// the transport, not a venue tuning knob. Above any venue's own server ping and steady-state
/// traffic, so a healthy socket can never trip it; the per-symbol idle sweep handles ordinary
/// quiet symbols long before this fires.
const SOCKET_STALL_TIMEOUT: Duration = Duration::from_secs(180);
const STALL_CHECK_INTERVAL: Duration = Duration::from_secs(15);

/// How long a bootstrap waits before asking for another snapshot, when the one it got did not
/// reach the diffs it had already buffered.
///
/// A venue that serves its snapshot from a short-lived cache - Bitstamp's advances about once a
/// second - needs the bar re-tested rather than hammered: re-requesting at the round-trip rate
/// would just collect the identical cached body several times over, at ~155 KB each.
const SNAPSHOT_REFETCH_DELAY: Duration = Duration::from_millis(250);

/// How many extra snapshots one bootstrap attempt may ask for before giving up and resyncing.
///
/// With [`SNAPSHOT_REFETCH_DELAY`] this spans two seconds, comfortably past any cache tick a
/// venue is likely to have. Bounded at all so a venue whose snapshot genuinely never reaches
/// the buffered diffs falls back to the clean restart rather than fetching forever.
const SNAPSHOT_REFETCH_LIMIT: u32 = 8;

/// One instruction to a connection.
pub enum LaneCommand {
    Subscribe {
        instrument_id: InstrumentId,
        reply: oneshot::Sender<anyhow::Result<BookReader>>,
    },
    Unsubscribe {
        instrument_id: InstrumentId,
    },
}

impl std::fmt::Debug for LaneCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Subscribe { instrument_id, .. } => f
                .debug_struct("Subscribe")
                .field("instrument_id", instrument_id)
                .finish_non_exhaustive(),
            Self::Unsubscribe {
                instrument_id: instrument,
            } => f
                .debug_struct("Unsubscribe")
                .field("instrument", instrument)
                .finish(),
        }
    }
}

/// Runs one connection until its subscription queue closes, closing its socket on the way out.
pub async fn run<V, R, W>(
    mut subs_rx: mpsc::Receiver<LaneCommand>,
    cfg: ConnectorConfig<V::Config>,
    client: R,
    ws: W,
    permits: Arc<Semaphore>,
) where
    V: VenueSpec,
    R: RestClient,
    W: WsConnector,
    V::ReplayError: From<SnapshotFetchError<R::Builder>>,
{
    let (snap_tx, mut snap_rx) = mpsc::channel(64);
    let max_backoff = cfg.core().max_backoff();
    let mut conn: Connection<V, R, W> = Connection {
        table: SlotTable::default(),
        handler: Handler {
            cfg,
            client,
            ws,
            permits,
            dec: Decoder::default(),
            snap_tx,
            generations: Generations::default(),
            pacer: V::Pacer::default(),
            last_message: Instant::now(),
        },
    };
    let mut backoff = Backoff::new(max_backoff);

    loop {
        // Whether this is the first connect or a reconnect, no book on this socket is
        // trustworthy until it has been rebuilt from a fresh snapshot. Each slot gets a fresh
        // generation so a fetch left over from before the reconnect cannot land on it.
        for slot in conn.table.iter_mut() {
            slot.reset(conn.handler.generations.take());
        }

        let started = Instant::now();
        // `AssertUnwindSafe` because the future holds `&mut conn`, which is not
        // `UnwindSafe` - and that is exactly the state the recovery below rebuilds: the
        // loop's head resets every slot under a fresh generation, and the decoder is
        // replaced outright.
        let outcome = AssertUnwindSafe(conn.session(&mut subs_rx, &mut snap_rx))
            .catch_unwind()
            .await;

        match outcome {
            Ok(Ok(SessionEnd::ShutDown)) => return,
            Ok(Ok(SessionEnd::Reconnect)) => {
                tracing::info!(
                    symbols = conn.table.symbol_count(),
                    "session ended, reconnecting"
                );
            }
            Ok(Err(err)) => {
                tracing::warn!(symbols = conn.table.symbol_count(), %err, "session failed");
            }
            Err(payload) => {
                tracing::error!(
                    symbols = conn.table.symbol_count(),
                    panic = panic_message(payload.as_ref()),
                    "session panicked, reconnecting"
                );
                // A `simd_json::Buffers` unwound out of mid-parse is not worth trusting, and
                // neither is whatever venue staging `V::Stage` was holding. Everything else
                // the connection owns is either rebuilt at the head of this loop (the slots)
                // or per-session (the socket, the pacer).
                conn.handler.dec = Decoder::default();
            }
        }

        // The supervisor holds the only sender, so a closed queue means it is winding down.
        // Reconnecting here would only build a socket to tear down.
        if subs_rx.is_closed() {
            return;
        }
        if started.elapsed() >= HEALTHY_SESSION {
            backoff.reset();
        }
        if !conn.wait_backoff(&mut subs_rx, backoff.next_delay()).await {
            return;
        }
    }
}

/// One socket's symbols, split from the state that acts on them.
///
/// The split exists so a frame handler can hold `&mut Slot<V::Ready>` borrowed out of `table` while
/// still needing `&mut` access to `handler` - the connection, the config, the decode scratch,
/// the snapshot channel. A flat struct could not express that: any method call on `self` while
/// a field of `self` was borrowed would conflict. Two fields, borrowed independently, can
/// coexist.
struct Connection<V: VenueSpec, R: RestClient, W> {
    table: SlotTable<V::Ready, V::Pending>,
    handler: Handler<V, R, W>,
}

/// Everything a frame handler needs while it may be holding a `&mut Slot<V::Ready>` borrowed
/// out of [`Connection::table`] - see that struct's doc for why this is split out at all.
///
/// Unlike [`crate::venue::table`]/[`crate::venue::spec`]'s types, `V` and `R` cannot be
/// unbounded here: this struct needs several of `V`'s associated types at once (`Config`,
/// `Stage`, `Pacer`) plus `R`'s, which is exactly the case - see those modules' docs - where
/// naming the whole trait as a bound is the right call rather than fighting it.
struct Handler<V: VenueSpec, R: RestClient, W> {
    cfg: ConnectorConfig<V::Config>,
    client: R,
    ws: W,
    permits: Arc<Semaphore>,
    /// The reusable decode scratch: the shared `Scratch`/`simd_json::Buffers` plus whatever
    /// venue-specific staging `V::Stage` needs. Reused for the life of the connection.
    dec: Decoder<V::Stage>,
    snap_tx: mpsc::Sender<SnapshotResult<R::Builder>>,
    generations: Generations,
    pacer: V::Pacer,
    /// When the most recent message of any kind - data, control, ping or pong - arrived, for
    /// [`SOCKET_STALL_TIMEOUT`].
    last_message: Instant,
}

impl<V, R, W> Connection<V, R, W>
where
    V: VenueSpec,
    R: RestClient,
    W: WsConnector,
    V::ReplayError: From<SnapshotFetchError<R::Builder>>,
{
    async fn session(
        &mut self,
        subs_rx: &mut mpsc::Receiver<LaneCommand>,
        snap_rx: &mut mpsc::Receiver<SnapshotResult<R::Builder>>,
    ) -> Result<SessionEnd, SessionError<W>> {
        // The pacer's queue belongs to one socket: whatever the dead session had not managed
        // to drain names streams the new socket knows nothing about, and the resubscribe
        // below re-enqueues every wire name anyway. Leaving them would re-send stale entries
        // behind the fresh ones - Bitstamp's queueing pacer drains one frame per tick, so a
        // socket dying mid-drain is the ordinary case, not a rare one.
        self.handler.pacer = V::Pacer::default();

        let url = V::stream_url(self.handler.cfg.inner());
        let mut stream = self.handler.ws.connect(&url).await.map_err(ws_err::<W>)?;
        tracing::info!(url, symbols = self.table.symbol_count(), "connected");
        self.handler.last_message = Instant::now();

        // No split: `stream.next()` below is one `select!` branch's future, and other
        // branches' bodies borrow `&mut stream` again for a send - `select!` drops every
        // losing branch's future before running the winner's body, so that borrow has already
        // ended by the time a body runs. One owned stream, no lock.

        // Re-establish every symbol this connection was already carrying, through the same
        // paced queue a fresh subscribe goes through.
        let existing = self.table.wire_names().cloned();
        for name in existing {
            self.handler.pacer.enqueue(Method::Subscribe, name);
        }
        self.handler.pacer.on_admitted::<W>(&mut stream).await?;

        let idle_enabled = self.handler.cfg.core().idle_symbol_timeout().is_some();
        let mut idle_scan = tokio::time::interval(self.handler.cfg.core().idle_scan_interval());
        let mut stall_check = tokio::time::interval(STALL_CHECK_INTERVAL);

        loop {
            tokio::select! {
                biased;

                // Ordered so a frame flood cannot starve control frames or the timers below:
                // `stream.next()` is checked last, so on any loop iteration where a frame is
                // ready every earlier arm still gets its turn first.
                //
                // This is a real starvation guard, not an oversight, and moving the frame arm
                // first is not the free win it looks like. Tokio's cooperative budget is spent
                // per TCP read, not per WebSocket frame, and tungstenite emits many frames
                // from one read - so nothing else here would get a turn between them. The
                // price of the current order is five futile polls per frame, on the order of
                // tens of nanoseconds; measure that against a live feed before trading the
                // guard away for it.

                admitted = subs_rx.recv() => {
                    let Some(first) = admitted else { break };
                    self.apply(first);
                    while let Ok(next) = subs_rx.try_recv() {
                        self.apply(next);
                    }
                    self.handler.pacer.on_admitted::<W>(&mut stream).await?;
                }

                () = pacer_wait(self.handler.pacer.next_deadline()) => {
                    self.handler.pacer.on_deadline::<W>(&mut stream).await?;
                }

                _ = idle_scan.tick(), if idle_enabled => {
                    let timeout = self.handler.cfg.core()
                        .idle_symbol_timeout()
                        .expect("guarded by idle_enabled");
                    self.sweep_idle(timeout);
                }

                _ = stall_check.tick() => {
                    if self.handler.last_message.elapsed() >= SOCKET_STALL_TIMEOUT {
                        tracing::warn!(
                            ?SOCKET_STALL_TIMEOUT,
                            "no frame of any kind in too long, treating the socket as dead"
                        );
                        return Ok(SessionEnd::Reconnect);
                    }
                }

                fetched = snap_rx.recv() => {
                    if let Some(snap) = fetched {
                        self.on_snapshot(snap);
                    }
                }

                polled = stream.next() => {
                    let Some(incoming) = polled else { return Err(SessionErrorImpl::Closed) };
                    self.handler.last_message = Instant::now();
                    match incoming.map_err(ws_err::<W>)? {
                        // A venue-requested reconnect leaves a perfectly healthy socket
                        // behind, so it gets the same close handshake the shutdown path does
                        // rather than being dropped on the floor. The stall timeout above
                        // deliberately does not: that socket is already dead, and waiting out
                        // `CLOSE_TIMEOUT` for an answer would just burn five seconds.
                        Message::Text(text) => {
                            if let Some(end) = self.on_frame(text.into()) {
                                close::<W>(&mut stream).await;
                                return Ok(end);
                            }
                        }
                        Message::Binary(bin) => {
                            if let Some(end) = self.on_frame(bin) {
                                close::<W>(&mut stream).await;
                                return Ok(end);
                            }
                        }
                        Message::Close(_) => {
                            close::<W>(&mut stream).await;
                            return Ok(SessionEnd::Reconnect);
                        }
                        // tungstenite queues the pong itself; we only have to keep polling.
                        // Still counts as proof of life for the stall watchdog.
                        Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
                    }
                }
            }
        }

        // Only `break` above reaches here: the supervisor dropped the lane.
        close::<W>(&mut stream).await;
        Ok(SessionEnd::ShutDown)
    }

    /// Sleeps out a reconnect's backoff while still admitting commands, returning `false` when
    /// the supervisor closed the queue and this connection should stop.
    ///
    /// A connection asleep in backoff is not draining its 32-slot queue, so the supervisor
    /// would block on `send().await` for as long as the backoff lasts - including for the
    /// `ShutDown` that is trying to end it. Commands admitted here only touch the slot table:
    /// there is no socket to send a control frame on, and the next `session` re-subscribes
    /// every wire name in the table anyway.
    async fn wait_backoff(
        &mut self,
        subs_rx: &mut mpsc::Receiver<LaneCommand>,
        wait: Duration,
    ) -> bool {
        let sleep = tokio::time::sleep(wait);
        tokio::pin!(sleep);

        loop {
            tokio::select! {
                () = &mut sleep => return true,

                received = subs_rx.recv() => {
                    let Some(cmd) = received else { return false };
                    self.apply(cmd);
                }
            }
        }
    }

    /// Applies one queued command: inserts or removes a slot and enqueues its control frame.
    /// Pacing and actually sending is the pacer's job.
    fn apply(&mut self, cmd: LaneCommand) {
        match cmd {
            LaneCommand::Subscribe {
                instrument_id,
                reply,
            } => {
                if let Some(wire_name) = self.insert_slot(Instrument::by_id(instrument_id), reply) {
                    self.handler.pacer.enqueue(Method::Subscribe, wire_name);
                }
            }
            LaneCommand::Unsubscribe { instrument_id } => {
                if let Some(wire_name) = self.remove_slot(Instrument::by_id(instrument_id)) {
                    self.handler.pacer.enqueue(Method::Unsubscribe, wire_name);
                }
            }
        }
    }

    /// Gives an instrument a slot on this connection and queues its subscribe frame.
    ///
    /// The snapshot is deliberately *not* requested here: a venue's bootstrap procedure needs
    /// the first buffered frame's cursor to validate the snapshot against, so the fetch is
    /// kicked off when that first frame arrives - see [`Handler::on_buffered`].
    ///
    /// This is also where the subscriber learns its fate: every path answers the request's
    /// reply channel, with the reader on success and the reason on rejection, so a caller
    /// never waits on a book that is not coming.
    ///
    /// Returns the wire name to subscribe, or `None` when the request was rejected. Queueing
    /// the control frame is the caller's call rather than this function's, because a command
    /// admitted while the connection is backing off has no socket to send one on - see
    /// [`Self::wait_backoff`].
    fn insert_slot(
        &mut self,
        instrument: Instrument,
        reply: oneshot::Sender<anyhow::Result<BookReader>>,
    ) -> Option<SharedString> {
        let wire_name = V::wire_name(self.handler.cfg.inner(), instrument);

        let (mut publisher, reader) = make_book_publisher_pair();
        // Seeds the slot readers see before the first snapshot lands, so the reader they are
        // about to receive reports "no book yet" rather than parking.
        publisher.publish_empty();

        let slot = Slot {
            instrument,
            wire_name: wire_name.clone(),
            book: IncrementalBook::new(),
            publisher,
            state: SlotState::bootstrapping(V::Pending::default()),
            generation: self.handler.generations.take(),
            last_frame: Instant::now(),
        };

        if self.table.insert(slot).is_err() {
            // The slot built above is dropped here, and its publisher with it.
            tracing::error!(%wire_name, "already subscribed on this connection");
            let _ = reply.send(Err(anyhow::anyhow!(
                "{wire_name} is already subscribed on this connection"
            )));
            return None;
        }

        let _ = reply.send(Ok(reader));
        tracing::info!(%wire_name, "subscribing");
        Some(wire_name)
    }

    /// Tears down `instrument`'s slot, if this connection carries it, returning the wire name
    /// to unsubscribe. Dropping the slot drops its `BookPublisher`, which is what tells the
    /// reader the stream is gone. As [`Self::insert_slot`], the control frame is the caller's
    /// to queue.
    fn remove_slot(&mut self, instrument: Instrument) -> Option<SharedString> {
        // The wire name is what the table is keyed by; a connection has to derive it the same
        // way it did when subscribing.
        let wire_name = V::wire_name(self.handler.cfg.inner(), instrument);
        let mut slot = self.table.remove(&wire_name)?;
        slot.abort_fetch();
        tracing::info!(wire_name = %slot.wire_name, "unsubscribing");
        Some(slot.wire_name)
    }

    /// Decodes one frame via [`VenueSpec::on_frame`] and acts on the result. Returns `Some` only
    /// when the session must end; every other outcome is handled in place.
    fn on_frame(&mut self, bytes: Bytes) -> Option<SessionEnd> {
        let ctx = FrameCtx {
            table: &mut self.table,
            dec: &mut self.handler.dec,
            generations: &mut self.handler.generations,
        };
        match V::on_frame(ctx, bytes) {
            FrameAction::Handled => None,
            FrameAction::Buffer { slot, cursor } => {
                self.handler.on_buffered(slot, cursor);
                None
            }
            FrameAction::Reconnect => Some(SessionEnd::Reconnect),
            FrameAction::Ignored { name } => {
                // Routine, not a warning: frames for a symbol unsubscribed a moment ago keep
                // arriving until the venue acts on the control frame.
                tracing::debug!(%name, "frame for a stream this connection does not carry");
                None
            }
            FrameAction::ControlRejected { id, code } => {
                // The pacer is the only thing that knows which symbols a request id named -
                // see `ControlPacer::names_for` for why that lookup lives there.
                let names = self.handler.pacer.names_for(id);
                tracing::error!(?id, ?code, ?names, "control request rejected");
                None
            }
            FrameAction::Undecodable { slot: blamed, err } => {
                if let Some(culprit) = blamed {
                    tracing::warn!(instrument = %culprit.instrument, %err, "decode failed, resyncing symbol");
                    culprit.reset(self.handler.generations.take());
                } else {
                    // Unattributable: a malformed control response, or a failure before any
                    // book was touched. Nothing to resync, so drop it.
                    tracing::warn!(%err, "undecodable frame");
                }
                None
            }
        }
    }

    /// Resyncs any slot that has gone `idle_timeout` without a frame. Config-driven and
    /// per-symbol: the socket and every neighbour are left untouched.
    ///
    /// Slots still bootstrapping are skipped. Their snapshot fetch is frame-triggered - it
    /// starts when the first diff arrives - so a symbol whose venue simply has nothing to send
    /// never gets one, and resetting it every `idle_symbol_timeout` would republish an empty
    /// book and log a warning every round without ever making progress. Such a symbol has no
    /// book until its first diff, which is correct by construction.
    fn sweep_idle(&mut self, idle_timeout: Duration) {
        let generations = &mut self.handler.generations;
        for slot in self.table.iter_mut() {
            if matches!(slot.state, SlotState::Bootstrapping(_)) {
                continue;
            }
            if slot.last_frame.elapsed() > idle_timeout {
                tracing::warn!(instrument = %slot.instrument, "no frame within idle_symbol_timeout, resyncing");
                slot.reset(generations.take());
            }
        }
    }

    /// Routes a completed REST fetch back to the slot it was requested for.
    ///
    /// Looked up by the wire name the instrument maps to - matching how [`SlotTable`] is keyed,
    /// the same derivation [`Self::remove_slot`] uses. A miss means the slot was dropped or
    /// never existed - nothing to apply the snapshot into, so it is discarded. A hit whose
    /// generation does not match is a result from a superseded attempt and is discarded too,
    /// before `body` is even looked at, so a stale failed fetch cannot reset a slot that has
    /// since moved on.
    fn on_snapshot(&mut self, snap: SnapshotResult<R::Builder>) {
        let wire_name = V::wire_name(self.handler.cfg.inner(), snap.instrument);
        let Some(slot) = self.table.get_mut(&wire_name) else {
            return;
        };

        if snap.generation != slot.generation {
            tracing::debug!(instrument = %slot.instrument, "snapshot for a superseded attempt, discarded");
            return;
        }

        let result: Result<(), V::ReplayError> = match snap.body {
            Ok(body) => self.handler.apply_snapshot(slot, body),
            // A fetch that never produced a body says nothing about the diffs this slot has
            // buffered, so it recovers down the same two paths as a bad body.
            Err(fetch_err) => Err(fetch_err.into()),
        };

        if let Err(err) = result {
            self.handler.recover_bootstrap(slot, &err);
        }
    }
}

impl<V, R, W> Handler<V, R, W>
where
    V: VenueSpec,
    R: RestClient,
    W: WsConnector,
    V::ReplayError: From<SnapshotFetchError<R::Builder>>,
{
    /// A diff arrived for a symbol that has no book yet. `VenueSpec::on_frame` has already staged
    /// it into `slot`'s own arena; this starts the snapshot fetch if it was the first one, and
    /// gives up on a bootstrap that has buffered more than it is allowed to.
    fn on_buffered(&mut self, slot: &mut Slot<V::Ready, V::Pending>, cursor: u64) {
        slot.last_frame = Instant::now();
        let limit = self.cfg.core().max_pending_frames();

        let mut needs_fetch = false;
        let mut overflowed = 0;

        match &mut slot.state {
            SlotState::Bootstrapping(boot) => {
                // The diff that tipped the arena over the limit is already in it - one frame
                // of overshoot, which the reset below throws away along with the rest.
                if boot.pending.buffered() >= limit {
                    overflowed = boot.pending.buffered();
                } else if boot.first_cursor.is_none() {
                    boot.first_cursor = Some(cursor);
                    needs_fetch = true;
                }
            }
            SlotState::Ready(_) => return,
        }

        if overflowed > 0 {
            tracing::warn!(
                instrument = %slot.instrument,
                buffered = overflowed,
                "snapshot never arrived, restarting bootstrap"
            );
            slot.reset(self.generations.take());
            return;
        }

        if needs_fetch {
            self.arm_fetch(slot, Duration::ZERO);
        }
    }

    /// Spawns a snapshot fetch for `slot` and records its handle, so `abort_fetch` can cancel
    /// it. `delay` holds the request back - see [`SNAPSHOT_REFETCH_DELAY`].
    fn arm_fetch(&self, slot: &mut Slot<V::Ready, V::Pending>, delay: Duration) {
        let handle = self.spawn_snapshot(slot, delay);
        if let SlotState::Bootstrapping(boot) = &mut slot.state {
            boot.abort = Some(handle);
        }
    }

    /// Spawns the REST fetch for `slot`'s outstanding bootstrap, stamped with its current
    /// generation, and returns the handle so the caller can store it for `abort_fetch`.
    ///
    /// The delay is inside the spawned task rather than on a timer the session loop owns:
    /// nothing here has to wake for it, and cancelling the task cancels the wait with it.
    fn spawn_snapshot(
        &self,
        slot: &Slot<V::Ready, V::Pending>,
        delay: Duration,
    ) -> tokio::task::AbortHandle {
        let instrument = slot.instrument;
        let client = self.client.clone();
        let cfg = self.cfg.clone();
        let permits = Arc::clone(&self.permits);
        let tx = self.snap_tx.clone();
        let generation = slot.generation;

        let task = tokio::spawn(async move {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            let body =
                rest::fetch_snapshot::<V, R>(&client, cfg.inner(), instrument, &permits).await;
            let _ = tx
                .send(crate::venue::spec::SnapshotResultImpl::new(
                    instrument, body, generation,
                ))
                .await;
        });
        task.abort_handle()
    }

    /// Recovers a slot whose bootstrap just failed, along whichever of [`Retry`]'s two paths
    /// `err` names: another snapshot against the diffs already buffered, or a clean restart.
    ///
    /// The refetch keeps the slot's `generation`, because this is the same bootstrap attempt -
    /// the fetch that just failed has already landed, so there is nothing older left in flight
    /// for the stamp to tell apart. That is exactly what makes `first_cursor` survivable, and
    /// `first_cursor` surviving is the point: see [`Retry::Refetch`].
    fn recover_bootstrap(&mut self, slot: &mut Slot<V::Ready, V::Pending>, err: &V::ReplayError) {
        let refetch = matches!(err.retry(), Retry::Refetch)
            && match &mut slot.state {
                SlotState::Bootstrapping(boot) => {
                    // The fetch that produced this failure is finished either way.
                    boot.abort = None;
                    let allowed = boot.refetches < SNAPSHOT_REFETCH_LIMIT;
                    boot.refetches += u32::from(allowed);
                    allowed
                }
                SlotState::Ready(_) => false,
            };

        if refetch {
            tracing::debug!(
                instrument = %slot.instrument,
                %err,
                "snapshot did not reach the buffered diffs, fetching another"
            );
            self.arm_fetch(slot, SNAPSHOT_REFETCH_DELAY);
            return;
        }

        tracing::warn!(instrument = %slot.instrument, %err, "bootstrap failed, restarting");
        // Back to buffering from nothing, under a generation the failed attempt's fetch
        // cannot match. The next diff to arrive starts a fresh fetch.
        slot.reset(self.generations.take());
    }

    /// Seeds `slot`'s book from a fetched snapshot and replays its buffered diffs onto it, via
    /// [`VenueSpec::seed_and_replay`]. On failure the arena is put back, so
    /// [`Self::recover_bootstrap`] still has the option of retrying against it.
    fn apply_snapshot(
        &mut self,
        slot: &mut Slot<V::Ready, V::Pending>,
        body: Bytes,
    ) -> Result<(), V::ReplayError> {
        // Lift the arena out so it and the book it replays into can be borrowed at the same
        // time. A snapshot that arrives for an already-ready slot is a leftover from a
        // previous attempt.
        let (pending, first_buffered) = match &mut slot.state {
            SlotState::Bootstrapping(boot) => {
                boot.abort = None;
                (std::mem::take(&mut boot.pending), boot.first_cursor)
            }
            SlotState::Ready(_) => return Ok(()),
        };

        match V::seed_and_replay(slot, &pending, first_buffered, body, &mut self.dec) {
            Ok(ready) => {
                slot.state = SlotState::Ready(ready);
                slot.last_frame = Instant::now();
                // The book only just came into existence, so publish it unconditionally
                // rather than waiting for the next frame that happens to move the top.
                slot.publisher.publish(&slot.book);
                tracing::info!(instrument = %slot.instrument, "book bootstrapped");
                Ok(())
            }
            Err(err) => {
                // The arena goes back where it came from: `recover_bootstrap` may well keep
                // this bootstrap alive and fetch another snapshot against exactly these diffs,
                // and it cannot do that if seeding consumed them. The slot is still
                // `Bootstrapping` - `seed_and_replay` never changes that on failure - so the
                // book it half-seeded is unreachable to readers and is cleared by the next
                // attempt's own `book.clear()`.
                if let SlotState::Bootstrapping(boot) = &mut slot.state {
                    boot.pending = pending;
                }
                Err(err)
            }
        }
    }
}

/// Waits until `deadline`, or forever when there is nothing queued - so a pacer with an empty
/// queue never wakes the session loop.
async fn pacer_wait(deadline: Option<Instant>) {
    match deadline {
        Some(instant) => tokio::time::sleep_until(instant.into()).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod test {
    use super::{Connection, Handler, LaneCommand, run};
    use crate::connector::book_publisher::{BookReader, make_book_publisher_pair};
    use crate::incremental_book::IncrementalBook;
    use crate::venue::ConnectorConfig;
    use crate::venue::config::CoreConfig;
    use crate::venue::spec::{Decoder, Generations, Method, SnapshotResult};
    use crate::venue::table::{Slot, SlotState, SlotTable};
    use crate::venue::test_util::{
        Incoming, ScriptedWs, StubRequest, StubRest, TestConfig, TestPending, TestReady, TestVenue,
        test_instrument_for,
    };
    use all_venues::Venue;
    use bytes::Bytes;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tokio::sync::{Semaphore, mpsc};

    type TestConn = Connection<TestVenue, StubRest, ScriptedWs>;

    /// A connection with no session running, for testing the pieces `run` calls into.
    ///
    /// The snapshot receiver comes back with it: dropping it would close the channel a spawned
    /// fetch sends on, which is not what any of these tests mean to exercise.
    fn connection(cfg: CoreConfig) -> (TestConn, mpsc::Receiver<SnapshotResult<StubRequest>>) {
        let (snap_tx, snap_rx) = mpsc::channel(8);
        let mut generations = Generations::default();
        // `slot` below stamps every test slot 0, and `Generations` hands out 0 first - so burn
        // it here, and a slot whose generation is still 0 is provably one nothing has reset.
        let _zero = generations.take();

        let conn = Connection {
            table: SlotTable::default(),
            handler: Handler {
                cfg: ConnectorConfig::new(cfg, TestConfig),
                client: StubRest::always("100"),
                ws: ScriptedWs::new(Vec::new()),
                permits: Arc::new(Semaphore::new(1)),
                dec: Decoder::default(),
                snap_tx,
                generations,
                pacer: crate::venue::test_util::TestPacer::default(),
                last_message: Instant::now(),
            },
        };
        (conn, snap_rx)
    }

    fn slot(name: &str, state: SlotState<TestReady, TestPending>) -> Slot<TestReady, TestPending> {
        let instrument = test_instrument_for(Venue::BinanceSpot, name);
        let (publisher, reader) = make_book_publisher_pair();
        // Leaked rather than dropped: a live reader is what a real slot has, and dropping it
        // would change what `publish` does.
        Box::leak(Box::new(reader));
        Slot {
            wire_name: instrument.name().into(),
            instrument,
            book: IncrementalBook::new(),
            publisher,
            state,
            generation: 0,
            // Far enough in the past that any idle timeout has already elapsed. Falls back
            // to `now` on a monotonic clock younger than an hour, which only costs this test
            // its point rather than making it flaky - `sweep_idle` is driven by a 1ms timeout.
            last_frame: Instant::now()
                .checked_sub(Duration::from_secs(3600))
                .unwrap_or_else(Instant::now),
        }
    }

    fn subscribe(name: &str) -> (LaneCommand, oneshot::Receiver<anyhow::Result<BookReader>>) {
        let (reply, rx) = oneshot::channel();
        (
            LaneCommand::Subscribe {
                instrument_id: test_instrument_for(Venue::BinanceSpot, name).id(),
                reply,
            },
            rx,
        )
    }

    /// A symbol whose venue simply has nothing to send never gets a snapshot - the fetch is
    /// triggered by its first diff - so resetting it every `idle_symbol_timeout` republished
    /// an empty book and logged a warning every round, forever, without ever making progress.
    #[test]
    fn sweep_idle_leaves_a_bootstrapping_slot_alone() {
        let (mut conn, _snap_rx) = connection(CoreConfig::default());
        conn.table
            .insert(slot(
                "btcusd",
                SlotState::bootstrapping(TestPending::default()),
            ))
            .unwrap();

        conn.sweep_idle(Duration::from_millis(1));

        let quiet = conn.table.get_mut("btcusd").unwrap();
        assert_eq!(
            quiet.generation, 0,
            "a bootstrapping slot must not be reset"
        );
        assert!(matches!(quiet.state, SlotState::Bootstrapping(_)));
    }

    #[test]
    fn sweep_idle_resyncs_a_ready_slot_that_has_gone_quiet() {
        let (mut conn, _snap_rx) = connection(CoreConfig::default());
        conn.table
            .insert(slot("btcusd", SlotState::Ready(TestReady { cursor: 7 })))
            .unwrap();
        conn.sweep_idle(Duration::from_millis(1));

        let stale = conn.table.get_mut("btcusd").unwrap();
        assert_ne!(stale.generation, 0, "a reset must take a fresh generation");
        assert!(
            matches!(stale.state, SlotState::Bootstrapping(_)),
            "an idle ready slot goes back to bootstrapping"
        );
    }

    /// A connection asleep in backoff used not to drain its queue at all, so the supervisor
    /// blocked on `send().await` for the whole backoff - `ShutDown` included.
    #[tokio::test(start_paused = true)]
    async fn commands_are_admitted_while_backing_off() {
        let (mut conn, _snap_rx) = connection(CoreConfig::default());
        let (tx, mut rx) = mpsc::channel(4);

        let (cmd, reply) = subscribe("btcusd");
        tx.send(cmd).await.unwrap();
        // Closing the queue is the supervisor winding down: `wait_backoff` must notice rather
        // than sleep out the rest of the backoff.
        drop(tx);

        let alive = conn.wait_backoff(&mut rx, Duration::from_secs(3600)).await;

        assert!(!alive, "a closed queue must end the connection immediately");
        assert_eq!(
            conn.table.symbol_count(),
            1,
            "the command still took effect"
        );
        assert!(
            reply.await.unwrap().is_ok(),
            "the subscriber gets its reader"
        );
        assert!(
            conn.handler.pacer.queued().is_empty(),
            "there is no socket to send a control frame on; the next session resubscribes"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_backoff_that_elapses_keeps_the_connection_alive() {
        let (mut conn, _snap_rx) = connection(CoreConfig::default());
        let (_tx, mut rx) = mpsc::channel(4);

        assert!(conn.wait_backoff(&mut rx, Duration::from_millis(50)).await);
    }

    /// A panic anywhere in a session used to end the connection task, and a `BookReader` is
    /// bound to one `BookPublisher` - so every reader on that socket was dead permanently
    /// rather than merely resynced.
    #[tokio::test(start_paused = true)]
    async fn a_panicking_session_reconnects_instead_of_killing_the_lane() {
        let ws = ScriptedWs::new(vec![vec![Incoming::Panics]]);
        let (tx, rx) = mpsc::channel(4);

        let lane = tokio::spawn(run::<TestVenue, StubRest, ScriptedWs>(
            rx,
            ConnectorConfig::new(CoreConfig::default(), TestConfig),
            StubRest::always("100"),
            ws.clone(),
            Arc::new(Semaphore::new(1)),
        ));

        // The first session panics on its first poll of the read half. Every later connect
        // parks, so the lane is idle but alive.
        while ws.connects() < 2 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let (cmd, reply) = subscribe("btcusd");
        tx.send(cmd)
            .await
            .expect("the lane must still be listening");
        assert!(
            reply.await.unwrap().is_ok(),
            "a subscribe after the panic must still get a reader"
        );

        drop(tx);
        // Comfortably past `CLOSE_TIMEOUT`: the socket never answers the close frame, so the
        // lane waits that out before returning.
        tokio::time::timeout(Duration::from_secs(120), lane)
            .await
            .expect("closing the queue must stop the lane")
            .unwrap();
    }

    /// The pacer's queue belongs to one socket. A reconnect re-enqueues every wire name, so
    /// anything the dead session had not drained would go out twice, behind the fresh ones.
    #[tokio::test(start_paused = true)]
    async fn a_reconnect_starts_from_an_empty_pacer_queue() {
        // One session that ends the moment it is polled, then one that parks.
        let ws = ScriptedWs::new(vec![vec![Incoming::Ended]]);
        let (tx, rx) = mpsc::channel(4);

        let lane = tokio::spawn(run::<TestVenue, StubRest, ScriptedWs>(
            rx,
            ConnectorConfig::new(CoreConfig::default(), TestConfig),
            StubRest::always("100"),
            ws.clone(),
            Arc::new(Semaphore::new(1)),
        ));

        let (cmd, reply) = subscribe("btcusd");
        tx.send(cmd).await.unwrap();
        assert!(reply.await.unwrap().is_ok());

        while ws.connects() < 2 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        drop(tx);
        tokio::time::timeout(Duration::from_secs(120), lane)
            .await
            .unwrap()
            .unwrap();

        let subscribes = ws
            .sent_text()
            .into_iter()
            .filter(|text| text.starts_with("SUBSCRIBE btcusd"))
            .count();
        assert_eq!(
            subscribes, 2,
            "once per socket - a queue carried across the reconnect would send it a third time"
        );
    }

    /// Runs one slot's bootstrap to a standstill against a venue that answers `stale` requests
    /// with a snapshot too old to reach the buffered diffs, then a good one. Returns the
    /// connection and how many snapshots were actually fetched.
    ///
    /// Every snapshot is fed straight back, for as long as one is in flight: a round that
    /// times out means the bootstrap has stopped asking, because it either landed or gave up.
    /// Under `start_paused` that wait costs no real time.
    async fn bootstrap_with_stale_snapshots(stale: usize) -> (TestConn, usize) {
        // The first diff is at cursor 100, so a snapshot below that does not reach it. Every
        // stale answer repeats the same too-old cursor, the way a cached snapshot does.
        let mut bodies: Vec<&str> = vec!["50"; stale];
        bodies.push("100");
        let client = StubRest::always_failing().with_changing_route("snapshot", &bodies);

        let (mut conn, mut snap_rx) = connection(CoreConfig::default());
        conn.handler.client = client.clone();
        conn.table
            .insert(slot(
                "btcusd",
                SlotState::bootstrapping(TestPending::default()),
            ))
            .unwrap();

        // The first diff is what arms the fetch, exactly as a live frame does.
        conn.on_frame(Bytes::from_static(b"btcusd:100"));

        while let Ok(Some(snap)) =
            tokio::time::timeout(Duration::from_secs(30), snap_rx.recv()).await
        {
            conn.on_snapshot(snap);
        }

        let fetches = client.urls().len();
        (conn, fetches)
    }

    /// The livelock this exists to prevent: restarting the bootstrap re-arms `first_cursor`
    /// from the *next* diff to arrive, so the bar each snapshot has to clear rises every
    /// attempt. On a venue whose snapshot advances more slowly than its diff stream, it never
    /// catches up and the symbol never gets a book.
    #[tokio::test(start_paused = true)]
    async fn a_snapshot_that_is_too_old_is_refetched_against_the_same_buffered_diffs() {
        let (mut conn, fetches) = bootstrap_with_stale_snapshots(2).await;

        assert_eq!(fetches, 3, "two stale snapshots, then the one that reached");
        let ready = conn.table.get_mut("btcusd").unwrap();
        assert!(
            matches!(ready.state, SlotState::Ready(TestReady { cursor: 100 })),
            "the bootstrap must land on the diff buffered before the first fetch: {:?}",
            ready.state
        );
        assert_eq!(
            ready.generation, 0,
            "a refetch is the same bootstrap attempt, so the generation must not move"
        );
    }

    /// The buffered diffs are what the retry is *for*, so they have to outlive a failed
    /// seeding rather than being consumed by it.
    #[tokio::test(start_paused = true)]
    async fn a_failed_seeding_leaves_the_buffered_diffs_in_place() {
        let client = StubRest::always_failing().with_route("snapshot", "50");
        let (mut conn, mut snap_rx) = connection(CoreConfig::default());
        conn.handler.client = client;
        conn.table
            .insert(slot(
                "btcusd",
                SlotState::bootstrapping(TestPending::default()),
            ))
            .unwrap();

        conn.on_frame(Bytes::from_static(b"btcusd:100"));
        conn.on_frame(Bytes::from_static(b"btcusd:101"));

        let snap = snap_rx.recv().await.unwrap();
        conn.on_snapshot(snap);

        let slot = conn.table.get_mut("btcusd").unwrap();
        let SlotState::Bootstrapping(boot) = &slot.state else {
            panic!("a stale snapshot must not end the bootstrap");
        };
        assert_eq!(boot.pending.cursors(), [100, 101]);
        assert_eq!(
            boot.first_cursor,
            Some(100),
            "the bar the next snapshot has to clear must not move"
        );
        assert_eq!(boot.refetches, 1);
    }

    /// Bounded, so a venue whose snapshot genuinely never reaches the buffered diffs falls
    /// back to the clean restart rather than fetching forever.
    #[tokio::test(start_paused = true)]
    async fn refetching_gives_up_and_resyncs_at_the_limit() {
        let limit = usize::try_from(super::SNAPSHOT_REFETCH_LIMIT).unwrap();
        // One more stale answer than the cap allows refetches, so the last one is refused.
        let (mut conn, fetches) = bootstrap_with_stale_snapshots(limit + 1).await;

        assert_eq!(
            fetches,
            limit + 1,
            "the first fetch plus `limit` refetches, and then no more"
        );
        let slot = conn.table.get_mut("btcusd").unwrap();
        let SlotState::Bootstrapping(boot) = &slot.state else {
            panic!("giving up returns the slot to a clean bootstrap");
        };
        assert_eq!(
            boot.pending.cursors(),
            [0_u64; 0],
            "the restart drops what it buffered"
        );
        assert_eq!(boot.first_cursor, None);
        assert_eq!(boot.refetches, 0);
        assert_ne!(slot.generation, 0, "a restart takes a fresh generation");
    }

    /// The other direction: a failure that discredits the buffer must not be retried against
    /// it. Another snapshot cannot fill a hole between two diffs the socket dropped.
    #[tokio::test(start_paused = true)]
    async fn a_failure_that_discredits_the_buffer_restarts_immediately() {
        let client = StubRest::always_failing().with_route("snapshot", "not a cursor");
        let (mut conn, mut snap_rx) = connection(CoreConfig::default());
        conn.handler.client = client.clone();
        conn.table
            .insert(slot(
                "btcusd",
                SlotState::bootstrapping(TestPending::default()),
            ))
            .unwrap();

        conn.on_frame(Bytes::from_static(b"btcusd:100"));
        let snap = snap_rx.recv().await.unwrap();
        conn.on_snapshot(snap);

        assert_eq!(client.urls().len(), 1, "no refetch for a `Resync` failure");
        let slot = conn.table.get_mut("btcusd").unwrap();
        let SlotState::Bootstrapping(boot) = &slot.state else {
            panic!("expected a clean bootstrap");
        };
        assert_eq!(boot.pending.cursors(), [0_u64; 0]);
        assert_ne!(slot.generation, 0);
    }

    #[test]
    fn a_panic_payload_is_reported_as_its_message() {
        let caught = std::panic::catch_unwind(|| panic!("scripted panic while polling"))
            .expect_err("the panic must propagate");
        assert_eq!(
            super::panic_message(caught.as_ref()),
            "scripted panic while polling"
        );

        let formatted = std::panic::catch_unwind(|| panic!("code {}", 7))
            .expect_err("the panic must propagate");
        assert_eq!(super::panic_message(formatted.as_ref()), "code 7");
    }

    #[test]
    fn an_unsubscribe_for_a_symbol_this_connection_never_had_is_a_no_op() {
        let (mut conn, _snap_rx) = connection(CoreConfig::default());
        conn.apply(LaneCommand::Unsubscribe {
            instrument_id: test_instrument_for(Venue::BinanceSpot, "btcusd").id(),
        });
        assert_eq!(conn.handler.pacer.queued(), []);
    }

    #[test]
    fn apply_queues_the_control_frame_a_command_implies() {
        let (mut conn, _snap_rx) = connection(CoreConfig::default());

        let (cmd, _reply) = subscribe("btcusd");
        conn.apply(cmd);
        conn.apply(LaneCommand::Unsubscribe {
            instrument_id: test_instrument_for(Venue::BinanceSpot, "btcusd").id(),
        });

        let queued: Vec<_> = conn
            .handler
            .pacer
            .queued()
            .iter()
            .map(|(method, name)| (*method, name.to_string()))
            .collect();
        assert_eq!(
            queued,
            vec![
                (Method::Subscribe, "btcusd".to_owned()),
                (Method::Unsubscribe, "btcusd".to_owned()),
            ]
        );
    }
}
