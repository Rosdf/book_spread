//! The fan-out itself: one task per `(venue, symbol)` that reads, encodes once, and offers the
//! resulting bytes to every attached client.
//!
//! The broadcaster owns those clients outright - their whole HTTP/2 connections, not just a
//! handle onto them. There is no per-client task and no channel between the encoder and the
//! wire: a book is encoded once and then offered to each session in turn, so the same `Bytes`
//! reaches every client with no copy of its own. See [`super::session`] for the epoch
//! bookkeeping and [`crate::client`] for what backpressure does to it.

use super::session::{Session, SessionCtx};
use crate::broadcast::queue::BroadcasterRx;
use crate::client::{ClientHandshake, ClientSink};
use crate::encode::{BookEncoder, BufferProvider};
use crate::registry::events::{Claim, RegistryTx};
use bytes::{Bytes, BytesMut};
use core_lib::connector::book_publisher::BookReader;
use core_lib::instrument::Instrument;
use md_wire::grpc::{RejectCode, Rejected, VenueIdx};
use std::future::poll_fn;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

/// How often a broadcaster re-checks whether anyone is left.
///
/// A client hanging up is noticed directly now - [`Session::poll_progress`] watches every
/// client for end-of-stream - so this is no longer the primary disconnect signal it was when
/// sessions were `watch` channels. What is left is a backstop: a client whose host vanished
/// without a `FIN` never closes anything, and this bounds how long its symbol's connector
/// subscription outlives it.
pub(crate) const SESSION_SWEEP: Duration = Duration::from_secs(5);

/// How long *every* remaining client together gets to read the status that ends its stream.
///
/// One budget for all of them rather than one each: the sessions are closed concurrently, from
/// the same poll loop that drives them in the steady state, so a client that will not read its
/// own trailers costs the deadline once rather than once per client. Past it, they see a closed
/// connection instead, which they have to handle anyway.
const TEARDOWN_TIMEOUT: Duration = Duration::from_secs(1);

/// Superseded buffers kept waiting for the last client holding one to let go.
///
/// The same bound as the pool they return to. Eight is far more than a healthy symbol needs -
/// a buffer is normally free again by the next book - and it is a bound on how long a stalled
/// client can pin memory rather than a target.
const COOLING: usize = 8;

#[derive(Debug)]
struct BufferPool {
    unused: heapless::Vec<BytesMut, 8>,
}

impl BufferPool {
    fn new() -> Self {
        Self {
            unused: heapless::Vec::new(),
        }
    }

    fn get(&mut self, capacity: usize) -> BytesMut {
        let pos = self.unused.iter_mut().position(|b| b.try_reclaim(capacity));

        if let Some(idx) = pos {
            self.unused.swap_remove(idx)
        } else {
            if self.unused.is_full() {
                self.unused.pop();
            }

            BytesMut::with_capacity(capacity)
        }
    }

    /// `buffer` is unique by construction - [`Ctx::return_buffer`] only calls this with what
    /// `Bytes::try_into_mut` handed back, which only succeeds when it is - so `clear` is free.
    ///
    /// It is also load-bearing: `try_reclaim` preserves `len`, so an uncleared buffer (`len`
    /// equal to the whole previous frame, with essentially no slack past it) can never be
    /// reclaimed, and this pool would silently recycle nothing.
    fn return_buffer(&mut self, mut buffer: BytesMut) {
        buffer.clear();
        let _ = self.unused.push(buffer);
    }
}

impl BufferProvider for BufferPool {
    fn get_buffer(&mut self, capacity: usize) -> BytesMut {
        self.get(capacity)
    }
}

/// The current book, its number, and the buffers it is cut from.
#[derive(Debug)]
struct Ctx {
    epoch: u64,
    payload: Bytes,
    pool: BufferPool,
    /// Payloads a newer book has replaced, still held by at least one client.
    ///
    /// A session no longer reports when it is done with a buffer - it hands the `Bytes` to a
    /// sink and never hears about it again - so reclaiming is driven from here instead: each
    /// new book is a chance to check which of the old ones nothing is holding any more.
    cooling: heapless::Vec<Bytes, COOLING>,
}

impl SessionCtx for Ctx {
    fn epoch(&self) -> u64 {
        self.epoch
    }

    fn payload(&self) -> &Bytes {
        &self.payload
    }
}

impl Ctx {
    fn new(payload: Bytes, pool: BufferPool) -> Self {
        Self {
            epoch: 0,
            payload,
            pool,
            cooling: heapless::Vec::new(),
        }
    }

    fn new_framed(&mut self, payload: Bytes) {
        self.epoch += 1;
        let superseded = std::mem::replace(&mut self.payload, payload);
        self.cool(superseded);
    }

    /// Returns to the pool every superseded buffer nothing is holding any more, then starts
    /// `superseded` cooling.
    ///
    /// `try_into_mut` succeeding *is* the test for "nothing is holding this": it only hands
    /// back the buffer when this is the last handle onto it. So a client that is behind pins
    /// exactly the buffer it is behind on, and no others.
    fn cool(&mut self, superseded: Bytes) {
        let mut still_held = heapless::Vec::<Bytes, COOLING>::new();
        while let Some(cooling) = self.cooling.pop() {
            match cooling.try_into_mut() {
                Ok(buffer) => self.pool.return_buffer(buffer),
                Err(pinned) => {
                    let _ = still_held.push(pinned);
                }
            }
        }
        self.cooling = still_held;

        // A full list means every slot is pinned by a client that has not drained. This
        // buffer is then simply not recycled, which costs one allocation next time round -
        // far better than letting a stalled client grow this without bound.
        let _ = self.cooling.push(superseded);
    }
}

/// A client waiting to be attached to a running broadcaster.
#[derive(Debug)]
pub(crate) struct Join<C> {
    client: C,
}

impl<C: ClientHandshake> Join<C> {
    pub(crate) fn new(client: C) -> Self {
        Self { client }
    }

    /// Answers the client and turns it into a session. Its response headers go out ahead of
    /// the opening snapshot the caller delivers immediately afterwards.
    fn into_session(self) -> Session<C::Sink> {
        Session::new(self.client.accept())
    }

    /// Hands the client back, for a join the registry declined to queue at all.
    pub(crate) fn into_client(self) -> C {
        self.client
    }

    /// Tells the client no stream is coming, then drops the connection.
    ///
    /// Only ever reached off the hot path: a broadcaster whose own subscribe was refused, or
    /// one that is on its way out.
    pub(super) async fn reject(self, rejected: Rejected) {
        self.client.reject(rejected).await;
    }
}

/// What woke the run loop.
///
/// The `select!` resolves to one of these rather than doing the work inline: its branch
/// futures still borrow `self` inside a handler body, and both `wait_update` and `get_last`
/// need `&mut self.reader`.
#[derive(Debug)]
enum Wake<C> {
    Book(Option<()>),
    Join(Option<Join<C>>),
    /// At least one session finished - the peer hung up, or a write failed.
    Ended,
    Sweep,
}

/// Owns one symbol's [`BookReader`] and every client attached to it.
#[derive(Debug)]
pub(crate) struct Broadcaster<C: ClientHandshake> {
    instrument: Instrument,
    reader: BookReader,
    /// One entry per attached client, offered the book in order on every update.
    sessions: Vec<Session<C::Sink>>,
    joins: BroadcasterRx<C>,
    /// Joins the registry has queued but this task has not taken yet. Also this
    /// broadcaster's identity to the registry - see [`Claim`].
    pending_joins: Arc<AtomicUsize>,
    /// Holds this symbol's encoded identity and the buffer every frame is cut from, so a
    /// book costs neither an allocation nor a re-encode of `venue`/`symbol`.
    encoder: BookEncoder,
    ctx: Ctx,
    registry: RegistryTx<C>,
}

impl<C: ClientHandshake> Broadcaster<C> {
    /// Waits for the connector's answer to the subscribe the registry has already sent for
    /// this key, then runs the fan-out loop.
    ///
    /// The wait happens here rather than on the registry task, which is what turns an
    /// unlisted symbol into a prompt refusal on the client's connection rather than a stream
    /// that never produces anything - and what keeps the registry free to serve other keys
    /// while a venue thinks about this one.
    ///
    /// This task's `RegistryTx` is also what keeps the registry task alive: it stops once the
    /// last of them is dropped, which is on the way out of this function.
    pub(crate) async fn start(
        registry: RegistryTx<C>,
        instrument: Instrument,
        venue_idx: VenueIdx,
        joins: BroadcasterRx<C>,
        pending_joins: Arc<AtomicUsize>,
        mut reader: BookReader,
    ) {
        let mut pool = BufferPool::new();
        // The venue index comes from the registry rather than from `instrument`: it is the
        // catalogue's numbering, and an `Instrument` knows only which `Venue` it belongs to.
        let encoder = BookEncoder::new(venue_idx);
        let latest = {
            let book = reader.get_last();
            encoder.encode(book.asks(), book.bids(), &mut pool)
        };

        Self {
            encoder,
            instrument,
            reader,
            sessions: Vec::new(),
            joins,
            pending_joins,
            ctx: Ctx::new(latest, pool),
            registry,
        }
        .run()
        .await;
    }

    async fn run(mut self) {
        let mut sweep = tokio::time::interval(SESSION_SWEEP);

        loop {
            // Named up front so the `select!` below borrows one field rather than all of
            // `self`, which the reader and joins branches need for themselves.
            let sessions = &mut self.sessions;
            let wake = tokio::select! {
                biased;
                update = self.reader.wait_update() => Wake::Book(update),
                join = self.joins.recv() => Wake::Join(join),
                () = poll_fn(|cx| poll_sessions(sessions, cx, &self.ctx)) => Wake::Ended,
                _ = sweep.tick() => Wake::Sweep,
            };

            // Only the two things that can end a session ask for a prune, so a book that
            // reached every client costs no scan of the session list at all.
            let prune = match wake {
                // The publisher is gone: the connector shut down, or the venue stopped
                // listing the symbol and the supervisor retired it. `Join(None)` is the
                // registry having dropped this key's entry, which is how a shutting-down
                // server ends every broadcaster. Terminal either way - the entry is removed
                // on the way out, so the next client for this key gets a fresh broadcaster
                // that retries the subscribe.
                Wake::Book(None) | Wake::Join(None) => break,
                Wake::Book(Some(())) => poll_fn(|cx| Poll::Ready(self.publish(cx))).await,
                Wake::Ended => true,
                Wake::Join(Some(join)) => {
                    with_context(|cx| self.attach(join, cx)).await;
                    false
                }
                Wake::Sweep => false,
            };

            if prune {
                self.sessions.retain(|session| !session.ended());
            }
            // A reply that never comes says the same thing `true` does: there is no registry
            // left to serve clients through, so there is nothing to stay alive for.
            if self.sessions.is_empty()
                && self
                    .registry
                    .retire_if_idle(self.claim())
                    .await
                    .unwrap_or(true)
            {
                break;
            }
        }

        // A no-op when the loop exited through `retire_if_idle`, which already removed the
        // entry and issued the connector unsubscribe.
        self.registry.retire(self.claim());
        // Said before the queue is drained, so a client that was already attached hears why
        // ahead of one that never got that far.
        let ended = Rejected::new(RejectCode::StreamEnded, Box::from("stream ended"));
        close(&mut self.sessions, &self.ctx, &ended).await;
        self.joins.drain(ended).await;
    }

    /// The serialize-once step: one book in, one encoding out, one refcount bump per session.
    ///
    /// Returns whether any session ended on the way, so the caller prunes only when there is
    /// something to prune.
    fn publish(&mut self, cx: &mut Context<'_>) -> bool {
        let frame = {
            let book = self.reader.get_last();
            // Encoded straight out of the slot - there is no intermediate `BookUpdate` to
            // copy the levels into. The guard pins a slot in the shared buffer, so it is
            // dropped at the end of this block, before anything is offered to a client, and
            // must never be held across an await.
            self.encoder
                .encode(book.asks(), book.bids(), &mut self.ctx.pool)
        };

        self.ctx.new_framed(frame);

        let ctx = &self.ctx;
        let mut ended = false;
        for session in &mut self.sessions {
            // At most a refcount bump, never a re-encode and never a copy: every session on
            // this symbol is offered the one buffer the encoder just produced.
            session.deliver(cx, ctx);
            ended |= session.ended();
        }
        ended
    }

    /// Attaches a client and gives it the current book straight away.
    ///
    /// The first message after the response headers is always the current snapshot -
    /// [`Ctx::new`] seeds it from `reader.get_last()` before any session exists, so a client
    /// that joins before anything has been published sees the empty book rather than nothing.
    /// No special case is needed: an empty book is already meaningful on this wire, as the
    /// resync signal (`SmallBook::is_empty`), so "nothing published yet" and "the connector is
    /// resyncing" look identical, which is correct - both mean the same thing to a client:
    /// there is no book right now.
    fn attach(&mut self, join: Join<C>, cx: &mut Context<'_>) {
        // Balances the increment the registry made before queuing this join. `Relaxed` is
        // enough: every increment and the decisive load happen on the registry task, in that
        // task's own program order, and this decrement can only make the load *smaller* -
        // which it may only do once this join can no longer be lost, that is, now.
        self.pending_joins.fetch_sub(1, Ordering::Relaxed);

        let mut session = join.into_session();
        session.deliver(cx, &self.ctx);

        if !session.ended() {
            self.sessions.push(session);
        }
    }

    /// Names this broadcaster to the registry. `Instrument` is `Copy`, so this costs nothing.
    fn claim(&self) -> Claim {
        Claim::new(
            self.instrument.id(),
            self.instrument.venue(),
            Arc::clone(&self.pending_joins),
        )
    }
}

/// Resolves `f` against the current task's [`Context`], for a call site that has one to give
/// but is not itself a poll function.
///
/// The `Option::take` is load-bearing: `poll_fn`'s closure is `FnMut`, but `f` is only ever
/// good for one call, and this always resolves on its first poll.
async fn with_context<T>(f: impl FnOnce(&mut Context<'_>) -> T) -> T {
    let mut once = Some(f);
    poll_fn(move |cx| Poll::Ready(once.take().expect("resolved on the first poll")(cx))).await
}

/// Drives every session as far as it will go, and resolves once at least one has finished.
///
/// This is the whole of the backpressure machinery's scheduling: end-of-stream interest on
/// every session, writability interest on just the ones with something left over - normally
/// none - and no allocation for either. It resolves only for a session that has *ended*,
/// because a session that merely made progress has already had that progress made here.
fn poll_sessions<K: ClientSink>(
    sessions: &mut [Session<K>],
    cx: &mut Context<'_>,
    session_ctx: &Ctx,
) -> Poll<()> {
    let mut ended = false;
    for session in sessions {
        ended |= session.poll_progress(cx, session_ctx).is_ended();
    }

    if ended {
        Poll::Ready(())
    } else {
        Poll::Pending
    }
}

/// Ends every remaining client's stream with a status, and waits - once, for all of them - for
/// those trailers to reach the wire.
///
/// The same poll loop the steady state uses, run to a different stopping condition: every
/// session finished rather than any one of them. `begin_finish` only queues, which is what
/// makes one deadline for the whole set possible; a client that will not read costs the
/// deadline once, not once per client.
async fn close<K: ClientSink>(sessions: &mut Vec<Session<K>>, session_ctx: &Ctx, rejected: &Rejected) {
    if sessions.is_empty() {
        return;
    }

    for session in &mut *sessions {
        session.begin_finish(rejected);
    }

    let flushed = poll_fn(|cx| {
        let mut all_ended = true;
        for session in &mut *sessions {
            all_ended &= session.poll_progress(cx, session_ctx).is_ended();
        }
        if all_ended {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    });
    if tokio::time::timeout(TEARDOWN_TIMEOUT, flushed).await.is_err() {
        tracing::debug!("gave up telling some clients why their stream ended");
    }
    // Dropping them closes what is left, which a client that stopped reading has to handle.
    sessions.clear();
}

#[cfg(test)]
mod test {
    use super::SESSION_SWEEP;
    use crate::client::mock::{MockClient, MockPeer, connected};
    use crate::encode::BufferProvider;
    use crate::registry::harness::{FIRST, Harness, registry_for};
    use crate::test_util::{FakeSource, book};
    use crate::venue::Venue;
    use bytes::BytesMut;
    use core_lib::instrument::Instrument;
    use core_lib::venue::test_util::test_instrument_for;
    use md_proto::md::v1 as proto;
    use md_wire::grpc::{MESSAGE_PREFIX, RejectCode, VenueIdx};
    use std::sync::Arc;
    use std::time::Duration;

    const SYMBOL: &str = "btcusdt-broadcast-test";

    fn key() -> Instrument {
        test_instrument_for(Venue::BinanceSpot, SYMBOL)
    }

    /// Subscribes one client and reads its opening snapshot, leaving it ready for real books.
    async fn attach(harness: &Harness) -> MockPeer {
        let peer = attach_answered(harness).await;
        peer.opening_snapshot().await;
        peer
    }

    /// The same, stopping at the answer - for a test that wants to see the opening snapshot
    /// itself rather than have it consumed.
    async fn attach_answered(harness: &Harness) -> MockPeer {
        let (peer, client) = connected();
        hand_over(harness, client).await;
        peer.accepted()
            .await
            .expect("the fake source accepts every symbol");
        peer
    }

    /// Queues a client on the registry and waits for it to be taken. Every attach in this
    /// module goes through here, so the double `expect` is written once.
    async fn hand_over(harness: &Harness, client: MockClient) {
        harness
            .registry
            .subscribe(FIRST, client)
            .await
            .expect("the registry task is alive")
            .expect("the registry is still spawning");
    }

    /// Whether the symbol still has a broadcaster.
    async fn is_registered(harness: &Harness) -> bool {
        harness
            .registry
            .is_registered(key().id())
            .await
            .expect("the registry task is alive")
    }

    struct TestBufferProvider;

    impl BufferProvider for TestBufferProvider {
        fn get_buffer(&mut self, capacity: usize) -> BytesMut {
            BytesMut::with_capacity(capacity)
        }
    }

    /// The claim the whole design rests on: one book in, one encoding out, and the same bytes
    /// reaching every client rather than an encoding per client.
    #[tokio::test]
    async fn one_book_is_encoded_once_and_shared_by_every_session() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source, &[SYMBOL]);

        let first = attach(&harness).await;
        let second = attach(&harness).await;

        source.publish(SYMBOL, &book(&[(100.5, 1.25)], &[(99.5, 2.0)]));

        let (left, right) = tokio::join!(first.next_frame(), second.next_frame());
        assert_eq!(
            left, right,
            "the same buffer reached both clients, so the bytes must match byte for byte"
        );
        assert_eq!(
            source.subscribed().len(),
            1,
            "the second client joins the running broadcaster instead of subscribing again"
        );
    }

    #[tokio::test]
    async fn every_level_carries_the_venue_its_key_holds_and_the_spread_is_derived() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source, &[SYMBOL]);
        let client = attach(&harness).await;

        source.publish(SYMBOL, &book(&[(100.5, 1.25)], &[(99.5, 2.0), (99.0, 4.0)]));
        let update = client.next_book().await;

        assert_eq!(
            update.asks,
            vec![proto::Level {
                price: 100.5,
                size: 1.25,
                venue_idx: 0,
            }],
            "asks travel best first"
        );
        assert_eq!(
            update.bids,
            vec![
                proto::Level {
                    price: 99.5,
                    size: 2.0,
                    venue_idx: 0,
                },
                proto::Level {
                    price: 99.0,
                    size: 4.0,
                    venue_idx: 0,
                },
            ],
            "bids travel best first"
        );
        assert_eq!(update.spread, 1.0, "asks[0].price - bids[0].price");
    }

    /// The resync signal: `SmallBook::is_empty` on the way in, both sides empty on the wire.
    #[tokio::test]
    async fn an_empty_book_reaches_the_client_as_empty_sides() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source, &[SYMBOL]);
        let client = attach(&harness).await;

        source.publish(SYMBOL, &book(&[(100.0, 1.0)], &[(99.0, 1.0)]));
        assert_eq!(
            client.next_book().await.asks.len(),
            1,
            "the real book lands first"
        );

        source.publish_empty(SYMBOL);
        let resync = client.next_book().await;
        assert!(
            resync.asks.is_empty() && resync.bids.is_empty(),
            "a resyncing connector must show up as no book at all"
        );
    }

    /// A client attaching to a symbol that is already ticking is handed the book that is
    /// already there, rather than waiting for the next one.
    #[tokio::test]
    async fn a_client_joining_a_quiet_symbol_gets_the_current_book() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source, &[SYMBOL]);
        let first = attach(&harness).await;

        source.publish(SYMBOL, &book(&[(100.5, 1.25)], &[(99.5, 2.0)]));
        let seen = first.next_frame().await;

        // Nothing is published from here on, so the second client can only be served out of
        // the broadcaster's `latest`. Attached with `attach_over` directly, rather than
        // `attach`, so its opening snapshot - the current book - is not consumed before the
        // comparison below.
        let second = attach_answered(&harness).await;
        assert_eq!(
            second.next_frame().await,
            seen,
            "a client attaching to a quiet symbol sees the book that is already there"
        );
    }

    /// A client that never reads must not stall the broadcaster or the clients beside it, and
    /// must end up with the newest book rather than a backlog of stale ones.
    #[tokio::test]
    async fn a_session_that_never_reads_sees_only_the_newest_book() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source, &[SYMBOL]);
        let idle = attach(&harness).await;
        let attentive = attach(&harness).await;
        idle.stall();

        for step in 1..=200 {
            source.publish(SYMBOL, &book(&[(f64::from(step), 1.0)], &[]));
            // The attentive client keeps up throughout, which is what shows the stalled one is
            // not holding the broadcaster back.
            assert_eq!(
                attentive.next_book().await.asks[0].price,
                f64::from(step),
                "a client that keeps reading sees every book"
            );
        }

        // Two hundred books went past while this client could take none of them. What it gets
        // when its window reopens is the newest, once - not a backlog, and not the first of
        // the two hundred.
        idle.resume();
        let caught_up = idle.next_book().await;
        assert_eq!(
            caught_up.asks[0].price, 200.0,
            "a client whose window reopens is given the current book, not the one it missed"
        );
        idle.assert_quiet().await;
    }

    /// The other half of newest-only: a book a client has already been given is not offered to
    /// it again. Nothing tracks what was delivered except the epoch, so an epoch that failed
    /// to settle would show up here as the same book arriving twice.
    #[tokio::test(start_paused = true)]
    async fn a_delivered_book_is_not_offered_again() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source, &[SYMBOL]);
        let client = attach(&harness).await;

        source.publish(SYMBOL, &book(&[(100.5, 1.25)], &[(99.5, 2.0)]));
        assert_eq!(client.next_book().await.asks[0].price, 100.5);

        // Nothing else is ever published, so a second message here could only be the book that
        // just went out being sent again.
        client.assert_quiet().await;
    }

    /// The other half of the backpressure contract: a client that has stopped reading must
    /// not stop the clients beside it, and must not stop the broadcaster.
    #[tokio::test]
    async fn a_client_that_stops_reading_does_not_block_the_others() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source, &[SYMBOL]);
        let stalled = attach(&harness).await;
        let attentive = attach(&harness).await;
        stalled.stall();

        let deep: Vec<(f64, f64)> = (1..=10).map(|i| (f64::from(i), f64::from(i))).collect();
        for step in 1..=200 {
            source.publish(SYMBOL, &book(&deep, &[(f64::from(step), 1.0)]));
            assert_eq!(
                attentive.next_book().await.bids[0].price,
                f64::from(step),
                "the attentive client keeps up while the stalled one is blocked"
            );
        }

        drop(stalled);
    }

    /// A payload is one whole gRPC length-prefixed message: the five-byte header the encoder
    /// wrote, then exactly the bytes it describes. The framing is not part of the protobuf,
    /// and h2 puts this into DATA frames without touching it.
    #[tokio::test]
    async fn a_payload_is_one_whole_length_prefixed_message() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source, &[SYMBOL]);
        let client = attach(&harness).await;

        source.publish(SYMBOL, &book(&[(100.5, 1.25)], &[]));
        let frame = client.next_frame().await;

        let encoder = crate::encode::BookEncoder::new(VenueIdx::new(0));
        let expected = encoder.encode(
            &[core_lib::incremental_book::Level::new(
                core_lib::positive_f64::PositiveF64::new(100.5).expect("positive"),
                core_lib::positive_f64::PositiveF64::new(1.25).expect("positive"),
            )],
            &[],
            &mut TestBufferProvider,
        );
        assert_eq!(
            frame, expected,
            "what a client is given is exactly the buffer the encoder produced, header included"
        );
        assert_eq!(
            md_wire::grpc::message_len(
                &frame[..MESSAGE_PREFIX]
                    .try_into()
                    .expect("a frame carries a whole message header")
            ),
            Some(frame.len() - MESSAGE_PREFIX),
            "the header must be uncompressed and describe the body that follows it"
        );
    }

    #[tokio::test]
    async fn losing_the_publisher_ends_every_session() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source, &[SYMBOL]);
        let client = attach(&harness).await;

        // What a connector shutting down, or a venue delisting the symbol, looks like.
        source.drop_stream(SYMBOL);

        // The publisher's own `Drop` publishes the empty book first, so the resync signal
        // arrives before the close.
        let farewell = client.next_book().await;
        assert!(
            farewell.asks.is_empty() && farewell.bids.is_empty(),
            "the last thing a client sees is the connector saying it has no book"
        );
        assert_eq!(
            client.ended().await.code(),
            RejectCode::StreamEnded,
            "the stream ends with a status rather than a bare disconnect"
        );

        assert!(
            !is_registered(&harness).await,
            "the entry goes with the broadcaster, so the next client retries the subscribe"
        );
    }

    /// The disconnect signal, which is the connection itself rather than a channel: a client
    /// hanging up has to release its symbol's connector subscription.
    #[tokio::test]
    async fn a_client_hanging_up_releases_the_symbol() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source, &[SYMBOL]);
        let client = attach(&harness).await;

        drop(client);

        let released = tokio::time::timeout(Duration::from_secs(5), async {
            while source.unsubscribed().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(released.is_ok(), "an end-of-stream must retire the symbol");
        assert!(!is_registered(&harness).await);
    }

    /// A client resetting its stream is done with it, and takes the same path a hang-up
    /// takes: noticed on the broadcaster's own poll, without waiting for the sweep.
    #[tokio::test]
    async fn a_client_resetting_its_stream_releases_the_symbol() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source, &[SYMBOL]);
        let client = attach(&harness).await;

        client.reset();

        let released = tokio::time::timeout(Duration::from_secs(5), async {
            while source.unsubscribed().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(released.is_ok(), "a reset stream must end the session");
    }

    /// A client whose host vanished sends no `FIN`, so nothing observes it leaving; the sweep
    /// is what bounds how long its connector subscription outlives it.
    #[tokio::test(start_paused = true)]
    async fn the_sweep_retires_a_symbol_no_one_is_left_reading() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source, &[SYMBOL]);
        let client = attach(&harness).await;

        // Vanishes rather than hangs up: the connection is gone, but nothing woke the
        // broadcaster to tell it, so there is no next poll until something schedules one.
        // That is exactly the case the sweep exists for - a plain `drop` wakes it and never
        // reaches the sweep at all.
        client.vanish();
        tokio::time::sleep(SESSION_SWEEP * 2).await;

        assert_eq!(
            source.unsubscribed(),
            vec![SYMBOL],
            "the last client leaving releases the connector subscription"
        );
        assert!(!is_registered(&harness).await);
    }

    /// A book a client is still holding cannot be recycled - and must not be lost either. It
    /// comes back to the pool as soon as that client lets go.
    ///
    /// This is the whole reason `Ctx::cooling` exists: a sink takes the `Bytes` and never
    /// reports back, so "is anything still holding this" has to be asked again on each new
    /// book rather than answered once when a write finishes.
    #[test]
    fn a_buffer_a_client_still_holds_is_reclaimed_once_it_lets_go() {
        let mut ctx = super::Ctx::new(frame(b"first"), super::BufferPool::new());

        // What a client that is behind holds: a handle onto the book it has not drained.
        let behind = super::SessionCtx::payload(&ctx).clone();

        ctx.new_framed(frame(b"second"));
        assert!(
            ctx.pool.unused.is_empty(),
            "a buffer a client is still holding must not be handed out to be overwritten"
        );
        assert_eq!(ctx.cooling.len(), 1, "it waits instead");

        drop(behind);
        ctx.new_framed(frame(b"third"));
        assert_eq!(
            ctx.pool.unused.len(),
            1,
            "the moment the last handle goes, the buffer is reusable again"
        );
    }

    /// The steady state, which is the point of the pool: with every client keeping up, each
    /// new book hands the one before it back, so a long run allocates nothing.
    ///
    /// Two books of slack rather than none - a buffer is superseded on one book and found
    /// unheld on the next - so three distinct allocations is the whole working set however
    /// long this runs.
    #[test]
    fn a_long_run_of_books_cycles_a_fixed_set_of_buffers() {
        let encoder = crate::encode::BookEncoder::new(VenueIdx::new(0));
        let asks = [level(100.5, 1.25)];
        let bids = [level(99.5, 2.0)];

        let mut pool = super::BufferPool::new();
        let first = encoder.encode(&asks, &bids, &mut pool);
        let mut ctx = super::Ctx::new(first, pool);

        let mut allocations = std::collections::HashSet::new();
        for _ in 0..1_000 {
            let frame = encoder.encode(&asks, &bids, &mut ctx.pool);
            allocations.insert(frame.as_ptr());
            ctx.new_framed(frame);
        }

        assert!(
            allocations.len() <= 3,
            "a thousand books must cycle a handful of buffers rather than allocate per book, \
             got {} distinct allocations",
            allocations.len()
        );
    }

    /// A book level, out of two prices a test spelled as plain floats.
    fn level(price: f64, size: f64) -> core_lib::incremental_book::Level {
        core_lib::incremental_book::Level::new(
            core_lib::positive_f64::PositiveF64::new(price).expect("positive"),
            core_lib::positive_f64::PositiveF64::new(size).expect("positive"),
        )
    }

    /// One payload a test wrote by hand - frozen from a `BytesMut` so it is reclaimable, which
    /// `Bytes::from_static` would not be.
    fn frame(body: &[u8]) -> bytes::Bytes {
        let mut buf = BytesMut::with_capacity(body.len());
        buf.extend_from_slice(body);
        buf.freeze()
    }

    /// A buffer that has been filled, frozen and returned comes back cleared and on the same
    /// allocation - `clear` before pooling is what lets `try_reclaim` see it as reusable.
    #[test]
    fn a_returned_buffer_is_cleared_and_reused() {
        let mut pool = super::BufferPool::new();

        let mut buf = pool.get(64);
        let original_ptr = buf.as_ptr();
        buf.extend_from_slice(&[1, 2, 3, 4]);
        let frozen = buf.freeze();
        pool.return_buffer(frozen.try_into_mut().expect("uniquely held"));

        let reused = pool.get(64);
        assert!(reused.is_empty(), "a pooled buffer must come back empty");
        assert_eq!(
            reused.as_ptr(),
            original_ptr,
            "a cleared buffer with enough slack must be reclaimed in place, not reallocated"
        );
    }

    /// With more than one buffer pooled, a request only the larger one can satisfy must get
    /// that buffer specifically, leaving the other one still pooled - not swap-remove the wrong
    /// index because of a reversed position.
    #[test]
    fn get_returns_the_buffer_that_actually_fits() {
        let mut pool = super::BufferPool::new();

        let small = pool.get(8);
        let large = pool.get(256);
        let large_ptr = large.as_ptr();
        pool.return_buffer(small);
        pool.return_buffer(large);

        let got = pool.get(200);
        assert_eq!(
            got.as_ptr(),
            large_ptr,
            "only the larger buffer can satisfy this request"
        );

        // The small buffer must still be pooled, not evicted by a `swap_remove` at the wrong
        // index.
        let still_pooled = pool.get(8);
        assert_ne!(
            still_pooled.as_ptr(),
            got.as_ptr(),
            "the small buffer should still have been served from the pool, not fresh"
        );
    }
}
