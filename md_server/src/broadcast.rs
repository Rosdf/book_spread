//! The fan-out itself: one task per `(venue, symbol)` that reads, encodes once, and writes
//! the resulting bytes into every attached client socket.
//!
//! The broadcaster owns those sockets. There is no per-client task and no channel between the
//! encoder and the kernel: a book is encoded once and then `try_write`-n into each session in
//! turn, so the same `Bytes` reaches every client with no copy of its own. See
//! [`crate::session`] for the write state machine and what backpressure does to it.

use crate::encode::{BookEncoder, BufferProvider};
use crate::registry::{Key, Registry};
use crate::session::{Session, SessionCtx};
use crate::venue::{BookSource as _, Connectors};
use bytes::{Bytes, BytesMut};
use core_lib::connector::book_publisher::BookReader;
use md_wire::framing::{self, RejectCode};
use std::convert::Infallible;
use std::future::poll_fn;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;

/// How often a broadcaster re-checks whether anyone is left.
///
/// A client hanging up is noticed directly now - [`Session::poll_progress`] watches every
/// socket for end-of-stream - so this is no longer the primary disconnect signal it was when
/// sessions were `watch` channels. What is left is a backstop: a client whose host vanished
/// without a `FIN` never closes anything, and this bounds how long its symbol's connector
/// subscription outlives it.
pub(crate) const SESSION_SWEEP: Duration = Duration::from_secs(5);

/// How long a refusal may take to write before the connection is simply dropped.
///
/// A client that will not read its own rejection does not get to hold a shutting-down
/// broadcaster up; it sees a closed socket instead, which it has to handle anyway.
const REJECT_TIMEOUT: Duration = Duration::from_secs(1);

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
        let pos = self
            .unused
            .iter_mut()
            .rposition(|b| b.try_reclaim(capacity));

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

#[derive(Debug)]
struct Ctx {
    epoch: u64,
    payload: Bytes,
    pool: BufferPool,
}

impl SessionCtx for Ctx {
    fn payload_for_epoch(&self, epoch: u64) -> Option<&[u8]> {
        (self.epoch >= epoch).then(|| self.payload.as_ref())
    }

    fn current_payload(&self) -> Bytes {
        self.payload.clone()
    }

    fn return_buffer(&mut self, buffer: Bytes) {
        if let Ok(buf) = buffer.try_into_mut() {
            self.pool.return_buffer(buf);
        }
    }
}

impl Ctx {
    fn new(payload: Bytes, pool: BufferPool) -> Self {
        Self {
            epoch: 0,
            payload,
            pool,
        }
    }

    fn new_framed(&mut self, payload: Bytes) {
        self.epoch += 1;
        self.payload = payload;
    }

    fn epoch(&self) -> u64 {
        self.epoch
    }
}

/// A client whose socket is waiting to be attached to a running broadcaster.
#[derive(Debug)]
pub struct Join<S> {
    sock: S,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Join<S> {
    pub(crate) fn new(sock: S) -> Self {
        Self { sock }
    }

    /// Turns the socket into a session. The acceptance header is already in flight on it -
    /// see [`Session::new`] - so nothing else needs writing before the first book.
    fn into_session(self) -> Session<S> {
        Session::new(self.sock)
    }

    /// Hands the socket back, for a join the registry declined to queue at all.
    pub(crate) fn into_socket(self) -> S {
        self.sock
    }

    /// Tells the client no stream is coming, then drops the connection.
    ///
    /// Only ever reached off the hot path: a broadcaster whose own subscribe was refused, or
    /// one that is on its way out.
    pub(crate) async fn reject(mut self, why: &str) {
        let written = tokio::time::timeout(
            REJECT_TIMEOUT,
            framing::write_reject(&mut self.sock, RejectCode::Unavailable, why),
        )
        .await;
        if !matches!(written, Ok(Ok(()))) {
            tracing::debug!("could not tell a client why it was refused");
        }
    }
}

/// What woke the run loop.
///
/// The `select!` resolves to one of these rather than doing the work inline: its branch
/// futures still borrow `self` inside a handler body, and both `wait_update` and `get_last`
/// need `&mut self.reader`.
#[derive(Debug)]
enum Wake<S> {
    Book(Option<()>),
    Join(Option<Join<S>>),
    /// At least one session finished - the peer hung up, or a write failed.
    Ended,
    Sweep,
}

/// Owns one symbol's [`BookReader`] and every client socket attached to it.
#[derive(Debug)]
pub struct Broadcaster<C: Connectors, S> {
    key: Key,
    reader: BookReader,
    /// One entry per attached client, written to in order on every update.
    sessions: Vec<Session<S>>,
    joins: mpsc::UnboundedReceiver<Join<S>>,
    /// Joins the registry has queued but this task has not taken yet. See
    /// [`Registry::retire_if_idle`].
    pending_joins: Arc<AtomicUsize>,
    /// Holds this symbol's encoded identity and the buffer every frame is cut from, so a
    /// book costs neither an allocation nor a re-encode of `venue`/`symbol`.
    encoder: BookEncoder,
    ctx: Ctx,
    registry: Arc<Registry<C, S>>,
}

impl<C: Connectors, S: AsyncRead + AsyncWrite + Unpin + Send + 'static> Broadcaster<C, S> {
    /// Runs one broadcaster to completion, then releases `token`.
    ///
    /// The token goes last on purpose: [`crate::server::serve`] treats the paired receiver
    /// closing as "every broadcaster has released its `Arc<Registry>`", and this task holds
    /// one until `serve` below returns.
    pub(crate) async fn start(
        registry: Arc<Registry<C, S>>,
        key: Key,
        joins: mpsc::UnboundedReceiver<Join<S>>,
        pending_joins: Arc<AtomicUsize>,
        token: mpsc::Sender<Infallible>,
    ) {
        Self::serve(registry, key, joins, pending_joins).await;
        drop(token);
    }

    /// Subscribes the symbol on its connector, then runs the fan-out loop.
    ///
    /// The subscribe is awaited here, outside every lock, which is what turns an unlisted
    /// symbol into a prompt refusal on the client's connection rather than a stream that
    /// never produces anything.
    async fn serve(
        registry: Arc<Registry<C, S>>,
        key: Key,
        mut joins: mpsc::UnboundedReceiver<Join<S>>,
        pending_joins: Arc<AtomicUsize>,
    ) {
        let mut reader = match registry
            .source(key.venue())
            .subscribe(key.symbol().into())
            .await
        {
            Ok(Ok(reader)) => reader,
            Ok(Err(err)) => {
                tracing::warn!(
                    venue = key.venue().as_str(),
                    symbol = key.symbol(),
                    %err,
                    "subscribe rejected"
                );
                // Removed first: no further join can be queued once the entry is gone, so
                // the drain below answers every one of them. Nothing to unsubscribe - this
                // broadcaster never held a subscription.
                registry.abandon(&key, &pending_joins);
                drain_joins(&mut joins, &err.to_string()).await;
                return;
            }
            Err(_) => {
                registry.abandon(&key, &pending_joins);
                drain_joins(&mut joins, "connector stopped before it could subscribe").await;
                return;
            }
        };

        let mut pool = BufferPool::new();
        let encoder = BookEncoder::new(key.venue().as_str());
        let latest = {
            let book = reader.get_last();
            encoder.encode(book.asks(), book.bids(), &mut pool)
        };

        Self {
            encoder,
            key,
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
                () = poll_fn(|cx| poll_sessions(sessions, cx, &mut self.ctx)) => Wake::Ended,
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
            if self.sessions.is_empty()
                && self.registry.retire_if_idle(&self.key, &self.pending_joins)
            {
                break;
            }
        }

        // A no-op when the loop exited through `retire_if_idle`, which already removed the
        // entry and issued the connector unsubscribe.
        self.registry.retire(&self.key, &self.pending_joins);
        drain_joins(&mut self.joins, "stream ended").await;
        // `sessions` drops with `self`, and dropping a session closes its socket - which is
        // exactly how this protocol says a stream is over. There is nothing to drain.
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
            // dropped at the end of this block, before anything is written to a socket, and
            // must never be held across an await.
            self.encoder
                .encode(book.asks(), book.bids(), &mut self.ctx.pool)
        };

        self.ctx.new_framed(frame);

        let mut ended = false;
        for session in &mut self.sessions {
            // A refcount bump and a `write`, not a re-encode and not a copy: every session on
            // this symbol hands the kernel the same buffer.
            session.deliver(self.ctx.epoch(), cx, &mut self.ctx);
            ended |= session.ended();
        }
        ended
    }

    /// Attaches a client and gives it the current book straight away.
    ///
    /// The first frame after the acceptance header is always the current snapshot -
    /// [`Ctx::new`] seeds it from `reader.get_last()` before any session exists, so a client
    /// that joins before anything has been published sees the empty book rather than nothing.
    /// No special case is needed: an empty book is already meaningful on this wire, as the
    /// resync signal (`SmallBook::is_empty`), so "nothing published yet" and "the connector is
    /// resyncing" look identical, which is correct - both mean the same thing to a client:
    /// there is no book right now.
    fn attach(&mut self, join: Join<S>, cx: &mut Context<'_>) {
        // Balances the increment `Registry::subscribe` made under its lock before queuing
        // this join. `Relaxed` is enough: every increment and the decisive load happen under
        // that mutex, and this decrement is on the same task as the load.
        self.pending_joins.fetch_sub(1, Ordering::Relaxed);

        let mut session = join.into_session();
        session.deliver(self.ctx.epoch(), cx, &mut self.ctx);

        if !session.ended() {
            self.sessions.push(session);
        }
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
fn poll_sessions<S: AsyncRead + AsyncWrite + Unpin>(
    sessions: &mut [Session<S>],
    cx: &mut Context<'_>,
    session_ctx: &mut Ctx,
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

/// Answers every queued join with `why` and returns once the channel is closed.
///
/// Only correct after the registry entry has been removed: that is what drops the sending
/// half, so `recv` reports `None` instead of parking forever.
async fn drain_joins<S: AsyncRead + AsyncWrite + Unpin>(
    joins: &mut mpsc::UnboundedReceiver<Join<S>>,
    why: &str,
) {
    while let Some(join) = joins.recv().await {
        join.reject(why).await;
    }
}

#[cfg(test)]
mod test {
    use super::SESSION_SWEEP;
    use crate::encode::BufferProvider;
    use crate::registry::Key;
    use crate::test_util::{
        Client, FakeSource, book, connected, connected_congested, registry_for,
    };
    use crate::venue::Venue;
    use bytes::BytesMut;
    use md_proto::md::v1 as proto;
    use md_wire::framing::LENGTH_PREFIX;
    use std::sync::Arc;
    use std::time::Duration;

    const SYMBOL: &str = "btcusdt";

    fn key() -> Key {
        Key::new(Venue::BinanceSpot, SYMBOL.into())
    }

    /// Subscribes one client, reads its acceptance header and its opening snapshot, leaving it
    /// ready for real books.
    async fn attach(harness: &crate::test_util::Harness) -> Client {
        let mut client = attach_over(harness, connected()).await;
        client.opening_snapshot().await;
        client
    }

    /// The same, over a connection whose write queue is pinned small enough to back up.
    async fn attach_congested(harness: &crate::test_util::Harness) -> Client {
        let mut client = attach_over(harness, connected_congested()).await;
        client.opening_snapshot().await;
        client
    }

    /// The same, but also hands back the [`crate::test_util::MockControl`] so a test can watch
    /// flushes on this connection.
    async fn attach_congested_watched(
        harness: &crate::test_util::Harness,
    ) -> (Client, crate::test_util::MockControl) {
        let (mut client, server) = connected_congested();
        let control = server.control();
        harness
            .registry
            .subscribe(key(), server)
            .expect("the registry is still spawning");
        client
            .accepted()
            .await
            .expect("the fake source accepts every symbol");
        client.opening_snapshot().await;
        (client, control)
    }

    async fn attach_over(
        harness: &crate::test_util::Harness,
        connection: (Client, crate::test_util::MockStream),
    ) -> Client {
        let (mut client, server) = connection;
        harness
            .registry
            .subscribe(key(), server)
            .expect("the registry is still spawning");
        client
            .accepted()
            .await
            .expect("the fake source accepts every symbol");
        client
    }

    struct TestBufferProvider;

    impl BufferProvider for TestBufferProvider {
        fn get_buffer(&mut self, capacity: usize) -> BytesMut {
            BytesMut::with_capacity(capacity)
        }
    }

    /// The claim the whole design rests on: one book in, one encoding out, and the same bytes
    /// reaching every socket rather than an encoding per client.
    #[tokio::test]
    async fn one_book_is_encoded_once_and_shared_by_every_session() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source);

        let mut first = attach(&harness).await;
        let mut second = attach(&harness).await;

        source.publish(SYMBOL, &book(&[(100.5, 1.25)], &[(99.5, 2.0)]));

        let (left, right) = tokio::join!(first.next_frame(), second.next_frame());
        assert_eq!(
            left, right,
            "the same buffer reached both sockets, so the bytes must match byte for byte"
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
        let harness = registry_for(&source);
        let mut client = attach(&harness).await;

        source.publish(SYMBOL, &book(&[(100.5, 1.25)], &[(99.5, 2.0), (99.0, 4.0)]));
        let update = client.next_book().await;

        assert_eq!(
            update.asks,
            vec![proto::Level {
                price: 100.5,
                size: 1.25,
                venue: "binance_spot".to_owned(),
            }],
            "asks travel best first"
        );
        assert_eq!(
            update.bids,
            vec![
                proto::Level {
                    price: 99.5,
                    size: 2.0,
                    venue: "binance_spot".to_owned(),
                },
                proto::Level {
                    price: 99.0,
                    size: 4.0,
                    venue: "binance_spot".to_owned(),
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
        let harness = registry_for(&source);
        let mut client = attach(&harness).await;

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
        let harness = registry_for(&source);
        let mut first = attach(&harness).await;

        source.publish(SYMBOL, &book(&[(100.5, 1.25)], &[(99.5, 2.0)]));
        let seen = first.next_frame().await;

        // Nothing is published from here on, so the second client can only be served out of
        // the broadcaster's `latest`. Attached with `attach_over` directly, rather than
        // `attach`, so its opening snapshot - the current book - is not consumed before the
        // comparison below.
        let mut second = attach_over(&harness, connected()).await;
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
        let harness = registry_for(&source);
        let mut idle = attach(&harness).await;
        let mut attentive = attach(&harness).await;

        for step in 1..=200 {
            source.publish(SYMBOL, &book(&[(f64::from(step), 1.0)], &[]));
            // The attentive client keeps up throughout, which is what shows the idle one is
            // not holding the broadcaster back.
            assert_eq!(
                attentive.next_book().await.asks[0].price,
                f64::from(step),
                "a client that keeps reading sees every book"
            );
        }

        // Whatever the idle client has buffered, the last frame it can ever read is the
        // newest book - the queue behind the one in flight only ever holds one. Compared by
        // bit pattern rather than by `<`: every price here is an exact small integer, so this
        // is an equality test that happens to be spelled without floats.
        let newest = 200.0_f64;
        let mut last = idle.next_book().await;
        while last.asks[0].price.to_bits() != newest.to_bits() {
            last = idle.next_book().await;
        }
        assert_eq!(
            last.asks[0].price, newest,
            "the slot holds the newest book, not a backlog"
        );
    }

    /// The splice hazard, and the reason `inflight` is not newest-only: a frame that was
    /// half written has to be finished before a newer one starts, or the client reads two
    /// messages run together. Every frame the client does read must decode on its own.
    #[tokio::test]
    async fn a_partly_written_frame_is_finished_before_a_newer_one_starts() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source);
        // Tiny kernel buffers, so a handful of books is enough to leave a frame part-written.
        let mut client = attach_congested(&harness).await;

        // Full depth, so every frame is as large as this protocol ever makes one: twenty of
        // them is roughly twice what the send buffer holds, and only one is drained per lap,
        // so the socket is full and a frame is left part-written on essentially every lap.
        let deep: Vec<(f64, f64)> = (1..=10).map(|i| (f64::from(i), f64::from(i))).collect();
        for _ in 0..100 {
            for _ in 0..20 {
                source.publish(SYMBOL, &book(&deep, &deep));
                tokio::task::yield_now().await;
            }

            // Every frame that arrives is a whole `BookUpdate` at the announced length. A
            // newer frame started on top of a half-written one would put the reader out of
            // step with the length prefixes for good, so this fails on the very next lap.
            let update = client.next_book().await;
            assert_eq!(
                update.asks.len(),
                10,
                "a spliced frame would not decode whole"
            );
            assert_eq!(update.bids.len(), 10);
            assert_eq!(update.asks[0].venue, "binance_spot");
        }
    }

    /// The other half of the splice hazard: a frame that finishes writing out of `inflight`
    /// must settle `Session::epoch` exactly like one written straight from `Ctx::payload`, or
    /// the next lap resends the very book that just finished.
    #[tokio::test(start_paused = true)]
    async fn a_finished_partial_write_is_not_repeated() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source);
        let (mut client, control) = attach_congested_watched(&harness).await;

        // Full depth on both sides, so the frame does not fit the 32-byte send buffer in one
        // write and is left part-written at least once.
        let deep: Vec<(f64, f64)> = (1..=10).map(|i| (f64::from(i), f64::from(i))).collect();
        source.publish(SYMBOL, &book(&deep, &deep));

        let update = client.next_book().await;
        assert_eq!(update.asks.len(), 10);
        assert_eq!(update.bids.len(), 10);
        assert!(
            control.flushes() > 0,
            "a session flushes once a frame is fully written, not just when the socket backs up"
        );

        // Nothing else was ever published: a second identical frame here can only be the one
        // that just finished being resent, because `Session::epoch` never advanced for it.
        client.assert_quiet().await;
    }

    /// The other half of the backpressure contract: a client that has stopped reading must
    /// not stop the clients beside it, and must not stop the broadcaster.
    #[tokio::test]
    async fn a_client_that_stops_reading_does_not_block_the_others() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source);
        let stalled = attach_congested(&harness).await;
        let mut attentive = attach(&harness).await;

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

    /// A frame is its length followed by exactly that many bytes of message - the framing is
    /// not part of the protobuf.
    #[tokio::test]
    async fn a_frame_is_its_length_followed_by_the_message() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source);
        let mut client = attach(&harness).await;

        source.publish(SYMBOL, &book(&[(100.5, 1.25)], &[]));
        let body = client.next_frame().await;

        let encoder = crate::encode::BookEncoder::new("binance_spot");
        let expected = encoder.encode(
            &[core_lib::incremental_book::Level::new(
                core_lib::positive_f64::PositiveF64::new(100.5).expect("positive"),
                core_lib::positive_f64::PositiveF64::new(1.25).expect("positive"),
            )],
            &[],
            &mut TestBufferProvider,
        );
        assert_eq!(
            body.as_slice(),
            &expected[LENGTH_PREFIX..],
            "what arrives behind the prefix is exactly what the encoder produced"
        );
    }

    #[tokio::test]
    async fn losing_the_publisher_ends_every_session() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source);
        let mut client = attach(&harness).await;

        // What a connector shutting down, or a venue delisting the symbol, looks like.
        source.drop_stream(SYMBOL);

        // The publisher's own `Drop` publishes the empty book first, so the resync signal
        // arrives before the close.
        let farewell = client.next_book().await;
        assert!(
            farewell.asks.is_empty() && farewell.bids.is_empty(),
            "the last thing a client sees is the connector saying it has no book"
        );
        client.ended().await;

        assert!(
            !harness.registry.is_registered(&key()),
            "the entry goes with the broadcaster, so the next client retries the subscribe"
        );
    }

    /// The disconnect signal, which is now the socket itself rather than a channel: a client
    /// hanging up has to release its symbol's connector subscription.
    #[tokio::test]
    async fn a_client_hanging_up_releases_the_symbol() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source);
        let client = attach(&harness).await;

        drop(client);

        let released = tokio::time::timeout(Duration::from_secs(5), async {
            while source.unsubscribed().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(released.is_ok(), "an end-of-stream must retire the symbol");
        assert!(!harness.registry.is_registered(&key()));
    }

    /// A client sending anything after its request is violating the protocol, and is dropped
    /// rather than tolerated - the same path a hang-up takes.
    #[tokio::test]
    async fn a_client_that_talks_out_of_turn_is_dropped() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source);
        let mut client = attach(&harness).await;

        client.misbehave().await;

        let released = tokio::time::timeout(Duration::from_secs(5), async {
            while source.unsubscribed().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(released.is_ok(), "an unexpected byte must end the session");
    }

    /// A client whose host vanished sends no `FIN`, so nothing observes it leaving; the sweep
    /// is what bounds how long its connector subscription outlives it.
    #[tokio::test(start_paused = true)]
    async fn the_sweep_retires_a_symbol_no_one_is_left_reading() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source);
        let client = attach(&harness).await;

        drop(client);
        tokio::time::sleep(SESSION_SWEEP * 2).await;

        assert_eq!(
            source.unsubscribed(),
            vec![Box::from(SYMBOL)],
            "the last client leaving releases the connector subscription"
        );
        assert!(!harness.registry.is_registered(&key()));
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
