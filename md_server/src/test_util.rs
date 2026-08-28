//! Test doubles: connectors with no sockets behind them, and an in-memory socket pair.
//!
//! The pair lets a test observe backpressure, write failures and shutdown deterministically
//! instead of racing a real kernel buffer.
//!
//! `make_book_publisher_pair` is public, so a fake source can hand out a real [`BookReader`]
//! and keep the matching [`BookPublisher`] for the test to drive - which is the whole reason
//! [`BookSource`] exists as a trait.
//!
//! Public rather than `pub(crate)`, and gated on the `test-util` feature as well as `cfg(test)`:
//! `md_server`'s own `tests/` integration target and any future consumer both need this without
//! being part of the crate itself.

use crate::registry::RegistryHandle;
use crate::registry::events::RegistryTx;
use crate::transport::Listener;
use crate::venue::{BookSource, Connectors, Venue};
use core_lib::connector::book_publisher::{BookPublisher, BookReader, make_book_publisher_pair};
use core_lib::incremental_book::IncrementalBook;
use core_lib::positive_f64::PositiveF64;
use md_proto::md::v1 as proto;
use md_wire::framing::{self, ReadFrameError, Rejected};
use prost::Message as _;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt as _, ReadBuf};

#[derive(Debug, Default)]
struct SourceState {
    /// The publisher half of every live subscription.
    live: HashMap<Box<str>, BookPublisher>,
    subscribed: Vec<Box<str>>,
    unsubscribed: Vec<Box<str>>,
    /// When set, every subscribe is answered with this rejection instead of a reader.
    reject: Option<String>,
    /// When set, `unsubscribe` panics rather than releasing the symbol.
    panic_on_unsubscribe: bool,
}

/// One venue's connector with the venue taken out of it.
#[derive(Debug, Default)]
pub struct FakeSource {
    state: Mutex<SourceState>,
}

impl FakeSource {
    /// A source that turns every subscribe down, the way a venue does for a symbol it does
    /// not list.
    pub fn rejecting(why: &str) -> Self {
        Self {
            state: Mutex::new(SourceState {
                reject: Some(why.to_owned()),
                ..SourceState::default()
            }),
        }
    }

    /// A source whose teardown blows up, so a test can watch what a panicking event handler
    /// does to the registry task around it. Nothing a venue does, and that is the point: it
    /// stands in for a bug anywhere inside a critical section.
    pub fn panicking_unsubscribe() -> Self {
        Self {
            state: Mutex::new(SourceState {
                panic_on_unsubscribe: true,
                ..SourceState::default()
            }),
        }
    }

    /// Every symbol `subscribe` was called with, in order. Duplicates are the point of
    /// looking: the registry's job is to make sure there are none.
    pub fn subscribed(&self) -> Vec<Box<str>> {
        self.lock().subscribed.clone()
    }

    /// Every symbol `unsubscribe` was called with, in order.
    pub fn unsubscribed(&self) -> Vec<Box<str>> {
        self.lock().unsubscribed.clone()
    }

    /// Publishes `book` on `symbol`'s stream.
    pub fn publish(&self, symbol: &str, book: &IncrementalBook) {
        self.with_publisher(symbol, |publisher| publisher.publish(book));
    }

    /// Publishes the empty book, which is how a connector says it is resyncing.
    pub fn publish_empty(&self, symbol: &str) {
        self.with_publisher(symbol, BookPublisher::publish_empty);
    }

    /// Drops `symbol`'s publisher: what a connector shutting down, or a venue delisting the
    /// symbol, looks like from the reader's side.
    pub fn drop_stream(&self, symbol: &str) {
        self.lock().live.remove(symbol);
    }

    fn with_publisher(&self, symbol: &str, f: impl FnOnce(&mut BookPublisher)) {
        let mut state = self.lock();
        let publisher = state
            .live
            .get_mut(symbol)
            .expect("the test published on a symbol nothing is subscribed to");
        f(publisher);
        drop(state);
    }

    /// Records the subscribe and opens a book channel for it, keeping the publisher half.
    fn open(&self, symbol: Box<str>) -> anyhow::Result<BookReader> {
        let mut state = self.lock();
        state.subscribed.push(symbol.clone());
        if let Some(why) = state.reject.clone() {
            return Err(anyhow::anyhow!(why));
        }
        let (publisher, reader) = make_book_publisher_pair();
        state.live.insert(symbol, publisher);
        drop(state);
        Ok(reader)
    }

    fn lock(&self) -> MutexGuard<'_, SourceState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl BookSource for FakeSource {
    fn subscribe(&self, symbol: Box<str>) -> oneshot::Receiver<anyhow::Result<BookReader>> {
        let (reply, result) = oneshot::channel();
        let _ = reply.send(self.open(symbol));
        result
    }

    fn unsubscribe(&self, symbol: Box<str>) {
        let mut state = self.lock();
        assert!(
            !state.panic_on_unsubscribe,
            "scripted panic while releasing {symbol}"
        );
        // A real unsubscribe drops the symbol's publisher, which is what ends the reader's
        // stream. The fake does the same so teardown looks identical from above.
        state.live.remove(&symbol);
        state.unsubscribed.push(symbol);
    }
}

/// Both venues, each backed by a [`FakeSource`].
#[derive(Debug)]
pub struct FakeConnectors {
    binance_spot: Arc<FakeSource>,
    bitstamp: FakeSource,
}

impl FakeConnectors {
    /// The Binance-side source is handed in as an `Arc` so the test keeps a handle on it
    /// after the connectors have been given away to a [`Registry`].
    pub fn new(binance_spot: Arc<FakeSource>, bitstamp: FakeSource) -> Self {
        Self {
            binance_spot,
            bitstamp,
        }
    }
}

impl Connectors for FakeConnectors {
    type Source = FakeSource;

    fn source(&self, venue: Venue) -> &FakeSource {
        match venue {
            Venue::BinanceSpot => &self.binance_spot,
            Venue::Bitstamp => &self.bitstamp,
        }
    }

    async fn shutdown(self) {}
}

/// A registry task over one Binance-side source, and the way in that a test drives it
/// through.
///
/// The [`RegistryHandle`] is dropped rather than kept: dropping its `JoinHandle` only
/// detaches the task, and the `RegistryTx` in the harness is what keeps it running - for
/// exactly as long as the test holds the harness. A test that wants the connectors back
/// stands its own handle up instead.
pub(crate) fn registry_for(source: &Arc<FakeSource>) -> Harness {
    let connectors = FakeConnectors::new(Arc::clone(source), FakeSource::default());
    let handle = RegistryHandle::spawn(connectors);

    Harness {
        registry: handle.tx(),
    }
}

/// What [`registry_for`] hands back: the registry under test, reached the same way a
/// broadcaster reaches it.
#[derive(Debug)]
pub(crate) struct Harness {
    pub(crate) registry: RegistryTx<MockStream>,
}

// ---------------------------------------------------------------------------------------------
// The mock socket pair.
//
// A session is generic over its transport precisely so a test never has to be a client on a
// real loopback port to observe it: `Session<MockStream>` behaves exactly as `Session<TcpStream>`
// does, and the pipe below gives a test full control over what the kernel would otherwise decide
// - a partial write, a stalled peer, a mid-stream I/O error, an exact byte-for-byte close.
// ---------------------------------------------------------------------------------------------

/// One direction of an in-memory socket: a bounded byte queue with a waker slot on each side.
#[derive(Debug)]
struct PipeState {
    buf: VecDeque<u8>,
    capacity: usize,
    /// The writing end has hung up: nothing more will ever arrive.
    write_closed: bool,
    /// The reading end has hung up: nothing written from here on will ever be read.
    read_closed: bool,
    read_waker: Option<Waker>,
    write_waker: Option<Waker>,
}

#[derive(Debug, Clone)]
struct Pipe(Arc<Mutex<PipeState>>);

impl Pipe {
    fn new(capacity: usize) -> Self {
        Self(Arc::new(Mutex::new(PipeState {
            buf: VecDeque::new(),
            capacity,
            write_closed: false,
            read_closed: false,
            read_waker: None,
            write_waker: None,
        })))
    }

    fn lock(&self) -> MutexGuard<'_, PipeState> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn set_capacity(&self, capacity: usize) {
        let mut state = self.lock();
        state.capacity = capacity;
        if let Some(waker) = state.write_waker.take() {
            waker.wake();
        }
    }

    /// The writer having gone: further reads drain what is left, then see a clean EOF.
    fn close_write(&self) {
        let mut state = self.lock();
        state.write_closed = true;
        if let Some(waker) = state.read_waker.take() {
            waker.wake();
        }
    }

    /// The reader having gone: further writes see `Ok(0)`, same as a peer that has hung up.
    fn close_read(&self) {
        let mut state = self.lock();
        state.read_closed = true;
        if let Some(waker) = state.write_waker.take() {
            waker.wake();
        }
    }

    fn poll_read(&self, cx: &Context<'_>, out: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let mut state = self.lock();
        if state.buf.is_empty() {
            if state.write_closed {
                return Poll::Ready(Ok(()));
            }
            state.read_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let take = out.remaining().min(state.buf.len());
        let drained: Vec<u8> = state.buf.drain(..take).collect();
        out.put_slice(&drained);
        let woken = state.write_waker.take();
        drop(state);
        if let Some(waker) = woken {
            waker.wake();
        }
        Poll::Ready(Ok(()))
    }

    /// Writes what fits and returns `Pending` when the queue is full - the deterministic
    /// stand-in for a kernel send buffer backing up.
    fn poll_write(&self, cx: &Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        let mut state = self.lock();
        if state.read_closed {
            return Poll::Ready(Ok(0));
        }
        let room = state.capacity.saturating_sub(state.buf.len());
        if room == 0 {
            state.write_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let take = room.min(buf.len());
        state.buf.extend(&buf[..take]);
        let woken = state.read_waker.take();
        drop(state);
        if let Some(waker) = woken {
            waker.wake();
        }
        Poll::Ready(Ok(take))
    }
}

/// Counters and failure injection [`MockControl`] gives a test over one [`MockStream`].
#[derive(Debug, Default)]
struct Counters {
    written: Vec<u8>,
    flushes: usize,
    shutdowns: usize,
    fail_next_write: Option<io::ErrorKind>,
    fail_next_read: Option<io::ErrorKind>,
}

/// The server side of a mock connection - what a [`Session`](crate::session::Session) holds in
/// every real test, in place of a `TcpStream`.
#[derive(Debug)]
pub struct MockStream {
    read: Pipe,
    write: Pipe,
    counters: Arc<Mutex<Counters>>,
}

impl MockStream {
    fn counters(&self) -> MutexGuard<'_, Counters> {
        self.counters.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A handle onto this stream's counters and failure injection, taken before the stream is
    /// handed away to whatever will own it from here (a `Session`, a `Registry::subscribe`).
    pub fn control(&self) -> MockControl {
        MockControl {
            counters: Arc::clone(&self.counters),
            write: self.write.clone(),
        }
    }
}

impl AsyncRead for MockStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let failure = self.counters().fail_next_read.take();
        if let Some(kind) = failure {
            return Poll::Ready(Err(io::Error::from(kind)));
        }
        self.read.poll_read(cx, buf)
    }
}

impl AsyncWrite for MockStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let failure = self.counters().fail_next_write.take();
        if let Some(kind) = failure {
            return Poll::Ready(Err(io::Error::from(kind)));
        }
        match self.write.poll_write(cx, buf) {
            Poll::Ready(Ok(count)) => {
                self.counters().written.extend_from_slice(&buf[..count]);
                Poll::Ready(Ok(count))
            }
            other => other,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.counters().flushes += 1;
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.counters().shutdowns += 1;
        self.write.close_write();
        Poll::Ready(Ok(()))
    }
}

impl Drop for MockStream {
    fn drop(&mut self) {
        self.write.close_write();
        self.read.close_read();
    }
}

/// A handle onto one [`MockStream`]'s behaviour, taken via [`MockStream::control`] before the
/// stream itself is handed away.
#[derive(Debug)]
pub struct MockControl {
    counters: Arc<Mutex<Counters>>,
    write: Pipe,
}

impl MockControl {
    fn counters(&self) -> MutexGuard<'_, Counters> {
        self.counters.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Every byte the stream has written so far.
    pub fn written(&self) -> Vec<u8> {
        self.counters().written.clone()
    }

    /// How many times `poll_flush` has resolved.
    pub fn flushes(&self) -> usize {
        self.counters().flushes
    }

    /// How many times `poll_shutdown` has resolved.
    pub fn shutdowns(&self) -> usize {
        self.counters().shutdowns
    }

    /// Makes the next write fail with `kind`, instead of touching the pipe at all.
    pub fn fail_next_write(&self, kind: io::ErrorKind) {
        self.counters().fail_next_write = Some(kind);
    }

    /// Makes the next read fail with `kind`, instead of touching the pipe at all.
    pub fn fail_next_read(&self, kind: io::ErrorKind) {
        self.counters().fail_next_read = Some(kind);
    }

    /// Caps how many bytes the stream can have in flight before a write returns `Pending` -
    /// the deterministic replacement for pinning a kernel send buffer to its floor.
    pub fn set_capacity(&self, bytes: usize) {
        self.write.set_capacity(bytes);
    }
}

/// The client side of a mock connection - what a test drives directly, reading and writing the
/// wire protocol the way a real client would.
#[derive(Debug)]
pub struct MockClient {
    read: Pipe,
    write: Pipe,
}

impl AsyncRead for MockClient {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.read.poll_read(cx, buf)
    }
}

impl AsyncWrite for MockClient {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.write.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.write.close_write();
        Poll::Ready(Ok(()))
    }
}

impl Drop for MockClient {
    fn drop(&mut self) {
        self.write.close_write();
        self.read.close_read();
    }
}

/// No cap large enough to matter: [`mock_pair`] is for tests that want every write to succeed
/// outright.
const UNBOUNDED: usize = usize::MAX;

/// A connected in-memory pair: the client half a test drives, and the server half to hand to
/// [`Registry::subscribe`](crate::registry::Registry::subscribe) or wrap in a
/// [`Session`](crate::session::Session) directly.
pub fn mock_pair() -> (MockClient, MockStream) {
    mock_pair_with_capacity(UNBOUNDED)
}

/// The same, with both directions' queues capped at `capacity` bytes.
///
/// This is what makes the backpressure path reachable in a test: a write past the cap returns
/// `Pending` deterministically rather than racing however big the kernel's own buffer happens
/// to be.
pub fn mock_pair_with_capacity(capacity: usize) -> (MockClient, MockStream) {
    let c2s = Pipe::new(capacity);
    let s2c = Pipe::new(capacity);
    (
        MockClient {
            read: s2c.clone(),
            write: c2s.clone(),
        },
        MockStream {
            read: c2s,
            write: s2c,
            counters: Arc::new(Mutex::new(Counters::default())),
        },
    )
}

/// Identifies a connection accepted by a [`MockListener`], the way a `SocketAddr` would for a
/// real one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MockPeer(pub usize);

#[derive(Debug, Default)]
struct ListenerState {
    incoming: Mutex<VecDeque<(MockStream, MockPeer)>>,
    waker: Mutex<Option<Waker>>,
    next_id: AtomicUsize,
}

/// A [`Listener`] with nothing behind it but [`MockConnector::connect`] calls - for a test that
/// wants to drive [`crate::framed::accept`] or [`crate::server::serve`] without a real port.
#[derive(Debug)]
pub struct MockListener(Arc<ListenerState>);

/// The other end of a [`MockListener`]: makes connections for it to accept.
#[derive(Debug, Clone)]
pub struct MockConnector(Arc<ListenerState>);

impl MockListener {
    pub fn new() -> (Self, MockConnector) {
        let state = Arc::new(ListenerState::default());
        (Self(Arc::clone(&state)), MockConnector(state))
    }
}

impl MockConnector {
    /// Connects to the listener synchronously, so a test can connect after the server under
    /// test is already running its accept loop.
    pub fn connect(&self) -> MockClient {
        let (client, server) = mock_pair();
        let peer = MockPeer(self.0.next_id.fetch_add(1, Ordering::Relaxed));
        self.0
            .incoming
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push_back((server, peer));
        let woken = self
            .0
            .waker
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(waker) = woken {
            waker.wake();
        }
        client
    }
}

impl Listener for MockListener {
    type Stream = MockStream;
    type Peer = MockPeer;

    fn poll_accept(&self, cx: &mut Context<'_>) -> Poll<io::Result<(MockStream, MockPeer)>> {
        let mut incoming = self
            .0
            .incoming
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let popped = incoming.pop_front();
        drop(incoming);
        if let Some(connection) = popped {
            return Poll::Ready(Ok(connection));
        }
        *self.0.waker.lock().unwrap_or_else(PoisonError::into_inner) = Some(cx.waker().clone());
        Poll::Pending
    }
}

// ---------------------------------------------------------------------------------------------
// The client-side protocol helper the broadcaster and registry tests drive directly.
// ---------------------------------------------------------------------------------------------

/// The client half of a mock connection, with the reads a test needs off it.
///
/// Built by [`connected`], which also hands back the server half to give to a broadcaster.
#[derive(Debug)]
pub struct Client {
    sock: MockClient,
    buf: Vec<u8>,
}

impl Client {
    /// Reads the response header, which a broadcaster writes as the first thing on a session.
    ///
    /// # Errors
    ///
    /// The server's own reason, when the subscription was turned down.
    pub async fn accepted(&mut self) -> Result<(), Rejected> {
        deadline(framing::read_response(&mut self.sock, &mut self.buf))
            .await
            .expect("a response header arrives promptly")
            .expect("the header is well formed")
    }

    /// The next book off the wire.
    pub async fn next_book(&mut self) -> proto::BookUpdate {
        self.next_frame().await;
        proto::BookUpdate::decode(self.buf.as_slice()).expect("the frame is a BookUpdate")
    }

    /// Reads the frame right after the acceptance header and asserts it is the empty book -
    /// the snapshot every session opens with. See [`crate::session`]'s module doc.
    pub async fn opening_snapshot(&mut self) {
        let snapshot = self.next_book().await;
        assert!(
            snapshot.asks.is_empty() && snapshot.bids.is_empty(),
            "a session's first frame is always its opening snapshot, empty here because \
             nothing had been published yet, got {snapshot:?}"
        );
    }

    /// Asserts no frame arrives within a short, deterministic window.
    ///
    /// Meant for a `#[tokio::test(start_paused = true)]` test: the sleep this races against
    /// never elapses in real time, so this is instant rather than a real wait.
    pub async fn assert_quiet(&mut self) {
        let raced = tokio::time::timeout(
            Duration::from_millis(50),
            framing::read_frame(&mut self.sock, &mut self.buf),
        )
        .await;
        assert!(
            raced.is_err(),
            "expected no frame to arrive, but got {raced:?}"
        );
    }

    /// The next frame's body, left in this client's buffer and also returned.
    pub async fn next_frame(&mut self) -> Vec<u8> {
        deadline(framing::read_frame(&mut self.sock, &mut self.buf))
            .await
            .expect("a frame arrives promptly")
            .expect("the stream is healthy");
        self.buf.clone()
    }

    /// Waits for the server to close the connection, and fails if it sends anything more.
    pub async fn ended(&mut self) {
        let outcome = deadline(framing::read_frame(&mut self.sock, &mut self.buf))
            .await
            .expect("the stream ends promptly");
        assert!(
            matches!(outcome, Err(ReadFrameError::Closed)),
            "the server must close the connection rather than send more, got {outcome:?}"
        );
    }

    /// Sends bytes the protocol does not allow, which is one of the two ways a session ends.
    pub async fn misbehave(&mut self) {
        self.sock
            .write_all(b"unexpected")
            .await
            .expect("the connection is still open");
    }
}

/// Every test read is bounded: a hang here is a bug, not a slow machine.
async fn deadline<F: Future>(work: F) -> Result<F::Output, tokio::time::error::Elapsed> {
    tokio::time::timeout(Duration::from_secs(5), work).await
}

/// A connected pair: the client half a test drives, and the server half to hand to
/// [`Registry::subscribe`](crate::registry::Registry::subscribe).
///
/// Synchronous, unlike the real-socket `connected` this replaced: a mock pair needs no accept
/// to complete.
pub fn connected() -> (Client, MockStream) {
    let (client, server) = mock_pair();
    (
        Client {
            sock: client,
            buf: Vec::new(),
        },
        server,
    )
}

/// The same, with the server's write queue capped small enough to back up.
///
/// This is what makes the backpressure path reachable in a test: a broadcaster can write a
/// good many books before a client that never reads causes a single `Pending`, so without a
/// small cap the partial write - and therefore the splice hazard `Session::inflight` exists to
/// prevent - would simply never happen.
pub fn connected_congested() -> (Client, MockStream) {
    let (client, server) = mock_pair_with_capacity(32);
    (
        Client {
            sock: client,
            buf: Vec::new(),
        },
        server,
    )
}

/// A book with the given `(price, size)` levels on each side.
pub fn book(asks: &[(f64, f64)], bids: &[(f64, f64)]) -> IncrementalBook {
    let mut built = IncrementalBook::new();
    for &(price, size) in asks {
        built.update_ask(positive(price), positive(size));
    }
    for &(price, size) in bids {
        built.update_bid(positive(price), positive(size));
    }
    built
}

fn positive(value: f64) -> PositiveF64 {
    PositiveF64::new(value).expect("test prices and sizes are positive")
}
