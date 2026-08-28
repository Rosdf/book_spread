//! The seam between the accept loop and whatever actually listens.
//!
//! [`crate::server::serve`] and [`crate::framed::accept`] are generic over [`Listener`] rather
//! than hard-wired to [`TcpListener`], which is what lets [`crate::test_util`] swap in a mock
//! that never touches a real socket. `pub` rather than `pub(crate)`: [`Listener`] appears in
//! `serve`'s public signature, and `private_bounds` is denied workspace-wide.

use std::fmt::Debug;
use std::io;
use std::net::SocketAddr;
use std::task::{Context, Poll, ready};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};

/// Something that accepts connections, each yielding a byte stream and a peer identity to log.
pub(crate) trait Listener: Send + Sync + 'static {
    /// One accepted connection's byte stream.
    type Stream: AsyncRead + AsyncWrite + Unpin + Send + 'static;
    /// What identifies the peer on the other end, for logging.
    type Peer: Debug + Send + 'static;

    /// Polls for the next accepted connection.
    ///
    /// # Errors
    ///
    /// Whatever the underlying transport reports.
    fn poll_accept(&self, cx: &mut Context<'_>) -> Poll<io::Result<(Self::Stream, Self::Peer)>>;
}

impl Listener for TcpListener {
    type Stream = TcpStream;
    type Peer = SocketAddr;

    fn poll_accept(&self, cx: &mut Context<'_>) -> Poll<io::Result<(TcpStream, SocketAddr)>> {
        let (sock, peer) = ready!(Self::poll_accept(self, cx))?;
        // Nagle would hold a ~400-byte book back waiting for company, which for a market-data
        // feed is tens of milliseconds of latency bought for nothing. Set here, on the way in,
        // so no socket is ever handed onward without it.
        if let Err(err) = sock.set_nodelay(true) {
            tracing::warn!(?peer, %err, "could not disable Nagle on an accepted connection");
        }
        Poll::Ready(Ok((sock, peer)))
    }
}

/// Resolves to the next accepted connection.
///
/// A free function rather than an inherent method on [`Listener`] because a trait method
/// cannot return `impl Future` without erasing `Self`, and `poll_accept` already gives
/// `select!` everything it needs directly.
pub(crate) async fn accept<L: Listener>(listener: &L) -> io::Result<(L::Stream, L::Peer)> {
    std::future::poll_fn(|cx| listener.poll_accept(cx)).await
}

/// In-memory sockets, and a listener with nothing behind it.
///
/// A session is generic over its transport precisely so a test never has to be a client on a
/// real loopback port to observe it: `Session<MockStream>` behaves exactly as
/// `Session<TcpStream>` does, and the pipe below gives a test full control over what the
/// kernel would otherwise decide - a partial write, a stalled peer, an exact byte-for-byte
/// close - by capping each direction's queue where a real one is at the kernel's whim. [`MockListener`] does the same one layer out, for
/// [`crate::framed::accept`].
///
/// Here rather than in [`crate::test_util`] because these mock what this module defines. The
/// doubles the end-to-end test needs are the ones that live in `test_util`; these are for
/// unit tests, so they are `cfg(test)` and never leave the crate.
#[cfg(test)]
pub(crate) mod mock {
    use super::Listener;
    use std::collections::VecDeque;
    use std::io;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
    use std::task::{Context, Poll, Waker};
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

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
        flushes: usize,
    }

    /// The server side of a mock connection - what a [`Session`](crate::session::Session) holds in
    /// every real test, in place of a `TcpStream`.
    #[derive(Debug)]
    pub(crate) struct MockStream {
        read: Pipe,
        write: Pipe,
        counters: Arc<Mutex<Counters>>,
    }

    impl MockStream {
        fn counters(&self) -> MutexGuard<'_, Counters> {
            self.counters.lock().unwrap_or_else(PoisonError::into_inner)
        }

        /// A handle onto this stream's counters, taken before the stream is handed away to
        /// whatever will own it from here (a `Session`, or the registry).
        pub(crate) fn control(&self) -> MockControl {
            MockControl {
                counters: Arc::clone(&self.counters),
            }
        }
    }

    impl AsyncRead for MockStream {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            self.read.poll_read(cx, buf)
        }
    }

    impl AsyncWrite for MockStream {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.write.poll_write(cx, buf)
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.counters().flushes += 1;
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
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
    pub(crate) struct MockControl {
        counters: Arc<Mutex<Counters>>,
    }

    impl MockControl {
        fn counters(&self) -> MutexGuard<'_, Counters> {
            self.counters.lock().unwrap_or_else(PoisonError::into_inner)
        }

        /// How many times `poll_flush` has resolved.
        pub(crate) fn flushes(&self) -> usize {
            self.counters().flushes
        }
    }

    /// The client side of a mock connection - what a test drives directly, reading and writing the
    /// wire protocol the way a real client would.
    #[derive(Debug)]
    pub(crate) struct MockClient {
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
    /// the registry, or to wrap in a [`Session`](crate::session::Session) directly.
    pub(crate) fn mock_pair() -> (MockClient, MockStream) {
        mock_pair_with_capacity(UNBOUNDED)
    }

    /// The same, with both directions' queues capped at `capacity` bytes.
    ///
    /// This is what makes the backpressure path reachable in a test: a write past the cap returns
    /// `Pending` deterministically rather than racing however big the kernel's own buffer happens
    /// to be.
    pub(crate) fn mock_pair_with_capacity(capacity: usize) -> (MockClient, MockStream) {
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
    pub(crate) struct MockPeer(pub(crate) usize);

    #[derive(Debug, Default)]
    struct ListenerState {
        incoming: Mutex<VecDeque<(MockStream, MockPeer)>>,
        waker: Mutex<Option<Waker>>,
        next_id: AtomicUsize,
    }

    /// A [`Listener`] with nothing behind it but [`MockConnector::connect`] calls - for a test that
    /// wants to drive [`crate::framed::accept`] or [`crate::server::serve`] without a real port.
    #[derive(Debug)]
    pub(crate) struct MockListener(Arc<ListenerState>);

    /// The other end of a [`MockListener`]: makes connections for it to accept.
    #[derive(Debug, Clone)]
    pub(crate) struct MockConnector(Arc<ListenerState>);

    impl MockListener {
        pub(crate) fn new() -> (Self, MockConnector) {
            let state = Arc::new(ListenerState::default());
            (Self(Arc::clone(&state)), MockConnector(state))
        }
    }

    impl MockConnector {
        /// Connects to the listener synchronously, so a test can connect after the server under
        /// test is already running its accept loop.
        pub(crate) fn connect(&self) -> MockClient {
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
}
