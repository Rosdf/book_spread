//! What a broadcaster can do to one attached client, with no mention of HTTP/2.
//!
//! The transport is gRPC, and gRPC is HTTP/2, but nothing outside [`crate::h2`] says so. Three
//! traits stand between the fan-out and the wire, for the same reason [`Listener`] stands
//! between the accept loop and `TcpListener`: a test that wants to watch one book reach three
//! clients should not have to run three HPACK handshakes to see it.
//!
//! - [`Handshaker`] turns an accepted byte stream into a client that has asked for a symbol.
//! - [`ClientHandshake`] answers that request - with a stream, or with a refusal.
//! - [`ClientSink`] is the answered client: the thing a broadcaster writes books into.
//!
//! The generic parameter that already threads through `RegistryTx`, `Join`, `Refused`,
//! `Broadcaster` and `Session` used to mean "the byte stream"; it now means "the client". The
//! byte stream is still generic too, one layer further out, so [`crate::transport::mock`] is
//! unchanged.
//!
//! # Nothing here is async
//!
//! Except [`ClientHandshake::reject`], which happens once per refused connection and never on
//! the hot path. Everything a broadcaster does is a poll function, because a broadcaster polls
//! every one of its clients from its own `select!` rather than giving any of them a task - see
//! [`crate::broadcast`]. An `async fn` here would need a task to be driven from, which is the
//! one thing this design does not have.
//!
//! [`Listener`]: crate::transport::Listener

use bytes::Bytes;
use md_proto::md::v1::SubscribeBookRequest;
use md_wire::grpc::Rejected;
use std::fmt::Debug;
use std::future::Future;
use std::task::Context;

/// Where one payload ended up.
///
/// [`Full`](Sent::Full) is the whole of the backpressure policy. The caller does *not* hold the
/// payload: it leaves its epoch behind and offers whatever is current the next time round, so a
/// client that falls behind loses the books it missed rather than receiving them late. A stale
/// book is worth less than the current one, which is why this is the right answer for market
/// data and would not be for much else.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum Sent {
    /// Accepted whole. Payloads are never split across calls, so there is no partial state to
    /// finish before the next one starts.
    Queued,
    /// No room right now. The waker on the `Context` passed in is what comes back to this.
    Full,
    /// The client is gone.
    Ended,
}

/// How far along a client is, as [`ClientSink::poll_progress`] reports it.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum State {
    /// Attached and being written to.
    Running,
    /// Finished, whether by hanging up, by failing, or by having been told the stream is over.
    Ended,
}

impl State {
    pub(crate) fn is_ended(self) -> bool {
        self == Self::Ended
    }
}

/// One answered client: the far end of a stream a broadcaster is writing books into.
///
/// # What TLS would cost
///
/// The byte stream underneath is `TcpStream` everywhere this is instantiated for real. A TLS
/// stream would live inside it - a `rustls::ServerConnection` and its own encrypt-then-write
/// buffer - because a TLS session buffers and encrypts per client. That per-client copy is
/// the same one this fan-out exists to avoid, so turning TLS on would hand a meaningful part
/// of the win back; better to know that here than to discover it after the fact.
pub(crate) trait ClientSink: Debug + Send + 'static {
    /// Offers `payload` whole, or says why it could not be taken.
    ///
    /// Takes `&Bytes` rather than `Bytes` so a refused offer costs nothing: the refcount is
    /// only bumped on [`Sent::Queued`]. That is what lets a broadcaster offer the same buffer
    /// to every client in turn without cloning for the ones that have no room.
    fn poll_send(&mut self, cx: &mut Context<'_>, payload: &Bytes) -> Sent;

    /// Drives this client's I/O, and registers interest in its hanging up.
    ///
    /// Called for every client on every lap of the broadcaster's loop, so the common case -
    /// nothing to write, nothing to read - must be cheap and must leave a waker behind.
    fn poll_progress(&mut self, cx: &mut Context<'_>) -> State;

    /// Queues the end of the stream and the status that explains it.
    ///
    /// Only queues: [`poll_progress`](ClientSink::poll_progress) is what flushes it, and the
    /// sink reaches [`State::Ended`] once it has. Splitting it that way is what lets a
    /// broadcaster close every one of its clients concurrently under a single deadline
    /// instead of one timeout each.
    fn begin_finish(&mut self, rejected: &Rejected);
}

/// A client that has asked for a symbol and has not been answered yet.
///
/// The answer is the broadcaster's to give - it is the only thing that knows whether the
/// venue accepted the subscribe - so this travels through the registry unanswered, and every
/// path out of the registry ends in exactly one of [`accept`](ClientHandshake::accept) or
/// [`reject`](ClientHandshake::reject).
pub(crate) trait ClientHandshake: Debug + Send + 'static {
    type Sink: ClientSink;

    /// Answers with response headers, and begins the stream.
    fn accept(self) -> Self::Sink;

    /// Answers with a refusal instead. Nothing follows it.
    ///
    /// Best effort, and bounded: a client that will not read its own rejection does not get to
    /// hold a shutting-down broadcaster up. It sees a closed connection instead, which it has
    /// to handle anyway.
    fn reject(self, rejected: Rejected) -> impl Future<Output = ()> + Send;
}

/// Turns an accepted byte stream into a client waiting for an answer.
///
/// The seam [`crate::framed::accept`] is generic over, the way it is already generic over
/// [`Listener`](crate::transport::Listener). Bundled as a value rather than a free function
/// because a real one holds settings - frame size limits, header list limits - that are built
/// once and shared by every connection.
pub(crate) trait Handshaker<S>: Debug + Send + Sync + 'static {
    type Client: ClientHandshake;

    /// Reads one client's request, up to but not including the answer.
    ///
    /// # Errors
    ///
    /// [`HandshakeError::Refused`] when the request arrived but is not one this server will
    /// serve, in which case the client is handed back so the refusal can be written on it.
    /// [`HandshakeError::Lost`] when there is nothing to answer on, which is not worth more
    /// than a line in the log.
    fn handshake(
        &self,
        sock: S,
    ) -> impl Future<Output = Result<(SubscribeBookRequest, Self::Client), HandshakeError<Self::Client>>>
    + Send;
}

/// Why a handshake produced no request to serve.
#[derive(Debug)]
pub(crate) enum HandshakeError<C> {
    /// The request is not one this server will serve. The client is still answerable.
    Refused(C, Rejected),
    /// The connection failed before there was anything to answer on.
    Lost,
}

/// A client with nothing behind it: the far end of the fan-out, as a test drives it.
///
/// A real client is a whole HTTP/2 connection, and a test that only wants to watch one book
/// reach three of them should not have to run three HPACK handshakes to see it. So the tests
/// for the broadcaster, the registry and the accept loop mock at these traits instead, and the
/// transport has its own tests over a real `h2::client` - see [`crate::grpc`].
///
/// It is also the more precise instrument. Backpressure here is [`MockPeer::stall`], a flag a
/// test sets and clears, rather than a byte cap on a pipe that has to be reasoned back to the
/// number of books it holds.
///
/// Here rather than in [`crate::test_util`] because these mock what this module defines, the
/// same way [`crate::transport::mock`] mocks what `transport` defines.
#[cfg(test)]
pub(crate) mod mock {
    use super::{ClientHandshake, ClientSink, HandshakeError, Handshaker, Sent, State};
    use bytes::Bytes;
    use md_proto::md::v1::SubscribeBookRequest;
    use md_wire::grpc::Rejected;
    use std::collections::VecDeque;
    use std::future::{Future, pending, poll_fn};
    use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;

    /// Everything one mock client is, shared between the half a broadcaster holds and the half
    /// a test drives.
    #[derive(Debug, Default)]
    struct Shared {
        /// The broadcaster's answer to the subscription: `Ok` once it has accepted, `Err` with
        /// the reason once it has refused. `None` until it has done either.
        answer: Option<Result<(), Rejected>>,
        /// Payloads queued on the stream, oldest first. Whole `Bytes` handles, so a test can
        /// assert two clients were given the *same* buffer and not merely equal bytes.
        sent: VecDeque<Bytes>,
        /// The status that ended the stream, once one has been queued.
        finished: Option<Rejected>,
        /// `finished` has been flushed, which is what puts the sink in [`State::Ended`].
        flushed: bool,
        /// While set, every offer comes back [`Sent::Full`] - a client whose flow-control
        /// window is shut.
        stalled: bool,
        /// The peer has gone: hung up, reset, or simply vanished.
        gone: bool,
        /// The broadcaster's waker, so `stall`/`hang_up` on the test's side reach it.
        server: Option<Waker>,
        /// The test's waker, so a delivered payload reaches whatever is awaiting one.
        client: Option<Waker>,
    }

    impl Shared {
        fn wake_server(&mut self) {
            if let Some(waker) = self.server.take() {
                waker.wake();
            }
        }

        fn wake_client(&mut self) {
            if let Some(waker) = self.client.take() {
                waker.wake();
            }
        }
    }

    #[derive(Debug, Clone, Default)]
    struct Handle(Arc<Mutex<Shared>>);

    impl Handle {
        fn lock(&self) -> MutexGuard<'_, Shared> {
            self.0.lock().unwrap_or_else(PoisonError::into_inner)
        }
    }

    /// The half a broadcaster is handed: a client that has asked for a symbol.
    #[derive(Debug)]
    pub(crate) struct MockClient(Handle);

    impl ClientHandshake for MockClient {
        type Sink = MockSink;

        fn accept(self) -> Self::Sink {
            let mut shared = self.0.lock();
            shared.answer = Some(Ok(()));
            shared.wake_client();
            drop(shared);
            MockSink(self.0)
        }

        async fn reject(self, rejected: Rejected) {
            let mut shared = self.0.lock();
            shared.answer = Some(Err(rejected));
            shared.gone = true;
            shared.wake_client();
        }
    }

    /// The half a broadcaster writes books into.
    #[derive(Debug)]
    pub(crate) struct MockSink(Handle);

    impl ClientSink for MockSink {
        fn poll_send(&mut self, cx: &mut Context<'_>, payload: &Bytes) -> Sent {
            let mut shared = self.0.lock();
            if shared.gone {
                return Sent::Ended;
            }
            if shared.stalled {
                // The same contract the real sink has: the waker registered here is what
                // brings the broadcaster back, and the payload is deliberately not held.
                shared.server = Some(cx.waker().clone());
                return Sent::Full;
            }
            shared.sent.push_back(payload.clone());
            shared.wake_client();
            Sent::Queued
        }

        fn poll_progress(&mut self, cx: &mut Context<'_>) -> State {
            let mut shared = self.0.lock();
            if shared.gone {
                return State::Ended;
            }
            if shared.finished.is_some() {
                // One poll to flush the trailers, exactly as the real sink needs one to put
                // them on the wire.
                shared.flushed = true;
                shared.wake_client();
                return State::Ended;
            }
            shared.server = Some(cx.waker().clone());
            State::Running
        }

        fn begin_finish(&mut self, rejected: &Rejected) {
            let mut shared = self.0.lock();
            shared.finished = Some(rejected.clone());
            shared.wake_client();
        }
    }

    /// The half a test drives, and asserts on.
    #[derive(Debug)]
    pub(crate) struct MockPeer {
        handle: Handle,
        /// Set by [`MockPeer::vanish`]: this peer's `Drop` says nothing, the way a host that
        /// disappears without a `FIN` says nothing.
        silent: bool,
    }

    impl MockPeer {
        /// The broadcaster's answer to this client's subscription.
        ///
        /// # Errors
        ///
        /// The server's own reason, when the subscription was turned down.
        pub(crate) async fn accepted(&self) -> Result<(), Rejected> {
            deadline(poll_fn(|cx| {
                let mut shared = self.handle.lock();
                if let Some(answer) = shared.answer.clone() {
                    return Poll::Ready(answer);
                }
                shared.client = Some(cx.waker().clone());
                Poll::Pending
            }))
            .await
            .expect("an answer arrives promptly")
        }

        /// The next payload this client was given, prefix and all.
        pub(crate) async fn next_frame(&self) -> Bytes {
            deadline(poll_fn(|cx| {
                let mut shared = self.handle.lock();
                if let Some(frame) = shared.sent.pop_front() {
                    return Poll::Ready(frame);
                }
                assert!(
                    shared.finished.is_none(),
                    "expected a book, but the stream ended: {:?}",
                    shared.finished
                );
                shared.client = Some(cx.waker().clone());
                Poll::Pending
            }))
            .await
            .expect("a book arrives promptly")
        }

        /// The next payload, decoded past its gRPC message header.
        pub(crate) async fn next_book(&self) -> md_proto::md::v1::BookUpdate {
            let frame = self.next_frame().await;
            let header = frame[..md_wire::grpc::MESSAGE_PREFIX]
                .try_into()
                .expect("a frame carries a whole message header");
            let body_len =
                md_wire::grpc::message_len(&header).expect("the header is well formed");
            assert_eq!(
                body_len,
                frame.len() - md_wire::grpc::MESSAGE_PREFIX,
                "the header must describe the body that follows it"
            );

            <md_proto::md::v1::BookUpdate as prost::Message>::decode(
                &frame[md_wire::grpc::MESSAGE_PREFIX..],
            )
            .expect("the message is a BookUpdate")
        }

        /// Reads the first message and asserts it is the empty book - the snapshot every
        /// stream opens with. See [`crate::broadcast::session`]'s module doc.
        pub(crate) async fn opening_snapshot(&self) {
            let snapshot = self.next_book().await;
            assert!(
                snapshot.asks.is_empty() && snapshot.bids.is_empty(),
                "a stream's first message is always its opening snapshot, empty here because \
                 nothing had been published yet, got {snapshot:?}"
            );
        }

        /// Asserts nothing arrives within a short, deterministic window.
        ///
        /// Meant for a `#[tokio::test(start_paused = true)]` test: the sleep this races
        /// against never elapses in real time, so this is instant rather than a real wait.
        pub(crate) async fn assert_quiet(&self) {
            let raced = tokio::time::timeout(Duration::from_millis(50), self.next_frame()).await;
            assert!(
                raced.is_err(),
                "expected nothing to arrive, but got {raced:?}"
            );
        }

        /// Waits for the stream to end, and hands back the status that ended it.
        pub(crate) async fn ended(&self) -> Rejected {
            deadline(poll_fn(|cx| {
                let mut shared = self.handle.lock();
                if let Some(why) = shared.finished.clone()
                    && shared.flushed
                {
                    return Poll::Ready(why);
                }
                shared.client = Some(cx.waker().clone());
                Poll::Pending
            }))
            .await
            .expect("the stream ends promptly")
        }

        /// Shuts this client's flow-control window: every offer from here is [`Sent::Full`]
        /// until [`resume`](MockPeer::resume).
        pub(crate) fn stall(&self) {
            self.handle.lock().stalled = true;
        }

        /// Reopens the window, and wakes the broadcaster to use it.
        pub(crate) fn resume(&self) {
            let mut shared = self.handle.lock();
            shared.stalled = false;
            shared.wake_server();
        }

        /// Resets the stream, the way a client that has seen enough does. Wakes the
        /// broadcaster, as a real reset does by arriving on the connection.
        pub(crate) fn reset(&self) {
            let mut shared = self.handle.lock();
            shared.gone = true;
            shared.wake_server();
        }

        /// Goes away without waking anything - a host that vanished rather than one that hung
        /// up.
        ///
        /// The distinction is the whole point of the sweep. A hang-up wakes the broadcaster,
        /// which notices on its very next poll; a host that simply disappears leaves the
        /// connection in a state nobody has been told about, so the *next poll* is the earliest
        /// this can be noticed - and only the sweep schedules one.
        pub(crate) fn vanish(mut self) {
            self.silent = true;
            self.handle.lock().gone = true;
        }
    }

    /// Hanging up, for the ordinary case of a client that is simply done.
    impl Drop for MockPeer {
        fn drop(&mut self) {
            if !self.silent {
                self.reset();
            }
        }
    }

    /// Every test wait is bounded: a hang here is a bug, not a slow machine.
    async fn deadline<F: Future>(work: F) -> Result<F::Output, tokio::time::error::Elapsed> {
        tokio::time::timeout(Duration::from_secs(5), work).await
    }

    /// A connected pair: the half a test drives, and the half to hand to the registry.
    pub(crate) fn connected() -> (MockPeer, MockClient) {
        let handle = Handle::default();
        (
            MockPeer {
                handle: handle.clone(),
                silent: false,
            },
            MockClient(handle),
        )
    }

    /// A [`Handshaker`] that reads no bytes at all: a test scripts what each connection asks
    /// for, in accept order.
    ///
    /// This is what lets the accept loop's own tests use [`crate::transport::mock`] without
    /// speaking HTTP/2. The accept loop's job is accepting, reaping and stopping, none of which
    /// is about what a request says - and one of its two tests is specifically about a client
    /// that says *nothing*, which is [`Script::says_nothing`] here.
    #[derive(Debug)]
    pub(crate) struct MockHandshaker(Arc<Mutex<Script>>);

    /// What the next connections will ask for. `None` is a connection that never finishes its
    /// handshake at all.
    #[derive(Debug, Default)]
    pub(crate) struct Script {
        answers: VecDeque<Option<SubscribeBookRequest>>,
        peers: VecDeque<MockPeer>,
        waiting: Option<Waker>,
    }

    /// A test's way in to a [`MockHandshaker`]: what the next connections say, and the peers
    /// they turn into.
    #[derive(Debug, Clone)]
    pub(crate) struct ScriptControl(Arc<Mutex<Script>>);

    impl ScriptControl {
        /// The next connection accepted asks for this pair.
        pub(crate) fn asks_for(&self, venue: &str, symbol: &str) {
            self.lock().answers.push_back(Some(SubscribeBookRequest {
                pairs: vec![md_proto::md::v1::Pair {
                    venue: venue.to_owned(),
                    symbol: symbol.to_owned(),
                }],
            }));
        }

        /// The next connection accepted connects and then says nothing, ever.
        pub(crate) fn says_nothing(&self) {
            self.lock().answers.push_back(None);
        }

        /// The peer of the next connection to finish its handshake.
        pub(crate) async fn next_peer(&self) -> MockPeer {
            deadline(poll_fn(|cx| {
                let mut script = self.lock();
                if let Some(peer) = script.peers.pop_front() {
                    return Poll::Ready(peer);
                }
                script.waiting = Some(cx.waker().clone());
                Poll::Pending
            }))
            .await
            .expect("a connection is handshaken promptly")
        }

        fn lock(&self) -> MutexGuard<'_, Script> {
            self.0.lock().unwrap_or_else(PoisonError::into_inner)
        }
    }

    pub(crate) fn scripted() -> (MockHandshaker, ScriptControl) {
        let script = Arc::new(Mutex::new(Script::default()));
        (MockHandshaker(Arc::clone(&script)), ScriptControl(script))
    }

    impl<S: Send + 'static> Handshaker<S> for MockHandshaker {
        type Client = MockClient;

        async fn handshake(
            &self,
            sock: S,
        ) -> Result<(SubscribeBookRequest, Self::Client), HandshakeError<Self::Client>> {
            // Held for the life of the handshake, the way a real one holds the connection.
            let held = sock;
            let scripted = {
                let mut script = self.0.lock().unwrap_or_else(PoisonError::into_inner);
                script.answers.pop_front().flatten()
            };
            let Some(request) = scripted else {
                // A client that connected and then said nothing. The accept loop's timeout,
                // or its shutdown, is what ends this.
                pending::<()>().await;
                unreachable!("`pending` never resolves");
            };

            let (peer, client) = connected();
            let mut script = self.0.lock().unwrap_or_else(PoisonError::into_inner);
            script.peers.push_back(peer);
            if let Some(waker) = script.waiting.take() {
                waker.wake();
            }
            drop(script);
            drop(held);
            Ok((request, client))
        }
    }
}
