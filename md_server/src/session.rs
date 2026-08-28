//! One client's socket, as the broadcaster holds it.
//!
//! This is what replaces the per-client task the gRPC transport needed. A broadcaster owns
//! the write half of every socket attached to its symbol outright - one connection per
//! subscription is what makes that possible - so a book goes from the encoder to the kernel
//! with no channel and no task hop in between.
//!
//! # Backpressure
//!
//! Non-blocking writes mean a frame can be half written, and a half-written frame *must* be
//! finished before a newer one starts or the peer sees two messages spliced together.
//! Newest-only therefore applies to the queue, not to the frame in flight: [`Session::epoch`]
//! is overwritten freely, [`Session::inflight`] is not. A client that falls behind loses the
//! books it missed, which is the right answer for market data - a stale book is worth less
//! than the current one, and HTTP/2 flow control would rather block the whole connection than
//! drop anything.
//!
//! # The opening snapshot
//!
//! A session's [`inflight`](Session::inflight) starts with the acceptance header, and its
//! `epoch` starts at 0 - the same epoch [`crate::broadcast::Ctx`] seeds at construction from
//! the reader's current book. So the first frame a client reads after the header is always
//! that snapshot, empty when nothing has been published yet. That is not a special case: an
//! empty book is already this wire's resync signal, and "nothing published yet" is
//! indistinguishable from - and no different in meaning to - a connector that is resyncing.

use bytes::Bytes;
use md_wire::framing::LENGTH_PREFIX;
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// The response header for an accepted subscription: a zero-length frame.
///
/// Seeded as the first thing in flight on a new session rather than written by the
/// broadcaster directly, so the accept goes out through the same state machine as every book
/// and needs no special case for a socket that will not take four bytes right now.
const ACCEPTED: Bytes = Bytes::from_static(&[0; LENGTH_PREFIX]);

/// Bytes read into a throwaway buffer just to tell "nothing" from "something", never kept.
const SCRATCH_LEN: usize = 32;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum State {
    Running,
    NeedsFlush,
    Ended,
}

impl State {
    pub(crate) fn is_ended(self) -> bool {
        self == Self::Ended
    }
}

/// A frame part-written on the socket, and therefore untouchable until it is finished.
#[derive(Debug)]
struct Inflight {
    frame: Bytes,
    written: usize,
    /// False only for the acceptance header, which is not a book and so does not settle
    /// [`Session::epoch`] when it finishes.
    is_book: bool,
}

pub(crate) trait SessionCtx {
    fn payload_for_epoch(&self, epoch: u64) -> Option<&[u8]>;
    fn current_payload(&self) -> Bytes;
    fn return_buffer(&mut self, buffer: Bytes);
}

/// One attached client: its socket, the frame being written, and the newest one waiting.
///
/// # What TLS would cost
///
/// `S` is `TcpStream` everywhere this is instantiated for real, but the bound is
/// `AsyncRead + AsyncWrite` rather than the concrete type on purpose: the shape it leaves room
/// for is the point. A TLS stream would live *inside* `S` - a `rustls::ServerConnection` and
/// its own encrypt-then-write buffer - because a TLS session buffers and encrypts per client.
/// That per-client copy is the same copy this whole transport rework removed from tonic's
/// codec. Turning TLS on would hand a meaningful part of the win back; better to know that
/// here than to discover it after the fact.
#[derive(Debug)]
pub(crate) struct Session<S> {
    sock: S,
    inflight: Option<Inflight>,
    state: State,
    epoch: u64,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Session<S> {
    /// Wraps a freshly accepted socket, with the acceptance header already in flight.
    pub(crate) fn new(sock: S) -> Self {
        Self {
            sock,
            inflight: Some(Inflight {
                frame: ACCEPTED,
                written: 0,
                is_book: false,
            }),
            state: State::Running,
            epoch: 0,
        }
    }

    pub(crate) fn ended(&self) -> bool {
        self.state.is_ended()
    }

    /// Makes `frame` the newest book waiting, and writes what the socket will take.
    ///
    /// Whatever was queued and unsent is dropped: a client that has not kept up wants the
    /// current book, not the one before it.
    pub(crate) fn deliver(
        &mut self,
        new_epoch: u64,
        cx: &mut Context<'_>,
        session_ctx: &mut impl SessionCtx,
    ) {
        self.epoch = new_epoch;
        self.pump(cx, session_ctx);
    }

    /// Writes until the socket is full, the queue is empty, or the session is finished, then
    /// flushes once there is nothing left queued.
    ///
    /// A `Pending` from `poll_write` leaves whatever is left in `inflight` for the waker it
    /// just registered to wake this back up - see [`Session::poll_progress`].
    pub(crate) fn pump(&mut self, async_cx: &mut Context<'_>, session_ctx: &mut impl SessionCtx) {
        loop {
            if self.state.is_ended() {
                return;
            }

            let frame = match self.inflight.as_ref() {
                None => match session_ctx.payload_for_epoch(self.epoch) {
                    None => break,
                    Some(frame) => frame,
                },
                Some(inflight) => &inflight.frame[inflight.written..],
            };

            match Pin::new(&mut self.sock).poll_write(async_cx, frame) {
                Poll::Ready(Ok(count)) if count > 0 => {
                    let fully_written = frame.len() == count;

                    if fully_written {
                        self.state = State::NeedsFlush;
                        if let Some(inflight) = self.inflight.take() {
                            if inflight.is_book {
                                self.epoch += 1;
                            }
                            session_ctx.return_buffer(inflight.frame);
                        } else {
                            self.epoch += 1;
                        }
                    } else if let Some(inflight) = &mut self.inflight {
                        inflight.written += count;
                    } else {
                        self.inflight = Some(Inflight {
                            frame: session_ctx.current_payload(),
                            written: count,
                            is_book: true,
                        });
                    }
                }
                // A zero-length write on a non-empty buffer is the peer having gone, same as
                // a write that failed outright.
                Poll::Ready(_) => {
                    self.state = State::Ended;
                    return;
                }
                // Full: whatever is left stays in flight, and the waker just registered is
                // what comes back to it.
                Poll::Pending => return,
            }
        }

        if self.state == State::NeedsFlush
            && Pin::new(&mut self.sock).poll_flush(async_cx).is_ready()
        {
            self.state = State::Running;
        }
    }

    /// Both halves are polled from the broadcaster's own `select!` rather than from a task of
    /// their own: end-of-stream interest, and a write poll that both drains the queue and
    /// registers for more room - normally none of it, so this normally registers one read
    /// interest per session and returns.
    ///
    /// A client sends its request and then nothing at all, so anything arriving afterwards is
    /// either the hang-up this is watching for or a protocol violation; both end the session.
    /// A clean hang-up gets a best-effort `poll_shutdown` back, its result discarded - the
    /// client is already gone, so there is nothing to do with a failure. The one case this
    /// reads too strictly is a client that shuts down its write half while still reading -
    /// nothing in this protocol asks it to, and treating that as "gone" is better than never
    /// noticing a real disconnect.
    pub(crate) fn poll_progress(
        &mut self,
        cx: &mut Context<'_>,
        session_ctx: &mut impl SessionCtx,
    ) -> State {
        if self.state.is_ended() {
            return State::Ended;
        }

        let mut scratch = const { [MaybeUninit::<u8>::uninit(); SCRATCH_LEN] };
        let mut read_buf = ReadBuf::uninit(&mut scratch);
        match Pin::new(&mut self.sock).poll_read(cx, &mut read_buf) {
            Poll::Ready(Ok(())) if read_buf.filled().is_empty() => {
                let _ = Pin::new(&mut self.sock).poll_shutdown(cx);
                self.state = State::Ended;
                return State::Ended;
            }
            Poll::Ready(Ok(())) => {
                // The client sent data after its subscription request - a protocol violation,
                // ended the same way a hang-up is.
                self.state = State::Ended;
                return State::Ended;
            }
            Poll::Ready(Err(_)) => {
                self.state = State::Ended;
                return State::Ended;
            }
            Poll::Pending => {}
        }

        self.pump(cx, session_ctx);
        self.state
    }
}

/// The client half of a mock connection, driven by the broadcaster and registry tests.
///
/// Here rather than in [`crate::test_util`] because this is the peer a [`Session`] writes to:
/// it reads the acceptance header, the opening snapshot and each book frame, and watches the
/// close. `cfg(test)` and crate-private - the end-to-end test is a real client on a real
/// socket and needs none of it.
#[cfg(test)]
pub(crate) mod peer {
    use crate::transport::mock::{MockClient, MockStream, mock_pair, mock_pair_with_capacity};
    use md_proto::md::v1 as proto;
    use md_wire::framing::{self, ReadFrameError, Rejected};
    use prost::Message as _;
    use std::future::Future;
    use std::time::Duration;
    use tokio::io::AsyncWriteExt as _;

    /// The client half of a mock connection, with the reads a test needs off it.
    ///
    /// Built by [`connected`], which also hands back the server half to give to a broadcaster.
    #[derive(Debug)]
    pub(crate) struct Client {
        sock: MockClient,
        buf: Vec<u8>,
    }

    impl Client {
        /// Wraps a socket a test got some other way than [`connected`] - from
        /// [`MockConnector::connect`](crate::transport::mock::MockConnector::connect), say,
        /// where the server half went to an accept loop rather than straight to a
        /// broadcaster.
        pub(crate) fn from_socket(sock: MockClient) -> Self {
            Self {
                sock,
                buf: Vec::new(),
            }
        }

        /// Sends the subscribe request a real client opens with. Only needed by a test that
        /// goes through [`crate::framed`]; every other one is handed to a broadcaster with
        /// the handshake already done.
        pub(crate) async fn request(&mut self, venue: &str, symbol: &str) {
            framing::write_request(
                &mut self.sock,
                &proto::SubscribeBookRequest {
                    pairs: vec![proto::Pair {
                        venue: venue.to_owned(),
                        symbol: symbol.to_owned(),
                    }],
                },
            )
            .await
            .expect("the request is written");
        }

        /// Reads the response header, which a broadcaster writes as the first thing on a session.
        ///
        /// # Errors
        ///
        /// The server's own reason, when the subscription was turned down.
        pub(crate) async fn accepted(&mut self) -> Result<(), Rejected> {
            deadline(framing::read_response(&mut self.sock, &mut self.buf))
                .await
                .expect("a response header arrives promptly")
                .expect("the header is well formed")
        }

        /// The next book off the wire.
        pub(crate) async fn next_book(&mut self) -> proto::BookUpdate {
            self.next_frame().await;
            proto::BookUpdate::decode(self.buf.as_slice()).expect("the frame is a BookUpdate")
        }

        /// Reads the frame right after the acceptance header and asserts it is the empty book -
        /// the snapshot every session opens with. See [`crate::session`]'s module doc.
        pub(crate) async fn opening_snapshot(&mut self) {
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
        pub(crate) async fn assert_quiet(&mut self) {
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
        pub(crate) async fn next_frame(&mut self) -> Vec<u8> {
            deadline(framing::read_frame(&mut self.sock, &mut self.buf))
                .await
                .expect("a frame arrives promptly")
                .expect("the stream is healthy");
            self.buf.clone()
        }

        /// Waits for the server to close the connection, and fails if it sends anything more.
        pub(crate) async fn ended(&mut self) {
            let outcome = deadline(framing::read_frame(&mut self.sock, &mut self.buf))
                .await
                .expect("the stream ends promptly");
            assert!(
                matches!(outcome, Err(ReadFrameError::Closed)),
                "the server must close the connection rather than send more, got {outcome:?}"
            );
        }

        /// Sends bytes the protocol does not allow, which is one of the two ways a session ends.
        pub(crate) async fn misbehave(&mut self) {
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
    /// the registry.
    ///
    /// Synchronous, unlike the real-socket `connected` this replaced: a mock pair needs no accept
    /// to complete.
    pub(crate) fn connected() -> (Client, MockStream) {
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
    pub(crate) fn connected_congested() -> (Client, MockStream) {
        let (client, server) = mock_pair_with_capacity(32);
        (
            Client {
                sock: client,
                buf: Vec::new(),
            },
            server,
        )
    }
}
