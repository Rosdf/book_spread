//! One answered client, as the broadcaster holds it.
//!
//! This is what replaces the per-client task a gRPC server usually needs. The broadcaster owns
//! the whole HTTP/2 connection - one stream per connection is what makes that possible - and
//! drives it from its own `select!`, so a book goes from the encoder into h2's write with no
//! channel and no task hop in between.
//!
//! # Backpressure
//!
//! A payload is offered whole or not at all: [`H2Sink::poll_send`] reserves the stream's flow
//! control window for the message's exact length and only hands it over if all of it fits.
//! There is therefore no half-sent message to finish before a newer one starts, which the
//! previous hand-rolled transport had to track explicitly - h2 owns splitting a message across
//! DATA frames, and guarantees the stream's ordering while it does.
//!
//! What a client that falls behind loses is the books in between, never the current one. That
//! is the opposite of what HTTP/2 flow control does left to itself - it would rather block than
//! drop anything - and it is the right answer for market data, where a stale book is worth less
//! than the current one. `send_data` without a reservation would take the blocking answer *and*
//! buffer without bound, which is why the capacity check is not optional.
//!
//! # The opening snapshot
//!
//! Nothing here special-cases it. The broadcaster seeds its epoch from the reader's current
//! book before any client exists, so the first message a client reads after the response
//! headers is that snapshot, empty when nothing has been published yet. An empty book is
//! already this wire's resync signal, and "nothing published yet" is indistinguishable from -
//! and no different in meaning to - a connector that is resyncing.

use crate::client::{ClientSink, Sent, State};
use bytes::Bytes;
use h2::SendStream;
use h2::server::Connection;
use http::HeaderMap;
use md_wire::grpc::Rejected;
use std::fmt;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite};

/// One attached client: its connection, its stream, and how far along it is.
pub(crate) struct H2Sink<S> {
    conn: Connection<S, Bytes>,
    /// `None` once the stream is over - reset by the client, failed, or finished by us. Kept
    /// alongside a live `conn` because the connection still has to be driven to flush whatever
    /// the stream's last act queued.
    send: Option<SendStream<Bytes>>,
    state: State,
}

// Hand-written: `h2::server::Connection` is not `Debug`.
impl<S> fmt::Debug for H2Sink<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("H2Sink")
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

impl<S> H2Sink<S> {
    /// Everything about this client is over: nothing more to queue, nothing left to flush.
    fn end(&mut self) {
        self.send = None;
        self.state = State::Ended;
    }

    /// `send` is `None` when the response headers could not be queued at all, which is a
    /// client that reset the stream during the handshake. Reported as `Ended` on the first
    /// poll rather than refused here, so the broadcaster has one way to lose a client.
    pub(super) fn new(conn: Connection<S, Bytes>, send: Option<SendStream<Bytes>>) -> Self {
        let state = if send.is_some() {
            State::Running
        } else {
            State::Ended
        };
        Self { conn, send, state }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send + 'static> ClientSink for H2Sink<S> {
    fn poll_send(&mut self, cx: &mut Context<'_>, payload: &Bytes) -> Sent {
        let Some(send) = self.send.as_mut() else {
            return Sent::Ended;
        };

        // `reserve_capacity` assigns from the connection's and the stream's windows on the
        // spot, so `capacity` below already reflects this request - there is no need to go
        // round the loop once to find out.
        send.reserve_capacity(payload.len());
        if send.capacity() < payload.len() {
            // Registers the waker that comes back when the peer's WINDOW_UPDATE arrives. The
            // payload is deliberately *not* held: whatever is current when that happens is
            // what gets sent.
            match send.poll_capacity(cx) {
                Poll::Ready(None | Some(Err(_))) => {
                    self.end();
                    return Sent::Ended;
                }
                // Capacity was just assigned - possibly not enough on its own, since a
                // reservation can be granted in parts. Fall through to the capacity check
                // below rather than discarding this book on a window that only just opened.
                Poll::Ready(Some(Ok(_))) => {
                    if send.capacity() < payload.len() {
                        return Sent::Full;
                    }
                }
                Poll::Pending => return Sent::Full,
            }
        }

        // The refcount bump, and the only one: every client on this symbol queues the same
        // buffer the encoder produced once.
        if send.send_data(payload.clone(), false).is_err() {
            self.end();
            return Sent::Ended;
        }
        Sent::Queued
    }

    fn poll_progress(&mut self, cx: &mut Context<'_>) -> State {
        if self.state.is_ended() {
            // `state` only reaches `Ended` once there is nothing left to flush, so driving the
            // connection further would only wait on a peer with no reason to speak again.
            return State::Ended;
        }

        // The one call that moves bytes in either direction. It is also where SETTINGS,
        // WINDOW_UPDATE and PING are serviced - all of which used to be a protocol violation
        // on this connection, and are now simply the transport doing its job.
        if self.conn.poll_closed(cx).is_ready() {
            self.end();
            return State::Ended;
        }

        // A client that reset just this stream, on a connection that is otherwise fine.
        if let Some(send) = self.send.as_mut()
            && send.poll_reset(cx).is_ready()
        {
            self.end();
        }

        self.state
    }

    fn begin_finish(&mut self, rejected: &Rejected) {
        let Some(mut send) = self.send.take() else {
            return;
        };

        let mut trailers = HeaderMap::with_capacity(3);
        super::put_status(&mut trailers, rejected);
        // Queued only. `poll_progress` is what flushes it, and `state` stays `Running` until
        // it does - which is what lets a broadcaster close every client concurrently under one
        // deadline rather than one timeout each.
        let _ = send.send_trailers(trailers);
        // Without this the connection would sit open after its one stream ended, waiting on a
        // client with nothing left to say, and every teardown would cost the full deadline.
        // The stream is complete as of the trailers above, so nothing is cut short.
        self.conn.graceful_shutdown();
    }
}
