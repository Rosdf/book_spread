//! One client's place in a broadcaster: which book it needs next, and where to put it.
//!
//! Almost nothing is left here, and that is the point. The transport used to be a socket this
//! module wrote into by hand, with a half-written frame to remember between polls; it is now a
//! [`ClientSink`], which takes a payload whole or not at all - h2 owns splitting a message
//! across DATA frames, and guarantees the stream's ordering while it does. What remains is the epoch
//! bookkeeping - the one thing that is about *fan-out* rather than about bytes.
//!
//! # Epochs, and why a session holds no payload
//!
//! [`Ctx`](super::broadcaster::Ctx) holds one book, and a counter that goes up each time it is
//! replaced. A session holds the number of the book it needs next. So "has this client seen the
//! current book" is an integer comparison, and a client that is behind is offered whatever is
//! current at the moment it can take something - never the book it missed. That is the whole of
//! the newest-only policy, and it is why a stalled client costs one `u64` rather than a queue.

use crate::client::{ClientSink, Sent, State};
use bytes::Bytes;
use md_wire::grpc::Rejected;
use std::task::Context;

/// What a session needs to know about the book its broadcaster is publishing.
pub(super) trait SessionCtx {
    /// The number of the current book. Goes up by one each time a new one replaces it.
    fn epoch(&self) -> u64;
    /// The current book, framed and ready to send.
    fn payload(&self) -> &Bytes;
}

/// One attached client, and the book it needs next.
#[derive(Debug)]
pub(super) struct Session<K> {
    sink: K,
    /// The epoch of the first book this client has not been given. Starts at 0, which is the
    /// epoch [`Ctx`](super::broadcaster::Ctx) seeds at construction from the reader's current
    /// book - so the first thing a client reads is always that snapshot, empty when nothing
    /// has been published yet.
    epoch: u64,
    state: State,
    /// The stream's closing status has been queued; only the flush is left. No further book
    /// is offered from here, so a teardown can keep polling this session to completion without
    /// it trying to send on a stream that is already over.
    finishing: bool,
}

impl<K: ClientSink> Session<K> {
    pub(super) fn new(sink: K) -> Self {
        Self {
            sink,
            epoch: 0,
            state: State::Running,
            finishing: false,
        }
    }

    pub(super) fn ended(&self) -> bool {
        self.state.is_ended()
    }

    /// Offers this client the current book, if it has not had it.
    pub(super) fn deliver(&mut self, cx: &mut Context<'_>, session_ctx: &impl SessionCtx) {
        if self.finishing || self.state.is_ended() || self.epoch > session_ctx.epoch() {
            return;
        }

        match self.sink.poll_send(cx, session_ctx.payload()) {
            Sent::Queued => self.epoch = session_ctx.epoch() + 1,
            // Deliberately leaves `epoch` behind. The waker the sink just registered brings
            // this back round, and what gets sent then is whatever is current *then*.
            Sent::Full => {}
            Sent::Ended => self.state = State::Ended,
        }
    }

    /// Offers the current book and then drives the transport, so the bytes just queued reach
    /// the wire in this pass rather than on a later lap of the run loop.
    ///
    /// The mirror of [`poll_progress`](Self::poll_progress), and the order is the difference:
    /// that one runs on laps with nothing new to offer, where driving first may open the
    /// window before the offer. Here there is something new, so the offer goes first.
    pub(super) fn deliver_and_flush(
        &mut self,
        cx: &mut Context<'_>,
        session_ctx: &impl SessionCtx,
    ) -> State {
        self.deliver(cx, session_ctx);
        if self.state.is_ended() {
            return State::Ended;
        }

        // Unconditionally, not only when `deliver` queued something: a client whose window is
        // shut has nothing to flush, but this is the only thing that will ever read its
        // WINDOW_UPDATE, so skipping it is what strands one.
        self.state = self.sink.poll_progress(cx);
        self.state
    }

    /// Drives this client's transport, then offers it anything it is missing.
    ///
    /// Called for every session on every lap of the broadcaster's loop. In the steady state it
    /// registers one interest and returns, which is what makes polling every client from one
    /// task cheap enough to be the design rather than a compromise.
    ///
    /// The mirror of [`deliver_and_flush`](Self::deliver_and_flush), which runs the two steps in
    /// the other order for the lap that has something new to offer.
    pub(super) fn poll_progress(
        &mut self,
        cx: &mut Context<'_>,
        session_ctx: &impl SessionCtx,
    ) -> State {
        if self.state.is_ended() {
            return State::Ended;
        }

        self.state = self.sink.poll_progress(cx);
        if self.state.is_ended() {
            return State::Ended;
        }

        self.deliver(cx, session_ctx);
        self.state
    }

    /// Queues the end of this client's stream and the status that explains it.
    ///
    /// Only queues: [`poll_progress`](Session::poll_progress) is what flushes it. That split is
    /// what lets a broadcaster close all of its clients at once under one deadline instead of
    /// waiting on each in turn.
    pub(super) fn begin_finish(&mut self, rejected: &Rejected) {
        if !self.state.is_ended() && !self.finishing {
            self.finishing = true;
            self.sink.begin_finish(rejected);
        }
    }
}
