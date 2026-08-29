//! The registry's queue: one event per thing that can happen to the `(venue, symbol)` map,
//! and the two halves of the channel that carries them.
//!
//! Deliberately the same shape as `core_lib::connector::events`: an unbounded
//! `tokio::sync::mpsc`, a `send` that swallows a closed channel, and correlation by a reply
//! channel carried on the event rather than by any kind of request id. The events that need
//! an answer carry an `oneshot::Sender` for it; the ones that do not are fire-and-forget,
//! and are still ordered against everything else by the queue.

use crate::registry::Refused;
use core_lib::instrument::{Instrument, InstrumentId};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use tokio::sync::mpsc;
use core_lib::Venue;

/// One generation of broadcaster for one key.
///
/// `pending_joins` is the identity: the registry compares it by pointer to tell the
/// broadcaster that sent this event from a later one that has since taken the same key over.
/// It is the same `Arc` the broadcaster counts its unclaimed joins on, so nothing extra is
/// allocated to name a generation.
#[derive(Debug)]
pub(crate) struct Claim {
    instrument_id: InstrumentId,
    venue: Venue,
    pending_joins: Arc<AtomicUsize>,
}

impl Claim {
    pub(crate) fn new(instrument_id: InstrumentId, venue: Venue, pending_joins: Arc<AtomicUsize>) -> Self {
        Self { instrument_id, venue, pending_joins }
    }

    pub(crate) fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    pub(crate) fn venue(&self) -> Venue {
        self.venue
    }

    pub(crate) fn pending_joins(&self) -> &Arc<AtomicUsize> {
        &self.pending_joins
    }
}

/// A client, on its way to whichever broadcaster will serve `key`.
///
/// The reply is only ever the client coming back: every other answer - the response headers,
/// or the venue's own refusal - is written by the broadcaster on the connection itself.
#[derive(Debug)]
pub(super) struct Subscribe<C> {
    key: Instrument,
    client: C,
    reply: oneshot::Sender<Result<(), Refused<C>>>,
}

impl<C> Subscribe<C> {
    fn new(key: Instrument, client: C) -> (Self, oneshot::Receiver<Result<(), Refused<C>>>) {
        let (reply, rx) = oneshot::channel();
        (Self { key, client, reply }, rx)
    }

    pub(super) fn into_parts(self) -> (Instrument, C, oneshot::Sender<Result<(), Refused<C>>>) {
        (self.key, self.client, self.reply)
    }
}

/// A broadcaster whose session list has just emptied, asking whether it may stop.
#[derive(Debug)]
pub(super) struct RetireIfIdle {
    claim: Claim,
    reply: oneshot::Sender<bool>,
}

impl RetireIfIdle {
    fn new(claim: Claim) -> (Self, oneshot::Receiver<bool>) {
        let (reply, rx) = oneshot::channel();
        (Self { claim, reply }, rx)
    }

    pub(super) fn into_parts(self) -> (Claim, oneshot::Sender<bool>) {
        (self.claim, self.reply)
    }
}

#[derive(Debug)]
pub(super) enum RegistryEvent<C> {
    Subscribe(Subscribe<C>),
    RetireIfIdle(RetireIfIdle),
    /// A broadcaster stopping for a reason other than an idle session list. Fire-and-forget:
    /// the sender has nothing left to decide, and the queue is what orders the connector
    /// unsubscribe this issues ahead of any later subscribe for the same key.
    Retire(Claim),
    /// The same, for a broadcaster whose subscribe the connector rejected - so, without the
    /// unsubscribe. See `Registry::abandon_entry`.
    Abandon(Claim),
    /// Stop taking new keys and end every running broadcaster. Fire-and-forget: the task
    /// stopping is not the acknowledgement, the queue draining is - see
    /// [`RegistryHandle::shutdown`](super::RegistryHandle::shutdown).
    ShutDown,
    #[cfg(test)]
    IsRegistered(InstrumentId, oneshot::Sender<bool>),
    #[cfg(test)]
    EntryToken(InstrumentId, oneshot::Sender<Option<Arc<AtomicUsize>>>),
}

/// The receiving half, owned by the registry task.
#[derive(Debug)]
pub(super) struct RegistryRx<C> {
    rx: mpsc::UnboundedReceiver<RegistryEvent<C>>,
}

impl<C> RegistryRx<C> {
    /// `None` once every [`RegistryTx`] has been dropped, which is the registry task's
    /// signal that no broadcaster and no handshake is left to serve.
    pub(super) async fn recv(&mut self) -> Option<RegistryEvent<C>> {
        self.rx.recv().await
    }
}

/// A handle on the registry task, held by the accept loop, by every handshake and by every
/// broadcaster.
///
/// Every method here *sends synchronously* and hands back the reply channel rather than
/// awaiting it. That is load-bearing rather than stylistic: two calls made with no `.await`
/// between them are guaranteed to reach the task's queue in that order with nothing
/// interleaved, which is what several of the teardown-race invariants rest on.
#[derive(Debug)]
pub(crate) struct RegistryTx<C> {
    tx: mpsc::UnboundedSender<RegistryEvent<C>>,
}

// Hand-written rather than derived: `#[derive(Clone)]` would ask for `C: Clone`, and a client
// connection is the one thing that never is.
impl<C> Clone for RegistryTx<C> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

impl<C> RegistryTx<C> {
    /// Hands `client` to `key`'s broadcaster, starting one if this is the first client.
    ///
    /// The reply carries the client back only when the registry declined to take it at all.
    /// A reply of `Err` - the task gone, or a handler that panicked on the way - means the
    /// same thing to a caller as a refusal it could not write: drop the connection.
    pub(crate) fn subscribe(
        &self,
        key: Instrument,
        client: C,
    ) -> oneshot::Receiver<Result<(), Refused<C>>> {
        let (event, rx) = Subscribe::new(key, client);
        self.send(RegistryEvent::Subscribe(event));
        rx
    }

    /// Asks whether `claim`'s broadcaster may stop now that its session list is empty.
    /// `true` means its entry is gone and it should. A reply of `Err` means the same: there
    /// is nothing left to serve clients through.
    pub(crate) fn retire_if_idle(&self, claim: Claim) -> oneshot::Receiver<bool> {
        let (event, rx) = RetireIfIdle::new(claim);
        self.send(RegistryEvent::RetireIfIdle(event));
        rx
    }

    pub(crate) fn retire(&self, claim: Claim) {
        self.send(RegistryEvent::Retire(claim));
    }

    pub(crate) fn abandon(&self, claim: Claim) {
        self.send(RegistryEvent::Abandon(claim));
    }

    pub(crate) fn shut_down(&self) {
        self.send(RegistryEvent::ShutDown);
    }

    /// A handle that does not keep the channel open, for the registry task's own copy - see
    /// [`RegistryWeakTx`].
    pub(super) fn downgrade(&self) -> RegistryWeakTx<C> {
        RegistryWeakTx {
            tx: self.tx.downgrade(),
        }
    }

    #[cfg(test)]
    pub(crate) fn is_registered(&self, key: InstrumentId) -> oneshot::Receiver<bool> {
        let (reply, rx) = oneshot::channel();
        self.send(RegistryEvent::IsRegistered(key, reply));
        rx
    }

    #[cfg(test)]
    pub(super) fn entry_token(
        &self,
        key: InstrumentId,
    ) -> oneshot::Receiver<Option<Arc<AtomicUsize>>> {
        let (reply, rx) = oneshot::channel();
        self.send(RegistryEvent::EntryToken(key, reply));
        rx
    }

    /// A closed channel is not an error to report here: the task is gone, so whatever this
    /// event was going to decide is already decided. Callers that need an answer see it as
    /// their reply channel closing.
    fn send(&self, event: RegistryEvent<C>) {
        let _ = self.tx.send(event);
    }
}

/// The registry task's own way of handing a [`RegistryTx`] to a broadcaster it spawns.
///
/// Weak because the task must not count towards its own channel: "every `RegistryTx` has been
/// dropped" is the signal that no broadcaster and no handshake is left, and a strong handle
/// held by the task itself would mean that never happens. `upgrade` returning `None` says the
/// last of them has gone, which is a refusal to start anything new rather than an error.
#[derive(Debug)]
pub(super) struct RegistryWeakTx<C> {
    tx: mpsc::WeakUnboundedSender<RegistryEvent<C>>,
}

impl<C> RegistryWeakTx<C> {
    pub(super) fn upgrade(&self) -> Option<RegistryTx<C>> {
        self.tx.upgrade().map(|tx| RegistryTx { tx })
    }
}

/// `(rx, tx)` rather than the usual order, matching `core_lib`'s `create_event_channel` -
/// the receiver is the half that goes to the task being started.
pub(super) fn create_event_channel<C>() -> (RegistryRx<C>, RegistryTx<C>) {
    let (tx, rx) = mpsc::unbounded_channel();
    (RegistryRx { rx }, RegistryTx { tx })
}
