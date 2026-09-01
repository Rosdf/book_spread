//! Which catalogue instruments are being broadcast, the two races around starting and
//! stopping one, and turning a catalogue index into the pairs a broadcaster subscribes - which
//! is now two jobs rather than one: checking that the index still means what the client thinks
//! it does, then resolving it against what the connectors have interned.

use crate::broadcast::broadcaster::{Broadcaster, Join, VenueReader, VenueReaders};
use crate::broadcast::queue::{BroadcasterRx, BroadcasterTx, make_broadcaster_channel};
use crate::catalogue::{AskedPair, Instruments, pairs_match};
use crate::client::ClientHandshake;
use crate::registry::events::{
    Claim, RegistryEvent, RegistryRx, RegistryTx, RegistryWeakTx, create_event_channel,
};
use crate::venue::{BookSource as _, Connectors};
use core_lib::Venue;
use core_lib::connector::book_publisher::BookReader;
use core_lib::instrument::Instrument;
use core_lib::map::{InternalHashMap, new_internal_map};
use core_lib::panic::panic_message;
use core_lib::venue::backoff::Backoff;
use md_wire::grpc::{CatalogueIdx, RejectCode, Rejected};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::time::Instant;

pub(crate) mod events;

/// The longest gap between two resolution sweeps.
///
/// The same order as the connectors' own hourly listing refresh is *not* what is wanted here:
/// a sweep is how a catalogue entry becomes servable at all, so a minute is the longest a
/// client that is retrying should have to wait after its venue has caught up. [`Backoff`]
/// starts at 250ms and jitters, so a cold start converges in well under a second and a name
/// that is never listed costs one sweep a minute.
const RESOLVE_SWEEP_MAX: Duration = Duration::from_secs(60);

/// One catalogue instrument's pairs, once every venue has interned its symbol.
///
/// The single representation of "which venues make up this instrument": the resolution map
/// holds one, the entry for a running broadcaster holds a clone of that same one, and the
/// task that subscribes them holds a third. Sharing rather than re-deriving is what keeps
/// them from drifting - a release names exactly the pairs the subscribe named - and a clone
/// is one atomic rather than a copy of the list.
type Pairs = Arc<[Instrument]>;

/// The registry's half of one running broadcaster.
#[derive(Debug)]
struct Entry<C> {
    joins: BroadcasterTx<C>,
    /// The pairs this key was opened with, released together when it retires.
    ///
    /// Recorded here rather than carried on a [`Claim`] because this task is what issued the
    /// subscribes: they are known synchronously, at insert, while a claim is only ever an
    /// identity and can stay allocation-free.
    pairs: Pairs,
    /// Joins already queued on `joins` that the broadcaster has not taken yet.
    ///
    /// This is what closes the teardown race: the increment happens on the registry task,
    /// right before the send, and the zero-check happens on that same task, so a broadcaster
    /// can never retire out from under a join that is already on its way. Doubles as
    /// identity - the `Arc` is compared by pointer to tell one generation of broadcaster for
    /// a key from the next.
    pending_joins: Arc<AtomicUsize>,
}

impl<C> Entry<C> {
    /// Releases every pair this key was opened with, in the order they were subscribed.
    ///
    /// That is what the registry *issued*, which on the unwind of a partly-successful
    /// subscribe is one pair wider than what the connectors actually carry. Deliberate: a
    /// release for a pair a connection has no slot for is a no-op down to the wire -
    /// `remove_slot` finds nothing and enqueues no control frame - and the registry is the
    /// only subscriber there is, so the pair cannot belong to anyone else.
    fn unsubscribe<V: Connectors>(&self, connectors: &V) {
        for pair in &*self.pairs {
            connectors.source(pair.venue()).unsubscribe(pair.id());
        }
    }
}

/// A client handed back because the registry would not start a broadcaster for it.
///
/// Returned rather than refused in place so the refusal is written by the caller, which is
/// already in the async context the handshake runs in.
#[derive(Debug)]
pub(crate) struct Refused<C> {
    client: C,
    rejected: Rejected,
}

impl<C> Refused<C> {
    #[cfg(test)]
    pub(crate) fn code(&self) -> RejectCode {
        self.rejected.code()
    }

    pub(crate) fn into_parts(self) -> (C, Rejected) {
        (self.client, self.rejected)
    }
}

/// The way in to the registry task: hands out [`RegistryTx`] clones, and reclaims the
/// connectors when the task is done with them.
///
/// One per server. Everything that talks to the registry - the accept loop, each handshake,
/// each broadcaster - holds a `RegistryTx` instead, which is what makes this the only place
/// that can shut the registry down.
#[derive(Debug)]
pub(crate) struct RegistryHandle<V, C> {
    tx: RegistryTx<C>,
    task: tokio::task::JoinHandle<V>,
}

impl<V: Connectors, C: ClientHandshake> RegistryHandle<V, C> {
    /// Spawns the registry task and gives it `connectors` and the catalogue's instruments to
    /// own.
    ///
    /// Every instrument starts unresolved, however long its venue has been running: resolving
    /// one is a lookup in the process-global instrument registry, and doing all of them here
    /// would be a lock acquisition per entry before the first client has asked for anything.
    /// The first subscribe is due a sweep immediately - `next_sweep` is now - so a cold start
    /// costs exactly one.
    pub(crate) fn spawn(connectors: V, catalogue: Instruments) -> Self {
        let (rx, tx) = create_event_channel();
        let unresolved = catalogue.iter().map(|(idx, _)| idx).collect();
        let registry = Registry {
            entries: new_internal_map(),
            connectors,
            catalogue,
            resolved: new_internal_map(),
            unresolved,
            resolve_backoff: Backoff::new(RESOLVE_SWEEP_MAX),
            next_sweep: Instant::now(),
            shutting_down: false,
            tx: tx.downgrade(),
        };

        Self {
            tx,
            task: tokio::spawn(registry.run(rx)),
        }
    }

    /// A handle for something that will talk to the registry but must not be able to end it.
    pub(crate) fn tx(&self) -> RegistryTx<C> {
        self.tx.clone()
    }

    /// Ends every running broadcaster, waits for them all, and hands the connectors back.
    ///
    /// Two steps in one call. [`RegistryEvent::ShutDown`] clears the entries, which drops the
    /// sending half of every broadcaster's join channel - the only stop signal a broadcaster
    /// has - and stops the registry taking new keys. Dropping this handle's own `RegistryTx`
    /// then makes the task's `recv` resolve to `None` the moment the last broadcaster and
    /// the last handshake have dropped theirs, which is exactly when the connectors are free
    /// to be shut down.
    ///
    /// `None` when the task did not finish - it panicked outside an event handler, or was
    /// aborted - in which case the connectors cannot be reclaimed and are left running.
    pub(crate) async fn shutdown(self) -> Option<V> {
        let Self { tx, task } = self;
        tx.shut_down();
        drop(tx);

        match task.await {
            Ok(connectors) => Some(connectors),
            Err(err) => {
                tracing::error!(%err, "the registry task did not finish");
                None
            }
        }
    }
}

/// The catalogue-index -> broadcaster map, plus the connectors those broadcasters use.
///
/// Lives on one task and is reached only through its queue, the same way a venue connector
/// is reached through `ConnectorTx`. That is what orders the connector `unsubscribe`s a
/// retiring broadcaster issues ahead of the `subscribe`s the next broadcaster for that key
/// needs: both calls are made from here, in queue order, and the connector's own queue
/// preserves it.
///
/// Every handler below is therefore *synchronous* - a hash lookup, a channel send and an
/// atomic - and that is a rule rather than an accident. `handle` is the critical section the
/// `std::sync::Mutex` this task replaced used to hold, so an `.await` inside it is the same
/// bug as holding a lock across a yield, minus the guard that would make it visible: the
/// increment-then-send in [`Registry::subscribe`] and the zero-check-then-remove in
/// [`Registry::retire_if_idle`] are indivisible only because nothing runs on this task
/// between their steps, while broadcasters concurrently decrement `pending_joins` and drop
/// the receiving half of the join channel. Being synchronous is also what lets
/// [`Registry::run`] wrap a handler in `catch_unwind`, and what keeps the one queue every
/// handshake and every broadcaster reaches the registry through from stalling on a venue
/// round trip - which is why `subscribe` takes the connector's reply channel here and awaits
/// it on the spawned task instead.
#[derive(Debug)]
struct Registry<V, C> {
    /// One entry per *catalogue instrument* being broadcast, not per symbol: a broadcaster
    /// serves an instrument's whole pair list as one merged book.
    entries: InternalHashMap<CatalogueIdx, Entry<C>>,
    connectors: V,
    /// Every slot the catalogue named. Immutable for the life of the process, and what a
    /// subscribe's pair list is checked against - whether or not the entry has resolved yet.
    catalogue: Instruments,
    /// Catalogue entries every one of whose venues has interned its symbol. Nothing ever
    /// leaves this map: an `Instrument` handle stays valid for the life of the process.
    ///
    /// The pair list is an `Arc` so a caller can be handed its own handle without keeping the
    /// map borrowed - one atomic, and only on the path that starts a broadcaster.
    resolved: InternalHashMap<CatalogueIdx, Pairs>,
    /// Indices not interned as of the last sweep, drained as the connectors catch up. Only the
    /// indices: the pairs live in `catalogue`. A `Vec` because a sweep is a `retain` over all of
    /// it, never a lookup of one.
    unresolved: Vec<CatalogueIdx>,
    /// One backoff for the whole sweep rather than one per entry: a sweep is a single pass of
    /// read-locked lookups over every unresolved name, so pacing it once is both cheaper and
    /// the thing that actually bounds how much traffic a hammering client can put on the
    /// process-global instrument lock.
    resolve_backoff: Backoff,
    /// When the next sweep becomes due. Until then an unresolved index is refused without any
    /// lookup at all.
    next_sweep: Instant,
    /// Set by [`RegistryEvent::ShutDown`]. "Do not start anything new" - an entry that is
    /// still being torn down is still served.
    shutting_down: bool,
    /// Cloned into every broadcaster this registry spawns.
    ///
    /// Weak on purpose: a strong handle held here would count towards its own channel and
    /// the task's `recv` would never report `None`, so the one signal this whole shutdown
    /// path rests on would never fire.
    tx: RegistryWeakTx<C>,
}

impl<V: Connectors, C: ClientHandshake> Registry<V, C> {
    /// Serves the queue until every [`RegistryTx`] is gone, then hands the connectors back.
    async fn run(mut self, mut rx: RegistryRx<C>) -> V {
        while let Some(event) = rx.recv().await {
            // A panicking handler must not take the map - or the connectors, or every
            // running broadcaster's only way to reach the registry - down with it. This is
            // the same judgement the `std::sync::Mutex` this task replaced used to make by
            // recovering its poison flag rather than re-panicking, and it rests on the same
            // fact: every handler is a hash lookup, a channel send and an atomic, so a panic
            // part-way through one cannot leave `entries` torn. `AssertUnwindSafe` because
            // `&mut self` is not `UnwindSafe`, which is precisely what that fact is about.
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| self.handle(event))) {
                tracing::error!(
                    panic = panic_message(payload.as_ref()),
                    "a registry event handler panicked"
                );
            }
        }

        self.connectors
    }

    /// The whole of the critical section, in one synchronous call. See the note on
    /// [`Registry`] about why nothing in here may await.
    fn handle(&mut self, event: RegistryEvent<C>) {
        match event {
            RegistryEvent::Subscribe(joining) => {
                let (idx, asked, sock, reply) = joining.into_parts();
                // A dropped receiver just means the handshake gave up; the client in a
                // `Refused` goes with it, which closes the connection - the same answer,
                // unwritten.
                let _ = reply.send(self.subscribe(idx, &asked, sock));
            }
            RegistryEvent::RetireIfIdle(retiring) => {
                let (claim, reply) = retiring.into_parts();
                let _ = reply.send(self.retire_if_idle(&claim));
            }
            RegistryEvent::Retire(claim) => self.retire(&claim),
            RegistryEvent::ShutDown => {
                self.shutting_down = true;
                self.entries.clear();
            }
            #[cfg(test)]
            RegistryEvent::IsRegistered(key, reply) => {
                let _ = reply.send(self.entries.contains_key(&key));
            }
            #[cfg(test)]
            RegistryEvent::EntryToken(key, reply) => {
                let _ = reply.send(
                    self.entries
                        .get(&key)
                        .map(|entry| Arc::clone(&entry.pending_joins)),
                );
            }
        }
    }

    /// Hands `client` to the broadcaster for catalogue instrument `idx`, starting one if this
    /// is the first client.
    ///
    /// Checked in this order, ahead of any broadcaster existing: an index the catalogue does
    /// not carry at all; one whose pairs no longer match what `asked` names, because the
    /// catalogue file was edited and the client's view of it is stale; and one whose venue has
    /// not interned its symbol yet. The stale check comes before resolution rather than after
    /// so a mismatch is reported as a mismatch whether or not the entry has resolved yet, and
    /// so a stale client cannot join a broadcaster that is already running for a fresh one.
    fn subscribe(&mut self, idx: CatalogueIdx, asked: &[AskedPair], client: C) -> Result<(), Refused<C>> {
        match self.check(idx, asked).and_then(|()| self.resolve(idx)) {
            Ok(pairs) => self.subscribe_resolved(idx, pairs, client),
            Err(rejected) => Err(Refused { client, rejected }),
        }
    }

    /// Whether `asked` is still what the catalogue lists under `idx`.
    ///
    /// # Errors
    ///
    /// [`RejectCode::UnknownInstrument`] for an index this server does not carry at all -
    /// permanent, since the catalogue is loaded once - and [`RejectCode::InstrumentChanged`]
    /// for one that is carried, but under different pairs than `asked` names - also permanent
    /// for this request, but for the opposite reason: the client's catalogue is stale, and
    /// re-fetching it is what fixes the next one.
    fn check(&self, idx: CatalogueIdx, asked: &[AskedPair]) -> Result<(), Rejected> {
        let Some(carried) = self.catalogue.get(idx) else {
            return Err(Rejected::new(
                RejectCode::UnknownInstrument,
                format!("this server does not carry instrument {}", idx.get()).into_boxed_str(),
            ));
        };
        if pairs_match(asked, carried) {
            return Ok(());
        }
        Err(Rejected::new(
            RejectCode::InstrumentChanged,
            format!(
                "instrument {} is no longer the pairs this request named - re-fetch the catalogue",
                idx.get()
            )
            .into_boxed_str(),
        ))
    }

    /// The catalogue entry `idx` names, if it can be served right now.
    ///
    /// Ordered so that the common case - an index already resolved - is one hash lookup, and
    /// so that a client retrying in a tight loop on an unresolved one costs nothing but two:
    /// the sweep is what touches the process-global instrument lock, and it happens at most
    /// once per [`Backoff`] step however many requests arrive in between.
    ///
    /// # Errors
    ///
    /// [`RejectCode::UnlistedSymbol`] for an index whose venue has not interned its symbol yet,
    /// which is worth retrying. [`Registry::check`] is what turns down an index this server
    /// does not carry at all before this is ever called.
    fn resolve(&mut self, idx: CatalogueIdx) -> Result<Pairs, Rejected> {
        if let Some(resolved) = self.resolved.get(&idx) {
            return Ok(Arc::clone(resolved));
        }

        let unlisted = || {
            Rejected::new(
                RejectCode::UnlistedSymbol,
                format!(
                    "instrument {} is not listed by every one of its venues yet",
                    idx.get()
                )
                .into_boxed_str(),
            )
        };
        if Instant::now() < self.next_sweep {
            return Err(unlisted());
        }

        self.sweep();
        self.resolved.get(&idx).map(Arc::clone).ok_or_else(unlisted)
    }

    /// Moves every catalogue entry whose venue has now interned its symbol into `resolved`.
    ///
    /// One pass over everything still unresolved rather than a lookup for the one index that
    /// was asked for: the lookups share a sweep, so the cost of answering the *next* request
    /// for a different index has already been paid, and the backoff below bounds how often
    /// this happens at all.
    ///
    /// An entry resolves all at once or not at all: every pair under it is served, so one
    /// venue that has not interned its symbol yet keeps the whole entry here. That is what
    /// makes a broadcaster's venue set fixed for its lifetime - it never starts on a partial
    /// book and then has to grow. An entry with no pairs at all never resolves either: it
    /// names nothing to subscribe to.
    fn sweep(&mut self) {
        let before = self.unresolved.len();
        let resolved = &mut self.resolved;
        let catalogue = &self.catalogue;
        self.unresolved.retain(|idx| {
            let pairs = catalogue
                .get(*idx)
                .expect("an unresolved index is always one the catalogue still carries");
            if pairs.is_empty() {
                return true;
            }
            let mut interned = heapless::Vec::<Instrument, { Venue::COUNT }>::new();
            for pair in pairs {
                // `true` keeps the entry here: it is still unresolved, and every pair
                // already looked up is discarded rather than remembered - the lookup is a
                // read-locked hash probe, and half an entry is not servable.
                let Some(instrument) = Instrument::lookup(pair.venue(), pair.symbol()) else {
                    return true;
                };
                interned
                    .push(instrument)
                    .map_err(|_| ())
                    .expect("one pair per venue, which the catalogue enforces");
            }
            // The one place a `Pairs` is built. Everything downstream shares this handle.
            resolved.insert(*idx, Pairs::from(&*interned));
            false
        });

        // Progress means the connectors are still catching up, so the next sweep is worth
        // making soon; no progress lets the interval grow towards `RESOLVE_SWEEP_MAX`.
        if self.unresolved.len() < before {
            self.resolve_backoff.reset();
        }
        self.next_sweep = Instant::now() + self.resolve_backoff.next_delay();
    }

    /// Hands `client` to `idx`'s broadcaster, starting one if this is the first client.
    ///
    /// The broadcaster answers on the connection itself - response headers, or a venue's own
    /// refusal - so an instrument a venue does not list ends the connection rather than
    /// opening a stream that never produces anything. Nothing comes back to the caller except
    /// a client the registry declined to take at all.
    ///
    /// Every client of a brand new instrument is served by this one task, and the entry goes
    /// into the map before the broadcaster is even spawned, so all but the first can only find
    /// it occupied and queue a join. Exactly one `BookSource::subscribe` is ever issued per
    /// pair - which is an invariant, not a nicety: the connector hard-errors a duplicate
    /// subscribe, and `BookReader` is not `Clone`. What keeps it true across instruments is
    /// the catalogue itself, which refuses to carry one `(venue, symbol)` under two of them.
    fn subscribe_resolved(
        &mut self,
        idx: CatalogueIdx,
        pairs: Pairs,
        client: C,
    ) -> Result<(), Refused<C>> {
        let join = match self.join_running(idx, Join::new(client)) {
            Ok(()) => return Ok(()),
            Err(returned) => returned,
        };

        // `upgrade` failing says the same thing `shutting_down` does, one step further along:
        // nothing outside this task holds a way in any more, so a broadcaster started now
        // would have nobody to serve.
        let started = (!self.shutting_down).then(|| self.tx.upgrade()).flatten();
        let Some(tx) = started else {
            return Err(Refused {
                client: join.into_client(),
                rejected: Rejected::new(
                    RejectCode::ShuttingDown,
                    Box::from("server is shutting down"),
                ),
            });
        };

        let (joins, queued) = make_broadcaster_channel();
        let pending_joins = Arc::new(AtomicUsize::new(1));
        joins
            .send(join)
            .map_err(|_| ())
            .expect("the receiving half is still in scope");
        self.entries.insert(
            idx,
            Entry {
                joins,
                pairs: Arc::clone(&pairs),
                pending_joins: Arc::clone(&pending_joins),
            },
        );

        let replies = self.issue_subscribes(&pairs);
        tokio::spawn(serve(tx, idx, pairs, replies, queued, pending_joins));
        Ok(())
    }

    /// Hands `join` to the broadcaster already serving `idx`, if there is one.
    ///
    /// `Err` gives it back, meaning this key needs a broadcaster of its own: either nothing
    /// was filed under it, or what was is a broadcaster that has already stopped and whose
    /// [`RegistryEvent::Retire`] is still queued behind this event - in which case the stale
    /// entry is dropped here so the caller's fresh one replaces it.
    fn join_running(&mut self, idx: CatalogueIdx, join: Join<C>) -> Result<(), Join<C>> {
        let Some(entry) = self.entries.get(&idx) else {
            return Err(join);
        };

        // Incremented before the send, so a broadcaster whose `RetireIfIdle` is queued behind
        // this event sees the join and stays alive for it.
        entry.pending_joins.fetch_add(1, Ordering::Relaxed);
        entry.joins.send(join).inspect_err(|_| {
            self.entries.remove(&idx);
        })
    }

    /// Asks every pair's connector for its book stream, and hands back the replies in the
    /// order the pairs are in.
    ///
    /// Issued here rather than on the broadcaster, because this is the only place that can
    /// put them after the `unsubscribe`s of whatever generation held this key before. It
    /// costs no await: `BookSource::subscribe` hands back the reply channel rather than a
    /// future, so the spawned task is what waits for the venues.
    fn issue_subscribes(&self, pairs: &[Instrument]) -> Replies {
        pairs
            .iter()
            .map(|pair| self.connectors.source(pair.venue()).subscribe(pair.id()))
            .collect()
    }

    /// Called for a broadcaster whose session list has just emptied. `true` means the entry
    /// is gone and the task should stop.
    fn retire_if_idle(&mut self, claim: &Claim) -> bool {
        let Some(entry) = self.entries.get(&claim.idx()) else {
            return true;
        };
        if !Arc::ptr_eq(&entry.pending_joins, claim.pending_joins()) {
            // A newer broadcaster owns this key; this one must not take its entry with it.
            return true;
        }
        if claim.pending_joins().load(Ordering::Relaxed) > 0 {
            // A join is already on its way. Stay alive for it: the next `recv` picks it up.
            return false;
        }

        self.take_entry(claim)
            .expect("checked just above: the entry is here and this claim owns it")
            .unsubscribe(&self.connectors);
        true
    }

    /// Removes a broadcaster that is stopping for a reason other than an idle session list -
    /// its connectors dropped the symbols, the server is shutting down, or its own subscribe
    /// failed part-way - and releases every subscription it holds with it.
    ///
    /// A no-op when the entry is already gone or belongs to a newer broadcaster, so a task
    /// that exited through [`Registry::retire_if_idle`] can still send this on its way out.
    fn retire(&mut self, claim: &Claim) {
        if let Some(entry) = self.take_entry(claim) {
            entry.unsubscribe(&self.connectors);
        }
    }

    /// Takes `claim`'s entry out if it is still the one `claim` names. `None` means it was
    /// already gone - [`RegistryEvent::ShutDown`] cleared it, say - or belongs to a newer
    /// broadcaster for the same key, which must not have its entry taken out from under it.
    fn take_entry(&mut self, claim: &Claim) -> Option<Entry<C>> {
        let entry = self.entries.get(&claim.idx())?;
        if !Arc::ptr_eq(&entry.pending_joins, claim.pending_joins()) {
            return None;
        }

        self.entries.remove(&claim.idx())
    }
}

/// One pending `BookSource::subscribe` reply per pair, in the pairs' own order.
type Replies = heapless::Vec<oneshot::Receiver<anyhow::Result<BookReader>>, { Venue::COUNT }>;

/// Waits for every venue to answer, then runs the broadcaster - or unwinds the whole
/// subscribe if any of them refused.
///
/// Off the registry task on purpose: waiting here is what keeps the one queue every handshake
/// and every broadcaster reaches the registry through from stalling on a venue round trip.
async fn serve<C: ClientHandshake>(
    registry: RegistryTx<C>,
    idx: CatalogueIdx,
    pairs: Pairs,
    replies: Replies,
    queued: BroadcasterRx<C>,
    pending_joins: Arc<AtomicUsize>,
) {
    match open_readers(&pairs, replies).await {
        Ok(readers) => Broadcaster::start(registry, idx, readers, queued, pending_joins).await,
        // All or nothing: a book missing a venue is not the book this instrument advertises,
        // and a client cannot tell one from the other on the wire. The readers that did open
        // went with the `Err`, and the entry's pairs are released by the retire below.
        Err(rejected) => {
            // Queued before the drain: once the registry has taken the entry out, no further
            // join can be sent here, so the drain answers every one there will ever be.
            registry.retire(Claim::new(idx, pending_joins));
            queued.drain(rejected).await;
        }
    }
}

/// One reader per pair, or the first reason there is not one for all of them.
///
/// Every reply is awaited even once one has failed: dropping the receiving half of a reply
/// channel the connector is about to send on would leave that subscription established with
/// nothing reading it, and the release that follows has to be ordered after it rather than
/// racing it.
async fn open_readers(pairs: &[Instrument], replies: Replies) -> Result<VenueReaders, Rejected> {
    let mut readers = VenueReaders::new();
    let mut refused: Option<Rejected> = None;

    for (pair, reply) in pairs.iter().zip(replies) {
        match opened(*pair, reply.await) {
            Ok(reader) => readers
                .push(VenueReader::new(pair.venue(), reader))
                .map_err(|_| ())
                .expect("one pair per venue, which the catalogue enforces"),
            Err(rejected) => {
                refused.get_or_insert(rejected);
            }
        }
    }

    if let Some(rejected) = refused {
        return Err(rejected);
    }
    Ok(readers)
}

/// One pair's answer: its reader, or the refusal a client should hear.
fn opened<E>(
    pair: Instrument,
    reply: Result<anyhow::Result<BookReader>, E>,
) -> Result<BookReader, Rejected> {
    match reply {
        Ok(Ok(reader)) => Ok(reader),
        Ok(Err(err)) => {
            tracing::warn!(
                venue = pair.venue().as_str(),
                symbol = pair.name(),
                %err,
                "subscribe rejected"
            );
            Err(Rejected::new(
                RejectCode::ConnectorRefused,
                err.to_string().into_boxed_str(),
            ))
        }
        // The reply channel closed without an answer, which only a connector that stopped
        // between taking the subscribe and answering it can do.
        Err(_) => Err(Rejected::new(
            RejectCode::ConnectorGone,
            Box::from("connector stopped before it could subscribe"),
        )),
    }
}

#[cfg(test)]
mod test {
    use super::events::Claim;
    use crate::broadcast::broadcaster::SESSION_SWEEP;
    use crate::catalogue::AskedPair;
    use crate::client::mock::{MockClient, MockPeer, connected};
    use crate::registry::harness::{
        FIRST, Harness, registry_for, registry_for_pairs, registry_unlisted,
        registry_unlisted_pairs,
    };
    use crate::test_util::FakeSource;
    use crate::venue::Venue;
    use core_lib::shared_string::SharedString;
    use core_lib::venue::test_util::test_instrument_for;
    use md_wire::grpc::{CatalogueIdx, RejectCode};
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    const SYMBOL: &str = "btcusdt-registry-test";

    /// Hands a fresh mock client to the registry and keeps the peer half.
    ///
    /// The send is synchronous - see [`super::events::RegistryTx`] - so the event is on the
    /// registry's queue by the time this returns, ahead of anything queued after it. The
    /// reply comes back with the peer, for the tests that care that it was taken.
    fn subscribe(
        harness: &Harness,
    ) -> (
        MockPeer,
        oneshot::Receiver<Result<(), super::Refused<MockClient>>>,
    ) {
        let (peer, client) = connected();
        let queued = harness.registry.subscribe(FIRST, harness.asked(FIRST), client);
        (peer, queued)
    }

    /// The same, for a test that only wants the peer half and expects it to be taken.
    async fn subscribed(harness: &Harness) -> MockPeer {
        let (client, queued) = subscribe(harness);
        queued
            .await
            .expect("the registry task is alive")
            .expect("the registry is still spawning");
        client
    }

    async fn is_registered(harness: &Harness) -> bool {
        harness
            .registry
            .is_registered(FIRST)
            .await
            .expect("the registry task is alive")
    }

    async fn entry_token(harness: &Harness) -> Option<Arc<AtomicUsize>> {
        harness
            .registry
            .entry_token(FIRST)
            .await
            .expect("the registry task is alive")
    }

    /// The connector hard-errors a duplicate subscribe and `BookReader` is not `Clone`, so
    /// "one subscribe per symbol" is an invariant rather than an optimisation. The entry goes
    /// into the map before the broadcaster is even spawned, which is what makes the second
    /// caller find it occupied.
    #[tokio::test]
    async fn concurrent_first_clients_produce_one_connector_subscribe() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source, &[SYMBOL]);

        // Every send happens before any reply is awaited, so all eight events are on the
        // registry's queue in a row and the entry the first one inserts is what the other
        // seven find.
        let mut clients: Vec<MockPeer> = std::iter::repeat_with(|| subscribe(&harness).0)
            .take(8)
            .collect();

        for client in &mut clients {
            client
                .accepted()
                .await
                .expect("the fake source accepts every symbol");
        }
        assert_eq!(
            source.subscribed(),
            vec![SYMBOL],
            "eight clients, one connector subscription"
        );
    }

    /// The teardown race, from the side where the join wins: a join already queued keeps its
    /// broadcaster alive, and is then served.
    ///
    /// Deterministic because both events are *sent* with no await between them, so the
    /// registry handles the subscribe and then the retire back to back, with no chance for
    /// the broadcaster to take the join in between - which is the same thing the mutex this
    /// task replaced used to guarantee by not being released.
    #[tokio::test]
    async fn a_join_queued_before_the_zero_check_keeps_the_broadcaster_alive() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source, &[SYMBOL]);
        let first = subscribed(&harness).await;
        first
            .accepted()
            .await
            .expect("the fake source accepts every symbol");

        // Taken before the drop, so the retire below names this broadcaster rather than
        // whichever one happens to hold the key by then.
        let token = entry_token(&harness)
            .await
            .expect("the broadcaster is still registered");
        drop(first);

        let (second, queued) = subscribe(&harness);
        let retiring = harness
            .registry
            .retire_if_idle(Claim::new(FIRST, Arc::clone(&token)));

        assert!(
            !retiring.await.expect("the registry task is alive"),
            "a queued join must stop the broadcaster from retiring"
        );
        assert!(is_registered(&harness).await);
        queued
            .await
            .expect("the registry task is alive")
            .expect("the registry is still spawning");
        second
            .accepted()
            .await
            .expect("the fake source accepts every symbol");
        assert_eq!(
            source.subscribed().len(),
            1,
            "the join was served by the running broadcaster"
        );
    }

    /// The other side of the same race: once the entry is gone, the next client gets a fresh
    /// broadcaster, and that means a fresh connector subscription.
    #[tokio::test(start_paused = true)]
    async fn a_retired_symbol_is_subscribed_again_for_the_next_client() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source, &[SYMBOL]);

        let first = subscribed(&harness).await;
        first
            .accepted()
            .await
            .expect("the fake source accepts every symbol");
        drop(first);
        tokio::time::sleep(SESSION_SWEEP * 2).await;
        assert!(!is_registered(&harness).await);

        let second = subscribed(&harness).await;
        second
            .accepted()
            .await
            .expect("the fake source accepts every symbol");
        second.opening_snapshot().await;

        assert_eq!(
            source.subscribed(),
            vec![SYMBOL, SYMBOL],
            "the second client subscribes the symbol again"
        );
        assert_eq!(
            source.unsubscribed(),
            vec![SYMBOL],
            "and the first teardown released it exactly once"
        );

        source.publish(SYMBOL, &crate::test_util::book(&[(1.0, 1.0)], &[]));
        assert_eq!(second.next_book().await.asks[0].price, 1.0);
    }

    /// An unlisted symbol has to be refused on the connection rather than opening a stream
    /// that never produces, and it must not leave an entry behind that would shadow a later,
    /// valid attempt.
    #[tokio::test]
    async fn a_rejected_subscribe_reaches_the_client_and_leaves_no_entry() {
        let source = Arc::new(FakeSource::rejecting("nosuch is not listed as tradable"));
        let harness = registry_for(&source, &[SYMBOL]);

        let client = subscribed(&harness).await;
        let rejected = client
            .accepted()
            .await
            .expect_err("the source rejects every symbol");

        assert_eq!(rejected.code(), RejectCode::ConnectorRefused);

        assert!(
            !is_registered(&harness).await,
            "a rejected symbol must not leave an entry that shadows a later attempt"
        );
        assert_eq!(
            source.unsubscribed(),
            vec![SYMBOL],
            "the release names every pair the entry was opened with, whether or not each one \
             took - a connector with no slot for it does nothing, and the registry is the \
             only subscriber there is"
        );
    }

    /// Every client queued behind a rejected subscribe hears about it, not just the first.
    #[tokio::test]
    async fn every_queued_client_hears_a_rejection() {
        let source = Arc::new(FakeSource::rejecting("nope"));
        let harness = registry_for(&source, &[SYMBOL]);

        let mut clients = Vec::new();
        for _ in 0..4 {
            clients.push(subscribed(&harness).await);
        }

        for client in &mut clients {
            client
                .accepted()
                .await
                .expect_err("the source rejects every symbol");
        }
    }

    /// After the server has shut the registry down, a late connection is turned down instead
    /// of starting a broadcaster that nothing is left to wait for.
    #[tokio::test]
    async fn subscribing_after_shutdown_is_refused() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source, &[SYMBOL]);
        harness.registry.shut_down();

        let (_peer, client) = connected();
        let refused = harness
            .registry
            .subscribe(FIRST, harness.asked(FIRST), client)
            .await
            .expect("the registry task is alive")
            .expect_err("nothing is spawned after shutdown");
        assert_eq!(refused.code(), RejectCode::ShuttingDown);
        let (_declined, _rejected) = refused.into_parts();

        assert!(
            source.subscribed().is_empty(),
            "nothing is subscribed on the connector after shutdown"
        );
    }

    /// Shutting the registry down ends the broadcasters that are already running, which is
    /// the only shutdown signal they have.
    #[tokio::test]
    async fn shutting_down_ends_a_running_broadcaster() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source, &[SYMBOL]);
        let client = subscribed(&harness).await;
        client
            .accepted()
            .await
            .expect("the fake source accepts every symbol");
        client.opening_snapshot().await;

        harness.registry.shut_down();

        assert_eq!(
            client.ended().await.code(),
            RejectCode::StreamEnded,
            "a shutting-down broadcaster says why the stream ended rather than just closing"
        );
    }

    /// An index the catalogue does not carry at all is a different answer from one whose venue
    /// has not listed its symbol yet - the catalogue is loaded once, so the first can never
    /// become servable and the second may at any moment. Only the reject code says which.
    #[tokio::test]
    async fn an_index_the_catalogue_does_not_carry_is_refused_as_unknown() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source, &[SYMBOL]);

        let (_peer, client) = connected();
        let refused = harness
            .registry
            .subscribe(CatalogueIdx::new(99), harness.asked(CatalogueIdx::new(99)), client)
            .await
            .expect("the registry task is alive")
            .expect_err("nothing is carried at that index");

        assert_eq!(refused.code(), RejectCode::UnknownInstrument);
        assert!(
            source.subscribed().is_empty(),
            "an index this server does not carry must not reach a connector"
        );
    }

    /// A wrong symbol under an otherwise correct venue is a mismatch, not a connector problem:
    /// the catalogue no longer lists what this request claims, so it is refused before the
    /// pairs ever reach a connector, and no entry is left behind to shadow a later, honest
    /// attempt.
    #[tokio::test]
    async fn a_wrong_symbol_is_refused_as_instrument_changed() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source, &[SYMBOL]);

        let (_peer, client) = connected();
        let wrong_symbol: Box<[AskedPair]> = vec![AskedPair::new(
            SharedString::from_static_str(Venue::BinanceSpot.as_str()),
            SharedString::from_static_str("not-the-symbol"),
        )]
        .into_boxed_slice();
        let refused = harness
            .registry
            .subscribe(FIRST, wrong_symbol, client)
            .await
            .expect("the registry task is alive")
            .expect_err("the symbol no longer matches what the catalogue carries");

        assert_eq!(refused.code(), RejectCode::InstrumentChanged);
        assert!(
            !is_registered(&harness).await,
            "a stale subscribe must not leave an entry that shadows a later, honest attempt"
        );
        assert!(
            source.subscribed().is_empty(),
            "a stale subscribe must not reach a connector"
        );
    }

    /// The same, for a wrong venue rather than a wrong symbol - either half of a pair failing
    /// to match is the same answer.
    #[tokio::test]
    async fn a_wrong_venue_is_refused_as_instrument_changed() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source, &[SYMBOL]);

        let (_peer, client) = connected();
        let wrong_venue: Box<[AskedPair]> = vec![AskedPair::new(
            SharedString::from_static_str(Venue::Bitstamp.as_str()),
            SharedString::from_static_str(SYMBOL),
        )]
        .into_boxed_slice();
        let refused = harness
            .registry
            .subscribe(FIRST, wrong_venue, client)
            .await
            .expect("the registry task is alive")
            .expect_err("the venue no longer matches what the catalogue carries");

        assert_eq!(refused.code(), RejectCode::InstrumentChanged);
    }

    /// The stale check runs ahead of resolution, so a mismatch is reported as a mismatch even
    /// when nothing has interned the (wrong) symbol yet - it must not be reported as
    /// `UnlistedSymbol` instead, which would tell a stale client to keep retrying forever.
    #[tokio::test]
    async fn a_mismatched_pair_list_is_refused_even_before_the_index_resolves() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_unlisted(&source, &[SYMBOL]);

        let (_peer, client) = connected();
        let wrong: Box<[AskedPair]> = vec![AskedPair::new(
            SharedString::from_static_str(Venue::BinanceSpot.as_str()),
            SharedString::from_static_str("not-the-symbol"),
        )]
        .into_boxed_slice();
        let refused = harness
            .registry
            .subscribe(FIRST, wrong, client)
            .await
            .expect("the registry task is alive")
            .expect_err("the pairs do not match, resolved or not");

        assert_eq!(refused.code(), RejectCode::InstrumentChanged);
        assert!(
            source.subscribed().is_empty(),
            "a stale subscribe must not reach a connector"
        );
    }

    /// An empty pair list is a mismatch too: a carried instrument always lists at least one
    /// pair, so naming none is never what the catalogue actually carries at that index.
    #[tokio::test]
    async fn an_empty_pair_list_against_a_carried_instrument_is_refused() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source, &[SYMBOL]);

        let (_peer, client) = connected();
        let refused = harness
            .registry
            .subscribe(FIRST, Vec::new().into_boxed_slice(), client)
            .await
            .expect("the registry task is alive")
            .expect_err("an empty pair list never matches a carried instrument");

        assert_eq!(refused.code(), RejectCode::InstrumentChanged);
    }

    /// A venue name in a different case still matches: `Venue::parse` is case-insensitive, and
    /// a correct client must not be told its catalogue changed just because it spelled the
    /// venue differently than this build's own `as_str`.
    #[tokio::test]
    async fn a_venue_name_in_the_wrong_case_still_matches() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source, &[SYMBOL]);

        let (_peer, client) = connected();
        let shouted: Box<[AskedPair]> = vec![AskedPair::new(
            SharedString::from(Venue::BinanceSpot.as_str().to_uppercase()),
            SharedString::from_static_str(SYMBOL),
        )]
        .into_boxed_slice();
        harness
            .registry
            .subscribe(FIRST, shouted, client)
            .await
            .expect("the registry task is alive")
            .expect("a different case is still the same venue");
    }

    /// A second client is checked the same as the first, even though a broadcaster is already
    /// running for the key: a stale client must not be able to join a fresh broadcaster simply
    /// by arriving after it started.
    #[tokio::test]
    async fn a_stale_client_is_refused_even_against_a_running_broadcaster() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source, &[SYMBOL]);
        let first = subscribed(&harness).await;
        first
            .accepted()
            .await
            .expect("the fake source accepts every symbol");

        let (_peer, client) = connected();
        let wrong: Box<[AskedPair]> = vec![AskedPair::new(
            SharedString::from_static_str(Venue::BinanceSpot.as_str()),
            SharedString::from_static_str("not-the-symbol"),
        )]
        .into_boxed_slice();
        let refused = harness
            .registry
            .subscribe(FIRST, wrong, client)
            .await
            .expect("the registry task is alive")
            .expect_err("the pairs do not match, even against a running broadcaster");

        assert_eq!(refused.code(), RejectCode::InstrumentChanged);
    }

    /// The pacing that keeps a client retrying in a tight loop off the process-global
    /// instrument lock: between sweeps, an unresolved index is refused without a lookup.
    ///
    /// Observable because the symbol is interned *between* the two attempts. The second is
    /// still refused - which it could only be if nothing probed - and the third, after the
    /// clock has moved past the next sweep, succeeds.
    #[tokio::test(start_paused = true)]
    async fn a_second_request_inside_the_sweep_window_does_not_probe_again() {
        const UNLISTED: &str = "sweepwindow-btcusdt";

        let source = Arc::new(FakeSource::default());
        let harness = registry_unlisted(&source, &[UNLISTED]);

        let (_first, first_reply) = subscribe(&harness);
        assert_eq!(
            first_reply
                .await
                .expect("the registry task is alive")
                .expect_err("nothing has interned the symbol yet")
                .code(),
            RejectCode::UnlistedSymbol
        );

        let _ = test_instrument_for(Venue::BinanceSpot, UNLISTED);

        let (_second, second_reply) = subscribe(&harness);
        assert_eq!(
            second_reply
                .await
                .expect("the registry task is alive")
                .expect_err("the next sweep is not due yet")
                .code(),
            RejectCode::UnlistedSymbol,
            "an unresolved index inside the sweep window is refused without a lookup"
        );

        tokio::time::sleep(Duration::from_secs(1)).await;
        let third = subscribed(&harness).await;
        third
            .accepted()
            .await
            .expect("the fake source accepts every symbol");
        assert_eq!(
            source.subscribed(),
            vec![UNLISTED],
            "the sweep that came due is what made the instrument servable"
        );
    }

    /// A sweep is one pass over everything unresolved, so it resolves what it can and leaves
    /// the rest - it is not all-or-nothing, and a name no venue has listed does not hold back
    /// the ones that have been.
    #[tokio::test]
    async fn one_sweep_resolves_what_it_can_and_leaves_the_rest() {
        const LISTED: &str = "partialsweep-listed";
        const UNLISTED: &str = "partialsweep-unlisted";

        let source = Arc::new(FakeSource::default());
        let harness = registry_unlisted(&source, &[LISTED, UNLISTED]);
        let _ = test_instrument_for(Venue::BinanceSpot, LISTED);

        let (peer, listed) = connected();
        listed_reply(&harness, listed, CatalogueIdx::new(0)).await;
        peer.accepted()
            .await
            .expect("the fake source accepts every symbol");

        let (_peer, unlisted) = connected();
        let refused = harness
            .registry
            .subscribe(CatalogueIdx::new(1), harness.asked(CatalogueIdx::new(1)), unlisted)
            .await
            .expect("the registry task is alive")
            .expect_err("nothing has interned the second symbol");
        assert_eq!(refused.code(), RejectCode::UnlistedSymbol);
        assert_eq!(
            source.subscribed(),
            vec![LISTED],
            "the entry that resolved was served, and the one that did not was not"
        );
    }

    /// The pair-list spellings the two-venue tests use. Distinct on purpose: a catalogue pair
    /// is one venue's own spelling of an instrument, and `FakeSource` files its live streams
    /// by symbol.
    const ON_BINANCE: &str = "btcusdt-registry-pairs";
    const ON_BITSTAMP: &str = "btcusd-registry-pairs";

    fn both_venues() -> [(Venue, &'static str); 2] {
        [
            (Venue::BinanceSpot, ON_BINANCE),
            (Venue::Bitstamp, ON_BITSTAMP),
        ]
    }

    /// One instrument, one broadcaster, one subscribe *per pair* - the invariant that used to
    /// be "one per symbol" now that a broadcaster is filed under a catalogue index.
    #[tokio::test]
    async fn every_pair_of_an_instrument_is_subscribed_once() {
        let binance_spot = Arc::new(FakeSource::default());
        let bitstamp = Arc::new(FakeSource::default());
        let harness = registry_for_pairs(&binance_spot, &bitstamp, &[&both_venues()]);

        let client = subscribed(&harness).await;
        client
            .accepted()
            .await
            .expect("the fake sources accept every symbol");

        assert_eq!(binance_spot.subscribed(), vec![ON_BINANCE]);
        assert_eq!(bitstamp.subscribed(), vec![ON_BITSTAMP]);
    }

    /// All-or-nothing resolution: an instrument is not servable until *every* one of its
    /// venues has interned its spelling, and it becomes servable the moment the last one does.
    #[tokio::test]
    async fn an_instrument_resolves_only_once_every_venue_lists_it() {
        const ONE: &str = "btcusdt-registry-halflisted";
        const OTHER: &str = "btcusd-registry-halflisted";

        let binance_spot = Arc::new(FakeSource::default());
        let bitstamp = Arc::new(FakeSource::default());
        let pairs = [(Venue::BinanceSpot, ONE), (Venue::Bitstamp, OTHER)];
        // Only the first venue has listed its spelling when the registry starts.
        let _ = test_instrument_for(Venue::BinanceSpot, ONE);
        let harness = registry_unlisted_pairs(&binance_spot, &bitstamp, &[&pairs]);

        let (_peer, client) = connected();
        let refused = harness
            .registry
            .subscribe(FIRST, harness.asked(FIRST), client)
            .await
            .expect("the registry task is alive")
            .expect_err("the second venue has not interned its spelling yet");
        assert_eq!(refused.code(), RejectCode::UnlistedSymbol);
        assert!(
            binance_spot.subscribed().is_empty(),
            "a half-resolved instrument must not subscribe the venue that is ready"
        );

        let _ = test_instrument_for(Venue::Bitstamp, OTHER);
        // Past the resolution backoff, so the next subscribe pays for a fresh sweep - the
        // same wait `a_second_request_inside_the_sweep_window_does_not_probe_again` makes.
        tokio::time::sleep(Duration::from_secs(1)).await;

        let peer = subscribed(&harness).await;
        peer.accepted()
            .await
            .expect("both venues have listed their spellings now");
        assert_eq!(binance_spot.subscribed(), vec![ONE]);
        assert_eq!(bitstamp.subscribed(), vec![OTHER]);
    }

    /// All-or-nothing subscription: a book missing a venue is not the book the catalogue
    /// advertises, so the venue that accepted is released again and the client hears why.
    #[tokio::test]
    async fn one_venue_refusing_releases_the_other_and_refuses_the_client() {
        let binance_spot = Arc::new(FakeSource::default());
        let bitstamp = Arc::new(FakeSource::rejecting("btcusd is not listed as tradable"));
        let harness = registry_for_pairs(&binance_spot, &bitstamp, &[&both_venues()]);

        let client = subscribed(&harness).await;
        let rejected = client
            .accepted()
            .await
            .expect_err("one of the two sources rejects every symbol");
        assert_eq!(rejected.code(), RejectCode::ConnectorRefused);

        let released = tokio::time::timeout(Duration::from_secs(5), async {
            while binance_spot.unsubscribed().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            released.is_ok(),
            "the venue that accepted must be released again, or its symbol is stuck \
             subscribed with nothing reading it"
        );
        assert_eq!(binance_spot.unsubscribed(), vec![ON_BINANCE]);
        assert!(
            !is_registered(&harness).await,
            "a refused instrument must not leave an entry that shadows a later attempt"
        );
    }

    /// Retiring releases every pair, not just the one that happens to be first.
    #[tokio::test]
    async fn retiring_releases_every_pair() {
        let binance_spot = Arc::new(FakeSource::default());
        let bitstamp = Arc::new(FakeSource::default());
        let harness = registry_for_pairs(&binance_spot, &bitstamp, &[&both_venues()]);

        let client = subscribed(&harness).await;
        client
            .accepted()
            .await
            .expect("the fake sources accept every symbol");
        drop(client);

        let released = tokio::time::timeout(Duration::from_secs(5), async {
            while binance_spot.unsubscribed().is_empty() || bitstamp.unsubscribed().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(released.is_ok(), "both pairs must be released");
        assert_eq!(binance_spot.unsubscribed(), vec![ON_BINANCE]);
        assert_eq!(bitstamp.unsubscribed(), vec![ON_BITSTAMP]);
    }

    /// Queues `client` on `idx` and expects the registry to take it.
    async fn listed_reply(harness: &Harness, client: MockClient, idx: CatalogueIdx) {
        harness
            .registry
            .subscribe(idx, harness.asked(idx), client)
            .await
            .expect("the registry task is alive")
            .expect("the instrument resolves");
    }

    /// A panicking event handler must not take the registry down with it - the map, the
    /// connectors and every running broadcaster's only way in all hang off this one task.
    ///
    /// The same judgement the `std::sync::Mutex` this task replaced made by recovering its
    /// poison flag rather than re-panicking, and it rests on the same fact: a handler is a
    /// hash lookup, a channel send and an atomic, so a panic part-way through one cannot
    /// leave the map torn.
    #[tokio::test]
    async fn a_panicking_handler_does_not_end_the_registry() {
        let source = Arc::new(FakeSource::panicking_unsubscribe());
        let harness = registry_for(&source, &[SYMBOL]);
        let client = subscribed(&harness).await;
        client
            .accepted()
            .await
            .expect("the fake source accepts every symbol");

        // Hanging up retires the symbol, and the connector unsubscribe that retirement
        // issues is what blows up - inside the handler, after the entry has been taken out.
        drop(client);

        let answering = tokio::time::timeout(Duration::from_secs(5), async {
            while is_registered(&harness).await {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            answering.is_ok(),
            "the registry must still be answering after a handler panicked"
        );

        let next = subscribed(&harness).await;
        next.accepted()
            .await
            .expect("the fake source accepts every symbol");
        assert_eq!(
            source.subscribed().len(),
            2,
            "and must still be starting broadcasters"
        );
    }
}

/// The registry task a unit test drives, over fake connectors and a catalogue built in place.
///
/// Here rather than in [`crate::test_util`] because it wraps this module's own task. The
/// end-to-end test stands a whole server up through [`crate::server::serve`] instead, so it
/// needs none of this.
#[cfg(test)]
pub(crate) mod harness {
    use super::RegistryHandle;
    use super::events::RegistryTx;
    use crate::catalogue::{AskedPair, Catalogue};
    use crate::client::mock::MockClient;
    use crate::test_util::{FakeConnectors, FakeSource};
    use crate::venue::Venue;
    use core_lib::shared_string::SharedString;
    use core_lib::venue::test_util::test_instrument_for;
    use md_wire::grpc::CatalogueIdx;
    use std::sync::Arc;

    /// The index of the first symbol a harness carries. Tests that name one instrument name
    /// this one.
    pub(crate) const FIRST: CatalogueIdx = CatalogueIdx::new(0);

    /// A registry over one Binance-side source, carrying `symbols` at their position in the
    /// slice - and with each of them interned, the way a connector's listing refresh would
    /// have, so every one resolves on the first sweep.
    pub(crate) fn registry_for(source: &Arc<FakeSource>, symbols: &[&'static str]) -> Harness {
        for symbol in symbols {
            let _ = test_instrument_for(Venue::BinanceSpot, symbol);
        }
        registry_unlisted(source, symbols)
    }

    /// The same, without interning anything: the catalogue names the symbols and whether
    /// their venue has listed them is the test's to arrange.
    pub(crate) fn registry_unlisted(source: &Arc<FakeSource>, symbols: &[&'static str]) -> Harness {
        let entries: Vec<[(Venue, &'static str); 1]> = symbols
            .iter()
            .map(|symbol| [(Venue::BinanceSpot, *symbol)])
            .collect();
        let instruments: Vec<&[(Venue, &'static str)]> = entries
            .iter()
            .map(<[(Venue, &'static str); 1]>::as_slice)
            .collect();

        build(source, &Arc::new(FakeSource::default()), &instruments)
    }

    /// A registry over both venues' sources, carrying one instrument per entry of
    /// `instruments` - each spelled out pair by pair, and each pair interned so the whole
    /// entry resolves on the first sweep.
    ///
    /// For the merged-book tests, which need a source per venue to publish a different book
    /// on each side of one instrument.
    pub(crate) fn registry_for_pairs(
        binance_spot: &Arc<FakeSource>,
        bitstamp: &Arc<FakeSource>,
        instruments: &[&[(Venue, &'static str)]],
    ) -> Harness {
        for pairs in instruments {
            for (venue, symbol) in *pairs {
                let _ = test_instrument_for(*venue, symbol);
            }
        }
        build(binance_spot, bitstamp, instruments)
    }

    /// The same, without interning anything: which of the pairs their venues have listed is
    /// the test's to arrange, one at a time.
    pub(crate) fn registry_unlisted_pairs(
        binance_spot: &Arc<FakeSource>,
        bitstamp: &Arc<FakeSource>,
        instruments: &[&[(Venue, &'static str)]],
    ) -> Harness {
        build(binance_spot, bitstamp, instruments)
    }

    /// The shared half: a catalogue filing `instruments` under their position in the slice,
    /// and a registry over both sources serving it.
    fn build(
        binance_spot: &Arc<FakeSource>,
        bitstamp: &Arc<FakeSource>,
        instruments: &[&[(Venue, &'static str)]],
    ) -> Harness {
        let carried: Vec<(u32, &[(Venue, &str)])> = instruments
            .iter()
            .enumerate()
            .map(|(position, pairs)| {
                let idx = u32::try_from(position).expect("a test carries a handful of symbols");
                (idx, *pairs)
            })
            .collect();

        let connectors = FakeConnectors::new(Arc::clone(binance_spot), Arc::clone(bitstamp));
        let catalogue = Catalogue::for_test(&carried);
        let handle = RegistryHandle::spawn(connectors, catalogue.into_instruments());

        Harness {
            registry: handle.tx(),
            instruments: instruments.iter().map(|pairs| pairs.to_vec()).collect(),
        }
    }

    /// What [`registry_for`] hands back: the registry under test, reached the same way a
    /// broadcaster reaches it.
    ///
    /// The [`super::RegistryHandle`] is dropped rather than kept: dropping its `JoinHandle`
    /// only detaches the task, and the `RegistryTx` here is what keeps it running - for
    /// exactly as long as the test holds the harness. A test that wants the connectors back
    /// stands its own handle up instead.
    #[derive(Debug)]
    pub(crate) struct Harness {
        pub(crate) registry: RegistryTx<MockClient>,
        /// Mirrors what `build` filed the catalogue as, so [`Harness::asked`] can hand back
        /// exactly the pairs a well-behaved client would echo for an index without a test
        /// re-deriving the catalogue's own shape.
        instruments: Vec<Vec<(Venue, &'static str)>>,
    }

    impl Harness {
        /// The pairs a well-behaved client would send back for `idx` - exactly what this
        /// harness's catalogue lists there. What every test but the ones about a *mismatched*
        /// pair list should pass to a subscribe.
        pub(crate) fn asked(&self, idx: CatalogueIdx) -> Box<[AskedPair]> {
            let position = usize::try_from(idx.get()).expect("a test index fits a usize");
            self.instruments
                .get(position)
                .into_iter()
                .flatten()
                .map(|&(venue, symbol)| {
                    AskedPair::new(
                        SharedString::from_static_str(venue.as_str()),
                        SharedString::from_static_str(symbol),
                    )
                })
                .collect()
        }
    }
}
