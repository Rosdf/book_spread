//! Which symbols are being broadcast, the two races around starting and stopping one, and
//! turning a catalogue index into the instrument a broadcaster is filed under.

use crate::catalogue::CataloguePair;
use crate::registry::events::{
    Claim, RegistryEvent, RegistryRx, RegistryTx, RegistryWeakTx, create_event_channel,
};
use crate::venue::{BookSource as _, Connectors};
use core_lib::Venue;
use core_lib::instrument::{Instrument, InstrumentId};
use core_lib::map::{InternalHashMap, new_internal_map};
use core_lib::panic::panic_message;
use core_lib::venue::backoff::Backoff;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::time::Instant;
use crate::broadcast::broadcaster::{Broadcaster, Join};
use crate::broadcast::queue::{make_broadcaster_channel, BroadcasterTx};
use crate::client::ClientHandshake;
use md_wire::grpc::{CatalogueIdx, RejectCode, Rejected, VenueIdx};

pub(crate) mod events;

/// The longest gap between two resolution sweeps.
///
/// The same order as the connectors' own hourly listing refresh is *not* what is wanted here:
/// a sweep is how a catalogue entry becomes servable at all, so a minute is the longest a
/// client that is retrying should have to wait after its venue has caught up. [`Backoff`]
/// starts at 250ms and jitters, so a cold start converges in well under a second and a name
/// that is never listed costs one sweep a minute.
const RESOLVE_SWEEP_MAX: Duration = Duration::from_secs(60);

/// A catalogue entry that has become servable: its venue's connector has interned the symbol,
/// and the index the encoder will stamp on every level of its book.
#[derive(Debug, Clone, Copy)]
struct Resolved {
    instrument: Instrument,
    venue_idx: VenueIdx,
}

/// The registry's half of one running broadcaster.
#[derive(Debug)]
struct Entry<C> {
    joins: BroadcasterTx<C>,
    /// Joins already queued on `joins` that the broadcaster has not taken yet.
    ///
    /// This is what closes the teardown race: the increment happens on the registry task,
    /// right before the send, and the zero-check happens on that same task, so a broadcaster
    /// can never retire out from under a join that is already on its way. Doubles as
    /// identity - the `Arc` is compared by pointer to tell one generation of broadcaster for
    /// a key from the next.
    pending_joins: Arc<AtomicUsize>,
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
    pub(crate) fn spawn(
        connectors: V,
        instruments: InternalHashMap<CatalogueIdx, Box<[CataloguePair]>>,
    ) -> Self {
        let (rx, tx) = create_event_channel();
        let registry = Registry {
            entries: new_internal_map(),
            connectors,
            resolved: new_internal_map(),
            unresolved: instruments,
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

/// The `(venue, symbol)` -> broadcaster map, plus the connectors those broadcasters use.
///
/// Lives on one task and is reached only through its queue, the same way a venue connector
/// is reached through `ConnectorTx`. That is what orders the connector `unsubscribe` a
/// retiring broadcaster issues ahead of the `subscribe` the next broadcaster for that key
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
    entries: InternalHashMap<InstrumentId, Entry<C>>,
    connectors: V,
    /// Catalogue entries whose venue has interned their symbol. Nothing ever leaves this map:
    /// an `Instrument` handle stays valid for the life of the process.
    resolved: InternalHashMap<CatalogueIdx, Resolved>,
    /// Catalogue entries not interned as of the last sweep. Emptied into `resolved` as the
    /// connectors catch up.
    unresolved: InternalHashMap<CatalogueIdx, Box<[CataloguePair]>>,
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
                let (idx, sock, reply) = joining.into_parts();
                // A dropped receiver just means the handshake gave up; the client in a
                // `Refused` goes with it, which closes the connection - the same answer,
                // unwritten.
                let _ = reply.send(self.subscribe(idx, sock));
            }
            RegistryEvent::RetireIfIdle(retiring) => {
                let (claim, reply) = retiring.into_parts();
                let _ = reply.send(self.retire_if_idle(&claim));
            }
            RegistryEvent::Retire(claim) => self.retire(&claim),
            RegistryEvent::Abandon(claim) => {
                self.take_entry(&claim);
            }
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
    /// An index the catalogue does not carry, or one whose venue has not interned its symbol
    /// yet, is refused here - before any broadcaster exists - which is what keeps an
    /// unservable subscribe from costing a connector round trip.
    fn subscribe(&mut self, idx: CatalogueIdx, client: C) -> Result<(), Refused<C>> {
        match self.resolve(idx) {
            Ok(resolved) => self.subscribe_resolved(resolved, client),
            Err(rejected) => Err(Refused { client, rejected }),
        }
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
    /// [`RejectCode::UnknownInstrument`] for an index this server does not carry at all -
    /// permanent, since the catalogue is loaded once - and [`RejectCode::UnlistedSymbol`] for
    /// one whose venue has not interned its symbol yet, which is worth retrying.
    fn resolve(&mut self, idx: CatalogueIdx) -> Result<Resolved, Rejected> {
        if let Some(resolved) = self.resolved.get(&idx) {
            return Ok(*resolved);
        }
        if !self.unresolved.contains_key(&idx) {
            return Err(Rejected::new(
                RejectCode::UnknownInstrument,
                format!("this server does not carry instrument {}", idx.get()).into_boxed_str(),
            ));
        }

        let unlisted = || {
            Rejected::new(
                RejectCode::UnlistedSymbol,
                format!(
                    "instrument {} is not listed by its venue yet",
                    idx.get()
                )
                .into_boxed_str(),
            )
        };
        if Instant::now() < self.next_sweep {
            return Err(unlisted());
        }

        self.sweep();
        self.resolved.get(&idx).copied().ok_or_else(unlisted)
    }

    /// Moves every catalogue entry whose venue has now interned its symbol into `resolved`.
    ///
    /// One pass over everything still unresolved rather than a lookup for the one index that
    /// was asked for: the lookups share a sweep, so the cost of answering the *next* request
    /// for a different index has already been paid, and the backoff below bounds how often
    /// this happens at all.
    ///
    /// An entry resolves on its first pair, which is the one that will be served. An entry
    /// with no pairs at all never resolves - it names nothing to subscribe to - and simply
    /// stays here.
    fn sweep(&mut self) {
        let before = self.unresolved.len();
        let resolved = &mut self.resolved;
        self.unresolved.retain(|idx, pairs| {
            let Some(first) = pairs.first() else {
                return true;
            };
            // `true` keeps the entry here: it is still unresolved.
            Instrument::lookup(first.venue(), first.symbol()).is_none_or(|instrument| {
                resolved.insert(
                    *idx,
                    Resolved {
                        instrument,
                        venue_idx: first.venue_idx(),
                    },
                );
                false
            })
        });

        // Progress means the connectors are still catching up, so the next sweep is worth
        // making soon; no progress lets the interval grow towards `RESOLVE_SWEEP_MAX`.
        if self.unresolved.len() < before {
            self.resolve_backoff.reset();
        }
        self.next_sweep = Instant::now() + self.resolve_backoff.next_delay();
    }

    /// Hands `client` to `resolved`'s broadcaster, starting one if this is the first client.
    ///
    /// The broadcaster answers on the connection itself - response headers, or a refusal
    /// carrying the venue's own reason - so a symbol the venue does not list ends the
    /// connection rather than opening a stream that never produces anything. Nothing comes
    /// back to the caller except a client the registry declined to take at all.
    ///
    /// Every client of a brand new symbol is served by this one task, and the entry goes into
    /// the map before the broadcaster is even spawned, so all but the first can only find it
    /// occupied and queue a join. Exactly one `BookSource::subscribe` is ever issued per key -
    /// which is an invariant, not a nicety: the connector hard-errors a duplicate subscribe,
    /// and `BookReader` is not `Clone`, so a symbol has exactly one reader.
    fn subscribe_resolved(&mut self, resolved: Resolved, client: C) -> Result<(), Refused<C>> {
        let Resolved {
            instrument,
            venue_idx,
        } = resolved;
        let mut join = Join::new(client);

        if let Some(entry) = self.entries.get(&instrument.id()) {
            // Incremented before the send, so a broadcaster whose `RetireIfIdle` is queued
            // behind this event sees the join and stays alive for it.
            entry.pending_joins.fetch_add(1, Ordering::Relaxed);
            match entry.joins.send(join) {
                Ok(()) => return Ok(()),
                // The broadcaster is gone but its `Retire` has not been handled yet. Drop the
                // stale entry and start a fresh one below.
                Err(returned) => {
                    join = returned;
                    self.entries.remove(&instrument.id());
                }
            }
        }

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
            instrument.id(),
            Entry {
                joins,
                pending_joins: Arc::clone(&pending_joins),
            },
        );

        // Issued here rather than on the broadcaster, because this is the only place that can
        // put it after the `unsubscribe` of whatever generation held this key before. It
        // costs no await: `BookSource::subscribe` hands back the reply channel rather than a
        // future, so the broadcaster is what waits for the venue.
        let sub = self.connectors.source(instrument.venue()).subscribe(instrument.id());

        tokio::spawn(async move {
            let reader = match sub.await {
                Ok(Ok(reader)) => reader,
                Ok(Err(err)) => {
                    tracing::warn!(
                        venue = instrument.venue().as_str(),
                        symbol = instrument.name(),
                        %err,
                        "subscribe rejected"
                    );
                    // Queued before the drain: once the registry has taken the entry out, no
                    // further join can be sent here, so the drain below answers every one there
                    // will ever be. Nothing to unsubscribe - this broadcaster never held a
                    // subscription.
                    tx.abandon(Claim::new(instrument.id(), instrument.venue(), pending_joins));
                    queued
                        .drain(Rejected::new(
                            RejectCode::ConnectorRefused,
                            err.to_string().into_boxed_str(),
                        ))
                        .await;
                    return;
                }
                Err(_) => {
                    tx.abandon(Claim::new(instrument.id(), instrument.venue(), pending_joins));
                    queued
                        .drain(Rejected::new(
                            RejectCode::ConnectorGone,
                            Box::from("connector stopped before it could subscribe"),
                        ))
                        .await;
                    return;
                }
            };

            Broadcaster::start(tx, instrument, venue_idx, queued, pending_joins, reader).await;
        });
        Ok(())
    }

    /// Called for a broadcaster whose session list has just emptied. `true` means the entry
    /// is gone and the task should stop.
    fn retire_if_idle(&mut self, claim: &Claim) -> bool {
        let Some(entry) = self.entries.get(&claim.instrument_id()) else {
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

        self.entries.remove(&claim.instrument_id());
        self.unsubscribe(claim.venue(), claim.instrument_id());
        true
    }

    /// Removes a broadcaster that is stopping for a reason other than an idle session list -
    /// its connector dropped the symbol, or the server is shutting down - and releases the
    /// connector subscription with it.
    ///
    /// A no-op when the entry is already gone or belongs to a newer broadcaster, so a task
    /// that exited through [`Registry::retire_if_idle`] can still send this on its way out.
    fn retire(&mut self, claim: &Claim) {
        if self.take_entry(claim) {
            self.unsubscribe(claim.venue(), claim.instrument_id());
        }
    }

    /// Removes `claim`'s entry if it is still the one `claim` names. `false` means it was
    /// already gone - [`RegistryEvent::ShutDown`] cleared it, say - or belongs to a newer
    /// broadcaster for the same key.
    ///
    /// On its own this is the `Abandon` case: a broadcaster whose subscribe the connector
    /// rejected sends no unsubscribe, because it never held a subscription, and if the
    /// rejection was "already subscribed" then the subscription belongs to someone else.
    fn take_entry(&mut self, claim: &Claim) -> bool {
        let Some(entry) = self.entries.get(&claim.instrument_id()) else {
            return false;
        };
        if !Arc::ptr_eq(&entry.pending_joins, claim.pending_joins()) {
            return false;
        }

        self.entries.remove(&claim.instrument_id());
        true
    }

    fn unsubscribe(&self, venue: Venue, instrument_id: InstrumentId) {
        self.connectors.source(venue).unsubscribe(instrument_id);
    }
}

#[cfg(test)]
mod test {
    use super::events::Claim;
    use crate::broadcast::broadcaster::SESSION_SWEEP;
    use crate::registry::harness::{FIRST, Harness, registry_for, registry_unlisted};
    use crate::client::mock::{MockClient, MockPeer, connected};
    use crate::test_util::FakeSource;
    use crate::venue::Venue;
    use core_lib::instrument::Instrument;
    use core_lib::venue::test_util::test_instrument_for;
    use md_wire::grpc::{CatalogueIdx, RejectCode};
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    const SYMBOL: &str = "btcusdt-registry-test";

    fn key() -> Instrument {
        test_instrument_for(Venue::BinanceSpot, SYMBOL)
    }

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
        let queued = harness.registry.subscribe(FIRST, client);
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
            .is_registered(key().id())
            .await
            .expect("the registry task is alive")
    }

    async fn entry_token(harness: &Harness) -> Option<Arc<AtomicUsize>> {
        harness
            .registry
            .entry_token(key().id())
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
        let retiring = harness.registry.retire_if_idle(Claim::new(
            key().id(),
            key().venue(),
            Arc::clone(&token),
        ));

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
        assert!(
            source.unsubscribed().is_empty(),
            "a broadcaster that never held a subscription must not release one"
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
            .subscribe(FIRST, client)
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
            .subscribe(CatalogueIdx::new(99), client)
            .await
            .expect("the registry task is alive")
            .expect_err("nothing is carried at that index");

        assert_eq!(refused.code(), RejectCode::UnknownInstrument);
        assert!(
            source.subscribed().is_empty(),
            "an index this server does not carry must not reach a connector"
        );
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
            .subscribe(CatalogueIdx::new(1), unlisted)
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

    /// Queues `client` on `idx` and expects the registry to take it.
    async fn listed_reply(harness: &Harness, client: MockClient, idx: CatalogueIdx) {
        harness
            .registry
            .subscribe(idx, client)
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
    use crate::catalogue::Catalogue;
    use crate::test_util::{FakeConnectors, FakeSource};
    use crate::client::mock::MockClient;
    use crate::venue::Venue;
    use core_lib::venue::test_util::test_instrument_for;
    use md_wire::grpc::CatalogueIdx;
    use std::sync::Arc;

    /// The index of the first symbol a harness carries. Tests that name one instrument name
    /// this one.
    pub(crate) const FIRST: CatalogueIdx = CatalogueIdx::new(0);

    /// A registry over one Binance-side source, carrying `symbols` at their position in the
    /// slice - and with each of them interned, the way a connector's listing refresh would
    /// have, so every one resolves on the first sweep.
    pub(crate) fn registry_for(source: &Arc<FakeSource>, symbols: &[&str]) -> Harness {
        for symbol in symbols {
            let _ = test_instrument_for(Venue::BinanceSpot, symbol);
        }
        registry_unlisted(source, symbols)
    }

    /// The same, without interning anything: the catalogue names the symbols and whether
    /// their venue has listed them is the test's to arrange.
    pub(crate) fn registry_unlisted(source: &Arc<FakeSource>, symbols: &[&str]) -> Harness {
        let entries: Vec<(u32, [(Venue, &str); 1])> = symbols
            .iter()
            .enumerate()
            .map(|(position, symbol)| {
                let idx = u32::try_from(position).expect("a test carries a handful of symbols");
                (idx, [(Venue::BinanceSpot, *symbol)])
            })
            .collect();
        let carried: Vec<(u32, &[(Venue, &str)])> = entries
            .iter()
            .map(|(idx, pairs)| (*idx, pairs.as_slice()))
            .collect();

        let connectors = FakeConnectors::new(Arc::clone(source), FakeSource::default());
        let catalogue = Catalogue::for_test(&carried);
        let handle = RegistryHandle::spawn(connectors, catalogue.into_instruments());

        Harness {
            registry: handle.tx(),
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
    }
}
