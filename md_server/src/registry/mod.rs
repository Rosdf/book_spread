//! Which symbols are being broadcast, and the two races around starting and stopping one.

use crate::broadcast::{Broadcaster, Join};
use crate::registry::events::{
    Claim, RegistryEvent, RegistryRx, RegistryTx, RegistryWeakTx, create_event_channel,
};
use crate::venue::{BookSource as _, Connectors, Venue};
use core_lib::panic::panic_message;
use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;

pub(crate) mod events;

/// Identifies one book stream.
///
/// Also where a `BookUpdate`'s levels get their `venue` from: `SmallBook` carries no identity
/// of its own, so the only thing that knows what a book is *of* is the key of the broadcaster
/// holding its reader. The symbol itself is no longer on the wire at all.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Key {
    venue: Venue,
    symbol: Box<str>,
}

impl Key {
    /// `symbol` must already be lowercase - see [`crate::request`], which is where a
    /// request's symbol is validated and normalised. Every connector keys its subscriptions
    /// by the lowercase form, so two keys differing only in case would race for one
    /// subscription and the loser would be rejected as already subscribed.
    pub(crate) fn new(venue: Venue, symbol: Box<str>) -> Self {
        debug_assert!(
            symbol.bytes().all(|b| !b.is_ascii_uppercase()),
            "keys must hold the lowercase symbol the connector uses"
        );
        Self { venue, symbol }
    }

    pub(crate) fn venue(&self) -> Venue {
        self.venue
    }

    pub(crate) fn symbol(&self) -> &str {
        &self.symbol
    }
}

/// The registry's half of one running broadcaster.
#[derive(Debug)]
struct Entry<S> {
    joins: mpsc::UnboundedSender<Join<S>>,
    /// Joins already queued on `joins` that the broadcaster has not taken yet.
    ///
    /// This is what closes the teardown race: the increment happens on the registry task,
    /// right before the send, and the zero-check happens on that same task, so a broadcaster
    /// can never retire out from under a join that is already on its way. Doubles as
    /// identity - the `Arc` is compared by pointer to tell one generation of broadcaster for
    /// a key from the next.
    pending_joins: Arc<AtomicUsize>,
}

/// A socket handed back because the registry would not start a broadcaster for it.
///
/// Returned rather than refused in place so the refusal is written by the caller, which is
/// already in the async context the handshake runs in.
#[derive(Debug)]
pub(crate) struct Refused<S> {
    sock: S,
    why: &'static str,
}

impl<S> Refused<S> {
    pub(crate) fn into_parts(self) -> (S, &'static str) {
        (self.sock, self.why)
    }
}

/// The way in to the registry task: hands out [`RegistryTx`] clones, and reclaims the
/// connectors when the task is done with them.
///
/// One per server. Everything that talks to the registry - the accept loop, each handshake,
/// each broadcaster - holds a `RegistryTx` instead, which is what makes this the only place
/// that can shut the registry down.
#[derive(Debug)]
pub(crate) struct RegistryHandle<C, S> {
    tx: RegistryTx<S>,
    task: tokio::task::JoinHandle<C>,
}

impl<C: Connectors, S: AsyncRead + AsyncWrite + Unpin + Send + 'static> RegistryHandle<C, S> {
    /// Spawns the registry task and gives it `connectors` to own.
    pub(crate) fn spawn(connectors: C) -> Self {
        let (rx, tx) = create_event_channel();
        let registry = Registry {
            entries: HashMap::new(),
            connectors,
            shutting_down: false,
            tx: tx.downgrade(),
        };

        Self {
            tx,
            task: tokio::spawn(registry.run(rx)),
        }
    }

    /// A handle for something that will talk to the registry but must not be able to end it.
    pub(crate) fn tx(&self) -> RegistryTx<S> {
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
    pub(crate) async fn shutdown(self) -> Option<C> {
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
/// atomic - and that is a rule rather than an accident. An `.await` inside one would let a
/// later event overtake an earlier one's effects and put both of those orderings back in
/// play.
#[derive(Debug)]
struct Registry<C, S> {
    entries: HashMap<Key, Entry<S>>,
    connectors: C,
    /// Set by [`RegistryEvent::ShutDown`]. "Do not start anything new" - an entry that is
    /// still being torn down is still served.
    shutting_down: bool,
    /// Cloned into every broadcaster this registry spawns.
    ///
    /// Weak on purpose: a strong handle held here would count towards its own channel and
    /// the task's `recv` would never report `None`, so the one signal this whole shutdown
    /// path rests on would never fire.
    tx: RegistryWeakTx<S>,
}

impl<C: Connectors, S: AsyncRead + AsyncWrite + Unpin + Send + 'static> Registry<C, S> {
    /// Serves the queue until every [`RegistryTx`] is gone, then hands the connectors back.
    async fn run(mut self, mut rx: RegistryRx<S>) -> C {
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
    fn handle(&mut self, event: RegistryEvent<S>) {
        match event {
            RegistryEvent::Subscribe(joining) => {
                let (key, sock, reply) = joining.into_parts();
                // A dropped receiver just means the handshake gave up; the socket in a
                // `Refused` goes with it, which closes it - the same answer, unwritten.
                let _ = reply.send(self.subscribe(key, sock));
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

    /// Hands `sock` to `key`'s broadcaster, starting one if this is the first client.
    ///
    /// The broadcaster answers on the socket itself - an acceptance header, or a refusal
    /// carrying the venue's own reason - so a symbol the venue does not list ends the
    /// connection rather than opening a stream that never produces anything. Nothing comes
    /// back to the caller except a socket the registry declined to take at all.
    ///
    /// Every client of a brand new symbol is served by this one task, and the entry goes into
    /// the map before the broadcaster is even spawned, so all but the first can only find it
    /// occupied and queue a join. Exactly one `BookSource::subscribe` is ever issued per key -
    /// which is an invariant, not a nicety: the connector hard-errors a duplicate subscribe,
    /// and `BookReader` is not `Clone`, so a symbol has exactly one reader.
    fn subscribe(&mut self, key: Key, sock: S) -> Result<(), Refused<S>> {
        let mut join = Join::new(sock);

        if let Some(entry) = self.entries.get(&key) {
            // Incremented before the send, so a broadcaster whose `RetireIfIdle` is queued
            // behind this event sees the join and stays alive for it.
            entry.pending_joins.fetch_add(1, Ordering::Relaxed);
            match entry.joins.send(join) {
                Ok(()) => return Ok(()),
                // The broadcaster is gone but its `Retire` has not been handled yet. Drop the
                // stale entry and start a fresh one below.
                Err(mpsc::error::SendError(returned)) => {
                    join = returned;
                    self.entries.remove(&key);
                }
            }
        }

        // `upgrade` failing says the same thing `shutting_down` does, one step further along:
        // nothing outside this task holds a way in any more, so a broadcaster started now
        // would have nobody to serve.
        let started = (!self.shutting_down).then(|| self.tx.upgrade()).flatten();
        let Some(tx) = started else {
            return Err(Refused {
                sock: join.into_socket(),
                why: "server is shutting down",
            });
        };

        let (joins, queued) = mpsc::unbounded_channel();
        let pending_joins = Arc::new(AtomicUsize::new(1));
        joins
            .send(join)
            .expect("the receiving half is still in scope");
        self.entries.insert(
            key.clone(),
            Entry {
                joins,
                pending_joins: Arc::clone(&pending_joins),
            },
        );

        // Issued here rather than on the broadcaster, because this is the only place that can
        // put it after the `unsubscribe` of whatever generation held this key before. It
        // costs no await: `BookSource::subscribe` hands back the reply channel rather than a
        // future, so the broadcaster is what waits for the venue.
        let reader = self
            .connectors
            .source(key.venue())
            .subscribe(key.symbol.clone());

        tokio::spawn(Broadcaster::start(tx, key, queued, pending_joins, reader));
        Ok(())
    }

    /// Called for a broadcaster whose session list has just emptied. `true` means the entry
    /// is gone and the task should stop.
    fn retire_if_idle(&mut self, claim: &Claim) -> bool {
        let Some(entry) = self.entries.get(claim.key()) else {
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

        self.entries.remove(claim.key());
        self.unsubscribe(claim.key());
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
            self.unsubscribe(claim.key());
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
        let Some(entry) = self.entries.get(claim.key()) else {
            return false;
        };
        if !Arc::ptr_eq(&entry.pending_joins, claim.pending_joins()) {
            return false;
        }

        self.entries.remove(claim.key());
        true
    }

    fn unsubscribe(&self, key: &Key) {
        self.connectors
            .source(key.venue())
            .unsubscribe(key.symbol.clone());
    }
}

#[cfg(test)]
mod test {
    use super::Key;
    use super::events::Claim;
    use crate::broadcast::SESSION_SWEEP;
    use crate::registry::harness::{Harness, registry_for};
    use crate::session::peer::{Client, connected};
    use crate::test_util::FakeSource;
    use crate::transport::mock::MockStream;
    use crate::venue::Venue;
    use md_wire::framing::RejectCode;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    const SYMBOL: &str = "btcusdt";

    fn key() -> Key {
        Key::new(Venue::BinanceSpot, SYMBOL.into())
    }

    /// Hands a fresh mock socket to the registry and keeps the client half.
    ///
    /// The send is synchronous - see [`super::events::RegistryTx`] - so the event is on the
    /// registry's queue by the time this returns, ahead of anything queued after it. The
    /// reply comes back with the client, for the tests that care that it was taken.
    fn subscribe(
        harness: &Harness,
    ) -> (
        Client,
        oneshot::Receiver<Result<(), super::Refused<MockStream>>>,
    ) {
        let (client, server) = connected();
        let queued = harness.registry.subscribe(key(), server);
        (client, queued)
    }

    /// The same, for a test that only wants the client half and expects it to be taken.
    async fn subscribed(harness: &Harness) -> Client {
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
            .is_registered(key())
            .await
            .expect("the registry task is alive")
    }

    async fn entry_token(harness: &Harness) -> Option<Arc<AtomicUsize>> {
        harness
            .registry
            .entry_token(key())
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
        let harness = registry_for(&source);

        // Every send happens before any reply is awaited, so all eight events are on the
        // registry's queue in a row and the entry the first one inserts is what the other
        // seven find.
        let mut clients: Vec<Client> = std::iter::repeat_with(|| subscribe(&harness).0)
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
            vec![Box::from(SYMBOL)],
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
        let harness = registry_for(&source);
        let mut first = subscribed(&harness).await;
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

        let (mut second, queued) = subscribe(&harness);
        let retiring = harness
            .registry
            .retire_if_idle(Claim::new(key(), Arc::clone(&token)));

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
        let harness = registry_for(&source);

        let mut first = subscribed(&harness).await;
        first
            .accepted()
            .await
            .expect("the fake source accepts every symbol");
        drop(first);
        tokio::time::sleep(SESSION_SWEEP * 2).await;
        assert!(!is_registered(&harness).await);

        let mut second = subscribed(&harness).await;
        second
            .accepted()
            .await
            .expect("the fake source accepts every symbol");
        second.opening_snapshot().await;

        assert_eq!(
            source.subscribed(),
            vec![Box::from(SYMBOL), Box::from(SYMBOL)],
            "the second client subscribes the symbol again"
        );
        assert_eq!(
            source.unsubscribed(),
            vec![Box::from(SYMBOL)],
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
        let harness = registry_for(&source);

        let mut client = subscribed(&harness).await;
        let rejected = client
            .accepted()
            .await
            .expect_err("the source rejects every symbol");

        assert_eq!(rejected.code(), RejectCode::Unavailable);
        assert!(
            rejected.reason().contains("not listed as tradable"),
            "the venue's own reason reaches the client, got {:?}",
            rejected.reason()
        );
        client.ended().await;

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
        let harness = registry_for(&source);

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
        let harness = registry_for(&source);
        harness.registry.shut_down();

        let (_client, server) = connected();
        let refused = harness
            .registry
            .subscribe(key(), server)
            .await
            .expect("the registry task is alive")
            .expect_err("nothing is spawned after shutdown");
        let (_sock, why) = refused.into_parts();

        assert!(why.contains("shutting down"), "got {why:?}");
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
        let harness = registry_for(&source);
        let mut client = subscribed(&harness).await;
        client
            .accepted()
            .await
            .expect("the fake source accepts every symbol");
        client.opening_snapshot().await;

        harness.registry.shut_down();

        client.ended().await;
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
        let harness = registry_for(&source);
        let mut client = subscribed(&harness).await;
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

        let mut next = subscribed(&harness).await;
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

/// The registry task a unit test drives, over fake connectors.
///
/// Here rather than in [`crate::test_util`] because it wraps this module's own task. The
/// end-to-end test stands a whole server up through [`crate::server::serve`] instead, so it
/// needs none of this.
#[cfg(test)]
pub(crate) mod harness {
    use super::RegistryHandle;
    use super::events::RegistryTx;
    use crate::test_util::{FakeConnectors, FakeSource};
    use crate::transport::mock::MockStream;
    use std::sync::Arc;

    /// A registry task over one Binance-side source, and the way in that a test drives it
    /// through.
    ///
    /// The [`super::RegistryHandle`] is dropped rather than kept: dropping its `JoinHandle` only
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
}
