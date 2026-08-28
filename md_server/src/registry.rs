//! Which symbols are being broadcast, and the two races around starting and stopping one.

use crate::broadcast::{Broadcaster, Join};
use crate::venue::{BookSource as _, Connectors, Venue};
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;

/// Identifies one book stream.
///
/// Also where a `BookUpdate`'s levels get their `venue` from: `SmallBook` carries no identity
/// of its own, so the only thing that knows what a book is *of* is the key of the broadcaster
/// holding its reader. The symbol itself is no longer on the wire at all.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Key {
    venue: Venue,
    symbol: Box<str>,
}

impl Key {
    /// `symbol` must already be lowercase - see [`crate::request`], which is where a
    /// request's symbol is validated and normalised. Every connector keys its subscriptions
    /// by the lowercase form, so two keys differing only in case would race for one
    /// subscription and the loser would be rejected as already subscribed.
    pub fn new(venue: Venue, symbol: Box<str>) -> Self {
        debug_assert!(
            symbol.bytes().all(|b| !b.is_ascii_uppercase()),
            "keys must hold the lowercase symbol the connector uses"
        );
        Self { venue, symbol }
    }

    pub fn venue(&self) -> Venue {
        self.venue
    }

    pub fn symbol(&self) -> &str {
        &self.symbol
    }
}

/// The registry's half of one running broadcaster.
#[derive(Debug)]
struct Entry<S> {
    joins: mpsc::UnboundedSender<Join<S>>,
    /// Joins already queued on `joins` that the broadcaster has not taken yet.
    ///
    /// This is what closes the teardown race: the increment happens under the registry
    /// mutex, right before the send, and the broadcaster's zero-check happens under the same
    /// mutex, so a broadcaster can never retire out from under a join that is already on its
    /// way. Doubles as identity - the `Arc` is compared by pointer to tell one generation of
    /// broadcaster for a key from the next.
    pending_joins: Arc<AtomicUsize>,
}

#[derive(Debug)]
struct State<S> {
    entries: HashMap<Key, Entry<S>>,
    /// Cloned into every broadcaster this registry spawns, and dropped by
    /// [`Registry::shut_down`] once the server has stopped accepting. The paired receiver in
    /// [`crate::server::serve`] therefore closes exactly when the last broadcaster has
    /// released its `Arc<Registry>`. `None` also means "do not start anything new".
    task_token: Option<mpsc::Sender<Infallible>>,
}

/// The `(venue, symbol)` -> broadcaster map, plus the connectors those broadcasters use.
///
/// A [`std::sync::Mutex`] rather than an async lock: every critical section is a hash lookup,
/// a channel send and an atomic increment, with no `.await` anywhere inside, and it is taken
/// once per RPC setup rather than once per book. An async `RwLock` would add an await point
/// and, worse, would make it possible to hold the guard across `subscribe().await` and
/// serialise every new symbol behind a network round trip.
#[derive(Debug)]
pub struct Registry<C: Connectors, S> {
    state: Mutex<State<S>>,
    connectors: C,
}

/// A socket handed back because the registry would not start a broadcaster for it.
///
/// Returned rather than refused in place so the refusal is written by the caller, which is
/// already in the async context the handshake runs in.
#[derive(Debug)]
pub struct Refused<S> {
    sock: S,
    why: &'static str,
}

impl<S> Refused<S> {
    pub fn into_parts(self) -> (S, &'static str) {
        (self.sock, self.why)
    }
}

impl<C: Connectors, S: AsyncRead + AsyncWrite + Unpin + Send + 'static> Registry<C, S> {
    pub fn new(connectors: C, task_token: mpsc::Sender<Infallible>) -> Self {
        Self {
            state: Mutex::new(State {
                entries: HashMap::new(),
                task_token: Some(task_token),
            }),
            connectors,
        }
    }

    /// The connector carrying `venue`.
    pub fn source(&self, venue: Venue) -> &C::Source {
        self.connectors.source(venue)
    }

    /// Hands `sock` to `key`'s broadcaster, starting one if this is the first client.
    ///
    /// The broadcaster answers on the socket itself - an acceptance header, or a refusal
    /// carrying the venue's own reason - so a symbol the venue does not list ends the
    /// connection rather than opening a stream that never produces anything. Nothing comes
    /// back to the caller except a socket the registry declined to take at all.
    ///
    /// Both clients of a brand new symbol take the same lock, and the entry is inserted
    /// before the broadcaster is even spawned, so the second one can only find it occupied
    /// and queue a join. Exactly one `BookSource::subscribe` is ever issued per key - which
    /// is an invariant, not a nicety: the connector hard-errors a duplicate subscribe, and
    /// `BookReader` is not `Clone`, so a symbol has exactly one reader.
    ///
    /// # Errors
    ///
    /// The socket back, once the server has stopped spawning broadcasters.
    pub fn subscribe(self: &Arc<Self>, key: Key, sock: S) -> Result<(), Refused<S>> {
        let mut join = Join::new(sock);
        let mut state = self.lock();

        if let Some(entry) = state.entries.get(&key) {
            // Incremented before the send and under this lock, so a broadcaster reaching
            // `retire_if_idle` after this point sees the join and stays alive for it.
            entry.pending_joins.fetch_add(1, Ordering::Relaxed);
            match entry.joins.send(join) {
                Ok(()) => return Ok(()),
                // The broadcaster is gone but had not removed its entry yet. Drop the stale
                // entry and start a fresh one below.
                Err(mpsc::error::SendError(returned)) => {
                    join = returned;
                    state.entries.remove(&key);
                }
            }
        }

        let Some(token) = state.task_token.clone() else {
            drop(state);
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
        state.entries.insert(
            key.clone(),
            Entry {
                joins,
                pending_joins: Arc::clone(&pending_joins),
            },
        );
        drop(state);

        tokio::spawn(Broadcaster::start(
            Arc::clone(self),
            key,
            queued,
            pending_joins,
            token,
        ));
        Ok(())
    }

    /// Called by a broadcaster whose session list has just emptied. `true` means the entry is
    /// gone and the task should stop.
    ///
    /// The connector `unsubscribe` is issued *inside* this lock, before the entry is removed.
    /// That is what makes an immediate re-subscribe of the same symbol safe: a connector
    /// processes its events in order and drops the symbol from its router synchronously, so
    /// the unsubscribe is always queued ahead of whatever subscribe the next broadcaster for
    /// this key sends. Releasing the lock first would let that subscribe overtake it and be
    /// rejected as already subscribed.
    #[expect(
        clippy::significant_drop_tightening,
        reason = "the guard is held across the whole decision on purpose, unsubscribe included - see above"
    )]
    pub fn retire_if_idle(&self, key: &Key, pending_joins: &Arc<AtomicUsize>) -> bool {
        let mut state = self.lock();
        let Some(entry) = state.entries.get(key) else {
            return true;
        };
        if !Arc::ptr_eq(&entry.pending_joins, pending_joins) {
            // A newer broadcaster owns this key; this one must not take its entry with it.
            return true;
        }
        if pending_joins.load(Ordering::Relaxed) > 0 {
            // A join is already on its way. Stay alive for it: the next `recv` picks it up.
            return false;
        }
        state.entries.remove(key);
        self.connectors
            .source(key.venue)
            .unsubscribe(key.symbol.clone());
        true
    }

    /// Removes a broadcaster that is stopping for a reason other than an idle session list -
    /// its connector dropped the symbol, or the server is shutting down - and releases the
    /// connector subscription with it.
    ///
    /// A no-op when the entry is already gone or belongs to a newer broadcaster, so a task
    /// that exited through [`Registry::retire_if_idle`] can still call this on its way out.
    pub fn retire(&self, key: &Key, pending_joins: &Arc<AtomicUsize>) {
        let mut state = self.lock();
        if !Self::take_entry(&mut state, key, pending_joins) {
            return;
        }
        self.connectors
            .source(key.venue)
            .unsubscribe(key.symbol.clone());
    }

    /// Removes a broadcaster whose subscribe the connector rejected.
    ///
    /// Unlike [`Registry::retire`] this sends no unsubscribe: the broadcaster never held a
    /// subscription, and if the rejection was "already subscribed" then the subscription
    /// belongs to someone else.
    pub fn abandon(&self, key: &Key, pending_joins: &Arc<AtomicUsize>) {
        let mut state = self.lock();
        Self::take_entry(&mut state, key, pending_joins);
    }

    /// Stops accepting new subscriptions and ends every running broadcaster.
    ///
    /// Both halves come from one line. Dropping the task token makes
    /// [`Registry::subscribe`] refuse rather than start a broadcaster nobody is left to wait
    /// for; clearing the entries drops the sending half of every broadcaster's join channel,
    /// which is what its `recv` reports as `None` and takes as "stop".
    ///
    /// That is deliberately the *only* shutdown signal. A `watch` receiver in the fan-out
    /// loop would re-register and de-register on a mutex-guarded waiter list on every single
    /// book, which is real per-book work in exchange for a signal that fires once. Nothing is
    /// lost by dropping it: a broadcaster on its way out drops its sessions, and dropping a
    /// session closes its socket, which is how this protocol ends a stream.
    pub fn shut_down(&self) {
        let mut state = self.lock();
        state.task_token = None;
        state.entries.clear();
    }

    /// Hands the connectors back so they can be shut down. Only reachable once every
    /// broadcaster has released its `Arc<Registry>`.
    pub fn into_connectors(self) -> C {
        self.connectors
    }

    /// Whether `key` currently has a broadcaster.
    #[cfg(test)]
    pub fn is_registered(&self, key: &Key) -> bool {
        self.lock().entries.contains_key(key)
    }

    /// `key`'s current entry token, so a test can drive [`Registry::retire_if_idle`] by hand
    /// the way its broadcaster would.
    #[cfg(test)]
    pub fn entry_token(&self, key: &Key) -> Option<Arc<AtomicUsize>> {
        self.lock()
            .entries
            .get(key)
            .map(|entry| Arc::clone(&entry.pending_joins))
    }

    /// Removes `key`'s entry if it is the one `pending_joins` identifies. `false` means it
    /// was already gone - [`Registry::shut_down`] cleared it, say - or belongs to a newer
    /// broadcaster for the same key.
    fn take_entry(state: &mut State<S>, key: &Key, pending_joins: &Arc<AtomicUsize>) -> bool {
        let Some(entry) = state.entries.get(key) else {
            return false;
        };
        if !Arc::ptr_eq(&entry.pending_joins, pending_joins) {
            return false;
        }
        state.entries.remove(key);
        true
    }

    /// A panic inside a critical section cannot leave the map torn - every one of them is a
    /// hash lookup, a channel send and an atomic - so the poison flag is recovered rather
    /// than turned into a panic of our own.
    fn lock(&self) -> MutexGuard<'_, State<S>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod test {
    use super::Key;
    use crate::broadcast::SESSION_SWEEP;
    use md_wire::framing::RejectCode;
    use crate::test_util::{Client, FakeSource, Harness, connected, registry_for};
    use crate::venue::Venue;
    use std::sync::Arc;

    const SYMBOL: &str = "btcusdt";

    fn key() -> Key {
        Key::new(Venue::BinanceSpot, SYMBOL.into())
    }

    /// Hands a fresh mock socket to the registry and keeps the client half.
    fn subscribe(harness: &Harness) -> Client {
        let (client, server) = connected();
        harness
            .registry
            .subscribe(key(), server)
            .expect("the registry is still spawning");
        client
    }

    /// The connector hard-errors a duplicate subscribe and `BookReader` is not `Clone`, so
    /// "one subscribe per symbol" is an invariant rather than an optimisation. The entry goes
    /// into the map before the broadcaster is even spawned, which is what makes the second
    /// caller find it occupied.
    #[tokio::test]
    async fn concurrent_first_clients_produce_one_connector_subscribe() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source);

        // Queued without an await between the subscribes, so every one of them lands before
        // the broadcaster has had a chance to run.
        let mut sockets = Vec::new();
        for _ in 0..8 {
            sockets.push(connected());
        }
        let mut clients: Vec<Client> = sockets
            .into_iter()
            .map(|(client, server)| {
                harness
                    .registry
                    .subscribe(key(), server)
                    .expect("the registry is still spawning");
                client
            })
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

    /// The teardown race, from the side where the join wins: a join queued under the lock
    /// keeps its broadcaster alive, and is then served.
    #[tokio::test]
    async fn a_join_queued_before_the_zero_check_keeps_the_broadcaster_alive() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source);
        let mut first = subscribe(&harness);
        first
            .accepted()
            .await
            .expect("the fake source accepts every symbol");
        drop(first);

        // Synchronous, so the broadcaster - which only runs at an await point on this
        // single-threaded runtime - cannot have taken the join yet.
        let mut second = subscribe(&harness);
        let token = harness
            .registry
            .entry_token(&key())
            .expect("the broadcaster is still registered");

        assert!(
            !harness.registry.retire_if_idle(&key(), &token),
            "a queued join must stop the broadcaster from retiring"
        );
        assert!(harness.registry.is_registered(&key()));
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

        let mut first = subscribe(&harness);
        first
            .accepted()
            .await
            .expect("the fake source accepts every symbol");
        drop(first);
        tokio::time::sleep(SESSION_SWEEP * 2).await;
        assert!(!harness.registry.is_registered(&key()));

        let mut second = subscribe(&harness);
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

        let mut client = subscribe(&harness);
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
            !harness.registry.is_registered(&key()),
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
            clients.push(subscribe(&harness));
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
        let mut client = subscribe(&harness);
        client
            .accepted()
            .await
            .expect("the fake source accepts every symbol");
        client.opening_snapshot().await;

        harness.registry.shut_down();

        client.ended().await;
    }
}
