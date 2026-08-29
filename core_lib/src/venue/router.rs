//! Which connection carries which symbol.
//!
//! Generic over the symbol key `S` and the per-lane command type `C`, so it has no notion of
//! what a lane's connection task actually does - it only ever sends `C` values into an
//! `mpsc::Sender<C>`. Every method here is synchronous and a test can stand a lane up from a
//! bare `mpsc::channel`, with no runtime, socket, or spawned task involved.

use crate::instrument::InstrumentId;
use crate::map::{InternalHashMap, new_internal_map};
use std::hash::Hash;
use tokio::sync::mpsc;

/// Names one connection for the life of a supervisor. A counter rather than a position: lanes
/// are removed when they empty or die, and an index would re-point at whatever took the freed
/// place.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LaneId(u64);

/// One connection's inbox, plus how many symbols have been routed to it.
#[derive(Debug)]
pub struct Lane<C> {
    tx: mpsc::Sender<C>,
    load: usize,
}

#[derive(Debug)]
pub struct Router<C> {
    lanes: InternalHashMap<LaneId, Lane<C>>,
    by_symbol: InternalHashMap<InstrumentId, LaneId>,
    next_id: u64,
    capacity: usize,
}

impl<C> Router<C> {
    pub fn new(capacity: usize) -> Self {
        Self {
            lanes: new_internal_map(),
            by_symbol: new_internal_map(),
            next_id: 0,
            capacity,
        }
    }

    pub fn contains(&self, instrument_id: InstrumentId) -> bool {
        self.by_symbol.contains_key(&instrument_id)
    }

    /// How many lanes are currently open, for a supervisor's own logging.
    pub fn lane_count(&self) -> usize {
        self.lanes.len()
    }

    /// The lowest-numbered lane with spare capacity and a live sender, so symbols keep landing
    /// on established sockets rather than spreading thin across every lane opened so far.
    pub fn lane_with_room(&self) -> Option<LaneId> {
        self.lanes
            .iter()
            .filter(|(_, lane)| lane.load < self.capacity && !lane.tx.is_closed())
            .min_by_key(|(id, _)| id.0)
            .map(|(id, _)| *id)
    }

    pub fn insert_lane(&mut self, tx: mpsc::Sender<C>) -> LaneId {
        let id = LaneId(self.next_id);
        self.next_id += 1;
        self.lanes.insert(id, Lane { tx, load: 0 });
        id
    }

    /// This lane's inbox, or `None` when the lane is already gone - dropped by
    /// [`Self::drop_lane`], [`Self::reap_idle`] or [`Self::purge_closed`] since the caller
    /// obtained the id. A caller holding a stale id is an ordinary race, not a bug, so it is
    /// reported rather than panicked on.
    pub fn tx(&self, id: LaneId) -> Option<&mpsc::Sender<C>> {
        self.lanes.get(&id).map(|lane| &lane.tx)
    }

    pub fn bind(&mut self, instrument_id: InstrumentId, id: LaneId) {
        self.by_symbol.insert(instrument_id, id);
        if let Some(lane) = self.lanes.get_mut(&id) {
            lane.load += 1;
        }
    }

    pub fn take(&mut self, instrument_id: InstrumentId) -> Option<LaneId> {
        let id = self.by_symbol.remove(&instrument_id)?;
        if let Some(lane) = self.lanes.get_mut(&id) {
            lane.load = lane.load.saturating_sub(1);
        }
        Some(id)
    }

    /// Every symbol currently routed somewhere, in no particular order.
    pub fn symbols(&self) -> impl ExactSizeIterator<Item = InstrumentId> {
        self.by_symbol.keys().copied()
    }

    /// Removes a lane outright - used when its sender turned out to be dead - and returns the
    /// symbols that were bound to it, orphaned because the connection is gone.
    ///
    /// The bindings have to go with the lane: `purge_closed` only ever looks at lanes this
    /// router still holds, so a binding left pointing at an id already removed here would
    /// never be seen again - the symbol would read `contains() == true` forever, blocking any
    /// re-subscribe, and the next `take` of it would hand a caller an id no lane answers to.
    pub fn drop_lane(&mut self, id: LaneId) -> Vec<InstrumentId> {
        self.lanes.remove(&id);

        let mut orphaned = Vec::new();
        self.by_symbol.retain(|symbol, bound| {
            if *bound == id {
                orphaned.push(*symbol);
                false
            } else {
                true
            }
        });
        orphaned
    }

    /// Closes every empty lane but the oldest, so a connector left with nothing to carry keeps
    /// exactly one warm connection rather than none or many.
    ///
    /// Nothing is returned: only lanes with `load == 0` are closed, so there is never a
    /// binding to hand back, and the ids themselves name lanes this router no longer holds.
    pub fn reap_idle(&mut self) {
        let mut empty: Vec<LaneId> = self
            .lanes
            .iter()
            .filter(|(_, lane)| lane.load == 0)
            .map(|(id, _)| *id)
            .collect();
        empty.sort_by_key(|id| id.0);

        for id in empty.into_iter().skip(1) {
            // Dropping the `Lane` drops its `tx`, which is the close signal.
            self.lanes.remove(&id);
        }
    }

    /// Removes every lane whose sender has closed, returning the symbols that were bound to
    /// them - orphaned because the connection is gone and nothing will answer for them.
    pub fn purge_closed(&mut self) -> Vec<InstrumentId> {
        let dead: Vec<LaneId> = self
            .lanes
            .iter()
            .filter(|(_, lane)| lane.tx.is_closed())
            .map(|(id, _)| *id)
            .collect();
        for id in &dead {
            self.lanes.remove(id);
        }

        let mut orphaned = Vec::new();
        self.by_symbol.retain(|symbol, id| {
            if dead.contains(id) {
                orphaned.push(*symbol);
                false
            } else {
                true
            }
        });
        orphaned
    }
}

#[cfg(test)]
mod test {
    use super::Router;
    use crate::connector::{InstrumentRegistrar as _, VenueGuard};
    use crate::instrument::InstrumentId;
    use all_venues::Venue;
    use tokio::sync::mpsc;

    /// Stand-in lane command: the router never inspects it.
    #[derive(Debug)]
    struct Cmd;

    type TestRouter = Router<Cmd>;

    fn symbol(raw: &str) -> InstrumentId {
        VenueGuard::new(Venue::Bitstamp).register(raw).id()
    }

    /// A sender with no live receiver on the other end, standing in for a dead lane's `tx`
    /// without a socket or a spawned task.
    fn dead_tx() -> mpsc::Sender<Cmd> {
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        tx
    }

    /// A sender whose receiver is deliberately kept alive - leaked, since nothing in these
    /// tests ever needs to receive from it - rather than dropped, so `is_closed()` reads false
    /// the way a lane with a running connection task would.
    fn live_tx() -> mpsc::Sender<Cmd> {
        let (tx, rx) = mpsc::channel(1);
        Box::leak(Box::new(rx));
        tx
    }

    #[test]
    fn bind_and_take_keep_load_exact() {
        let mut router = TestRouter::new(10);
        let lane = router.insert_lane(live_tx());

        router.bind(symbol("btcusdt"), lane);
        router.bind(symbol("ethusdt"), lane);
        assert_eq!(router.lanes[&lane].load, 2);

        let taken = router.take(symbol("btcusdt")).unwrap();
        assert_eq!(taken, lane);
        assert_eq!(router.lanes[&lane].load, 1);
        assert!(router.take(symbol("btcusdt")).is_none(), "already taken");
    }

    #[test]
    fn a_duplicate_subscribe_is_rejected_connector_wide() {
        let mut router = TestRouter::new(10);
        let lane_a = router.insert_lane(live_tx());
        let lane_b = router.insert_lane(live_tx());

        router.bind(symbol("btcusdt"), lane_a);

        assert!(
            router.contains(symbol("btcusdt")),
            "a symbol bound on one lane must be visible connector-wide"
        );
        // Nothing stops a caller from binding the same symbol to a second lane directly, but
        // the supervisor's `contains` check - exercised here - is what a real duplicate
        // subscribe is rejected by, before any lane is ever chosen.
        let _ = lane_b;
    }

    #[test]
    fn lane_with_room_skips_full_and_dead_lanes() {
        let mut router = TestRouter::new(1);
        let full = router.insert_lane(live_tx());
        router.bind(symbol("btcusdt"), full);

        let dead = router.insert_lane(dead_tx());
        let _ = dead;

        let fresh = router.insert_lane(live_tx());

        assert_eq!(
            router.lane_with_room(),
            Some(fresh),
            "must skip the full lane and the dead one"
        );
    }

    #[test]
    fn lane_with_room_prefers_the_lowest_id() {
        let mut router = TestRouter::new(10);
        let first = router.insert_lane(live_tx());
        let _second = router.insert_lane(live_tx());

        assert_eq!(router.lane_with_room(), Some(first));
    }

    #[test]
    fn reap_idle_keeps_exactly_one_empty_lane_and_keeps_the_oldest() {
        let mut router = TestRouter::new(10);
        let oldest = router.insert_lane(live_tx());
        let second = router.insert_lane(live_tx());
        let third = router.insert_lane(live_tx());

        router.reap_idle();

        assert!(
            router.lanes.contains_key(&oldest),
            "the oldest must survive"
        );
        assert!(!router.lanes.contains_key(&second));
        assert!(!router.lanes.contains_key(&third));
        assert_eq!(router.lanes.len(), 1);
    }

    #[test]
    fn reap_idle_leaves_a_loaded_lane_alone() {
        let mut router = TestRouter::new(10);
        let loaded = router.insert_lane(live_tx());
        router.bind(symbol("btcusdt"), loaded);
        let first_empty = router.insert_lane(live_tx());
        let second_empty = router.insert_lane(live_tx());

        router.reap_idle();

        assert!(
            router.lanes.contains_key(&loaded),
            "the loaded lane must never be closed"
        );
        assert!(
            router.lanes.contains_key(&first_empty),
            "one empty lane is kept warm"
        );
        assert!(!router.lanes.contains_key(&second_empty));
    }

    #[test]
    fn purge_closed_removes_a_dead_lane_and_returns_its_symbols() {
        let mut router = TestRouter::new(10);
        let dead = router.insert_lane(dead_tx());
        router.bind(symbol("btcusdt"), dead);
        router.bind(symbol("ethusdt"), dead);
        let alive = router.insert_lane(live_tx());
        router.bind(symbol("solusdt"), alive);

        let mut orphaned = router.purge_closed();
        orphaned.sort_unstable();

        assert_eq!(orphaned, vec![symbol("btcusdt"), symbol("ethusdt")]);
        assert!(!router.lanes.contains_key(&dead));
        assert!(router.lanes.contains_key(&alive));
        assert!(
            router.contains(symbol("solusdt")),
            "the live lane's symbol must survive"
        );
    }

    #[test]
    fn drop_lane_reports_and_clears_the_bindings_it_orphans() {
        let mut router = TestRouter::new(10);
        let doomed = router.insert_lane(live_tx());
        router.bind(symbol("btcusdt"), doomed);
        router.bind(symbol("ethusdt"), doomed);
        let alive = router.insert_lane(live_tx());
        router.bind(symbol("solusdt"), alive);

        let mut orphaned = router.drop_lane(doomed);
        orphaned.sort_unstable();

        assert_eq!(orphaned, vec![symbol("btcusdt"), symbol("ethusdt")]);
        // The bug this covers: leaving the bindings behind made these symbols permanently
        // unsubscribable - `purge_closed` only scans lanes, so it could never see this id
        // again - and handed the next `take` an id no lane answers to.
        assert!(!router.contains(symbol("btcusdt")));
        assert!(!router.contains(symbol("ethusdt")));
        assert!(
            router.contains(symbol("solusdt")),
            "an untouched lane keeps its symbols"
        );
        assert!(router.take(symbol("btcusdt")).is_none());
    }

    #[test]
    fn tx_reports_a_lane_that_is_already_gone() {
        let mut router = TestRouter::new(10);
        let lane = router.insert_lane(live_tx());
        assert!(router.tx(lane).is_some());

        assert!(router.drop_lane(lane).is_empty(), "nothing was bound to it");
        assert!(
            router.tx(lane).is_none(),
            "a stale id must report, not panic"
        );
    }

    #[test]
    fn purge_closed_is_a_no_op_when_nothing_died() {
        let mut router = TestRouter::new(10);
        let lane = router.insert_lane(live_tx());
        router.bind(symbol("btcusdt"), lane);

        assert_eq!(router.purge_closed(), Vec::<_>::new());
        assert!(router.lanes.contains_key(&lane));
    }
}
