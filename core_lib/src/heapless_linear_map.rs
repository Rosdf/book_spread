use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::mem::MaybeUninit;
use std::slice;

pub struct HeaplessLinearMap<K, V, const N: usize> {
    /// Physical index of the best entry; entries occupy `[base, N)`, sorted ascending.
    ///
    /// The length is `N - base` rather than a field of its own. Right-alignment makes the
    /// two redundant, and keeping only this one means no operation below can leave them
    /// disagreeing - which, over `MaybeUninit` storage, is the difference between a bug and
    /// a read of uninitialised memory.
    base: usize,
    keys: [MaybeUninit<K>; N],
    values: [MaybeUninit<V>; N],
}

impl<K, V, const N: usize> Default for HeaplessLinearMap<K, V, N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Clone, V: Clone, const N: usize> Clone for HeaplessLinearMap<K, V, N> {
    fn clone(&self) -> Self {
        let mut res = Self::new();

        for (k, v) in self {
            // SAFETY:
            // we're inserting in valid order, and `res` cannot be full before the
            // last of `self`'s `N - self.base <= N` entries has been written.
            unsafe {
                res.insert_last_unchecked(k.clone(), v.clone());
            }
        }

        res
    }

    fn clone_from(&mut self, source: &Self) {
        let held = self.len();
        let incoming = source.len();
        let base = self.base;

        for (idx, (k, v)) in source.iter().take(held).enumerate() {
            // SAFETY:
            // `idx < held` (bounded by `.take(held)`), so `base + idx` is within the
            // live range `[base, N)`: already initialized and safe to
            // `assume_init_mut`/overwrite in place.
            unsafe {
                self.keys
                    .get_unchecked_mut(base + idx)
                    .assume_init_mut()
                    .clone_from(k);
                self.values
                    .get_unchecked_mut(base + idx)
                    .assume_init_mut()
                    .clone_from(v);
            }
        }

        // Compared as lengths, not bases: `self.base > source.base` means `self` is the
        // *shorter* of the two, and reading the comparison backwards here is exactly the
        // kind of mistake that still compiles.
        match held.cmp(&incoming) {
            Ordering::Less => {
                for (k, v) in source.iter().skip(held) {
                    // SAFETY:
                    // `source` is sorted ascending and these are its remaining
                    // (larger) entries, appended in order; the loop above already
                    // matched `self`'s first `held` entries to `source`'s, so
                    // each of these is greater than everything currently in `self`.
                    // `self` holds fewer entries than `source`, so it has room.
                    unsafe {
                        self.insert_last_unchecked(k.clone(), v.clone());
                    }
                }
            }
            Ordering::Equal => {}
            Ordering::Greater => {
                // Drop the entries `source` has no counterpart for, largest first. One
                // eviction at a time rather than a bulk truncation, so that a panicking
                // `K`/`V` destructor leaves behind a map whose `base` still describes
                // exactly the entries that are still alive.
                for _ in incoming..held {
                    // SAFETY:
                    // the map still holds more than `incoming` entries at this point,
                    // so it is non-empty.
                    drop(unsafe { self.evict_last_unchecked() });
                }
            }
        }
    }
}

impl<K, V, const N: usize> HeaplessLinearMap<K, V, N> {
    const KEY: MaybeUninit<K> = MaybeUninit::uninit();
    const VALUE: MaybeUninit<V> = MaybeUninit::uninit();

    const KEYS: [MaybeUninit<K>; N] = [Self::KEY; N];
    const VALUES: [MaybeUninit<V>; N] = [Self::VALUE; N];

    pub const fn new() -> Self {
        Self {
            base: N,
            keys: Self::KEYS,
            values: Self::VALUES,
        }
    }

    pub fn iter(&self) -> Iter<'_, K, V, N> {
        Iter {
            pos: 0,
            parent: self,
        }
    }

    /// The worst (largest) entry, the one an insertion past the end would evict.
    pub fn worst(&self) -> Option<(&K, &V)> {
        if self.is_empty() {
            None
        } else {
            // SAFETY:
            // the map is non-empty, so its live range `[base, N)` ends at the
            // initialized slot `N - 1`, which holds the largest entry.
            let key = unsafe { self.keys.get_unchecked(N - 1).assume_init_ref() };
            // SAFETY: same as above.
            let value = unsafe { self.values.get_unchecked(N - 1).assume_init_ref() };
            Some((key, value))
        }
    }

    /// The value at logical position `pos`, to read or to overwrite in place.
    ///
    /// Paired with [`Self::locate`], which is where a position worth holding on to comes
    /// from: `Occupied(pos)` says the slot is there and the key in it is the one asked for.
    ///
    /// # Safety
    /// `pos < self.len()`.
    pub unsafe fn value_mut_unchecked(&mut self, pos: usize) -> &mut V {
        debug_assert!(pos < self.len(), "position must be within bounds");

        let slot = self.phys(pos);
        // SAFETY: `slot` is within the live range `[base, N)` (per fn-level `# Safety`),
        // so it is initialized.
        unsafe { self.values.get_unchecked_mut(slot).assume_init_mut() }
    }

    pub const fn len(&self) -> usize {
        N - self.base
    }

    pub const fn is_empty(&self) -> bool {
        self.base == N
    }

    pub const fn is_full(&self) -> bool {
        self.base == 0
    }

    /// Physical index of the entry at logical position `i`.
    const fn phys(&self, i: usize) -> usize {
        self.base + i
    }

    /// Appends one entry at the worst end.
    ///
    /// Not the O(1) push it reads as: right-alignment means the whole run slides down a
    /// slot to make room, so this costs `len` moves, and a loop of these costs `len` of
    /// them each. It is here for [`Clone`] and [`Clone::clone_from`], which append into a
    /// map that is still filling up; anything appending a batch wants
    /// [`Self::extend_worst_unchecked`], which pays those moves once.
    ///
    /// # Safety
    /// `key` should be grater then all elements in map and `self.base > 0`, so that there is
    /// a free slot at the front for the live range to grow into
    pub unsafe fn insert_last_unchecked(&mut self, key: K, value: V) {
        // SAFETY:
        // `self.len() <= self.len()` trivially, and the caller guarantees the free slot
        // the shift needs (per fn-level `# Safety`).
        unsafe {
            self.insert_at_unchecked(self.len(), key, value);
        }
    }

    /// Inserts `key`/`value` at logical position `pos`, shifting everything better than it
    /// one slot to the left - down into the free slot at the front.
    ///
    /// Costs `pos` moves, not `len`: an insertion near the best end is nearly free, and
    /// only one at the worst end pays for the whole run.
    ///
    /// # Safety
    /// * `pos <= self.len()`;
    /// * `!self.is_full()`, so there is a free slot at the front to shift into;
    /// * `key` belongs at `pos`, i.e. it is greater than every key before it and less than
    ///   the one currently there - what [`Self::locate`] reports as `Vacant(pos)`.
    pub unsafe fn insert_at_unchecked(&mut self, pos: usize, key: K, value: V) {
        debug_assert!(pos <= self.len(), "insertion position must be within bounds");
        debug_assert!(self.base > 0, "caller must ensure there is spare capacity");

        let base = self.base;
        // SAFETY: `base >= 1` and `base + pos <= N`, so both the source range
        // `[base, base + pos)` and the destination `[base - 1, base - 1 + pos)` lie
        // within the arrays' bounds. The ranges overlap, hence `ptr::copy` (not
        // `copy_nonoverlapping`). The slot the copy vacates - `base - 1 + pos` - is
        // treated as logically moved-out and is immediately reinitialized below.
        unsafe {
            let keys_ptr = self.keys.as_mut_ptr();
            std::ptr::copy(keys_ptr.add(base), keys_ptr.add(base - 1), pos);

            let values_ptr = self.values.as_mut_ptr();
            std::ptr::copy(values_ptr.add(base), values_ptr.add(base - 1), pos);
        }

        self.base -= 1;

        let slot = self.phys(pos);
        // SAFETY:
        // `slot < N` (as `pos < self.len()` after the decrement above) and the copy
        // just vacated it.
        unsafe {
            self.keys.get_unchecked_mut(slot).write(key);
            self.values.get_unchecked_mut(slot).write(value);
        }
    }

    /// Removes and returns the entry at logical position `pos`, shifting everything better
    /// than it one slot to the right so the live range stays right-aligned.
    ///
    /// Costs `pos` moves, the mirror of [`Self::insert_at_unchecked`].
    ///
    /// # Safety
    /// `pos < self.len()`.
    pub unsafe fn remove_at_unchecked(&mut self, pos: usize) -> (K, V) {
        debug_assert!(pos < self.len(), "removal position must be within bounds");

        let base = self.base;
        let slot = self.phys(pos);
        // SAFETY: `slot` is within the live range `[base, N)`, so it is initialized;
        // it is considered removed from here on, so this is the only read of it.
        let removed = unsafe {
            let key = self.keys.get_unchecked(slot).assume_init_read();
            let value = self.values.get_unchecked(slot).assume_init_read();
            (key, value)
        };

        // SAFETY: the source range `[base, slot)` and the destination
        // `[base + 1, slot + 1)` both lie within the arrays, and they overlap, hence
        // `ptr::copy`. The slot at `base` is left logically moved-out, which the
        // `base` bump below excludes from the live range.
        unsafe {
            let keys_ptr = self.keys.as_mut_ptr();
            std::ptr::copy(keys_ptr.add(base), keys_ptr.add(base + 1), pos);

            let values_ptr = self.values.as_mut_ptr();
            std::ptr::copy(values_ptr.add(base), values_ptr.add(base + 1), pos);
        }

        self.base += 1;

        removed
    }

    /// Removes and returns the last (largest) entry.
    ///
    /// # Safety
    /// `!self.is_empty()`.
    unsafe fn evict_last_unchecked(&mut self) -> (K, V) {
        debug_assert!(!self.is_empty(), "caller must ensure the map is non-empty");
        // SAFETY: the map is non-empty (per fn-level `# Safety`), so `len() - 1` is a
        // valid logical position.
        unsafe { self.remove_at_unchecked(self.len() - 1) }
    }


    /// Moves the worst `count` entries out of the map, best-first, leaving the `len - count`
    /// best ones behind.
    ///
    /// The survivors are shifted back into `[base, N)` when the returned iterator is
    /// dropped - one `ptr::copy` per array for the whole batch, rather than the one per
    /// entry that `count` separate evictions would cost.
    ///
    /// # Panics
    ///
    /// When `count` exceeds the number of entries held.
    pub fn drain_worst(&mut self, count: usize) -> DrainWorst<'_, K, V, N> {
        assert!(
            count <= self.len(),
            "cannot drain more entries than the map holds"
        );

        let survivors_start = self.base;
        // The map disowns everything for as long as the drain is alive - the entries being
        // handed out and the survivors alike - because it has no way to describe a run with
        // a hole at its worst end. Leaking the iterator then leaks those entries, which is
        // the one thing a leak is allowed to do; leaving `base` covering slots the drain has
        // already moved out of would not be.
        self.base = N;

        DrainWorst {
            next: N - count,
            end: N,
            survivors_start,
            survivors_end: N - count,
            parent: self,
        }
    }

    /// Appends `entries` at the worst end of the map.
    ///
    /// One `ptr::copy` per array to open the room, then one write per entry - the batch
    /// pays the `len` moves that [`Self::insert_last_unchecked`] would pay per entry.
    ///
    /// # Safety
    ///
    /// * `entries.len() <= N - self.len()`, so the run has room to grow into;
    /// * every key yielded is greater than every key already held;
    /// * the keys arrive best-first, i.e. ascending.
    ///
    /// An iterator that yields fewer entries than its [`ExactSizeIterator::len`] promised,
    /// or that panics part-way through, is not a safety violation: the map is left holding
    /// exactly the entries that did arrive.
    pub unsafe fn extend_worst_unchecked(&mut self, entries: impl ExactSizeIterator<Item = (K, V)>) {
        let count = entries.len();
        debug_assert!(count <= self.base, "caller must ensure there is spare capacity");

        let base = self.base;
        let len = self.len();

        // SAFETY: `count <= base` (per fn-level `# Safety`), so the destination
        // `[base - count, N - count)` lies within the arrays, as does the source
        // `[base, N)`. The two overlap, hence `ptr::copy` (not `copy_nonoverlapping`).
        // The `count` slots the copy vacates at the worst end are filled in below.
        unsafe {
            let keys_ptr = self.keys.as_mut_ptr();
            std::ptr::copy(keys_ptr.add(base), keys_ptr.add(base - count), len);

            let values_ptr = self.values.as_mut_ptr();
            std::ptr::copy(values_ptr.add(base), values_ptr.add(base - count), len);
        }

        // Disowned while the tail has a hole in it, for the same reason as in
        // `drain_worst`, and put back by `FillWorst`'s `Drop` however this loop ends.
        self.base = N;
        let mut fill = FillWorst {
            parent: self,
            start: base - count,
            filled: 0,
            count,
        };

        // `take` because `ExactSizeIterator` is a safe trait to implement badly: an
        // iterator that yields more than it promised must not write past the room made
        // for it.
        for (key, value) in entries.take(count) {
            fill.write(key, value);
        }
    }

    /// The value under `key`, or `None` when nothing is filed there.
    ///
    /// Borrowed the way [`std::collections::HashMap::get`] is, so a map keyed on `String` is
    /// searched with a `&str` and one keyed on a newtype with whatever that newtype borrows
    /// as - no key has to be built to ask a question about one.
    ///
    /// A linear scan rather than the binary search the sorted keys would allow, because `Q`
    /// is only [`Eq`]: ordering a borrowed form against the owned one would need
    /// `K: Borrow<Q>` *and* the two orderings to agree, which is a heavier promise than this
    /// map - a handful of entries, where the scan is free - has any reason to ask for.
    ///
    /// No `K` bound at all, for the same reason: nothing about looking a key up needs the
    /// [`Ord`] that keeping them sorted does.
    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        let pos = self.keys().iter().position(|k| k.borrow() == key)?;
        // SAFETY:
        // `pos` came from searching `self.keys()`, the live range seen as a slice, so
        // the value at the matching physical slot is initialized.
        Some(unsafe { self.values.get_unchecked(self.phys(pos)).assume_init_ref() })
    }

    fn keys(&self) -> &[K] {
        // SAFETY:
        // the `N - base` elements starting at `base` are active
        unsafe { slice::from_raw_parts(self.keys.as_ptr().add(self.base).cast(), self.len()) }
    }

    pub fn clear(&mut self) {
        let base = self.base;
        self.base = N;

        for idx in base..N {
            // SAFETY:
            // `idx` was within the live range `[base, N)` (before we emptied it above),
            // so every index in this range is initialized.
            unsafe {
                self.keys.get_unchecked_mut(idx).assume_init_drop();
                self.values.get_unchecked_mut(idx).assume_init_drop();
            }
        }
    }
}

/// What a successful [`HeaplessLinearMap::insert`] reports: the logical position the key
/// ended up at, and the pair evicted to make room for it - `None` unless the map was full.
pub type Insertion<K, V> = (usize, Option<(K, V)>);

/// What [`HeaplessLinearMap::locate`] found: a logical position, and whether the key asked
/// about is the one already sitting at it.
///
/// Two variants rather than a `(usize, bool)`, because the position means different things
/// in each - a slot to write through, or a slot to shift into - and a bool that says which
/// is a bool that can be read backwards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Position {
    /// The key is held at this position.
    Occupied(usize),
    /// The key is not held; it belongs at this position, before whatever is there now.
    Vacant(usize),
}

impl Position {
    /// The position, whichever of the two it is.
    pub const fn index(self) -> usize {
        match self {
            Self::Occupied(pos) | Self::Vacant(pos) => pos,
        }
    }
}

impl<K, V, const N: usize> Drop for HeaplessLinearMap<K, V, N> {
    fn drop(&mut self) {
        self.clear();
    }
}

impl<K: Ord, V, const N: usize> HeaplessLinearMap<K, V, N> {
    /// Inserts `value` under `key`, keeping the keys sorted.
    ///
    /// `Ok` carries the logical position `key` ended up at - what a caller that publishes
    /// a prefix of the map needs in order to tell an update it has to send from one it can
    /// drop - and the pair evicted to make room for it, if the map was full.
    ///
    /// # Errors
    ///
    /// The pair back, when the map is already full and `key` is not in it - there is nowhere
    /// to put it and no heap to grow into.
    pub fn insert(&mut self, key: K, value: V) -> Result<Insertion<K, V>, (K, V)> {
        let pos = match self.locate(&key) {
            Position::Occupied(pos) => {
                // SAFETY:
                // `Occupied(pos)` is only reported for a `pos` a key was found at, so
                // `pos < self.len()`.
                *unsafe { self.value_mut_unchecked(pos) } = value;
                return Ok((pos, None));
            }
            Position::Vacant(pos) => pos,
        };

        // `key` is not present yet, it needs to be inserted at `pos`, keeping
        // the array sorted ascending.
        if !self.is_full() {
            // SAFETY:
            // `pos <= self.len()` (`locate` scans the live range) and the map is not
            // full, so `self.base > 0`.
            unsafe {
                self.insert_at_unchecked(pos, key, value);
            }
            Ok((pos, None))
        } else if pos == self.len() {
            // `key` is larger than every stored key, so it would become the
            // new largest entry: there is nothing smaller to evict for it.
            Err((key, value))
        } else {
            // Evict the current largest entry to make room, then insert
            // `key` at its sorted position among the remaining entries.

            // SAFETY:
            // if self was empty, we would have entered first branch
            let evicted = unsafe { self.evict_last_unchecked() };
            // SAFETY:
            // we would have entered prev branch if pos == self.len(), so `pos` is still
            // within the shortened range; the eviction freed the slot the shift needs.
            unsafe {
                self.insert_at_unchecked(pos, key, value);
            }
            Ok((pos, Some(evicted)))
        }
    }

    /// Where `key` is, or where it would go: `Occupied(pos)` when it is already held at
    /// `pos`, `Vacant(pos)` when it is not and belongs there. `Vacant(self.len())` when
    /// every key held is smaller.
    ///
    /// The position is what [`Self::value_mut_unchecked`], [`Self::insert_at_unchecked`]
    /// and [`Self::remove_at_unchecked`] take, so a caller that needs to decide between
    /// them - update in place, or make room, or route the key elsewhere entirely - pays
    /// for one search rather than one per branch.
    ///
    /// A forward scan rather than the binary search the sorted keys would allow. Over a
    /// map this small the binary search's ~log2(N) comparisons are a fixed cost paid on
    /// every call, while the scan stops at the first key that is not smaller: one or two
    /// comparisons for the near-best keys that dominate, degrading a step at a time
    /// towards the worst case instead of cliffing to it. The same argument [`Self::get`]
    /// already makes one function up.
    pub fn locate(&self, key: &K) -> Position {
        for (pos, k) in self.keys().iter().enumerate() {
            match k.cmp(key) {
                Ordering::Less => {}
                Ordering::Equal => return Position::Occupied(pos),
                Ordering::Greater => return Position::Vacant(pos),
            }
        }

        Position::Vacant(self.len())
    }

    /// Removes `key`, returning the position it was removed from along with its value.
    ///
    /// The position is the one the entry had *before* the removal, so everything that was
    /// worse than it has shifted down by one - which is what a caller republishing a
    /// prefix of the map has to redraw.
    pub fn remove(&mut self, key: &K) -> Option<(usize, V)> {
        let Position::Occupied(pos) = self.locate(key) else {
            return None;
        };

        // SAFETY:
        // `Occupied(pos)` is only reported for a `pos` a key was found at, so
        // `pos < self.len()`.
        let (_, value) = unsafe { self.remove_at_unchecked(pos) };
        Some((pos, value))
    }
}

impl<K: Debug, V: Debug, const N: usize> Debug for HeaplessLinearMap<K, V, N> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut map = f.debug_map();

        for (key, value) in self {
            map.entry(key, value);
        }

        map.finish()
    }
}

/// The iterator [`HeaplessLinearMap::drain_worst`] hands back: the entries it took, in
/// ascending (best-first) order.
///
/// Whatever is left un-yielded when this is dropped is dropped with it, and the survivors
/// are shifted back into place then - so the map is only whole again once the drain is.
pub struct DrainWorst<'a, K, V, const N: usize> {
    /// The entries still to hand out, at `[next, end)`; the two ends close towards each
    /// other as they are taken from either side.
    next: usize,
    end: usize,
    /// The entries the drain left behind, at `[survivors_start, survivors_end)`.
    survivors_start: usize,
    survivors_end: usize,
    parent: &'a mut HeaplessLinearMap<K, V, N>,
}

impl<K, V, const N: usize> DrainWorst<'_, K, V, N> {
    /// Moves the entry at physical index `slot` out of the map.
    ///
    /// # Safety
    /// `slot` is within `[next, end)`, the entries drained but not yet handed out.
    unsafe fn take(&mut self, slot: usize) -> (K, V) {
        // SAFETY: the entries in `[next, end)` were live when the drain started and have
        // not been read since (per fn-level `# Safety`), so `slot` is initialized and this
        // is the only read of it.
        unsafe {
            let key = self.parent.keys.get_unchecked(slot).assume_init_read();
            let value = self.parent.values.get_unchecked(slot).assume_init_read();
            (key, value)
        }
    }

    /// Slides the survivors up against the worst end and hands them back to the map.
    fn restore(&mut self) {
        let drained = N - self.survivors_end;
        let len = self.survivors_end - self.survivors_start;

        // SAFETY: the source `[survivors_start, survivors_end)` and the destination
        // `[survivors_start + drained, N)` both lie within the arrays, and they overlap,
        // hence `ptr::copy`. The source slots this leaves behind are excluded from the
        // live range by the `base` written below.
        unsafe {
            let keys_ptr = self.parent.keys.as_mut_ptr();
            std::ptr::copy(
                keys_ptr.add(self.survivors_start),
                keys_ptr.add(self.survivors_start + drained),
                len,
            );

            let values_ptr = self.parent.values.as_mut_ptr();
            std::ptr::copy(
                values_ptr.add(self.survivors_start),
                values_ptr.add(self.survivors_start + drained),
                len,
            );
        }

        self.parent.base = self.survivors_start + drained;
    }
}

/// Only what is left to hand out: the entries themselves are the map's, spoken for by
/// nothing while the drain is alive, and printing them would need a `K: Debug` bound that
/// draining does not otherwise ask for.
impl<K, V, const N: usize> Debug for DrainWorst<'_, K, V, N> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DrainWorst")
            .field("remaining", &self.len())
            .finish_non_exhaustive()
    }
}

impl<K, V, const N: usize> Iterator for DrainWorst<'_, K, V, N> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.next == self.end {
            return None;
        }

        let slot = self.next;
        self.next += 1;

        // SAFETY: `slot` was inside the map's live range when the drain started and lies
        // in the not-yet-handed-out `[next, end)`, so it is initialized and this is the
        // only read of it.
        Some(unsafe { self.take(slot) })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }
}

/// The worst end first, which is the order a deeper tier that is also kept sorted wants
/// them in when it is grown from its own best end.
impl<K, V, const N: usize> DoubleEndedIterator for DrainWorst<'_, K, V, N> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.next == self.end {
            return None;
        }

        self.end -= 1;

        // SAFETY: as in `next`, from the other end of the same range.
        Some(unsafe { self.take(self.end) })
    }
}

impl<K, V, const N: usize> ExactSizeIterator for DrainWorst<'_, K, V, N> {
    fn len(&self) -> usize {
        self.end - self.next
    }
}

impl<K, V, const N: usize> Drop for DrainWorst<'_, K, V, N> {
    fn drop(&mut self) {
        /// Restores the map even if one of the destructors below panics, in which case the
        /// entries not yet reached are leaked rather than left owned by a `base` that no
        /// longer covers them.
        struct Restore<'d, 'a, K, V, const N: usize>(&'d mut DrainWorst<'a, K, V, N>);

        impl<K, V, const N: usize> Drop for Restore<'_, '_, K, V, N> {
            fn drop(&mut self) {
                self.0.restore();
            }
        }

        let guard = Restore(self);
        guard.0.by_ref().for_each(drop);
    }
}

/// Holds the map together while [`HeaplessLinearMap::extend_worst_unchecked`] fills the room
/// it made at the worst end, and closes whatever gap is left over when the fill ends - which
/// is none at all in the normal case, and the tail of a short or panicking iterator otherwise.
struct FillWorst<'a, K, V, const N: usize> {
    parent: &'a mut HeaplessLinearMap<K, V, N>,
    /// Physical index the run starts at, once it is full.
    start: usize,
    filled: usize,
    count: usize,
}

impl<K, V, const N: usize> FillWorst<'_, K, V, N> {
    fn write(&mut self, key: K, value: V) {
        debug_assert!(self.filled < self.count, "no room was made for this entry");

        let slot = N - self.count + self.filled;
        // SAFETY: `slot < N`, and it is one of the slots the caller's `ptr::copy` vacated,
        // so nothing is being overwritten.
        unsafe {
            self.parent.keys.get_unchecked_mut(slot).write(key);
            self.parent.values.get_unchecked_mut(slot).write(value);
        }

        self.filled += 1;
    }
}

impl<K, V, const N: usize> Drop for FillWorst<'_, K, V, N> {
    fn drop(&mut self) {
        let missing = self.count - self.filled;

        if missing > 0 {
            let end = N - missing;

            // SAFETY: the run of entries that did arrive is `[start, end)` - the shifted
            // survivors followed by `filled` new ones - and the destination
            // `[start + missing, N)` is that run slid up against the worst end. Both lie
            // within the arrays and they overlap, hence `ptr::copy`.
            unsafe {
                let keys_ptr = self.parent.keys.as_mut_ptr();
                std::ptr::copy(
                    keys_ptr.add(self.start),
                    keys_ptr.add(self.start + missing),
                    end - self.start,
                );

                let values_ptr = self.parent.values.as_mut_ptr();
                std::ptr::copy(
                    values_ptr.add(self.start),
                    values_ptr.add(self.start + missing),
                    end - self.start,
                );
            }
        }

        self.parent.base = self.start + missing;
    }
}

#[derive(Debug)]
pub struct Iter<'a, K, V, const N: usize> {
    pos: usize,
    parent: &'a HeaplessLinearMap<K, V, N>,
}

impl<'a, K, V, const N: usize> Iterator for Iter<'a, K, V, N> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.parent.len() {
            return None;
        }

        let slot = self.parent.phys(self.pos);
        // SAFETY:
        // `self.pos < self.parent.len()`, checked just above, so `slot` is inside the
        // live range and initialized.
        let key = unsafe { self.parent.keys.get_unchecked(slot).assume_init_ref() };
        // SAFETY: same as above.
        let value = unsafe { self.parent.values.get_unchecked(slot).assume_init_ref() };
        self.pos += 1;

        Some((key, value))
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.pos = self.pos.saturating_add(n);
        self.next()
    }
}

impl<K, V, const N: usize> ExactSizeIterator for Iter<'_, K, V, N> {
    fn len(&self) -> usize {
        self.parent.len().saturating_sub(self.pos)
    }
}

impl<'a, K, V, const N: usize> IntoIterator for &'a HeaplessLinearMap<K, V, N> {
    type Item = <Self::IntoIter as Iterator>::Item;
    type IntoIter = Iter<'a, K, V, N>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(test)]
mod test {
    use super::{HeaplessLinearMap, Position};
    use std::cell::Cell;
    use std::rc::Rc;

    fn empty<const N: usize>() -> HeaplessLinearMap<i32, i32, N> {
        HeaplessLinearMap::new()
    }

    fn empty_tracked<const N: usize>() -> HeaplessLinearMap<i32, DropTracker, N> {
        HeaplessLinearMap::new()
    }

    /// A value that records how many times it has been dropped, so
    /// `clone_from`'s in-place drop of truncated entries can be verified.
    #[derive(Clone, Debug)]
    struct DropTracker(Rc<Cell<usize>>);

    impl Drop for DropTracker {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
        }
    }

    #[test]
    fn fills_then_rejects_new_largest() {
        let mut m = empty::<3>();
        assert_eq!(m.insert(5, 50), Ok((0, None)));
        assert_eq!(m.insert(1, 10), Ok((0, None)));
        assert_eq!(m.insert(3, 30), Ok((1, None)));
        assert_eq!(m.keys(), &[1, 3, 5]);
        // full now; inserting something larger than max should be rejected
        assert_eq!(m.insert(10, 100), Err((10, 100)));
        assert_eq!(m.keys(), &[1, 3, 5]);
    }

    #[test]
    fn evicts_largest_when_full_and_smaller_key_inserted() {
        let mut m = empty::<3>();
        m.insert(5, 50).unwrap();
        m.insert(1, 10).unwrap();
        m.insert(9, 90).unwrap();
        assert_eq!(m.keys(), &[1, 5, 9]);
        // 3 is smaller than max (9) -> evict 9, insert 3 sorted
        let evicted = m.insert(3, 30).unwrap();
        assert_eq!(evicted, (1, Some((9, 90))), "3 goes in at 1, 9 comes out");
        assert_eq!(m.keys(), &[1, 3, 5]);
    }

    #[test]
    fn update_existing_key_returns_none_and_replaces_value() {
        let mut m = empty::<3>();
        m.insert(5, 50).unwrap();
        let r = m.insert(5, 999).unwrap();
        assert_eq!(r, (0, None), "the value at 0 is replaced, nothing is evicted");
        assert_eq!(m.keys(), &[5]);
        assert_eq!(m.worst(), Some((&5, &999)));
    }

    #[test]
    fn update_existing_key_when_full_still_works() {
        let mut m = empty::<2>();
        m.insert(1, 10).unwrap();
        m.insert(2, 20).unwrap();
        let r = m.insert(1, 111).unwrap();
        assert_eq!(r, (0, None), "a full map still has room for a value it already keys");
        assert_eq!(m.keys(), &[1, 2]);
        assert_eq!(m.worst(), Some((&2, &20)));
    }

    #[test]
    fn get_finds_a_value_by_its_key_and_nothing_else() {
        let mut m = empty::<3>();
        assert_eq!(m.get(&1), None, "an empty map has nothing under any key");

        assert_eq!(m.insert(5, 50), Ok((0, None)));
        assert_eq!(m.insert(1, 10), Ok((0, None)));
        assert_eq!(m.insert(3, 30), Ok((1, None)));

        assert_eq!(m.get(&1), Some(&10), "the first key");
        assert_eq!(m.get(&3), Some(&30), "a middle key");
        assert_eq!(m.get(&5), Some(&50), "the last key");
        assert_eq!(m.get(&4), None, "a key between two that are present");
        assert_eq!(m.get(&9), None, "a key past the largest present");

        // The entries shift on a remove, so what `get` reads has to follow them rather than
        // the position the key went in at.
        assert_eq!(m.remove(&1), Some((0, 10)));
        assert_eq!(m.get(&1), None);
        assert_eq!(m.get(&3), Some(&30));
        assert_eq!(m.get(&5), Some(&50));
    }

    /// The point of the `Borrow` bound: a map keyed on an owned type is searched with the
    /// borrowed one, so a lookup allocates nothing.
    #[test]
    fn get_takes_a_borrowed_key() {
        let mut m: HeaplessLinearMap<String, i32, 2> = HeaplessLinearMap::new();
        assert_eq!(m.insert("btcusdt".to_owned(), 1), Ok((0, None)));
        assert_eq!(m.insert("ethusdt".to_owned(), 2), Ok((1, None)));

        assert_eq!(m.get("btcusdt"), Some(&1));
        assert_eq!(m.get("ethusdt"), Some(&2));
        assert_eq!(m.get("solusdt"), None);
        // The owned form still works, since `String: Borrow<String>`.
        assert_eq!(m.get(&"btcusdt".to_owned()), Some(&1));
    }

    #[test]
    fn remove_missing_key_returns_none_and_leaves_map_unchanged() {
        let mut m = empty::<3>();
        m.insert(1, 10).unwrap();
        m.insert(2, 20).unwrap();

        assert_eq!(m.remove(&99), None);
        assert_eq!(m.len(), 2);
        assert_eq!(m.keys(), &[1, 2]);
        assert_eq!(m.worst(), Some((&2, &20)));
    }

    #[test]
    fn remove_from_empty_map_returns_none() {
        let mut m = empty::<3>();
        assert_eq!(m.remove(&1), None);
        assert_eq!(m.len(), 0);
    }

    #[test]
    fn remove_last_key() {
        let mut m = empty::<3>();
        m.insert(1, 10).unwrap();
        m.insert(2, 20).unwrap();
        m.insert(3, 30).unwrap();

        assert_eq!(m.remove(&3), Some((2, 30)));
        assert_eq!(m.len(), 2);
        assert_eq!(m.keys(), &[1, 2]);
        assert_eq!(m.worst(), Some((&2, &20)));
    }

    #[test]
    fn remove_first_key_shifts_remaining_entries_left() {
        let mut m = empty::<3>();
        m.insert(1, 10).unwrap();
        m.insert(2, 20).unwrap();
        m.insert(3, 30).unwrap();

        assert_eq!(m.remove(&1), Some((0, 10)));
        assert_eq!(m.len(), 2);
        assert_eq!(m.keys(), &[2, 3]);
        assert_eq!(m.worst(), Some((&3, &30)));
    }

    #[test]
    fn remove_middle_key_keeps_keys_and_values_paired() {
        let mut m = empty::<4>();
        m.insert(1, 10).unwrap();
        m.insert(2, 20).unwrap();
        m.insert(3, 30).unwrap();
        m.insert(4, 40).unwrap();

        assert_eq!(m.remove(&2), Some((1, 20)));
        assert_eq!(m.len(), 3);
        assert_eq!(m.keys(), &[1, 3, 4]);
        // values must have shifted in lock-step with keys, not just keys
        assert_eq!(m.worst(), Some((&4, &40)));
        assert_eq!(m.remove(&3), Some((1, 30)), "3 shifted down when 2 left");
        assert_eq!(m.keys(), &[1, 4]);
        assert_eq!(m.worst(), Some((&4, &40)));
        assert_eq!(m.remove(&1), Some((0, 10)));
        assert_eq!(m.keys(), &[4]);
        assert_eq!(m.worst(), Some((&4, &40)));
    }

    #[test]
    fn remove_all_entries_drains_map() {
        let mut m = empty::<3>();
        m.insert(1, 10).unwrap();
        m.insert(2, 20).unwrap();
        m.insert(3, 30).unwrap();

        assert_eq!(m.remove(&2), Some((1, 20)));
        assert_eq!(m.remove(&1), Some((0, 10)));
        assert_eq!(m.remove(&3), Some((0, 30)));
        assert_eq!(m.len(), 0);
        assert_eq!(m.worst(), None);
        // removing again from the now-empty map is a no-op
        assert_eq!(m.remove(&3), None);
    }

    #[test]
    fn remove_then_insert_reuses_freed_capacity() {
        let mut m = empty::<2>();
        m.insert(1, 10).unwrap();
        m.insert(2, 20).unwrap();
        assert!(m.is_full());

        assert_eq!(m.remove(&1), Some((0, 10)));
        assert!(!m.is_full());

        assert_eq!(m.insert(3, 30), Ok((1, None)));
        assert_eq!(m.keys(), &[2, 3]);
        assert_eq!(m.worst(), Some((&3, &30)));
    }

    #[test]
    fn locate_reports_a_position_and_whether_the_key_is_at_it() {
        let mut m = empty::<4>();
        assert_eq!(m.locate(&1), Position::Vacant(0), "an empty map");

        m.insert(2, 20).unwrap();
        m.insert(4, 40).unwrap();
        m.insert(6, 60).unwrap();

        assert_eq!(m.locate(&2), Position::Occupied(0));
        assert_eq!(m.locate(&4), Position::Occupied(1));
        assert_eq!(m.locate(&6), Position::Occupied(2));
        assert_eq!(m.locate(&1), Position::Vacant(0), "better than everything");
        assert_eq!(m.locate(&5), Position::Vacant(2), "between two held keys");
        assert_eq!(m.locate(&9), Position::Vacant(3), "worse than everything");
        assert_eq!(m.locate(&9).index(), m.len());
    }

    #[test]
    fn a_located_position_can_be_written_through() {
        let mut m = empty::<3>();
        m.insert(1, 10).unwrap();
        m.insert(2, 20).unwrap();

        let Position::Occupied(pos) = m.locate(&2) else {
            panic!("the key is held");
        };
        // SAFETY: `pos` came from an `Occupied`, so it is within bounds.
        *unsafe { m.value_mut_unchecked(pos) } = 222;

        assert_eq!(m.get(&2), Some(&222));
        assert_eq!(m.keys(), &[1, 2], "and the keys are untouched");
    }

    #[test]
    fn locate_then_insert_at_puts_the_key_where_it_was_told() {
        let mut m = empty::<4>();
        m.insert(1, 10).unwrap();
        m.insert(3, 30).unwrap();

        let Position::Vacant(pos) = m.locate(&2) else {
            panic!("the key is not held");
        };
        assert_eq!(pos, 1);
        // SAFETY: `pos` is where `locate` says 2 belongs, and the map is not full.
        unsafe {
            m.insert_at_unchecked(pos, 2, 20);
        }

        assert_eq!(m.keys(), &[1, 2, 3]);
        assert_eq!(m.iter().collect::<Vec<_>>(), vec![(&1, &10), (&2, &20), (&3, &30)]);
    }

    #[test]
    fn remove_at_takes_the_entry_the_position_names() {
        let mut m = empty::<4>();
        m.insert(1, 10).unwrap();
        m.insert(2, 20).unwrap();
        m.insert(3, 30).unwrap();

        // SAFETY: 1 < 3 entries held.
        let taken = unsafe { m.remove_at_unchecked(1) };
        assert_eq!(taken, (2, 20));
        assert_eq!(m.keys(), &[1, 3]);

        // SAFETY: 1 < 2 entries held.
        assert_eq!(unsafe { m.remove_at_unchecked(1) }, (3, 30));
        // SAFETY: 0 < 1 entry held.
        assert_eq!(unsafe { m.remove_at_unchecked(0) }, (1, 10));
        assert!(m.is_empty());
    }

    #[test]
    fn a_drain_can_be_taken_from_its_worst_end() {
        let mut m = empty::<4>();
        m.insert(1, 10).unwrap();
        m.insert(2, 20).unwrap();
        m.insert(3, 30).unwrap();
        m.insert(4, 40).unwrap();

        // Worst-first is the order a sorted deeper tier grown from its front wants.
        assert_eq!(
            m.drain_worst(3).rev().collect::<Vec<_>>(),
            vec![(4, 40), (3, 30), (2, 20)]
        );
        assert_eq!(m.keys(), &[1]);
    }

    #[test]
    fn a_drain_taken_from_both_ends_hands_out_each_entry_once() {
        let counter = Rc::new(Cell::new(0));
        let mut m = empty_tracked::<4>();
        for key in 1..=4 {
            m.insert(key, DropTracker(Rc::clone(&counter))).unwrap();
        }

        {
            let mut drain = m.drain_worst(4);
            assert_eq!(drain.len(), 4);
            let (best, _) = drain.next().expect("four to hand out");
            let (worst, _) = drain.next_back().expect("three left");
            assert_eq!((best, worst), (1, 4));
            assert_eq!(drain.len(), 2, "and the two in the middle are still owed");
        }

        // The two handed out were dropped by this test, the two left over by the drain.
        assert_eq!(counter.get(), 4);
        assert!(m.is_empty());
    }

    #[test]
    fn drain_worst_takes_the_largest_entries_best_first() {
        let mut m = empty::<4>();
        m.insert(1, 10).unwrap();
        m.insert(2, 20).unwrap();
        m.insert(3, 30).unwrap();
        m.insert(4, 40).unwrap();

        assert_eq!(
            m.drain_worst(2).collect::<Vec<_>>(),
            vec![(3, 30), (4, 40)],
            "the two worst, in ascending order"
        );
        assert_eq!(m.len(), 2);
        assert_eq!(m.keys(), &[1, 2]);
        assert_eq!(m.worst(), Some((&2, &20)));
    }

    #[test]
    fn drain_worst_leaves_the_survivors_usable() {
        let mut m = empty::<3>();
        m.insert(1, 10).unwrap();
        m.insert(2, 20).unwrap();
        m.insert(3, 30).unwrap();

        m.drain_worst(2).for_each(drop);

        // The capacity the drain freed is the capacity an insert finds.
        assert_eq!(m.insert(9, 90), Ok((1, None)));
        assert_eq!(m.insert(5, 50), Ok((1, None)));
        assert_eq!(m.keys(), &[1, 5, 9]);
        assert_eq!(m.insert(7, 70), Ok((2, Some((9, 90)))));
        assert_eq!(m.keys(), &[1, 5, 7]);
    }

    #[test]
    fn drain_worst_of_nothing_and_of_everything() {
        let mut m = empty::<3>();
        m.insert(1, 10).unwrap();
        m.insert(2, 20).unwrap();

        assert_eq!(m.drain_worst(0).count(), 0);
        assert_eq!(m.keys(), &[1, 2], "a zero-count drain is a no-op");

        assert_eq!(m.drain_worst(2).collect::<Vec<_>>(), vec![(1, 10), (2, 20)]);
        assert!(m.is_empty());
        assert_eq!(m.worst(), None);

        assert_eq!(m.drain_worst(0).count(), 0, "and so is one on an empty map");
    }

    #[test]
    #[should_panic(expected = "cannot drain more entries than the map holds")]
    fn drain_worst_rejects_a_count_past_the_end() {
        let mut m = empty::<3>();
        m.insert(1, 10).unwrap();
        m.drain_worst(2).for_each(drop);
    }

    #[test]
    fn dropping_a_drain_early_drops_what_it_did_not_yield() {
        let counter = Rc::new(Cell::new(0));
        let mut m = empty_tracked::<4>();
        m.insert(1, DropTracker(Rc::clone(&counter))).unwrap();
        m.insert(2, DropTracker(Rc::clone(&counter))).unwrap();
        m.insert(3, DropTracker(Rc::clone(&counter))).unwrap();
        m.insert(4, DropTracker(Rc::clone(&counter))).unwrap();

        {
            let mut drain = m.drain_worst(3);
            drop(drain.next().expect("three entries were drained"));
            assert_eq!(counter.get(), 1, "only the one that was handed out");
        }
        assert_eq!(counter.get(), 3, "the other two go with the drain");

        assert_eq!(m.len(), 1);
        assert_eq!(m.keys(), &[1]);
        drop(m);
        assert_eq!(counter.get(), 4, "and the survivor is still the map's to drop");
    }

    #[test]
    fn extend_worst_appends_a_batch_at_the_worst_end() {
        let mut m = empty::<4>();
        m.insert(1, 10).unwrap();

        // SAFETY: 3 <= 4 - 1 spare slots, and 2 < 3 < 4 are ascending and all greater
        // than the only key held.
        unsafe {
            m.extend_worst_unchecked([(2, 20), (3, 30), (4, 40)].into_iter());
        }

        assert_eq!(m.keys(), &[1, 2, 3, 4]);
        assert_eq!(m.len(), 4);
        assert!(m.is_full());
        assert_eq!(m.worst(), Some((&4, &40)));
        assert_eq!(m.get(&3), Some(&30), "values follow their keys");
    }

    #[test]
    fn extend_worst_into_an_empty_map_and_by_nothing() {
        let mut m = empty::<3>();

        // SAFETY: the map is empty, so there is room and nothing to be greater than.
        unsafe {
            m.extend_worst_unchecked([(1, 10), (2, 20)].into_iter());
        }
        assert_eq!(m.keys(), &[1, 2]);

        // SAFETY: an empty batch needs no room and violates no ordering.
        unsafe {
            m.extend_worst_unchecked(std::iter::empty());
        }
        assert_eq!(m.keys(), &[1, 2], "an empty batch is a no-op");
    }

    #[test]
    fn extend_worst_keeps_only_what_a_short_iterator_gave() {
        /// An `ExactSizeIterator` that promises three entries and yields one.
        struct Liar(Option<(i32, i32)>);

        impl Iterator for Liar {
            type Item = (i32, i32);

            fn next(&mut self) -> Option<Self::Item> {
                self.0.take()
            }
        }

        impl ExactSizeIterator for Liar {
            fn len(&self) -> usize {
                3
            }
        }

        let mut m = empty::<4>();
        m.insert(1, 10).unwrap();

        // SAFETY: room was reserved for the three it claims, and the one it yields is
        // greater than the key held.
        unsafe {
            m.extend_worst_unchecked(Liar(Some((2, 20))));
        }

        assert_eq!(m.keys(), &[1, 2], "the room it did not use is given back");
        assert_eq!(m.len(), 2);
        assert_eq!(m.worst(), Some((&2, &20)));
        assert_eq!(m.insert(3, 30), Ok((2, None)));
        assert_eq!(m.keys(), &[1, 2, 3]);
    }

    #[test]
    fn a_drain_and_an_extend_are_inverses() {
        let mut m = empty::<4>();
        m.insert(2, 20).unwrap();
        m.insert(1, 10).unwrap();
        m.insert(4, 40).unwrap();
        m.insert(3, 30).unwrap();

        let spilled = m.drain_worst(2).collect::<Vec<_>>();
        assert_eq!(m.keys(), &[1, 2]);

        // SAFETY: exactly the entries just drained, in the order they came out, going back
        // into the room their removal made.
        unsafe {
            m.extend_worst_unchecked(spilled.into_iter());
        }

        assert_eq!(m.keys(), &[1, 2, 3, 4]);
        assert_eq!(
            m.iter().collect::<Vec<_>>(),
            vec![(&1, &10), (&2, &20), (&3, &30), (&4, &40)]
        );
    }

    #[test]
    fn iter_yields_entries_in_ascending_order() {
        let mut m = empty::<4>();
        m.insert(3, 30).unwrap();
        m.insert(1, 10).unwrap();
        m.insert(2, 20).unwrap();

        assert_eq!(
            m.iter().collect::<Vec<_>>(),
            vec![(&1, &10), (&2, &20), (&3, &30)]
        );
    }

    #[test]
    fn iter_of_empty_map_yields_nothing() {
        let m = empty::<3>();
        assert_eq!(m.iter().next(), None);
    }

    #[test]
    fn iter_nth_skips_the_right_number_of_entries() {
        let mut m = empty::<4>();
        m.insert(1, 10).unwrap();
        m.insert(2, 20).unwrap();
        m.insert(3, 30).unwrap();

        let mut it = m.iter();
        assert_eq!(it.nth(1), Some((&2, &20)));
        assert_eq!(it.next(), Some((&3, &30)));
        assert_eq!(it.next(), None);
    }

    #[test]
    fn clone_produces_an_equal_and_independent_copy() {
        let mut original = empty::<4>();
        original.insert(1, 10).unwrap();
        original.insert(2, 20).unwrap();
        original.insert(3, 30).unwrap();

        let cloned = original.clone();
        assert_eq!(
            cloned.iter().collect::<Vec<_>>(),
            original.iter().collect::<Vec<_>>()
        );

        // Mutating the original after cloning must not affect the clone.
        original.insert(4, 40).unwrap();
        original.remove(&1);
        assert_eq!(
            cloned.iter().collect::<Vec<_>>(),
            vec![(&1, &10), (&2, &20), (&3, &30)]
        );
    }

    #[test]
    fn clone_of_empty_map_is_empty() {
        let original = empty::<3>();
        let cloned = original.clone();
        assert_eq!(cloned.len(), 0);
        assert_eq!(cloned.iter().next(), None);
        assert_eq!(original.len(), 0, "and the source is left as it was");
    }

    #[test]
    fn clone_from_overwrites_in_place_when_same_length() {
        let mut target = empty::<3>();
        target.insert(1, 111).unwrap();
        target.insert(2, 222).unwrap();

        let mut source = empty::<3>();
        source.insert(1, 10).unwrap();
        source.insert(2, 20).unwrap();

        target.clone_from(&source);
        assert_eq!(target.len(), 2);
        assert_eq!(
            target.iter().collect::<Vec<_>>(),
            vec![(&1, &10), (&2, &20)]
        );
    }

    #[test]
    fn clone_from_grows_when_source_is_longer() {
        let mut target = empty::<4>();
        target.insert(1, 111).unwrap();

        let mut source = empty::<4>();
        source.insert(1, 10).unwrap();
        source.insert(2, 20).unwrap();
        source.insert(3, 30).unwrap();

        target.clone_from(&source);
        assert_eq!(target.len(), 3);
        assert_eq!(
            target.iter().collect::<Vec<_>>(),
            vec![(&1, &10), (&2, &20), (&3, &30)]
        );
    }

    #[test]
    fn clone_from_into_empty_target_copies_everything() {
        let mut target = empty::<4>();

        let mut source = empty::<4>();
        source.insert(1, 10).unwrap();
        source.insert(2, 20).unwrap();

        target.clone_from(&source);
        assert_eq!(target.len(), 2);
        assert_eq!(
            target.iter().collect::<Vec<_>>(),
            vec![(&1, &10), (&2, &20)]
        );
    }

    #[test]
    fn clone_from_shrinks_and_drops_truncated_entries_when_source_is_shorter() {
        let counter = Rc::new(Cell::new(0));
        let mut target = empty_tracked::<4>();
        target.insert(1, DropTracker(Rc::clone(&counter))).unwrap();
        target.insert(2, DropTracker(Rc::clone(&counter))).unwrap();
        target.insert(3, DropTracker(Rc::clone(&counter))).unwrap();

        let mut source = empty_tracked::<4>();
        source.insert(1, DropTracker(Rc::clone(&counter))).unwrap();

        target.clone_from(&source);

        assert_eq!(target.len(), 1);
        // 1 drop from overwriting index 0 in place, plus 2 drops from
        // truncating the now-stale entries at indices 1 and 2.
        assert_eq!(counter.get(), 3);
    }

    #[test]
    fn clear_drops_all_entries_and_resets_len() {
        let counter = Rc::new(Cell::new(0));
        let mut m = empty_tracked::<4>();
        m.insert(1, DropTracker(Rc::clone(&counter))).unwrap();
        m.insert(2, DropTracker(Rc::clone(&counter))).unwrap();
        m.insert(3, DropTracker(Rc::clone(&counter))).unwrap();

        m.clear();

        assert_eq!(m.len(), 0);
        assert_eq!(counter.get(), 3);

        // capacity is usable again after clearing
        m.insert(1, DropTracker(Rc::clone(&counter))).unwrap();
        assert_eq!(m.len(), 1);
    }

    #[test]
    fn clear_on_empty_map_is_a_no_op() {
        let mut m = empty::<3>();
        m.clear();
        assert_eq!(m.len(), 0);
    }

    #[test]
    fn drop_drops_all_remaining_entries() {
        let counter = Rc::new(Cell::new(0));
        {
            let mut m = empty_tracked::<4>();
            m.insert(1, DropTracker(Rc::clone(&counter))).unwrap();
            m.insert(2, DropTracker(Rc::clone(&counter))).unwrap();
            m.insert(3, DropTracker(Rc::clone(&counter))).unwrap();
            assert_eq!(counter.get(), 0);
        }
        assert_eq!(counter.get(), 3);
    }

    #[test]
    fn drop_of_empty_map_drops_nothing() {
        let counter = Rc::new(Cell::new(0));
        {
            let _m = empty_tracked::<4>();
        }
        assert_eq!(counter.get(), 0);
    }

    #[test]
    fn drop_only_drops_entries_still_present() {
        let counter = Rc::new(Cell::new(0));
        {
            let mut m = empty_tracked::<4>();
            m.insert(1, DropTracker(Rc::clone(&counter))).unwrap();
            m.insert(2, DropTracker(Rc::clone(&counter))).unwrap();
            m.insert(3, DropTracker(Rc::clone(&counter))).unwrap();

            // Removing an entry drops its value right away, when the
            // returned `Option<V>` is discarded.
            m.remove(&2);
            assert_eq!(counter.get(), 1);
        }
        // Dropping the map drops only what's left (keys 1 and 3).
        assert_eq!(counter.get(), 3);
    }
}
