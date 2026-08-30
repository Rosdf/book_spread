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

    pub fn last(&self) -> Option<(&K, &V)> {
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

    /// # Safety
    /// `key` should be grater then all elements in map and `self.base > 0`, so that there is
    /// a free slot at the front for the live range to grow into
    pub unsafe fn insert_last_unchecked(&mut self, key: K, value: V) {
        // SAFETY:
        // `self.len() <= self.len()` trivially, and the caller guarantees the free slot
        // the shift needs (per fn-level `# Safety`).
        unsafe {
            self.shift_left_and_insert(self.len(), key, value);
        }
    }

    /// Shifts the elements in `[base, base + pos)` one slot to the left - down into the
    /// free slot at the front - and writes `key`/`value` into the slot at logical `pos`
    /// that this opens up.
    ///
    /// # Safety
    /// `pos <= self.len()` and `self.base > 0`.
    unsafe fn shift_left_and_insert(&mut self, pos: usize, key: K, value: V) {
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

    /// Removes and returns the entry at logical position `pos`, shifting everything
    /// smaller than it one slot to the right so the live range stays right-aligned.
    ///
    /// # Safety
    /// `pos < self.len()`.
    unsafe fn remove_at_unchecked(&mut self, pos: usize) -> (K, V) {
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

impl<K, V, const N: usize> Drop for HeaplessLinearMap<K, V, N> {
    fn drop(&mut self) {
        self.clear();
    }
}

impl<K: Ord, V, const N: usize> HeaplessLinearMap<K, V, N> {
    /// Inserts `value` under `key`, keeping the keys sorted. `Ok(Some(..))` is the pair this
    /// replaced.
    ///
    /// # Errors
    ///
    /// The pair back, when the map is already full and `key` is not in it - there is nowhere
    /// to put it and no heap to grow into.
    pub fn insert(&mut self, key: K, value: V) -> Result<Option<(K, V)>, (K, V)> {
        let (pos, existing) = self.locate(&key);

        if existing {
            // SAFETY:
            // `locate` reports `existing` only for a `pos` it found a key at, so
            // `pos < self.len()` and this slot is initialized.
            let v = unsafe { self.values.get_unchecked_mut(self.base + pos) };
            // SAFETY:
            // `v` is initialized, as above, and is about to be overwritten with a
            // freshly written value below.
            unsafe {
                v.assume_init_drop();
            }
            v.write(value);

            return Ok(None);
        }

        // `key` is not present yet, it needs to be inserted at `pos`, keeping
        // the array sorted ascending.
        if !self.is_full() {
            // SAFETY:
            // `pos <= self.len()` (`locate` scans the live range) and the map is not
            // full, so `self.base > 0`.
            unsafe {
                self.shift_left_and_insert(pos, key, value);
            }
            Ok(None)
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
                self.shift_left_and_insert(pos, key, value);
            }
            Ok(Some(evicted))
        }
    }

    /// The position `key` belongs at, and whether the key already sitting there is `key`
    /// itself. `(self.len(), false)` when every stored key is smaller.
    ///
    /// A forward scan rather than the binary search the sorted keys would allow. Over a
    /// map this small the binary search's ~log2(N) comparisons are a fixed cost paid on
    /// every call, while the scan stops at the first key that is not smaller: one or two
    /// comparisons for the near-best insertions that dominate, degrading a step at a time
    /// towards the worst case instead of cliffing to it. The same argument `remove` and
    /// [`Self::get`] already make one function up.
    fn locate(&self, key: &K) -> (usize, bool) {
        for (pos, k) in self.keys().iter().enumerate() {
            match k.cmp(key) {
                Ordering::Less => {}
                Ordering::Equal => return (pos, true),
                Ordering::Greater => return (pos, false),
            }
        }

        (self.len(), false)
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        let pos = self.keys().iter().position(|k| *k == *key)?;
        // SAFETY:
        // `pos` came from searching `self.keys()`, the live range seen as a slice, so
        // `pos < self.len()`.
        let (_, value) = unsafe { self.remove_at_unchecked(pos) };
        Some(value)
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
    use super::HeaplessLinearMap;
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
        assert_eq!(m.insert(5, 50), Ok(None));
        assert_eq!(m.insert(1, 10), Ok(None));
        assert_eq!(m.insert(3, 30), Ok(None));
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
        assert_eq!(evicted, Some((9, 90)));
        assert_eq!(m.keys(), &[1, 3, 5]);
    }

    #[test]
    fn update_existing_key_returns_none_and_replaces_value() {
        let mut m = empty::<3>();
        m.insert(5, 50).unwrap();
        let r = m.insert(5, 999).unwrap();
        assert_eq!(r, None);
        assert_eq!(m.keys(), &[5]);
        assert_eq!(m.last(), Some((&5, &999)));
    }

    #[test]
    fn update_existing_key_when_full_still_works() {
        let mut m = empty::<2>();
        m.insert(1, 10).unwrap();
        m.insert(2, 20).unwrap();
        let r = m.insert(1, 111).unwrap();
        assert_eq!(r, None);
        assert_eq!(m.keys(), &[1, 2]);
        assert_eq!(m.last(), Some((&2, &20)));
    }

    #[test]
    fn get_finds_a_value_by_its_key_and_nothing_else() {
        let mut m = empty::<3>();
        assert_eq!(m.get(&1), None, "an empty map has nothing under any key");

        assert_eq!(m.insert(5, 50), Ok(None));
        assert_eq!(m.insert(1, 10), Ok(None));
        assert_eq!(m.insert(3, 30), Ok(None));

        assert_eq!(m.get(&1), Some(&10), "the first key");
        assert_eq!(m.get(&3), Some(&30), "a middle key");
        assert_eq!(m.get(&5), Some(&50), "the last key");
        assert_eq!(m.get(&4), None, "a key between two that are present");
        assert_eq!(m.get(&9), None, "a key past the largest present");

        // The entries shift on a remove, so what `get` reads has to follow them rather than
        // the position the key went in at.
        assert_eq!(m.remove(&1), Some(10));
        assert_eq!(m.get(&1), None);
        assert_eq!(m.get(&3), Some(&30));
        assert_eq!(m.get(&5), Some(&50));
    }

    /// The point of the `Borrow` bound: a map keyed on an owned type is searched with the
    /// borrowed one, so a lookup allocates nothing.
    #[test]
    fn get_takes_a_borrowed_key() {
        let mut m: HeaplessLinearMap<String, i32, 2> = HeaplessLinearMap::new();
        assert_eq!(m.insert("btcusdt".to_owned(), 1), Ok(None));
        assert_eq!(m.insert("ethusdt".to_owned(), 2), Ok(None));

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
        assert_eq!(m.last(), Some((&2, &20)));
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

        assert_eq!(m.remove(&3), Some(30));
        assert_eq!(m.len(), 2);
        assert_eq!(m.keys(), &[1, 2]);
        assert_eq!(m.last(), Some((&2, &20)));
    }

    #[test]
    fn remove_first_key_shifts_remaining_entries_left() {
        let mut m = empty::<3>();
        m.insert(1, 10).unwrap();
        m.insert(2, 20).unwrap();
        m.insert(3, 30).unwrap();

        assert_eq!(m.remove(&1), Some(10));
        assert_eq!(m.len(), 2);
        assert_eq!(m.keys(), &[2, 3]);
        assert_eq!(m.last(), Some((&3, &30)));
    }

    #[test]
    fn remove_middle_key_keeps_keys_and_values_paired() {
        let mut m = empty::<4>();
        m.insert(1, 10).unwrap();
        m.insert(2, 20).unwrap();
        m.insert(3, 30).unwrap();
        m.insert(4, 40).unwrap();

        assert_eq!(m.remove(&2), Some(20));
        assert_eq!(m.len(), 3);
        assert_eq!(m.keys(), &[1, 3, 4]);
        // values must have shifted in lock-step with keys, not just keys
        assert_eq!(m.last(), Some((&4, &40)));
        assert_eq!(m.remove(&3), Some(30));
        assert_eq!(m.keys(), &[1, 4]);
        assert_eq!(m.last(), Some((&4, &40)));
        assert_eq!(m.remove(&1), Some(10));
        assert_eq!(m.keys(), &[4]);
        assert_eq!(m.last(), Some((&4, &40)));
    }

    #[test]
    fn remove_all_entries_drains_map() {
        let mut m = empty::<3>();
        m.insert(1, 10).unwrap();
        m.insert(2, 20).unwrap();
        m.insert(3, 30).unwrap();

        assert_eq!(m.remove(&2), Some(20));
        assert_eq!(m.remove(&1), Some(10));
        assert_eq!(m.remove(&3), Some(30));
        assert_eq!(m.len(), 0);
        assert_eq!(m.last(), None);
        // removing again from the now-empty map is a no-op
        assert_eq!(m.remove(&3), None);
    }

    #[test]
    fn remove_then_insert_reuses_freed_capacity() {
        let mut m = empty::<2>();
        m.insert(1, 10).unwrap();
        m.insert(2, 20).unwrap();
        assert!(m.is_full());

        assert_eq!(m.remove(&1), Some(10));
        assert!(!m.is_full());

        assert_eq!(m.insert(3, 30), Ok(None));
        assert_eq!(m.keys(), &[2, 3]);
        assert_eq!(m.last(), Some((&3, &30)));
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
