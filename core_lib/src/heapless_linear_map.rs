use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::mem::MaybeUninit;
use std::slice;

pub struct HeaplessLinearMap<K, V, const N: usize> {
    len: usize,
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
            // we're inserting in valid order
            unsafe {
                res.insert_last_unchecked(k.clone(), v.clone());
            }
        }

        res
    }

    fn clone_from(&mut self, source: &Self) {
        for (idx, (k, v)) in source.iter().take(self.len).enumerate() {
            // SAFETY:
            // `idx < self.len` (bounded by `.take(self.len)`), so this slot is
            // already initialized and safe to `assume_init_mut`/overwrite in place.
            unsafe {
                self.keys
                    .get_unchecked_mut(idx)
                    .assume_init_mut()
                    .clone_from(k);
                self.values
                    .get_unchecked_mut(idx)
                    .assume_init_mut()
                    .clone_from(v);
            }
        }

        match self.len.cmp(&source.len) {
            Ordering::Less => {
                for (k, v) in source.iter().skip(self.len) {
                    // SAFETY:
                    // `source` is sorted ascending and these are its remaining
                    // (larger) entries, appended in order; the loop above already
                    // matched `self`'s first `self.len` entries to `source`'s, so
                    // each of these is greater than everything currently in `self`.
                    unsafe {
                        self.insert_last_unchecked(k.clone(), v.clone());
                    }
                }
            }
            Ordering::Equal => {}
            Ordering::Greater => {
                let old_len = self.len;
                self.len = source.len();
                for idx in self.len..old_len {
                    // SAFETY:
                    // these indices held valid entries before truncation (they were
                    // part of the original `0..old_len`), and haven't been touched
                    // since, so they're still initialized and safe to drop.
                    unsafe {
                        self.keys.get_unchecked_mut(idx).assume_init_drop();
                        self.values.get_unchecked_mut(idx).assume_init_drop();
                    }
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
            len: 0,
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
        if self.len == 0 {
            None
        } else {
            // SAFETY:
            // `self.len > 0` here, so index `self.len - 1` is initialized.
            let key = unsafe { self.keys.get_unchecked(self.len - 1).assume_init_ref() };
            // SAFETY: same as above.
            let value = unsafe { self.values.get_unchecked(self.len - 1).assume_init_ref() };
            Some((key, value))
        }
    }

    pub const fn len(&self) -> usize {
        self.len
    }

    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub const fn is_full(&self) -> bool {
        self.len == N
    }

    /// # Safety
    /// `key` should be grater then all elements in map and map should have capacity for at least 1 element
    pub unsafe fn insert_last_unchecked(&mut self, key: K, value: V) {
        // SAFETY:
        // caller guarantees spare capacity (per fn-level `# Safety`), so index
        // `self.len` is in bounds and not currently initialized.
        unsafe {
            self.keys.get_unchecked_mut(self.len).write(key);
        }
        // SAFETY: same as above.
        unsafe {
            self.values.get_unchecked_mut(self.len).write(value);
        }
        self.len += 1;
    }

    pub fn clear(&mut self) {
        let len = self.len;
        self.len = 0;

        for idx in 0..len {
            // SAFETY:
            // `idx < len == self.len` (before we zeroed it above), so every
            // index in this range is initialized.
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
        // Position of the first existing key that is >= `key` (or `self.len` if
        // every existing key is smaller).
        let pos = self.keys().partition_point(|k| *k < key);

        if pos < self.len {
            // SAFETY:
            // `pos < self.len`, so this slot is initialized.
            let existing_equal = *unsafe { self.keys.get_unchecked(pos).assume_init_ref() } == key;

            if existing_equal {
                // SAFETY:
                // `pos < self.len`, so this slot is initialized.
                let v = unsafe { self.values.get_unchecked_mut(pos) };
                // SAFETY:
                // `v` was just read as initialized above, and is about to be
                // overwritten with a freshly written value below.
                unsafe {
                    v.assume_init_drop();
                }
                v.write(value);

                return Ok(None);
            }
        }

        // `key` is not present yet, it needs to be inserted at `pos`, keeping
        // the array sorted ascending.
        if self.len < N {
            // SAFETY:
            // `pos <= self.len` (it's a `partition_point` over a `self.len`-long
            // slice) and `self.len < N` was just checked above.
            unsafe {
                self.shift_right_and_insert(pos, key, value);
            }
            Ok(None)
        } else if pos == self.len {
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
            // we would have entered prev branch if pos == self.len
            unsafe {
                self.shift_right_and_insert(pos, key, value);
            }
            Ok(Some(evicted))
        }
    }

    /// Shifts the elements in `[pos, self.len)` one slot to the right and
    /// writes `key`/`value` into the now-empty slot at `pos`.
    ///
    /// # Safety
    /// `pos <= self.len`.
    unsafe fn shift_right_and_insert(&mut self, pos: usize, key: K, value: V) {
        debug_assert!(pos <= self.len, "insertion position must be within bounds");
        debug_assert!(self.len < N, "caller must ensure there is spare capacity");

        let count = self.len - pos;
        // SAFETY: `pos + count == self.len < N` and `pos + 1 + count == self.len + 1 <= N`,
        // so both the source and destination ranges lie within the arrays'
        // bounds. The ranges may overlap, hence `ptr::copy` (not
        // `copy_nonoverlapping`). The slot at `pos` is treated as logically
        // moved-out afterwards and immediately reinitialized below.
        unsafe {
            let keys_ptr = self.keys.as_mut_ptr();
            std::ptr::copy(keys_ptr.add(pos), keys_ptr.add(pos + 1), count);

            let values_ptr = self.values.as_mut_ptr();
            std::ptr::copy(values_ptr.add(pos), values_ptr.add(pos + 1), count);

            self.keys.get_unchecked_mut(pos).as_mut_ptr().write(key);
            self.values.get_unchecked_mut(pos).as_mut_ptr().write(value);
        }
        self.len += 1;
    }

    /// Removes and returns the last (largest) entry.
    ///
    /// # Safety
    /// `self.len > 0`.
    unsafe fn evict_last_unchecked(&mut self) -> (K, V) {
        debug_assert!(self.len > 0, "caller must ensure the map is non-empty");
        self.len -= 1;
        // SAFETY: index `self.len` (post-decrement) was initialized and is
        // now considered removed, so reading it out here is the only read.
        unsafe {
            let key = self.keys.get_unchecked(self.len).assume_init_read();
            let value = self.values.get_unchecked(self.len).assume_init_read();
            (key, value)
        }
    }

    fn keys(&self) -> &[K] {
        // SAFETY:
        // self.len elements are active
        unsafe { slice::from_raw_parts(self.keys.as_ptr().cast(), self.len) }
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        if let Some(pos) = self.keys().iter().position(|k| *k == *key) {
            // SAFETY:
            // `pos` came from searching `self.keys()`, a `self.len`-long slice,
            // so `pos..self.len` is in bounds. Rotating left moves the removed
            // entry to the end (index `self.len - 1`), where `evict_last_unchecked`
            // below reads it out.
            unsafe { self.keys.get_unchecked_mut(pos..self.len) }.rotate_left(1);
            // SAFETY: same as above.
            unsafe { self.values.get_unchecked_mut(pos..self.len) }.rotate_left(1);

            // SAFETY:
            // we found an element, so it is not empty
            let (_, value) = unsafe { self.evict_last_unchecked() };
            Some(value)
        } else {
            None
        }
    }
}

impl<K: Debug, V: Debug, const N: usize> Debug for HeaplessLinearMap<K, V, N> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut map = f.debug_map();

        for idx in 0..self.len {
            // SAFETY:
            // `idx < self.len`, so this slot is initialized.
            let key = unsafe { self.keys.get_unchecked(idx).assume_init_ref() };
            // SAFETY: same as above.
            let value = unsafe { self.values.get_unchecked(idx).assume_init_ref() };
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
        if self.pos >= self.parent.len {
            return None;
        }

        // SAFETY:
        // `self.pos < self.parent.len`, checked just above, so this slot is initialized.
        let key = unsafe { self.parent.keys.get_unchecked(self.pos).assume_init_ref() };
        // SAFETY: same as above.
        let value = unsafe { self.parent.values.get_unchecked(self.pos).assume_init_ref() };
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
        self.parent.len.saturating_sub(self.pos)
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
    use std::mem::MaybeUninit;
    use std::rc::Rc;

    fn empty<const N: usize>() -> HeaplessLinearMap<i32, i32, N> {
        HeaplessLinearMap {
            len: 0,
            keys: [const { MaybeUninit::uninit() }; N],
            values: [const { MaybeUninit::uninit() }; N],
        }
    }

    fn empty_tracked<const N: usize>() -> HeaplessLinearMap<i32, DropTracker, N> {
        HeaplessLinearMap {
            len: 0,
            keys: [const { MaybeUninit::uninit() }; N],
            values: [const { MaybeUninit::uninit() }; N],
        }
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
