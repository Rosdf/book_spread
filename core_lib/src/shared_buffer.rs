use crate::sync::{AtomicU64, AtomicUsize, ConstPtr, Ordering, UnsafeCell};
use crossbeam_utils::CachePadded;
use std::hint::{cold_path, spin_loop};
use std::ops::{Deref, Not as _};

const WRITE_FLAG: u64 = 1 << 63;

#[derive(Debug)]
struct Slot<T> {
    data: UnsafeCell<T>,
    state: CachePadded<AtomicU64>,
}

// `Slot<T>: Send` needs no impl here: `UnsafeCell<T>` is `Send` whenever `T: Send`, and
// `CachePadded<AtomicU64>` is unconditionally `Send`, so the auto impl already applies.
// Only `Sync` is missing, because `UnsafeCell` opts out of it.
//
// SAFETY: `T: Send` is required because `Publisher::update` reaches `data` through a shared
// `&Slot<T>`, moving a new `T` in and dropping the old one on the writer's thread, which
// need not be the thread either value came from. `T: Sync` is required because
// `ReaderGuard::deref` hands out `&T` to threads other than the writer's.
//
// Given those, all access to `data` is gated by the per-slot `state` protocol: the writer's
// `compare_exchange` of `0 -> WRITE_FLAG` grants it exclusive access for the whole of its
// store, and a reader's `fetch_add(1)` keeps the count nonzero for the entire lifetime of
// its `ReaderGuard`, which makes that same CAS fail. So a `&mut T` handed to the writer and
// a `&T` handed to any reader never overlap in time.
unsafe impl<T: Send + Sync> Sync for Slot<T> {}

#[cfg(not(loom))]
type Inner<T> = triomphe::HeaderSlice<CachePadded<AtomicUsize>, [Slot<T>]>;
#[cfg(not(loom))]
type TArc<T> = triomphe::Arc<T>;

// Under loom the slots must live behind loom's own `Arc`, because loom cannot
// see through `triomphe`'s refcount to the atomics inside it. That costs no
// coverage of the part being tested: the slot protocol below is untouched, and
// the refcount itself is `triomphe`'s to get right, not this module's.
#[cfg(loom)]
#[derive(Debug)]
struct Inner<T> {
    header: CachePadded<AtomicUsize>,
    slice: Box<[Slot<T>]>,
}
#[cfg(loom)]
type TArc<T> = loom::sync::Arc<T>;

#[cfg(not(loom))]
fn new_inner<T>(
    header: CachePadded<AtomicUsize>,
    slots: impl ExactSizeIterator<Item = Slot<T>>,
) -> TArc<Inner<T>> {
    TArc::from_header_and_iter(header, slots)
}

#[cfg(loom)]
fn new_inner<T>(
    header: CachePadded<AtomicUsize>,
    slots: impl ExactSizeIterator<Item = Slot<T>>,
) -> TArc<Inner<T>> {
    TArc::new(Inner {
        header,
        slice: slots.collect(),
    })
}

#[derive(Debug)]
pub struct Publisher<T> {
    inner: TArc<Inner<T>>,
}

#[derive(Debug)]
pub struct Reader<T> {
    inner: TArc<Inner<T>>,
}

impl<T> Publisher<T> {
    pub fn new(max_readers: usize) -> Self
    where
        T: Default,
    {
        let iterator = std::iter::repeat_with(|| Slot {
            data: UnsafeCell::new(T::default()),
            state: CachePadded::new(AtomicU64::new(0)),
        })
        .take(max_readers + 2);

        let inner = new_inner(CachePadded::new(AtomicUsize::new(0)), iterator);

        Self { inner }
    }

    pub fn reader(&mut self) -> Option<Reader<T>> {
        (TArc::strong_count(&self.inner) + 1 < self.inner.slice.len()).then(|| Reader {
            inner: TArc::clone(&self.inner),
        })
    }

    pub fn update(&mut self, value: T) {
        self.update_with(|slot| *slot = value);
    }

    /// Writes the next value in place, without building one on the stack first.
    ///
    /// [`update`](Self::update) moves a whole `T` into a claimed slot, which for a large `T`
    /// is a stack temporary plus a full copy. `update_with` hands `f` the slot itself, so a
    /// caller that can refill a value in place - clearing and re-extending its collections,
    /// say - pays neither.
    ///
    /// # Contract
    ///
    /// `f` must *fully overwrite* what it is given. Slots rotate, so the value it receives is
    /// not the previously published one: it is whatever was in this slot several publishes
    /// ago, or `T::default()` if the slot has never been written. Anything `f` leaves behind
    /// is what readers will see. `clear()` followed by an `extend` satisfies this; a partial
    /// update does not.
    ///
    /// `f` must also not unwind. A panic inside it leaves this slot's `WRITE_FLAG` set for
    /// good, and a reader that lands on the slot spins on that flag forever. This is the same
    /// hazard `update` already carries through `T`'s `Drop`, not a new one, but `f` is a much
    /// easier place to hit it.
    pub fn update_with(&mut self, f: impl FnOnce(&mut T)) {
        let idx = self.claim_slot();

        // SAFETY: `claim_slot` only ever returns an index it took from `0..len`.
        let slot = unsafe { self.inner.slice.get(idx).unwrap_unchecked() };

        // SAFETY: `claim_slot` succeeded its CAS, so this thread exclusively holds
        // WRITE_FLAG on this slot - no reader or other writer can be accessing `slot.data`
        // right now, for the whole of `f`.
        slot.data.with_mut(|data| unsafe { f(&mut *data) });

        // Release: publishes the write above to whoever next observes
        // WRITE_FLAG cleared here - a reader spinning on this slot (Acquire
        // load below in `get`), or the next writer's Acquire pre-check/CAS.
        slot.state.fetch_and(WRITE_FLAG.not(), Ordering::Release);
        // Release: publishes `idx`, and everything sequenced-before it
        // (including the slot write above), to readers/writers that
        // Acquire-load `header`.
        self.inner.header.store(idx, Ordering::Release);
    }

    /// Takes exclusive ownership of a free slot and returns its index, with `WRITE_FLAG` set.
    ///
    /// The caller must clear the flag once it has finished writing, or readers landing on
    /// that slot spin forever.
    fn claim_slot(&mut self) -> usize {
        let len = self.inner.slice.len();

        // Relaxed: `header` is only ever written by the writer, and `update`
        // takes `&mut self`, so calls are serialized - this thread always sees
        // its own prior store regardless of ordering. `last` is also only used
        // as a heuristic (start point, and which slot to avoid re-picking to
        // reduce reader contention); the actual safety of the write below comes
        // from the per-slot `state` CAS, not from this load.
        let last = self.inner.header.load(Ordering::Relaxed);
        let mut idx = last;
        loop {
            if idx == last {
                idx += 1;
                continue;
            }
            if idx == len {
                idx = 0;
                continue;
            }

            // SAFETY: the wraparound check above (`idx == len`) keeps `idx` in `0..len`.
            let slot = unsafe { self.inner.slice.get(idx).unwrap_unchecked() };
            // Relaxed: cheap pre-check to avoid an unnecessary CAS under contention.
            // Correctness of actually claiming the slot comes from the CAS below
            // (AcqRel on success), so this load doesn't need to synchronize with
            // anything itself - a stale read here only costs an extra CAS attempt
            // (stale `0`) or an extra lap of the loop (stale nonzero), never UB.
            let slot_flag = slot.state.load(Ordering::Relaxed);
            if slot_flag != 0 {
                idx += 1;
                continue;
            }

            // AcqRel on success: Acquire synchronizes-with whichever reader's
            // `fetch_sub(.., Release)` (drop) or the previous writer's
            // `fetch_and(.., Release)` produced the `0` this CAS observes, so our
            // write to `slot.data` below correctly happens-after their prior
            // access. Release publishes this thread's claim to later Acquire
            // loads of `state`. Relaxed on failure: we don't act on the read
            // value, we just retry with a different slot.
            if slot
                .state
                .compare_exchange_weak(0, WRITE_FLAG, Ordering::AcqRel, Ordering::Relaxed)
                .is_err()
            {
                idx += 1;
                continue;
            }

            return idx;
        }
    }
}

#[derive(Debug)]
pub struct ReaderGuard<'a, T> {
    slot: &'a Slot<T>,
    /// Kept alive for the whole guard so loom can see this read overlapping any
    /// concurrent write. `Option` only so `Drop` can release it before the slot;
    /// on the `std` build this is a niche-optimised raw pointer.
    data: Option<ConstPtr<T>>,
}

impl<T> Drop for ReaderGuard<'_, T> {
    fn drop(&mut self) {
        // Release the borrow before the slot itself: a writer claiming this slot
        // straight after the decrement below is not concurrent with our read,
        // and dropping in the other order would make loom report that it is.
        //
        // Only `loom::cell::ConstPtr` has a `Drop` to run here - the plain build's
        // `ConstPtr` is a bare `*const T` - so clippy, which only ever sees that
        // build, is right about the type and wrong about the point.
        #[cfg_attr(
            not(loom),
            expect(
                clippy::drop_non_drop,
                reason = "under `--cfg loom` this releases the cell borrow, and the order \
                          relative to the decrement below is what the model checks"
            )
        )]
        drop(self.data.take());
        // Release: makes this reader's access to `slot.data` (in `deref`, below)
        // happen-before whichever writer's Acquire operation (the CAS pre-check
        // load, or the CAS itself) next observes the decremented count.
        self.slot.state.fetch_sub(1, Ordering::Release);
    }
}

impl<T> Deref for ReaderGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        // SAFETY: `Reader::get` incremented `state` before returning this guard,
        // which prevents the writer's CAS from claiming this slot for as long as
        // the guard is alive, so no exclusive writer access can be happening
        // concurrently with this shared read.
        // SAFETY: the guard holds the borrow taken in `Reader::get`, and the
        // slot's refcount keeps the writer's CAS from claiming this slot for as
        // long as the guard is alive, so no exclusive access overlaps this one.
        unsafe { self.data.as_ref().unwrap_unchecked().deref() }
    }
}
impl<T> Reader<T> {
    pub fn get(&mut self) -> ReaderGuard<'_, T> {
        // Acquire: pairs with the writer's `header.store(idx, Release)`, so
        // everything the writer did to publish this slot (including the write
        // to `slot.data`) happens-before this read.
        let idx = self.inner.header.load(Ordering::Acquire);

        // SAFETY: `idx` was just read from `header`, which the writer always
        // keeps in `0..len`.
        let slot = unsafe { self.inner.slice.get(idx).unwrap_unchecked() };

        // AcqRel: Acquire lets us correctly observe WRITE_FLAG's current state
        // (and the writer's data belonging to it) at the moment we register;
        // Release publishes our registration so the writer's next CAS on this
        // slot is guaranteed to see the incremented count.
        let mut slot_state = slot.state.fetch_add(1, Ordering::AcqRel);

        while slot_state & WRITE_FLAG != 0 {
            // if we are here it means that somehow publisher managed to make several writes
            // after we read idx
            cold_path();
            spin_loop();
            // Acquire: once WRITE_FLAG is observed cleared here, this
            // synchronizes-with the writer's `fetch_and(.., Release)`, so its
            // write to `slot.data` happens-before the read in `deref` below.
            slot_state = slot.state.load(Ordering::Acquire);
        }

        ReaderGuard {
            slot,
            data: Some(slot.data.const_ptr()),
        }
    }
}

#[cfg(test)]
mod test {
    use super::{Publisher, Reader, Slot};

    const fn assert_send<T: Send>() {}
    const fn assert_sync<T: Sync>() {}

    /// `Slot<T>` gets `Send` from the auto impl (`UnsafeCell<T>: Send where T: Send`) and
    /// `Sync` from the `unsafe impl` above, and `triomphe::Arc<T>: Send` requires
    /// `T: Send + Sync` - so both handles cross threads exactly when `T: Send + Sync`.
    #[test]
    const fn handles_cross_threads() {
        assert_send::<Slot<u64>>();
        assert_sync::<Slot<u64>>();
        assert_send::<Publisher<u64>>();
        assert_sync::<Publisher<u64>>();
        assert_send::<Reader<u64>>();
        assert_sync::<Reader<u64>>();
    }
}
