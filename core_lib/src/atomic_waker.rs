//! Single-waiter, single-notifier async notification with a sticky pending flag.
//!
//! [`pair`] hands out a [`Notifier`] and a [`Waiter`] over one shared state: the
//! notifier calls [`Notifier::notify`] when it has made something available, and
//! the waiter awaits [`Waiter::wait`] until it has. A notification that arrives
//! while nobody is waiting is remembered rather than dropped, so the waiter
//! cannot miss one by being late, and [`Waiter::wait`] is cancel-safe - the flag
//! lives in the shared state, not in the returned future, so dropping that future
//! and awaiting a fresh one (as `select!` does to every losing branch) loses
//! nothing.
//!
//! # Halves
//!
//! The protocol admits exactly one notifier and one waiter. That is a soundness
//! requirement, not just a usage note: two registrations racing would both enter
//! the critical section and alias the waker. [`pair`] is the only constructor and
//! neither half is `Clone`, so the invariant holds by construction and the whole
//! public API is safe.
//!
//! The two halves share one heap [`Shared`] through a raw pointer, kept alive by
//! a two-bit reference count: whichever half drops last frees it. That is
//! deliberately hand-rolled rather than an `Arc` - it costs the same single RMW
//! on drop, and it rides in the same word as the rest of the protocol.
//!
//! # State bits
//!
//! The protocol flags are negative - set means "nothing to do" - so the notifier
//! can clear the ones it needs with a single `fetch_and` on its hot path.
//!
//! | bit | meaning when **set** | cleared by |
//! |-----|----------------------|------------|
//! | `NOTIFIER_REF` | the [`Notifier`] still owns a share of the allocation | `Notifier::drop`, last |
//! | `WAITER_REF` | the [`Waiter`] still owns a share of the allocation | `Waiter::drop` |
//! | `NOTIFIER_ACTIVE` | the [`Notifier`] may still notify | `Notifier::drop`, first |
//! | `NOT_WAITING` | the `waker` cell is uninitialised | `register` |
//! | `REGISTERING` | *inverted*: a `register` is inside its critical section | `register` |
//! | `NOT_WAKING` | no `notify` is in flight | `notify` |
//! | `NOT_PENDING` | no notification is waiting to be reported | `notify` |
//!
//! ## Invariants
//!
//! * The `waker` cell may only be touched by a caller that has just observed
//!   `NOT_WAKING` set on its own RMW (`register`), or `NOT_WAKING` clear on the
//!   RMW that published its registration (`wake_after_register`), or both
//!   `NOT_WAKING` set and `REGISTERING` clear (`notify`). Those three conditions
//!   are mutually exclusive, so the accesses never overlap.
//! * `NOT_WAITING` clear implies the cell holds an initialised `Waker`. It starts
//!   set, and every path that moves the `Waker` out sets it again.
//! * Leaving `NOT_WAKING` clear across a return is the handoff from `notify` to
//!   `register`: it means "a wake was owed but could not be delivered, the
//!   registering side must deliver it". Every other path restores it.
//! * `NOT_PENDING` is cleared by every `notify` and set only by `try_resolve`,
//!   so a notification that finds nobody waiting is still recorded.
//! * `NOTIFIER_ACTIVE` and `NOTIFIER_REF` are deliberately distinct bits, and
//!   the gap between them is the whole reason a dropping notifier can safely
//!   deliver a final wake. See "Shutdown" below.
//!
//! # Shutdown
//!
//! [`Waiter::wait`] resolves to `None` once the notifier is gone and nothing is
//! pending, so a waiter never parks forever. Delivering that reliably is subtler
//! than it looks, because two constraints pull in opposite directions:
//!
//! * The final wake must happen *after* `NOTIFIER_ACTIVE` is cleared. A waiter
//!   that registers in between would otherwise park having seen the notifier
//!   still live, and nothing would ever wake it again.
//! * Clearing the notifier's reference bit is what permits the waiter to
//!   deallocate, so touching the allocation afterwards is a use-after-free.
//!
//! One bit cannot satisfy both, and no reordering helps - waking first and
//! clearing after just restores the first hazard. So [`Notifier::drop`] runs
//! three steps against two bits:
//!
//! 1. clear `NOTIFIER_ACTIVE`, so any waiter from here on observes the shutdown;
//! 2. wake the parked waiter, still holding `NOTIFIER_REF`, which is what makes
//!    the allocation safe to touch - the waiter's own drop sees that bit set and
//!    declines to free;
//! 3. clear `NOTIFIER_REF`, freeing if the waiter has already gone. Last touch.
//!
//! Step 2 wakes without recording a notification, so the woken waiter re-polls,
//! finds nothing pending, sees the cleared flag and reports `None` rather than a
//! spurious `Some`.

use crate::sync::{AtomicU8, Ordering, UnsafeCell};
use std::fmt;
use std::mem::MaybeUninit;
use std::ops::Not as _;
use std::ptr::NonNull;
use std::task::Poll;
use std::task::Waker;

/// Ownership of the shared allocation; the last of the two to clear frees it.
const NOTIFIER_REF: u8 = 1 << 0;
const WAITER_REF: u8 = 1 << 1;
/// Liveness, distinct from `NOTIFIER_REF` so the dropping notifier can announce
/// its departure and still safely deliver one last wake. See the module docs.
const NOTIFIER_ACTIVE: u8 = 1 << 2;
const NOT_WAITING: u8 = 1 << 3;
const REGISTERING: u8 = 1 << 4;
const NOT_WAKING: u8 = 1 << 5;
const NOT_PENDING: u8 = 1 << 6;

/// The heap block both halves point at.
struct Shared {
    state: AtomicU8,
    waker: UnsafeCell<MaybeUninit<Waker>>,
}

impl fmt::Debug for Shared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.load(Ordering::Relaxed);
        f.debug_struct("Shared")
            .field("notifier_ref", &(state & NOTIFIER_REF != 0))
            .field("waiter_ref", &(state & WAITER_REF != 0))
            .field("notifier_active", &(state & NOTIFIER_ACTIVE != 0))
            .field("waiting", &(state & NOT_WAITING == 0))
            .field("registering", &(state & REGISTERING != 0))
            .field("waking", &(state & NOT_WAKING == 0))
            .field("pending", &(state & NOT_PENDING == 0))
            .finish()
    }
}

// `Shared: Send` needs no impl here: `AtomicU8` is unconditionally `Send` and
// `UnsafeCell<MaybeUninit<Waker>>` is `Send` because `Waker` is, so the auto impl
// already applies. Only `Sync` is missing, because `UnsafeCell` opts out of it.
//
// SAFETY: `Sync` is the claim this whole state machine exists to make - the two
// halves reach `Shared` from different threads, and every access to `waker` is
// gated by the `state` protocol documented above, which grants exclusive access
// to exactly one of them at a time. `Waker` is itself `Send + Sync`.
//
// This also carries the halves' `Send`: `Waiter::wait` holds a `&Shared` across
// its await point, and `&T: Send` requires `T: Sync`, so without this the future
// it returns is not `Send` and cannot be spawned.
unsafe impl Sync for Shared {}

impl Shared {
    /// Registers `waker`, waking it immediately instead if a wake is already in
    /// flight or is owed.
    ///
    /// # Safety
    ///
    /// No other `register` may run concurrently.
    unsafe fn register(&self, waker: &Waker) {
        // Acquire: we act on `old` below - its `NOT_WAITING` bit decides whether
        // the cell holds an initialised `Waker` - so this must happen-after the
        // Release RMW of whoever last wrote or consumed that cell.
        let old = self.state.fetch_or(REGISTERING, Ordering::Acquire);

        if old & NOT_WAKING == 0 {
            // A wake is in flight, or one is owed to us. Either way the cell is
            // not ours to touch, so hand the wake straight to the caller.
            // Relaxed: nothing was written, so there is nothing to release.
            self.state.fetch_and(REGISTERING.not(), Ordering::Relaxed);
            waker.wake_by_ref();
            return;
        }

        self.waker.with_mut(|slot| {
            // SAFETY: `REGISTERING` is now set and we observed `NOT_WAKING` set, so
            // no `notify` can be inside the cell; by the invariant above no other
            // `register` exists. This pointer is the only live one.
            let slot = unsafe { &mut *slot };

            if old & NOT_WAITING == 0 {
                // SAFETY: `NOT_WAITING` clear means the cell is initialised.
                let stored = unsafe { slot.assume_init_mut() };
                if !stored.will_wake(waker) {
                    stored.clone_from(waker);
                }
            } else {
                slot.write(waker.clone());
            }
        });

        // Release: publishes the cell write above to the next Acquire reader of
        // `state`. The loaded value is only inspected for `NOT_WAKING`, which
        // needs no Acquire of its own - the cell it guards is one we wrote.
        let old = self
            .state
            .fetch_and((NOT_WAITING | REGISTERING).not(), Ordering::Release);

        if old & NOT_WAKING == 0 {
            // SAFETY: we just initialised the cell, and observing `NOT_WAKING`
            // clear on the RMW that published the registration means a `notify`
            // handed us its turn, so the cell is ours to consume.
            unsafe { self.wake_after_register() }
        }
    }

    /// Delivers a wake that [`Self::notify`] could not deliver because it
    /// collided with our registration.
    ///
    /// # Safety
    ///
    /// The caller must have just registered a waker and observed `NOT_WAKING`
    /// clear on the RMW that published that registration.
    unsafe fn wake_after_register(&self) {
        // SAFETY: the caller guarantees the cell is initialised and that the
        // handoff granted us exclusive access to it.
        let waker = self.waker.with(|slot| unsafe { (*slot).assume_init_read() });

        // Relaxed: the cell we emptied is only rewritten by this same caller, in
        // program order. Sequenced before `wake()` all the same, because that
        // call runs executor code which may re-enter `register` on this thread
        // and has to see a finished transition rather than a half-done one.
        self.state
            .fetch_or(NOT_WAITING | NOT_WAKING, Ordering::Relaxed);

        waker.wake();
    }

    /// Records a notification and wakes the waiter, if one is parked.
    ///
    /// # Safety
    ///
    /// No other `notify` or `wake` may run concurrently.
    unsafe fn notify(&self) {
        // SAFETY: forwarded from this function's own contract.
        unsafe { self.wake_with(NOT_WAKING | NOT_PENDING) }
    }

    /// Wakes the parked waiter *without* recording a notification, so it re-polls
    /// and re-reads the state rather than being told something arrived.
    ///
    /// # Safety
    ///
    /// No other `notify` or `wake` may run concurrently.
    unsafe fn wake(&self) {
        // SAFETY: forwarded from this function's own contract.
        unsafe { self.wake_with(NOT_WAKING) }
    }

    /// Shared body of [`Self::notify`] and [`Self::wake`]; `clear` is the set of
    /// bits the claiming RMW takes down.
    ///
    /// # Safety
    ///
    /// No other `notify` or `wake` may run concurrently.
    unsafe fn wake_with(&self, clear: u8) {
        // Clearing `NOT_PENDING` rides along on the RMW that claims the wake, so
        // recording the notification costs nothing beyond the atomic already
        // being performed - and it happens before every early return below, so a
        // notification that finds nobody parked is still reported to the next
        // `wait`.
        //
        // AcqRel: Release publishes whatever the caller did before this call to
        // the waiter's Acquire `take_pending`, so a waiter that observes the
        // cleared flag also sees the work it announces. Acquire is needed because
        // we act on every bit of `old`, and the cell write we may read below must
        // happen-after the `register` Release RMW that published it.
        let old = self.state.fetch_and(clear.not(), Ordering::AcqRel);

        if old & NOT_WAKING == 0 {
            // A wake is already in flight or owed; it will deliver ours too. The
            // notification itself is already recorded above, so nothing is lost.
            return;
        }

        if old & REGISTERING != 0 {
            // A registration owns the cell. Leave `NOT_WAKING` clear: that is the
            // handoff, and `register` will deliver on our behalf.
            return;
        }

        if old & NOT_WAITING != 0 {
            // Nobody is parked, so there is no waker to consume. Give
            // `NOT_WAKING` back - failing to do so wedges every later `register`
            // and `notify`. `NOT_PENDING` stays clear: the notification is still
            // pending until a waiter takes it.
            // Relaxed: covered by the release sequence headed by the RMW above.
            self.state.fetch_or(NOT_WAKING, Ordering::Relaxed);
            return;
        }

        // SAFETY: `NOT_WAKING` was set and `REGISTERING` clear when we cleared
        // `NOT_WAKING`, so no `register` is in the cell and no other `notify` can
        // enter until we restore the bit. `NOT_WAITING` clear means the cell is
        // initialised.
        let waker = self.waker.with(|slot| unsafe { (*slot).assume_init_read() });

        // Release, and before `wake()`, for the same reason as in
        // `wake_after_register`.
        self.state
            .fetch_or(NOT_WAITING | NOT_WAKING, Ordering::Release);

        waker.wake();
    }

    /// Resolves a wait if it can be resolved without parking.
    ///
    /// One RMW does the whole job. `fetch_or` takes the pending flag *and*
    /// returns the state it replaced, so the notification and the shutdown are
    /// read from a single snapshot. Reading them separately would leave a window
    /// in between - a notification landing there loses to the shutdown and is
    /// silently dropped - so taking them together rules that out by construction
    /// rather than by re-checking.
    ///
    /// Acquire: pairs with the Release half of `wake_with`'s claiming `fetch_and`
    /// and with the Release `fetch_and` in `Notifier::drop`, so a waiter that
    /// observes either flag also sees the work behind it.
    fn try_resolve(&self) -> Poll<Option<()>> {
        let old = self.state.fetch_or(NOT_PENDING, Ordering::Acquire);

        if old & NOT_PENDING == 0 {
            // A notification was pending, and we have just taken it.
            return Poll::Ready(Some(()));
        }

        if old & NOTIFIER_ACTIVE == 0 {
            // Nothing pending, and at that same instant the notifier had already
            // announced its departure. `notify` needs `&mut Notifier` and
            // `Notifier::drop` clears the flag before its final act, so every
            // notification is sequenced before the clear: none can still be in
            // flight. This is settled rather than merely observed.
            return Poll::Ready(None);
        }

        Poll::Pending
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        // Relaxed, and a `load` rather than `get_mut`: `&mut self` already rules
        // out any concurrent access, so no ordering is needed.
        if self.state.load(Ordering::Relaxed) & NOT_WAITING == 0 {
            // SAFETY: `NOT_WAITING` clear means the cell holds an initialised
            // `Waker`, and `&mut self` means nothing else can reach it.
            self.waker
                .with_mut(|slot| unsafe { (*slot).assume_init_drop() });
        }
    }
}

/// Gives up this half's reference to the shared state, freeing it if the other
/// half has already let go.
///
/// # Safety
///
/// `shared` must have come from `Box::into_non_null` in [`pair`], and this must
/// be the calling half's last use of it.
unsafe fn release(shared: NonNull<Shared>, own_bit: u8, other_bit: u8) {
    // SAFETY: the caller still holds a live pointer; this is its last use.
    let state = unsafe { &shared.as_ref().state };

    // AcqRel, and the last touch of `state` from this half: Release publishes
    // this half's accesses to the other one, and Acquire pairs with the other's
    // Release when it cleared its bit, so if we free below, all of its accesses
    // happen-before the deallocation.
    let old = state.fetch_and(own_bit.not(), Ordering::AcqRel);
    debug_assert!(old & own_bit != 0, "half released twice");

    if old & other_bit == 0 {
        // SAFETY: the other half had already cleared its bit before our RMW, and
        // it never touches the allocation again after doing so, so this half is
        // now the sole owner.
        drop(unsafe { Box::from_raw(shared.as_ptr()) });
    }
}

/// The notifying half. Exactly one exists per [`pair`].
#[derive(Debug)]
pub struct Notifier {
    shared: NonNull<Shared>,
}

// SAFETY: `Shared` is only reached through the `state` protocol documented at the
// top of this module, which grants exclusive access to the waker cell to one side
// at a time, and `Waker` is `Send + Sync`. `NonNull` is the only reason the auto
// impls do not apply; the reference count keeps the target alive for as long as
// either half holds a pointer to it.
unsafe impl Send for Notifier {}
// SAFETY: every method takes `&mut self`, so a shared reference grants no access
// beyond the derived `Debug`, which reads the pointer without dereferencing it.
unsafe impl Sync for Notifier {}

impl Notifier {
    /// Records a notification and wakes the waiter, if one is parked.
    ///
    /// If nobody is waiting the notification is remembered, and the next
    /// [`Waiter::wait`] returns immediately. Consecutive notifications with
    /// nobody parked coalesce into one.
    pub fn notify(&mut self) {
        // SAFETY: the allocation outlives this half, and `&mut self` on the only
        // `Notifier` means no other `notify` can be running.
        unsafe { self.shared.as_ref().notify() }
    }
}

impl Drop for Notifier {
    fn drop(&mut self) {
        // SAFETY: this half still holds `NOTIFIER_REF`, so the allocation cannot
        // have been freed yet.
        let shared = unsafe { self.shared.as_ref() };

        // Step 1. Announce departure before the wake below, so a waiter that
        // registers too late to be woken by it still observes the cleared flag
        // and reports `None` instead of parking forever.
        //
        // Release: pairs with the Acquire load in `Shared::notifier_gone`, so a
        // waiter that sees this also sees whatever we published beforehand.
        shared
            .state
            .fetch_and(NOTIFIER_ACTIVE.not(), Ordering::Release);

        // Step 2. Wake whoever is already parked. Touching `shared` is sound here
        // only because `NOTIFIER_REF` is still set: the waiter's own drop sees
        // that bit and declines to free. `wake` rather than `notify` - there is
        // nothing to report, and recording a notification would hand the waiter a
        // spurious `Some` ahead of its `None`.
        //
        // SAFETY: `&mut self` on the only `Notifier` means no other `notify` or
        // `wake` can be running.
        unsafe { shared.wake() }

        // Step 3. Give up the reference. Last touch of the allocation.
        //
        // SAFETY: `shared` came from `Box::into_non_null` in `pair`, and this is
        // this half's last use of it.
        unsafe { release(self.shared, NOTIFIER_REF, WAITER_REF) }
    }
}

/// The waiting half. Exactly one exists per [`pair`].
#[derive(Debug)]
pub struct Waiter {
    shared: NonNull<Shared>,
}

// SAFETY: as for `Notifier`.
unsafe impl Send for Waiter {}
// SAFETY: as for `Notifier`.
unsafe impl Sync for Waiter {}

impl Waiter {
    /// Waits for a notification, returning immediately if one is already pending.
    ///
    /// Cancel-safe: the pending flag lives in the shared state rather than in
    /// this future, so a future that parked can be dropped and a fresh one
    /// awaited without losing anything. Note the guarantee's shape - a poll that
    /// returned `Ready` has already consumed the notification, so discarding that
    /// result does lose it. `select!` never does that: a branch that completes is
    /// the branch it picks.
    ///
    /// Parks forever if the notifier is already gone; see the module docs.
    pub async fn wait(&mut self) -> Option<()> {
        // SAFETY: the allocation outlives this half.
        let shared = unsafe { self.shared.as_ref() };

        std::future::poll_fn(|cx| {
            if let Poll::Ready(resolved) = shared.try_resolve() {
                return Poll::Ready(resolved);
            }

            // SAFETY: `&mut self` on the only `Waiter` means no other `register`
            // can be running.
            unsafe { shared.register(cx.waker()) }

            // Resolve again after registering. A notification - or a shutdown -
            // that landed between the attempt above and the registration could
            // not have seen our waker, so without this it would be missed and we
            // would park forever. The shutdown half of it is exactly what the
            // two-bit split in `Notifier::drop` exists to make reliable.
            shared.try_resolve()
        })
        .await
    }
}

impl Drop for Waiter {
    fn drop(&mut self) {
        // SAFETY: `shared` came from `Box::into_non_null` in `pair`, and this is
        // this half's last use of it.
        unsafe { release(self.shared, WAITER_REF, NOTIFIER_REF) }
    }
}

/// Creates a connected [`Notifier`] and [`Waiter`].
#[must_use]
pub fn pair() -> (Notifier, Waiter) {
    let shared = Box::into_non_null(Box::new(Shared {
        state: AtomicU8::new(
            NOTIFIER_REF
                | WAITER_REF
                | NOTIFIER_ACTIVE
                | NOT_WAITING
                | NOT_WAKING
                | NOT_PENDING,
        ),
        waker: UnsafeCell::new(MaybeUninit::uninit()),
    }));

    (Notifier { shared }, Waiter { shared })
}
