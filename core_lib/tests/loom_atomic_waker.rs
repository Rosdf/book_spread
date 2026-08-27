//! Loom model checks for the `atomic_waker` primitive.
//!
//! ```text
//! RUSTFLAGS="--cfg loom" cargo test -p core_lib --release --test loom_atomic_waker
//! ```
//!
//! A lost notification surfaces here as loom reporting a deadlock, because the
//! parked thread never becomes runnable again.
//!
//! Both sides run in spawned threads. Doing one side's work on the main thread
//! makes loom explore a single execution, which passes without proving anything;
//! `explores_many_interleavings` guards against drifting back into that shape.
#![cfg(loom)]

use core_lib::atomic_waker::pair;
use loom::thread;
use std::sync::atomic::{AtomicUsize, Ordering as StdOrd};

/// A notification racing a park must never be lost.
#[test]
fn a_notification_racing_a_park_is_never_lost() {
    loom::model(|| {
        let (mut notifier, mut waiter) = pair();

        let producer = thread::spawn(move || {
            notifier.notify();
        });

        let consumer = thread::spawn(move || {
            // Hangs, and loom reports a deadlock, if the wake-up is lost.
            assert_eq!(loom::future::block_on(waiter.wait()), Some(()));
        });

        producer.join().unwrap();
        consumer.join().unwrap();
    });
}

/// A notification delivered before anyone waits is remembered, not dropped.
#[test]
fn a_notification_with_nobody_waiting_is_remembered() {
    loom::model(|| {
        let (mut notifier, mut waiter) = pair();

        notifier.notify();

        let consumer = thread::spawn(move || {
            // The notification was issued before the notifier departed, so it is
            // still owed to us: drain before close.
            assert_eq!(loom::future::block_on(waiter.wait()), Some(()));
        });
        let producer = thread::spawn(move || {
            drop(notifier);
        });

        consumer.join().unwrap();
        producer.join().unwrap();
    });
}

/// After a notification that found nobody parked, a later wait must still park
/// properly rather than spin.
///
/// This is the shape that catches a `notify` which fails to restore `NOT_WAKING`
/// on its no-waiter path: the flag stays clear, every later `register` takes the
/// wake-immediately branch, and the waiter busy-loops instead of parking. It
/// needs two rounds, because the first wait only consumes the pending flag and
/// never registers.
///
/// Round one is driven to completion on this thread before round two is issued.
/// Overlapping them would be a broken model rather than a stricter one: the
/// pending flag is sticky, so two notifications that both find nobody parked
/// coalesce into a single wake-up by design.
#[test]
fn a_second_wait_after_an_early_notification_still_parks() {
    loom::model(|| {
        let (mut notifier, mut waiter) = pair();

        // Round one, sequential: a notification with nobody parked, then the
        // wait that consumes it. Returns without ever registering a waker.
        notifier.notify();
        assert_eq!(loom::future::block_on(waiter.wait()), Some(()));

        // Round two, concurrent: this wait has no pending flag to take, so it
        // must genuinely park - and be woken.
        let consumer = thread::spawn(move || {
            assert_eq!(loom::future::block_on(waiter.wait()), Some(()));
        });
        let producer = thread::spawn(move || {
            notifier.notify();
        });

        consumer.join().unwrap();
        producer.join().unwrap();
    });
}

/// Two notifications racing one waiter. The second may collide with the waiter
/// mid-registration, which is the case the `REGISTERING` / `NOT_WAKING` handoff
/// exists for.
#[test]
fn registration_racing_a_notification_still_delivers() {
    loom::model(|| {
        let (mut notifier, mut waiter) = pair();

        let producer = thread::spawn(move || {
            notifier.notify();
            notifier.notify();
        });

        let consumer = thread::spawn(move || {
            assert_eq!(loom::future::block_on(waiter.wait()), Some(()));
        });

        producer.join().unwrap();
        consumer.join().unwrap();
    });
}

/// Dropping a wait future that *parked*, then awaiting a fresh one, must not
/// lose a notification - this is what `select!` does to every losing branch.
///
/// Note the guarantee's exact shape: it covers a future that returned `Pending`.
/// A future whose poll returned `Ready` has already consumed the notification,
/// and throwing that result away loses it. `select!` never does that - a branch
/// that completes is the branch it picks.
#[test]
fn wait_is_cancel_safe() {
    loom::model(|| {
        let (mut notifier, mut waiter) = pair();

        let producer = thread::spawn(move || {
            notifier.notify();
        });

        let consumer = thread::spawn(move || {
            let parked = {
                let mut abandoned = Box::pin(waiter.wait());
                let waker = noop_waker();
                let mut cx = std::task::Context::from_waker(&waker);
                std::future::Future::poll(abandoned.as_mut(), &mut cx).is_pending()
                // ... and dropped here, mid-wait, if it parked.
            };

            if parked {
                // The abandoned future consumed nothing, so a fresh one must
                // still be delivered the notification.
                assert_eq!(loom::future::block_on(waiter.wait()), Some(()));
            }
        });

        producer.join().unwrap();
        consumer.join().unwrap();
    });
}

/// Either drop order must free the shared state exactly once; loom fails the
/// model on a leak or a double free.
#[test]
fn halves_free_the_shared_state_exactly_once() {
    loom::model(|| {
        let (mut notifier, waiter) = pair();

        let producer = thread::spawn(move || {
            notifier.notify();
            drop(notifier);
        });
        let consumer = thread::spawn(move || {
            drop(waiter);
        });

        producer.join().unwrap();
        consumer.join().unwrap();
    });
}

/// A waiter parked when the notifier drops must be released with `None`, not
/// left blocked. This is the case the two-bit split in `Notifier::drop` exists
/// for: the liveness flag is cleared first, the final wake is delivered while the
/// reference is still held, and only then is the reference given up.
#[test]
fn a_dropping_notifier_releases_a_parked_waiter() {
    loom::model(|| {
        let (notifier, mut waiter) = pair();

        let producer = thread::spawn(move || {
            drop(notifier);
        });

        let consumer = thread::spawn(move || {
            // Deadlocks under loom if the shutdown wake is lost.
            assert_eq!(loom::future::block_on(waiter.wait()), None);
        });

        producer.join().unwrap();
        consumer.join().unwrap();
    });
}

/// A notification issued just before the notifier departs is still owed, no
/// matter how the two interleave with the waiter.
///
/// This is a regression test for a real race: `try_resolve` checks the pending
/// flag and then the liveness flag, and a notification landing between those two
/// reads used to lose to the shutdown and be reported as `None`. Only the
/// re-check inside the shutdown branch makes `Some` guaranteed here.
#[test]
fn a_notification_issued_just_before_shutdown_is_still_delivered() {
    loom::model(|| {
        let (mut notifier, mut waiter) = pair();

        let producer = thread::spawn(move || {
            notifier.notify();
            drop(notifier);
        });

        let consumer = thread::spawn(move || {
            // Never `None`: the notification predates the shutdown, so it is
            // owed whatever the interleaving.
            assert_eq!(loom::future::block_on(waiter.wait()), Some(()));
        });

        producer.join().unwrap();
        consumer.join().unwrap();
    });
}

/// Once `None` is reported it stays reported.
#[test]
fn none_is_sticky() {
    loom::model(|| {
        let (notifier, mut waiter) = pair();
        drop(notifier);

        assert_eq!(loom::future::block_on(waiter.wait()), None);
        assert_eq!(loom::future::block_on(waiter.wait()), None);
    });
}

fn noop_waker() -> std::task::Waker {
    struct Noop;
    impl std::task::Wake for Noop {
        fn wake(self: std::sync::Arc<Self>) {}
        fn wake_by_ref(self: &std::sync::Arc<Self>) {}
    }
    std::task::Waker::from(std::sync::Arc::new(Noop))
}

/// Guards the shape of the models above; see the module comment.
#[test]
fn explores_many_interleavings() {
    static ITERATIONS: AtomicUsize = AtomicUsize::new(0);

    loom::model(|| {
        ITERATIONS.fetch_add(1, StdOrd::SeqCst);
        let (mut notifier, mut waiter) = pair();

        let producer = thread::spawn(move || notifier.notify());
        let consumer = thread::spawn(move || loom::future::block_on(waiter.wait()));
        producer.join().unwrap();
        consumer.join().unwrap();
    });

    let explored = ITERATIONS.load(StdOrd::SeqCst);
    assert!(
        explored > 1,
        "loom explored only {explored} execution(s); the models are not \
         branching and their assertions prove nothing"
    );
}
