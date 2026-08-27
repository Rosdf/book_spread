//! Loom model checks for the `shared_buffer` slot protocol.
//!
//! ```text
//! RUSTFLAGS="--cfg loom" cargo test -p core_lib --release --test loom_shared_buffer
//! ```
//!
//! Both sides of every model run in spawned threads. That matters: when the
//! main thread performs one side's work directly, loom explores a single
//! execution and the assertions below pass without proving anything. The
//! `explores_many_interleavings` test guards against regressing into that shape.
#![cfg(loom)]

use core_lib::shared_buffer::Publisher;
use loom::thread;
use std::sync::atomic::{AtomicUsize, Ordering as StdOrd};

/// A writer and a reader racing: whatever the reader observes must be a value
/// the writer actually stored, never a torn or partially written slot.
#[test]
fn reader_never_observes_a_torn_slot() {
    loom::model(|| {
        let mut publisher = Publisher::new(1);
        let mut reader = publisher.reader().expect("one reader fits");

        let writer = thread::spawn(move || {
            publisher.update(1u64);
            publisher.update(2u64);
        });

        let observer = thread::spawn(move || {
            let seen = *reader.get();
            assert!(
                seen == 0 || seen == 1 || seen == 2,
                "observed a value never published: {seen}"
            );
            reader
        });

        writer.join().unwrap();
        let mut reader = observer.join().unwrap();

        // Both writes have retired, so the newest must now be visible.
        assert_eq!(*reader.get(), 2, "newest value not visible after the writer finished");
    });
}

/// A live `ReaderGuard` pins its slot. The writer has to route around it rather
/// than overwrite data being read.
///
/// Scope: this covers the writer routing around a pinned slot on the paths that
/// do not collide. It deliberately does NOT cover the wrap-around case - the
/// writer skips the slot named by `header`, so with `max_readers + 2 == 3` slots
/// it takes three publishes to land back on a slot a guard still holds, and at
/// three publishes `Reader::get`'s `WRITE_FLAG` spin loop exhausts loom's branch
/// budget (100k branches at a preemption bound of 2 was not enough). Loom cannot
/// model spin loops that depend on another thread making progress. That
/// wrap-around property is left to Miri instead.
#[test]
fn a_live_guard_pins_its_slot() {
    loom::model(|| {
        let mut publisher = Publisher::new(1);
        let mut reader = publisher.reader().expect("one reader fits");

        let writer = thread::spawn(move || {
            publisher.update(1u64);
            publisher.update(2u64);
        });

        let observer = thread::spawn(move || {
            let guard = reader.get();
            let first = *guard;
            // Re-reading through the same guard must yield the same value: the
            // writer must not have claimed this slot while the guard is alive.
            assert_eq!(*guard, first, "writer mutated a slot under a live guard");
        });

        writer.join().unwrap();
        observer.join().unwrap();
    });
}

/// Two sequential readers on one handle, racing a writer, must each see a
/// coherent value, and the second must never go backwards past what the writer
/// had already retired before it started.
#[test]
fn repeated_reads_stay_coherent() {
    loom::model(|| {
        let mut publisher = Publisher::new(1);
        let mut reader = publisher.reader().expect("one reader fits");

        let writer = thread::spawn(move || {
            publisher.update(1u64);
        });

        let observer = thread::spawn(move || {
            let first = *reader.get();
            let second = *reader.get();
            assert!(first == 0 || first == 1, "first read observed {first}");
            assert!(second == 0 || second == 1, "second read observed {second}");
            // The buffer is single-slot-newest: once 1 is seen it cannot revert.
            assert!(!(first == 1 && second == 0), "read went backwards: 1 then 0");
        });

        writer.join().unwrap();
        observer.join().unwrap();
    });
}

/// Guards the shape of the models above. A loom test that explores exactly one
/// execution proves nothing, and the failure mode is silent - it just passes.
#[test]
fn explores_many_interleavings() {
    static ITERATIONS: AtomicUsize = AtomicUsize::new(0);

    loom::model(|| {
        ITERATIONS.fetch_add(1, StdOrd::SeqCst);
        let mut publisher = Publisher::new(1);
        let mut reader = publisher.reader().expect("one reader fits");

        let writer = thread::spawn(move || {
            publisher.update(1u64);
        });
        let observer = thread::spawn(move || {
            let _ = *reader.get();
        });
        writer.join().unwrap();
        observer.join().unwrap();
    });

    let explored = ITERATIONS.load(StdOrd::SeqCst);
    assert!(
        explored > 1,
        "loom explored only {explored} execution(s); the models are not \
         branching and their assertions prove nothing"
    );
}
