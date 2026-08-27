//! The diffs a symbol piles up while it waits for its REST snapshot.
//!
//! These used to be raw `Bytes` off the socket, re-parsed once the snapshot landed. They are
//! now parsed once, on arrival, into an arena the venue owns and this module only names.
//!
//! Keeping the raw bytes was a real hazard, not just extra work: simd-json unescapes strings
//! *into its input buffer*, so the buffer stashed for replay was the one the first parse had
//! already rewritten - re-parsing an escaped payload fails outright. That was safe only by
//! accident, because depth payloads happen to contain no backslashes.
//!
//! What a venue stages is entirely its own business - Binance needs `U`/`u` and two level
//! ranges per diff, Bitstamp a microtimestamp and the same - so this is a trait with the two
//! questions the generic connection machinery actually asks: how many diffs are buffered (for
//! `max_pending_frames`), and empty it.

use std::fmt::Debug;

/// One bootstrapping slot's parsed diffs, waiting for the snapshot they replay onto.
pub trait PendingDiffs: Default + Debug + Send {
    /// How many diffs are buffered - not how many levels, and named rather than `len` for
    /// that reason. The connection compares this against `CoreConfig::max_pending_frames` to
    /// decide a snapshot is never coming.
    fn buffered(&self) -> usize;

    /// Throws the buffered diffs away while keeping whatever the arena has already allocated,
    /// so a resync does not re-grow what the last attempt already paid for.
    fn clear(&mut self);
}
