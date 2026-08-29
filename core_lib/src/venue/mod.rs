//! The generic venue connector: everything a venue needs beyond its own wire format and
//! sequencing rules.
//!
//! This used to hold only the genuinely venue-agnostic pieces - retry pacing, closing a socket
//! by the book, a scratch buffer for in-place JSON parsing, the buffer a symbol fills while it
//! bootstraps, and the connector-wide symbol -> connection routing table - while the
//! `Connection`/`*Handler` borrow-split, the slot state machine, and the supervisor glue each
//! lived in their own venue crate, on the judgment that a shared generic would cost more than
//! the duplication did. Comparing `binance_spot` and `bitstamp` once both existed showed that
//! judgment was wrong: `connection.rs` alone was 55% identical text, `table.rs` and `rest.rs`
//! nearly all of it, and the parts that actually varied - a config field name, a URL, a cursor
//! field name, one extra slot state - all fit behind [`spec::VenueSpec`] without forcing either
//! venue's observable wire behavior to change.
//!
//! So this module now owns the connection loop, the slot table, the supervisor, the REST
//! snapshot fetch and the hourly symbol listing, all generic over [`spec::VenueSpec`]. A venue
//! crate keeps its own `decode.rs`, its wire naming, its config extras, and a `impl VenueSpec`
//! block wiring the two together - see `spec.rs`'s module doc for why that trait carries no
//! transport generics of its own.
//!
//! What each venue buffers while a symbol bootstraps is now its own too: those frames used to
//! be kept as raw JSON and re-parsed after the snapshot landed, which [`pending`]'s module doc
//! explains was never sound. [`levels`] went the other way, collecting the parts of level
//! decoding both venues had written identically.

pub mod backoff;
pub mod config;
pub mod connection;
pub mod levels;
pub mod pending;
pub mod rest;
pub mod router;
pub mod scratch;
pub mod session;
pub mod spec;
pub mod supervisor;
pub mod table;
#[cfg(any(test, feature = "test_util"))]
pub mod test_util;
pub mod universe;

pub use backoff::{Backoff, jitter};
pub use config::{ConnectorConfig, CoreConfig, Defaults};
pub use connection::LaneCommand;
pub use levels::{
    BookSink, Decimal, LevelSink, LevelsSeed, MalformedDecimal, Side, apply_level, merge,
    worth_publishing,
};
pub use pending::PendingDiffs;
pub use router::{Lane, LaneId, Router};
pub use scratch::Scratch;
pub use session::{CLOSE_TIMEOUT, SessionEnd, SessionError, close, ws_err};
pub use spec::{
    BootstrapRetry, ControlPacer, Decoder, FrameAction, FrameCtx, Generations, Method, Retry,
    SnapshotFetchError, SnapshotResult, VenueSpec,
};
pub use table::{Bootstrap, Slot, SlotState, SlotTable};
pub use universe::ListingError;
