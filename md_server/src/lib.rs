//! A book-feed front end for the venue connectors.
//!
//! One broadcaster per catalogue instrument owns one
//! [`BookReader`](core_lib::connector::book_publisher::BookReader) per venue quoting it,
//! merges their books into one, encodes that exactly once, and hands the resulting bytes to
//! every attached client. A client joining an instrument that is already streaming is added
//! to the running broadcaster; it never causes an encode of its own.
//!
//! The broadcaster owning the clients is the point. The transport is gRPC, and a gRPC server
//! normally gives each connection a task to drive it; this one does not. A broadcaster owns
//! each client's whole HTTP/2 connection and drives it from its own `select!`, so a book
//! crosses no channel and no task boundary between the encoder and the wire, and the same
//! `Bytes` is handed to every client with no per-client copy. The price is that the writes for
//! one instrument are serialised on one task; the syscall count is unchanged, only their
//! parallelism. If an instrument ever outgrows that, shard it across a second broadcaster
//! rather than reintroducing a hop.
//!
//! [`crate::client`] is what keeps that arrangement honest: everything above it works against
//! three traits, and [`crate::grpc`] is the only module that knows the wire is HTTP/2.
//!
//! One reader per symbol is not a choice. `BookReader` is one half of a loom-verified
//! `shared_buffer` + `atomic_waker` pair built for exactly one consumer, so it is neither
//! `Clone` nor shareable. What keeps a symbol from being read twice once broadcasters are
//! filed by instrument rather than by symbol is [`crate::catalogue`], which refuses to load a
//! file naming one `(venue, symbol)` under two instruments.
//!
//! Symbols are subscribed on demand: the first client for an instrument subscribes every one
//! of its pairs on that pair's connector, and the last client to leave releases them all.
//!
//! What a client may ask for at all is [`crate::catalogue`]: a file read once at startup,
//! which is where the instrument and venue indices a request travels as come from.

// Used by the `md_server` binary, which compiles as a crate of its own. Naming them here is
// what keeps `unused_crate_dependencies` quiet for this, the library target.
use tikv_jemallocator as _;
use tracing_subscriber as _;
// The self dev-dependency exists for `tests/end_to_end.rs`, which needs `test-util` built from
// outside the crate; `md_client`, `tonic` and `tonic_prost` are that test's real gRPC client.
// None of them is reachable from the lib target's own unit tests, which mock at
// [`crate::client`] instead - naming them here is what keeps `unused_crate_dependencies`
// quiet for this, the library target.
#[cfg(test)]
use {md_client as _, md_server as _, tonic as _, tonic_prost as _};

pub(crate) mod broadcast;
pub(crate) mod catalogue;
pub(crate) mod client;
pub mod config;
pub(crate) mod encode;
pub(crate) mod framed;
pub(crate) mod grpc;
pub(crate) mod registry;
pub mod server;
#[cfg(any(test, feature = "test-util"))]
pub mod test_util;
pub(crate) mod transport;
pub(crate) mod venue;
