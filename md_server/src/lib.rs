//! A book-feed front end for the venue connectors.
//!
//! One broadcaster per `(venue, symbol)` owns that symbol's single
//! [`BookReader`](core_lib::connector::book_publisher::BookReader), encodes each book it
//! publishes exactly once, and hands the resulting bytes to every attached client.
//! A client joining a symbol that is already streaming is added to the running broadcaster;
//! it never causes an encode of its own.
//!
//! The broadcaster owning the clients is the point. The transport is gRPC, and a gRPC server
//! normally gives each connection a task to drive it; this one does not. A broadcaster owns
//! each client's whole HTTP/2 connection and drives it from its own `select!`, so a book
//! crosses no channel and no task boundary between the encoder and the wire, and the same
//! `Bytes` is handed to every client with no per-client copy. The price is that the writes for
//! one symbol are serialised on one task; the syscall count is unchanged, only their
//! parallelism. If a symbol ever outgrows that, shard it across a second broadcaster rather
//! than reintroducing a hop.
//!
//! [`crate::client`] is what keeps that arrangement honest: everything above it works against
//! three traits, and [`crate::grpc`] is the only module that knows the wire is HTTP/2.
//!
//! The single reader is not a choice. `BookReader` is one half of a loom-verified
//! `shared_buffer` + `atomic_waker` pair built for exactly one consumer, so it is neither
//! `Clone` nor shareable, and there is exactly one per symbol. Everything here is a layer on
//! top of that one reader.
//!
//! Symbols are subscribed on demand: the first client for a symbol subscribes it on its
//! venue's connector, and the last client to leave unsubscribes it again.

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
pub(crate) mod client;
pub(crate) mod encode;
pub(crate) mod framed;
pub(crate) mod grpc;
pub(crate) mod registry;
pub(crate) mod request;
pub mod server;
#[cfg(any(test, feature = "test-util"))]
pub mod test_util;
pub(crate) mod transport;
pub(crate) mod venue;
