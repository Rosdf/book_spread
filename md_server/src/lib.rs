//! A book-feed front end for the venue connectors.
//!
//! One broadcaster per `(venue, symbol)` owns that symbol's single
//! [`BookReader`](core_lib::connector::book_publisher::BookReader), encodes each book it
//! publishes exactly once, and writes the resulting bytes into every attached client socket.
//! A client joining a symbol that is already streaming is added to the running broadcaster;
//! it never causes an encode of its own.
//!
//! The broadcaster owning the sockets is the point. A book crosses no channel and no task
//! boundary between the encoder and the kernel, and the same `Bytes` is handed to every
//! client with no per-client copy. The price is that the writes for one symbol are serialised
//! on one task; the syscall count is unchanged, only their parallelism. If a symbol ever
//! outgrows that, shard it across a second broadcaster rather than reintroducing a hop.
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
// outside the crate. The lib target's own unit tests never touch it.
#[cfg(test)]
use md_server as _;

pub(crate) mod broadcast;
pub(crate) mod encode;
pub(crate) mod framed;
pub(crate) mod registry;
pub(crate) mod request;
pub mod server;
pub(crate) mod session;
#[cfg(any(test, feature = "test-util"))]
pub mod test_util;
pub mod transport;
pub mod venue;
