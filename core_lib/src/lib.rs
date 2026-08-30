#![feature(btree_cursors)]

pub use all_venues::Venue;

pub mod atomic_waker;
pub mod heapless_linear_map;
pub mod incremental_book;
// Reaches `connector`, which does not exist under `--cfg loom` for the same reason `net` and
// `venue` are gated below.
#[cfg(not(loom))]
pub mod instrument;
pub mod panic;
pub mod positive_f64;
pub mod shared_buffer;
pub mod shared_string;
pub mod small_book;
pub(crate) mod sync;

// Everything below reaches the network, and `tokio::net` does not exist under `--cfg loom` -
// tokio compiles its I/O driver out of a loom build. The loom targets only model
// `shared_buffer` and `atomic_waker`, so gating these off is what lets that build link at all.
#[cfg(not(loom))]
pub mod connector;
pub mod map;
#[cfg(not(loom))]
pub mod net;
#[cfg(not(loom))]
pub mod venue;
