#![feature(btree_cursors)]

pub mod atomic_waker;
pub mod heapless_linear_map;
pub mod incremental_book;
pub mod positive_f64;
pub mod shared_buffer;
pub mod small_book;
pub(crate) mod sync;

// Everything below reaches the network, and `tokio::net` does not exist under `--cfg loom` -
// tokio compiles its I/O driver out of a loom build. The loom targets only model
// `shared_buffer` and `atomic_waker`, so gating these off is what lets that build link at all.
#[cfg(not(loom))]
pub mod connector;
#[cfg(not(loom))]
pub mod net;
#[cfg(not(loom))]
pub mod venue;
