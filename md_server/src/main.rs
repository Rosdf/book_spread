//! Runs the market-data server.
//!
//! Everything is in the library; this binary only initialises tracing and hands over.

#![allow(
    unused_crate_dependencies,
    reason = "lib/bin split: main.rs is its own crate, and the library target is what uses the rest of the manifest"
)]

use std::net::SocketAddr;
use tracing_subscriber::EnvFilter;

/// jemalloc, process-wide, for every allocation this server makes - `core_lib` and both venue
/// crates included, since a global allocator is chosen by the binary, not per crate.
///
/// The decode path is deliberately allocation-free per frame with one exception it cannot
/// remove: `simd_json::Deserializer::from_slice_with_buffers` allocates a fresh parse tape on
/// every call, because `fill_tape` - the entry point that would reuse one - is private. So one
/// malloc/free pair per WebSocket frame is unavoidable, and the cheapest thing left to do is
/// make it cheap. Everything else allocating on a warm path gets the same benefit.
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Where the server listens unless `MD_SERVER_ADDR` says otherwise.
const DEFAULT_ADDR: &str = "127.0.0.1:50051";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let addr: SocketAddr = std::env::var("MD_SERVER_ADDR")
        .unwrap_or_else(|_| DEFAULT_ADDR.to_owned())
        .parse()?;
    md_server::server::run(addr).await
}
