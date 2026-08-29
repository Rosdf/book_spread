//! Follows top-of-book for one or more `(venue, symbol)` pairs and prints what arrives.
//!
//! ```text
//! cargo run -p md_client -- binance_spot BTCUSDT bitstamp btcusd
//! ```
//!
//! One stream carries every pair the command line names; today only the first is served,
//! merging the rest into one book is the next stage - see [`md_wire::grpc`]. The server
//! address comes from `MD_SERVER_ADDR`, the same variable the server itself reads.

// The binary is a thin shell over the library, so these reach it only through `md_client`.
// Naming them here is what keeps `unused_crate_dependencies` quiet for this target.
use md_wire as _;
use tonic as _;
use tonic_prost as _;

use md_client::{ADDR_VAR, DEFAULT_ADDR, follow, reject_code};
use md_proto::md::v1 as proto;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() || !args.len().is_multiple_of(2) {
        anyhow::bail!("usage: md_client <venue> <symbol> [<venue> <symbol> ...]");
    }

    let addr = std::env::var(ADDR_VAR).unwrap_or_else(|_| DEFAULT_ADDR.to_owned());

    let pairs: Vec<proto::Pair> = args
        .as_chunks::<2>()
        .0
        .iter()
        .map(|[venue, symbol]| proto::Pair {
            venue: venue.clone(),
            symbol: symbol.clone(),
        })
        .collect();

    if let Err(status) = follow(&addr, proto::SubscribeBookRequest { pairs }).await {
        // The canonical status says what kind of problem it was; the metadata says which one
        // exactly, and therefore whether trying the same request again could ever work.
        match reject_code(&status) {
            Some(code) if code.retryable() => {
                eprintln!("stream ended: {} ({code:?}, worth retrying)", status.message());
            }
            Some(code) => eprintln!("stream ended: {} ({code:?})", status.message()),
            None => eprintln!("stream ended: {status}"),
        }
    }
    Ok(())
}
