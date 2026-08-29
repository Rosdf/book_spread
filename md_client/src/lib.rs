//! A gRPC client for the `md.v1.MarketData` book feed.
//!
//! The generated tonic stubs, and the follow loop the binary and `md_server`'s end-to-end test
//! both drive the server with. tonic lives here and nowhere else in the workspace: a client
//! opens one stream and fans nothing out, so a generated codec costs it nothing - and using
//! the real thing is what proves the hand-written server is gRPC rather than something
//! gRPC-shaped.

// `tonic_prost` is named by the generated code below rather than by anything written here;
// `anyhow` and `tokio` belong to the binary target, which compiles as a crate of its own.
// Naming all three is what keeps `unused_crate_dependencies` quiet for the library target.
use anyhow as _;
use tokio as _;
use tonic_prost as _;

use md_proto::md::v1 as proto;
use md_wire::grpc::{REJECT_CODE_HEADER, RejectCode};
use std::time::{SystemTime, UNIX_EPOCH};

pub mod pb {
    //! The generated `MarketDataClient`.
    #![allow(
        clippy::pedantic,
        clippy::nursery,
        clippy::restriction,
        missing_debug_implementations,
        missing_docs,
        reason = "tonic-prost-build generated code, not ours to lint"
    )]

    include!(concat!(env!("OUT_DIR"), "/md.v1.rs"));
}

pub use pb::market_data_client::MarketDataClient;

/// The address both this client and the server read, and what they fall back to.
pub const ADDR_VAR: &str = "MD_SERVER_ADDR";
/// Where the server listens unless told otherwise.
pub const DEFAULT_ADDR: &str = "127.0.0.1:50051";

/// The exact reason a subscription was refused, when the server said one.
///
/// `grpc-status` alone cannot carry it: `NOT_FOUND` for an unlisted symbol is permanent while
/// `UNAVAILABLE` for a connector that went away is not, and only [`RejectCode::retryable`]
/// tells those apart. A server that did not send the metadata is not an error - it simply
/// means this client has to make do with the status.
#[must_use]
pub fn reject_code(status: &tonic::Status) -> Option<RejectCode> {
    status
        .metadata()
        .get(REJECT_CODE_HEADER)?
        .to_str()
        .ok()?
        .parse::<u8>()
        .ok()
        .and_then(RejectCode::from_byte)
}

/// Streams the first pair until the server ends the stream.
///
/// # Errors
///
/// A connection that could not be made, or a stream the server ended with a non-`OK` status.
pub async fn follow(
    addr: &str,
    request: proto::SubscribeBookRequest,
) -> Result<(), tonic::Status> {
    let mut client = MarketDataClient::connect(format!("http://{addr}"))
        .await
        .map_err(|err| tonic::Status::unavailable(err.to_string()))?;

    let requested = request.pairs.first().cloned().unwrap_or_default();
    let mut books = client.subscribe_book(request).await?.into_inner();

    // The one place a client pays for parsing: the message arrived as bytes and would have
    // stayed that way if all it did was forward it.
    while let Some(book) = books.message().await? {
        println!("{}", line(&book, &requested));
    }
    Ok(())
}

/// One line of output, stamped with the local microsecond it was received - so a run of these
/// is something latency can be measured out of.
#[must_use]
pub fn line(book: &proto::BookUpdate, requested: &proto::Pair) -> String {
    let at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros();

    let Some((bid, ask)) = book.bids.first().zip(book.asks.first()) else {
        // Both sides empty is the connector saying it has no book - bootstrapping, or
        // resyncing. Whatever was on screen a moment ago is not the market any more. No
        // level is available to name a venue, so the requested pair stands in for it.
        return format!(
            "{at} {:<13} {:<10} no book",
            requested.venue, requested.symbol
        );
    };
    format!(
        "{at} spread {:.8}\n  bid {:<13} {:>14.8} x {:<12.8}\n  ask {:<13} {:>14.8} x {:<12.8}",
        book.spread, bid.venue, bid.price, bid.size, ask.venue, ask.price, ask.size
    )
}
