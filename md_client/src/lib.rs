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
use std::collections::HashMap;
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

/// Where this client looks for the server. The server itself is told where to listen by its
/// own config file (`MD_SERVER_CONFIG`), so this is the client's variable alone.
pub const ADDR_VAR: &str = "MD_SERVER_ADDR";
/// Where this client looks unless told otherwise.
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

/// What this server carries: the venue index table, and every instrument it will serve.
///
/// The call a client makes first. Nothing on this wire spells a venue or a symbol out except
/// this response - a subscribe names an instrument index and a level carries a venue index -
/// so a client that wants to talk about `binance_spot/BTCUSDT` resolves it here.
///
/// # Errors
///
/// A connection that could not be made, or a server that refused the call.
pub async fn catalogue(addr: &str) -> Result<proto::CatalogueResponse, tonic::Status> {
    let mut client = MarketDataClient::connect(format!("http://{addr}"))
        .await
        .map_err(|err| tonic::Status::unavailable(err.to_string()))?;
    Ok(client
        .get_catalogue(proto::CatalogueRequest {})
        .await?
        .into_inner())
}

/// Venue names by the index every level carries, as one [`catalogue`] call gave them.
///
/// A level says *which* venue quoted it, not what that venue is called, so printing one takes
/// this alongside it.
#[derive(Debug, Default)]
pub struct VenueNames(HashMap<u32, String>);

impl VenueNames {
    #[must_use]
    pub fn from_catalogue(catalogue: &proto::CatalogueResponse) -> Self {
        Self(
            catalogue
                .venues
                .iter()
                .map(|venue| (venue.idx, venue.name.clone()))
                .collect(),
        )
    }

    /// The venue at `idx`, or a placeholder naming the index when this catalogue has no entry
    /// for it - a server that streams an index it did not advertise is a server bug, not
    /// something worth failing a print over.
    #[must_use]
    pub fn name(&self, idx: u32) -> String {
        self.0
            .get(&idx)
            .cloned()
            .unwrap_or_else(|| format!("venue#{idx}"))
    }
}

/// Streams `instrument`'s book until the server ends the stream.
///
/// `instrument` is an index from [`catalogue`]; `label` is only what an empty book is printed
/// against, since no level is available to name a venue in that case.
///
/// # Errors
///
/// A connection that could not be made, or a stream the server ended with a non-`OK` status.
pub async fn follow(
    addr: &str,
    instrument: u32,
    venues: &VenueNames,
    label: &str,
) -> Result<(), tonic::Status> {
    let mut client = MarketDataClient::connect(format!("http://{addr}"))
        .await
        .map_err(|err| tonic::Status::unavailable(err.to_string()))?;

    let mut books = client
        .subscribe_book(proto::SubscribeBookRequest {
            instrument_idx: instrument,
        })
        .await?
        .into_inner();

    // The one place a client pays for parsing: the message arrived as bytes and would have
    // stayed that way if all it did was forward it.
    while let Some(book) = books.message().await? {
        println!("{}", line(&book, venues, label));
    }
    Ok(())
}

/// One line of output, stamped with the local microsecond it was received - so a run of these
/// is something latency can be measured out of.
#[must_use]
pub fn line(book: &proto::BookUpdate, venues: &VenueNames, label: &str) -> String {
    let at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros();

    let Some((bid, ask)) = book.bids.first().zip(book.asks.first()) else {
        // Both sides empty is the connector saying it has no book - bootstrapping, or
        // resyncing. Whatever was on screen a moment ago is not the market any more. No
        // level is available to name a venue, so the requested instrument stands in for it.
        return format!("{at} {label:<24} no book");
    };
    format!(
        "{at} spread {:.8}\n  bid {:<13} {:>14.8} x {:<12.8}\n  ask {:<13} {:>14.8} x {:<12.8}",
        book.spread,
        venues.name(bid.venue_idx),
        bid.price,
        bid.size,
        venues.name(ask.venue_idx),
        ask.price,
        ask.size
    )
}
