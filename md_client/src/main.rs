//! Follows top-of-book for one `(venue, symbol)` pair and prints what arrives.
//!
//! ```text
//! cargo run -p md_client -- binance_spot BTCUSDT
//! ```
//!
//! Two calls: `GetCatalogue` to find out what the server carries and which index names the
//! pair asked for, then `SubscribeBook` on that index. Nothing on this wire spells a venue or
//! a symbol out except the catalogue - see [`md_wire::grpc`] - so the first call is what makes
//! the command line's own `<venue> <symbol>` form possible at all. The server address comes
//! from `MD_SERVER_ADDR`.

// The binary is a thin shell over the library, so these reach it only through `md_client`.
// Naming them here is what keeps `unused_crate_dependencies` quiet for this target.
use tonic as _;
use tonic_prost as _;

use md_client::{ADDR_VAR, DEFAULT_ADDR, VenueNames, catalogue, follow, reject_code};
use md_proto::md::v1 as proto;
use md_wire::grpc::RejectCode;
use std::fmt::Write as _;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [venue, symbol] = args.as_slice() else {
        anyhow::bail!("usage: md_client <venue> <symbol>");
    };

    let addr = std::env::var(ADDR_VAR).unwrap_or_else(|_| DEFAULT_ADDR.to_owned());
    let carried = catalogue(&addr).await?;
    let venues = VenueNames::from_catalogue(&carried);
    let instrument = find(&carried, venue, symbol)
        .ok_or_else(|| anyhow::anyhow!("{}", not_carried(&carried, venue, symbol)))?;

    let pairs = instrument
        .pairs
        .iter()
        .map(|pair| proto::SubscribePair {
            venue: venues.name(pair.venue_idx),
            symbol: pair.symbol.clone(),
        })
        .collect();
    let label = format!("{venue}/{symbol}");
    if let Err(status) = follow(&addr, instrument.idx, pairs, &venues, &label).await {
        // The canonical status says what kind of problem it was; the metadata says which one
        // exactly, and therefore whether trying the same request again could ever work.
        match reject_code(&status) {
            Some(code) if code.retryable() => {
                eprintln!("stream ended: {} ({code:?}, worth retrying)", status.message());
            }
            Some(RejectCode::InstrumentChanged) => {
                eprintln!(
                    "stream ended: {} (InstrumentChanged) - the catalogue changed since it was \
                     read; re-run to fetch it again",
                    status.message()
                );
            }
            Some(code) => eprintln!("stream ended: {} ({code:?})", status.message()),
            None => eprintln!("stream ended: {status}"),
        }
    }
    Ok(())
}

/// The instrument carrying `(venue, symbol)`, if this server carries one.
///
/// The venue name is matched case-insensitively - it is this build's own spelling of a venue -
/// while the symbol is not: a venue's symbol is whatever that venue calls it, and the
/// catalogue carries it verbatim.
fn find<'a>(
    catalogue: &'a proto::CatalogueResponse,
    venue: &str,
    symbol: &str,
) -> Option<&'a proto::InstrumentEntry> {
    let venue_idx = catalogue
        .venues
        .iter()
        .find(|entry| entry.name.eq_ignore_ascii_case(venue))?
        .idx;

    catalogue.instruments.iter().find(|instrument| {
        instrument
            .pairs
            .iter()
            .any(|pair| pair.venue_idx == venue_idx && pair.symbol == symbol)
    })
}

/// What to print when the server does not carry what was asked for: the pair, and everything
/// it does carry - which is the only way a user finds out what to type instead.
fn not_carried(catalogue: &proto::CatalogueResponse, venue: &str, symbol: &str) -> String {
    let mut message = format!("this server does not carry {venue}/{symbol}. It carries:");
    for instrument in &catalogue.instruments {
        for pair in &instrument.pairs {
            let name = catalogue
                .venues
                .iter()
                .find(|entry| entry.idx == pair.venue_idx)
                .map_or("?", |entry| entry.name.as_str());
            let _ = write!(message, "\n  {name} {}", pair.symbol);
        }
    }
    message
}
