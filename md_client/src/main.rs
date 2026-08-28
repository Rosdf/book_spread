//! Follows top-of-book for one or more `(venue, symbol)` pairs and prints what arrives.
//!
//! ```text
//! cargo run -p md_client -- binance_spot BTCUSDT bitstamp btcusd
//! ```
//!
//! One connection carries every pair the command line names; today only the first is served,
//! merging the rest into one book is the next stage - see [`md_wire::framing`]. The server
//! address comes from `MD_SERVER_ADDR`, the same variable the server itself reads.
//!
//! Every line is stamped with the local microsecond it was received, so a run of these is
//! something latency can be measured out of.

use md_proto::md::v1 as proto;
use md_wire::framing;
use prost::Message as _;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;

const DEFAULT_ADDR: &str = "127.0.0.1:50051";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() || !args.len().is_multiple_of(2) {
        anyhow::bail!("usage: md_client <venue> <symbol> [<venue> <symbol> ...]");
    }

    let addr = std::env::var("MD_SERVER_ADDR").unwrap_or_else(|_| DEFAULT_ADDR.to_owned());

    let pairs: Vec<proto::Pair> = args
        .as_chunks::<2>()
        .0
        .iter()
        .map(|[venue, symbol]| proto::Pair {
            venue: venue.clone(),
            symbol: symbol.clone(),
        })
        .collect();

    if let Err(err) = follow(addr, proto::SubscribeBookRequest { pairs }).await {
        eprintln!("stream ended: {err}");
    }
    Ok(())
}

/// Streams the first pair until the server closes the connection.
async fn follow(addr: String, request: proto::SubscribeBookRequest) -> anyhow::Result<()> {
    let mut sock = TcpStream::connect(&addr).await?;
    // The server disables Nagle on its side; this is the same courtesy for the request.
    sock.set_nodelay(true)?;

    let mut buf = Vec::new();
    framing::write_request(&mut sock, &request).await?;
    if let Err(rejected) = framing::read_response(&mut sock, &mut buf).await? {
        anyhow::bail!("refused: {rejected}");
    }

    loop {
        match framing::read_frame(&mut sock, &mut buf).await {
            // The server closing the socket is how this protocol ends a stream.
            Err(framing::ReadFrameError::Closed) => return Ok(()),
            Err(err) => return Err(err.into()),
            Ok(()) => {}
        }
        // The one place a client pays for parsing: the frame arrived as bytes and would have
        // stayed that way if all it did was forward it.
        let book = proto::BookUpdate::decode(buf.as_slice())?;
        println!("{}", line(&book, &request.pairs[0]));
    }
}

fn line(book: &proto::BookUpdate, requested: &proto::Pair) -> String {
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
