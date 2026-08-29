//! The whole pipeline over a real loopback socket - `impl Listener for TcpListener`,
//! `poll_accept` and `set_nodelay` included, none of which a mocked transport would exercise.
//!
//! An integration test rather than `#[cfg(test)]` inside `server.rs`: it needs
//! `md_server::test_util` built with the `test-util` feature from outside the crate, which is
//! exactly what `[dev-dependencies] md_server = { path = ".", features = ["test-util"] }` sets
//! up.

#![allow(
    unused_crate_dependencies,
    reason = "an integration test target is checked against the whole manifest's dependencies, most of which only the library target itself uses"
)]

use core_lib::Venue;
use core_lib::venue::test_util::test_instrument_for;
use md_proto::md::v1 as proto;
use md_server::test_util::serve;
use md_server::test_util::{FakeConnectors, FakeSource, book};
use md_wire::framing::{self, RejectCode};
use prost::Message as _;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};

/// Registers `name` as tradable on `venue`, the way a connector's own listing refresh would -
/// the wire contract is case-sensitive now, so a test has to name exactly what it registers.
fn list(venue: Venue, name: &str) {
    let _ = test_instrument_for(venue, name);
}

/// A server on a loopback port, with the handle a test needs to stop it.
struct Running {
    addr: SocketAddr,
    stop: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<anyhow::Result<()>>,
}

async fn start(source: &Arc<FakeSource>) -> Running {
    let connectors = FakeConnectors::new(Arc::clone(source), FakeSource::default());
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a loopback port is available");
    let addr = listener.local_addr().expect("the listener is bound");
    let (stop, stopped) = oneshot::channel::<()>();
    let task = tokio::spawn(serve(listener, connectors, async {
        let _ = stopped.await;
    }));

    Running { addr, stop, task }
}

async fn stop(server: Running) {
    let _ = server.stop.send(());
    tokio::time::timeout(Duration::from_secs(5), server.task)
        .await
        .expect("shutdown must not hang on an open stream")
        .expect("the server task does not panic")
        .expect("the server stops cleanly");
}

/// Opens a connection and sends a one-pair request, leaving the response header unread.
async fn subscribe(addr: SocketAddr, venue: &str, symbol: &str) -> TcpStream {
    subscribe_pairs(addr, &[(venue, symbol)]).await
}

/// Opens a connection and sends a request naming every one of `pairs`, leaving the response
/// header unread.
async fn subscribe_pairs(addr: SocketAddr, pairs: &[(&str, &str)]) -> TcpStream {
    let mut sock = TcpStream::connect(addr)
        .await
        .expect("the listener is accepting");
    framing::write_request(
        &mut sock,
        &proto::SubscribeBookRequest {
            pairs: pairs
                .iter()
                .map(|&(venue, symbol)| proto::Pair {
                    venue: venue.to_owned(),
                    symbol: symbol.to_owned(),
                })
                .collect(),
        },
    )
    .await
    .expect("the request is written");
    sock
}

async fn next_book(sock: &mut TcpStream, buf: &mut Vec<u8>) -> proto::BookUpdate {
    tokio::time::timeout(Duration::from_secs(5), framing::read_frame(sock, buf))
        .await
        .expect("a book arrives promptly")
        .expect("the stream is healthy");
    proto::BookUpdate::decode(buf.as_slice()).expect("the frame is a BookUpdate")
}

/// Reads the frame right after the acceptance header and asserts it is the empty book - the
/// snapshot every session opens with. See `md_server::session`'s module doc.
async fn opening_snapshot(sock: &mut TcpStream, buf: &mut Vec<u8>) {
    let snapshot = next_book(sock, buf).await;
    assert!(
        snapshot.asks.is_empty() && snapshot.bids.is_empty(),
        "a session's first frame is always its opening snapshot, empty here because nothing \
         had been published yet, got {snapshot:?}"
    );
}

/// The whole pipeline over a real socket: connect, handshake, and a book off the wire.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_streams_a_book_over_the_wire() {
    let source = Arc::new(FakeSource::default());
    let server = start(&source).await;

    // The wire contract is case-sensitive: a client sends the venue's own spelling.
    list(Venue::BinanceSpot, "WIREBTCUSDT");
    let mut sock = subscribe(server.addr, "binance_spot", "WIREBTCUSDT").await;
    let mut buf = Vec::new();
    framing::read_response(&mut sock, &mut buf)
        .await
        .expect("the header is well formed")
        .expect("the fake source accepts every symbol");
    opening_snapshot(&mut sock, &mut buf).await;

    source.publish("WIREBTCUSDT", &book(&[(100.5, 1.25)], &[(99.5, 2.0)]));

    let body = next_book(&mut sock, &mut buf).await;
    assert_eq!(body.asks.len(), 1);
    assert_eq!(body.asks[0].price, 100.5);
    assert_eq!(body.asks[0].venue, "binance_spot");
    assert_eq!(body.bids[0].size, 2.0);
    assert_eq!(body.spread, 1.0);

    drop(sock);
    stop(server).await;
}

/// A request naming more than one pair streams the first pair's book - merging the rest into
/// one book is the next stage, but every pair is still validated up front.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_multi_pair_request_streams_the_first_pairs_book() {
    let source = Arc::new(FakeSource::default());
    let server = start(&source).await;

    list(Venue::BinanceSpot, "MULTIBTCUSDT");
    list(Venue::Bitstamp, "multibtcusd");
    let mut sock = subscribe_pairs(
        server.addr,
        &[
            ("binance_spot", "MULTIBTCUSDT"),
            ("bitstamp", "multibtcusd"),
        ],
    )
    .await;
    let mut buf = Vec::new();
    framing::read_response(&mut sock, &mut buf)
        .await
        .expect("the header is well formed")
        .expect("the fake source accepts every symbol");
    opening_snapshot(&mut sock, &mut buf).await;

    source.publish("MULTIBTCUSDT", &book(&[(100.5, 1.25)], &[(99.5, 2.0)]));

    let body = next_book(&mut sock, &mut buf).await;
    assert_eq!(
        body.asks[0].venue, "binance_spot",
        "only the first pair - binance_spot/MULTIBTCUSDT - is served today"
    );

    drop(sock);
    stop(server).await;
}

/// A request the server will not serve is refused in the handshake, with a reason, and no
/// stream follows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_request_says_why_and_ends_the_connection() {
    let source = Arc::new(FakeSource::default());
    let server = start(&source).await;

    let mut sock = subscribe(server.addr, "nope", "REFUSEDBTCUSDT").await;
    let mut buf = Vec::new();
    let rejected = framing::read_response(&mut sock, &mut buf)
        .await
        .expect("the header is well formed")
        .expect_err("an unknown venue is refused");

    assert_eq!(rejected.code(), RejectCode::UnknownVenue);
    assert!(
        matches!(
            framing::read_frame(&mut sock, &mut buf).await,
            Err(framing::ReadFrameError::Closed)
        ),
        "a refusal must not be followed by a stream"
    );

    stop(server).await;
}

/// Two symbols means two connections, which is what the protocol is built around.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_symbols_are_two_connections() {
    let source = Arc::new(FakeSource::default());
    let server = start(&source).await;

    list(Venue::BinanceSpot, "twobtcusdt");
    list(Venue::BinanceSpot, "twoethusdt");
    let mut btc = subscribe(server.addr, "binance_spot", "twobtcusdt").await;
    let mut eth = subscribe(server.addr, "binance_spot", "twoethusdt").await;
    let (mut btc_buf, mut eth_buf) = (Vec::new(), Vec::new());
    framing::read_response(&mut btc, &mut btc_buf)
        .await
        .expect("the header is well formed")
        .expect("the fake source accepts every symbol");
    framing::read_response(&mut eth, &mut eth_buf)
        .await
        .expect("the header is well formed")
        .expect("the fake source accepts every symbol");
    opening_snapshot(&mut btc, &mut btc_buf).await;
    opening_snapshot(&mut eth, &mut eth_buf).await;

    source.publish("twobtcusdt", &book(&[(100.5, 1.25)], &[]));
    source.publish("twoethusdt", &book(&[(3.5, 2.0)], &[]));

    assert_eq!(next_book(&mut btc, &mut btc_buf).await.asks[0].price, 100.5);
    assert_eq!(next_book(&mut eth, &mut eth_buf).await.asks[0].price, 3.5);

    drop(btc);
    drop(eth);
    stop(server).await;
}

/// Shutting the server down with a client still attached must end that client's stream rather
/// than hang waiting for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_ends_an_attached_client() {
    let source = Arc::new(FakeSource::default());
    let server = start(&source).await;

    list(Venue::BinanceSpot, "shutdownbtcusdt");
    let mut sock = subscribe(server.addr, "binance_spot", "shutdownbtcusdt").await;
    let mut buf = Vec::new();
    framing::read_response(&mut sock, &mut buf)
        .await
        .expect("the header is well formed")
        .expect("the fake source accepts every symbol");
    opening_snapshot(&mut sock, &mut buf).await;

    stop(server).await;

    assert!(
        matches!(
            framing::read_frame(&mut sock, &mut buf).await,
            Err(framing::ReadFrameError::Closed)
        ),
        "a shutting-down server closes every attached connection"
    );
}

/// A connection that says nothing is dropped rather than held, and does not stop the server
/// from shutting down.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_silent_connection_does_not_hold_shutdown_up() {
    let source = Arc::new(FakeSource::default());
    let server = start(&source).await;

    let _silent = TcpStream::connect(server.addr)
        .await
        .expect("the listener is accepting");

    stop(server).await;
}
