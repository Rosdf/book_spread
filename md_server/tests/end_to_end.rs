//! The whole pipeline over a real loopback socket, driven by a real gRPC client.
//!
//! `impl Listener for TcpListener`, `poll_accept` and `set_nodelay` are all exercised here and
//! nowhere else, and so is the HTTP/2 handshake against something that was not written in this
//! repository: the client is tonic's, generated from the same `.proto` the server serves. That
//! is the point of this file. If the hand-written server drifted from gRPC in any way tonic
//! notices, these fail.
//!
//! An integration test rather than `#[cfg(test)]` inside `server.rs`: it needs
//! `md_server::test_util` built with the `test-util` feature from outside the crate, which is
//! exactly what `[dev-dependencies] md_server = { path = ".", features = ["test-util"] }` sets
//! up - and `md_client`, which is where tonic lives.

#![allow(
    unused_crate_dependencies,
    reason = "an integration test target is checked against the whole manifest's dependencies, most of which only the library target itself uses"
)]

use core_lib::Venue;
use core_lib::venue::test_util::test_instrument_for;
use md_client::{MarketDataClient, reject_code};
use md_proto::md::v1 as proto;
use md_server::test_util::serve;
use md_server::test_util::{FakeConnectors, FakeSource, book};
use md_wire::grpc::RejectCode;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tonic::Streaming;
use tonic::transport::Channel;

/// Registers `name` as tradable on `venue`, the way a connector's own listing refresh would -
/// the wire contract is case-sensitive, so a test has to name exactly what it registers.
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

async fn connect(addr: SocketAddr) -> MarketDataClient<Channel> {
    MarketDataClient::connect(format!("http://{addr}"))
        .await
        .expect("the listener is accepting")
}

/// Opens a stream for a one-pair request.
async fn subscribe(
    addr: SocketAddr,
    venue: &str,
    symbol: &str,
) -> Result<Streaming<proto::BookUpdate>, tonic::Status> {
    subscribe_pairs(addr, &[(venue, symbol)]).await
}

/// Opens a stream for a request naming every one of `pairs`.
async fn subscribe_pairs(
    addr: SocketAddr,
    pairs: &[(&str, &str)],
) -> Result<Streaming<proto::BookUpdate>, tonic::Status> {
    let request = proto::SubscribeBookRequest {
        pairs: pairs
            .iter()
            .map(|&(venue, symbol)| proto::Pair {
                venue: venue.to_owned(),
                symbol: symbol.to_owned(),
            })
            .collect(),
    };
    Ok(connect(addr).await.subscribe_book(request).await?.into_inner())
}

async fn next_book(books: &mut Streaming<proto::BookUpdate>) -> proto::BookUpdate {
    tokio::time::timeout(Duration::from_secs(5), books.message())
        .await
        .expect("a book arrives promptly")
        .expect("the stream is healthy")
        .expect("the stream has not ended")
}

/// Reads the first message and asserts it is the empty book - the snapshot every stream opens
/// with. See `md_server`'s `broadcast::session` module doc.
async fn opening_snapshot(books: &mut Streaming<proto::BookUpdate>) {
    let snapshot = next_book(books).await;
    assert!(
        snapshot.asks.is_empty() && snapshot.bids.is_empty(),
        "a stream's first message is always its opening snapshot, empty here because nothing \
         had been published yet, got {snapshot:?}"
    );
}

/// The whole pipeline over a real socket, spoken by a real gRPC client: connect, handshake,
/// and a book off the wire.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tonic_client_streams_a_book_over_the_wire() {
    let source = Arc::new(FakeSource::default());
    let server = start(&source).await;

    // The wire contract is case-sensitive: a client sends the venue's own spelling.
    list(Venue::BinanceSpot, "WIREBTCUSDT");
    let mut books = subscribe(server.addr, "binance_spot", "WIREBTCUSDT")
        .await
        .expect("the fake source accepts every symbol");
    opening_snapshot(&mut books).await;

    source.publish("WIREBTCUSDT", &book(&[(100.5, 1.25)], &[(99.5, 2.0)]));

    let update = next_book(&mut books).await;
    assert_eq!(update.asks.len(), 1);
    assert_eq!(update.asks[0].price, 100.5);
    assert_eq!(update.asks[0].venue, "binance_spot");
    assert_eq!(update.bids[0].size, 2.0);
    assert_eq!(update.spread, 1.0);

    drop(books);
    stop(server).await;
}

/// The claim the whole design rests on, end to end: one book in, one encoding out, and two
/// real gRPC clients receiving the very same bytes rather than an encoding each.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_book_reaches_two_tonic_clients_identically() {
    let source = Arc::new(FakeSource::default());
    let server = start(&source).await;

    list(Venue::BinanceSpot, "SHAREDBTCUSDT");
    let mut first = subscribe(server.addr, "binance_spot", "SHAREDBTCUSDT")
        .await
        .expect("the fake source accepts every symbol");
    let mut second = subscribe(server.addr, "binance_spot", "SHAREDBTCUSDT")
        .await
        .expect("the fake source accepts every symbol");
    opening_snapshot(&mut first).await;
    opening_snapshot(&mut second).await;

    let deep: Vec<(f64, f64)> = (1..=10).map(|i| (f64::from(i), f64::from(i))).collect();
    source.publish("SHAREDBTCUSDT", &book(&deep, &deep));

    let (left, right) = tokio::join!(next_book(&mut first), next_book(&mut second));
    assert_eq!(
        left, right,
        "the same buffer reached both connections, so both clients decode the same book"
    );
    assert_eq!(left.asks.len(), 10);
    assert_eq!(
        source.subscribed().len(),
        1,
        "the second client joins the running broadcaster instead of subscribing again"
    );

    drop(first);
    drop(second);
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
    let mut books = subscribe_pairs(
        server.addr,
        &[
            ("binance_spot", "MULTIBTCUSDT"),
            ("bitstamp", "multibtcusd"),
        ],
    )
    .await
    .expect("the fake source accepts every symbol");
    opening_snapshot(&mut books).await;

    source.publish("MULTIBTCUSDT", &book(&[(100.5, 1.25)], &[(99.5, 2.0)]));

    let update = next_book(&mut books).await;
    assert_eq!(
        update.asks[0].venue, "binance_spot",
        "only the first pair - binance_spot/MULTIBTCUSDT - is served today"
    );

    drop(books);
    stop(server).await;
}

/// A request the server will not serve is refused in the handshake, as a Trailers-Only
/// response, and no stream follows.
///
/// Both halves of the refusal are asserted: the canonical `grpc-status`, which is all a client
/// that knows only gRPC gets, and the `md-reject-code` metadata, which is the only thing that
/// says whether retrying could ever work.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_request_is_a_status_with_a_reason() {
    let source = Arc::new(FakeSource::default());
    let server = start(&source).await;

    let status = subscribe(server.addr, "nope", "REFUSEDBTCUSDT")
        .await
        .expect_err("an unknown venue is refused");

    assert_eq!(status.code(), tonic::Code::NotFound);
    assert!(
        status.message().contains("nope"),
        "the refusal names the venue it did not recognise, got {:?}",
        status.message()
    );
    assert_eq!(reject_code(&status), Some(RejectCode::UnknownVenue));
    assert!(
        !RejectCode::UnknownVenue.retryable(),
        "an unknown venue is permanent, which is what the metadata is carried for"
    );

    stop(server).await;
}

/// Every kind of refusal a client can provoke, mapped to the status it must arrive as.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_refusal_arrives_as_its_status() {
    /// One request the server will not serve, and both halves of the answer it must give.
    struct Case {
        pairs: Vec<(&'static str, &'static str)>,
        expected: RejectCode,
        status: tonic::Code,
    }

    fn case(
        pairs: Vec<(&'static str, &'static str)>,
        expected: RejectCode,
        status: tonic::Code,
    ) -> Case {
        Case {
            pairs,
            expected,
            status,
        }
    }

    let source = Arc::new(FakeSource::default());
    let server = start(&source).await;

    list(Venue::BinanceSpot, "DUPEBTCUSDT");
    let cases = vec![
        case(Vec::new(), RejectCode::EmptyRequest, tonic::Code::InvalidArgument),
        case(
            vec![("kraken", "BTCUSD")],
            RejectCode::UnknownVenue,
            tonic::Code::NotFound,
        ),
        case(
            vec![("binance_spot", "btc-usd")],
            RejectCode::MalformedSymbol,
            tonic::Code::InvalidArgument,
        ),
        case(
            vec![("binance_spot", "NEVERLISTEDXYZ")],
            RejectCode::UnlistedSymbol,
            tonic::Code::NotFound,
        ),
        case(
            vec![("binance_spot", "DUPEBTCUSDT"), ("binance_spot", "DUPEBTCUSDT")],
            RejectCode::DuplicatePair,
            tonic::Code::InvalidArgument,
        ),
    ];

    for Case {
        pairs,
        expected,
        status,
    } in cases
    {
        let refusal = subscribe_pairs(server.addr, &pairs)
            .await
            .expect_err("every case here is refused");
        assert_eq!(refusal.code(), status, "for {pairs:?}");
        assert_eq!(reject_code(&refusal), Some(expected), "for {pairs:?}");
    }

    stop(server).await;
}

/// A connector that turns the subscribe down is reported on the client's own stream, and is
/// marked retryable - unlike an unlisted symbol, trying again later could work.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_connector_refusal_reaches_the_client_as_retryable() {
    let source = Arc::new(FakeSource::rejecting("the venue is not ready"));
    let server = start(&source).await;

    list(Venue::BinanceSpot, "CONNREFUSEDBTC");
    let status = subscribe(server.addr, "binance_spot", "CONNREFUSEDBTC")
        .await
        .expect_err("the source rejects every symbol");

    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert_eq!(reject_code(&status), Some(RejectCode::ConnectorRefused));
    assert!(
        RejectCode::ConnectorRefused.retryable(),
        "a connector that was not ready may be ready later"
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
    let mut btc = subscribe(server.addr, "binance_spot", "twobtcusdt")
        .await
        .expect("the fake source accepts every symbol");
    let mut eth = subscribe(server.addr, "binance_spot", "twoethusdt")
        .await
        .expect("the fake source accepts every symbol");
    opening_snapshot(&mut btc).await;
    opening_snapshot(&mut eth).await;

    source.publish("twobtcusdt", &book(&[(100.5, 1.25)], &[]));
    source.publish("twoethusdt", &book(&[(3.5, 2.0)], &[]));

    assert_eq!(next_book(&mut btc).await.asks[0].price, 100.5);
    assert_eq!(next_book(&mut eth).await.asks[0].price, 3.5);

    drop(btc);
    drop(eth);
    stop(server).await;
}

/// Shutting the server down with a client still attached must end that client's stream with a
/// status rather than hang waiting for it - or drop the connection without saying why.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_ends_an_attached_client_with_a_status() {
    let source = Arc::new(FakeSource::default());
    let server = start(&source).await;

    list(Venue::BinanceSpot, "shutdownbtcusdt");
    let mut books = subscribe(server.addr, "binance_spot", "shutdownbtcusdt")
        .await
        .expect("the fake source accepts every symbol");
    opening_snapshot(&mut books).await;

    stop(server).await;

    let ended = tokio::time::timeout(Duration::from_secs(5), books.message())
        .await
        .expect("the stream ends promptly");
    match ended {
        Err(status) => {
            assert_eq!(status.code(), tonic::Code::Unavailable);
            assert_eq!(reject_code(&status), Some(RejectCode::StreamEnded));
        }
        Ok(update) => panic!("a shutting-down server must not send another book, got {update:?}"),
    }
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

/// A peer speaking HTTP/2 but asking for something else is turned away with `UNIMPLEMENTED`,
/// the way any gRPC server answers a method it does not have.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unknown_method_is_unimplemented() {
    let source = Arc::new(FakeSource::default());
    let server = start(&source).await;

    let channel = Channel::from_shared(format!("http://{}", server.addr))
        .expect("a loopback address is a valid endpoint")
        .connect()
        .await
        .expect("the listener is accepting");
    let mut grpc = tonic::client::Grpc::new(channel);
    grpc.ready().await.expect("the connection is usable");

    let path = http::uri::PathAndQuery::from_static("/md.v1.MarketData/NoSuchMethod");
    let status = grpc
        .server_streaming::<_, proto::BookUpdate, _>(
            tonic::Request::new(proto::SubscribeBookRequest { pairs: Vec::new() }),
            path,
            tonic_prost::ProstCodec::default(),
        )
        .await
        .expect_err("this server has one method");

    assert_eq!(status.code(), tonic::Code::Unimplemented);
    assert_eq!(reject_code(&status), Some(RejectCode::NotThisService));

    stop(server).await;
}
