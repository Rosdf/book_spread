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
use md_server::test_util::{FakeConnectors, FakeSource, TestCatalogue, book};
use md_wire::grpc::RejectCode;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tonic::Streaming;
use tonic::transport::Channel;

/// Long enough for the registry's resolution backoff to come due again after one failed sweep.
///
/// The backoff starts at 250ms plus up to 50% jitter, and every test here retries at most
/// once, so this is a bound rather than a guess. A real wait rather than a paused clock: these
/// tests drive a real socket from a multi-threaded runtime, which is the point of the file.
const AFTER_A_SWEEP: Duration = Duration::from_millis(600);

/// Registers `name` as tradable on `venue`, the way a connector's own listing refresh would -
/// a catalogue entry is only servable once its venue has interned its symbol, and the spelling
/// is case-sensitive on both sides.
fn list(venue: Venue, name: &str) {
    let _ = test_instrument_for(venue, name);
}

/// A server on a loopback port, with the handle a test needs to stop it.
struct Running {
    addr: SocketAddr,
    stop: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<anyhow::Result<()>>,
}

async fn start(source: &Arc<FakeSource>, catalogue: TestCatalogue) -> Running {
    start_on(source, &Arc::new(FakeSource::default()), catalogue).await
}

/// The same, with a handle kept on the second venue's source too - for the one test that
/// publishes on both sides of a merged instrument.
async fn start_on(
    binance_spot: &Arc<FakeSource>,
    bitstamp: &Arc<FakeSource>,
    catalogue: TestCatalogue,
) -> Running {
    let connectors = FakeConnectors::new(Arc::clone(binance_spot), Arc::clone(bitstamp));
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a loopback port is available");
    let addr = listener.local_addr().expect("the listener is bound");
    let (stop, stopped) = oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        serve(listener, connectors, &catalogue, async {
            let _ = stopped.await;
        })
        .await
    });

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

/// Opens a stream for the instrument the catalogue carries at `idx`.
async fn subscribe(
    addr: SocketAddr,
    idx: u32,
) -> Result<Streaming<proto::BookUpdate>, tonic::Status> {
    let request = proto::SubscribeBookRequest {
        instrument_idx: idx,
    };
    Ok(connect(addr).await.subscribe_book(request).await?.into_inner())
}

/// What the server says it carries.
async fn catalogue_of(addr: SocketAddr) -> proto::CatalogueResponse {
    connect(addr)
        .await
        .get_catalogue(proto::CatalogueRequest {})
        .await
        .expect("the catalogue call is answered")
        .into_inner()
}

/// The venue index a level from `venue` carries, as both sides agree on it.
fn venue_idx(venue: Venue) -> u32 {
    md_server::test_util::TestCatalogue::venue_idx(venue).get()
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
    // The catalogue is what spells a symbol; the venue's own casing travels verbatim.
    let server = start(
        &source,
        TestCatalogue::new().with(0, Venue::BinanceSpot, "WIREBTCUSDT"),
    )
    .await;

    list(Venue::BinanceSpot, "WIREBTCUSDT");
    let mut books = subscribe(server.addr, 0)
        .await
        .expect("the fake source accepts every symbol");
    opening_snapshot(&mut books).await;

    source.publish("WIREBTCUSDT", &book(&[(100.5, 1.25)], &[(99.5, 2.0)]));

    let update = next_book(&mut books).await;
    assert_eq!(update.asks.len(), 1);
    assert_eq!(update.asks[0].price, 100.5);
    assert_eq!(update.asks[0].venue_idx, venue_idx(Venue::BinanceSpot));
    assert_eq!(update.bids[0].size, 2.0);
    assert_eq!(update.spread, 1.0);

    drop(books);
    stop(server).await;
}

/// The call a client makes first: what this server carries, as the indices everything else
/// travels as.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_catalogue_says_what_the_server_carries() {
    let source = Arc::new(FakeSource::default());
    let server = start(
        &source,
        TestCatalogue::new()
            .with(0, Venue::BinanceSpot, "CATBTCUSDT")
            .with_pairs(
                4,
                &[(Venue::BinanceSpot, "CATETHUSDT"), (Venue::Bitstamp, "catethusd")],
            ),
    )
    .await;

    let carried = catalogue_of(server.addr).await;

    let venues: Vec<(u32, &str)> = carried
        .venues
        .iter()
        .map(|venue| (venue.idx, venue.name.as_str()))
        .collect();
    assert!(
        venues.contains(&(venue_idx(Venue::BinanceSpot), "binance_spot"))
            && venues.contains(&(venue_idx(Venue::Bitstamp), "bitstamp")),
        "a level's venue idx is only meaningful through this table, got {venues:?}"
    );

    let indices: Vec<u32> = carried.instruments.iter().map(|entry| entry.idx).collect();
    assert_eq!(
        indices,
        vec![0, 4],
        "the catalogue's own indices travel, sparse and in order"
    );
    let multi = carried
        .instruments
        .iter()
        .find(|entry| entry.idx == 4)
        .expect("the instrument is carried");
    assert_eq!(multi.pairs.len(), 2, "every spelling is advertised");
    assert_eq!(multi.pairs[0].symbol, "CATETHUSDT");
    assert_eq!(multi.pairs[1].venue_idx, venue_idx(Venue::Bitstamp));

    // Nothing was subscribed to answer it: a catalogue call never reaches the registry.
    assert!(
        source.subscribed().is_empty(),
        "a catalogue call subscribes nothing"
    );

    stop(server).await;
}

/// What the two calls are for, together: read the catalogue, find the pair, stream its book -
/// which is exactly what `md_client`'s binary does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_resolves_a_pair_through_the_catalogue_and_streams_it() {
    let source = Arc::new(FakeSource::default());
    let server = start(
        &source,
        TestCatalogue::new()
            .with(2, Venue::BinanceSpot, "RESOLVEBTCUSDT")
            .with(3, Venue::BinanceSpot, "RESOLVEETHUSDT"),
    )
    .await;
    list(Venue::BinanceSpot, "RESOLVEETHUSDT");

    let carried = catalogue_of(server.addr).await;
    let wanted = carried
        .instruments
        .iter()
        .find(|entry| {
            entry.pairs.iter().any(|pair| {
                pair.venue_idx == venue_idx(Venue::BinanceSpot) && pair.symbol == "RESOLVEETHUSDT"
            })
        })
        .expect("the server carries the pair this test asked about");

    let mut books = subscribe(server.addr, wanted.idx)
        .await
        .expect("the fake source accepts every symbol");
    opening_snapshot(&mut books).await;
    source.publish("RESOLVEETHUSDT", &book(&[(3.5, 2.0)], &[]));
    assert_eq!(next_book(&mut books).await.asks[0].price, 3.5);

    drop(books);
    stop(server).await;
}

/// The claim the whole design rests on, end to end: one book in, one encoding out, and two
/// real gRPC clients receiving the very same bytes rather than an encoding each.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_book_reaches_two_tonic_clients_identically() {
    let source = Arc::new(FakeSource::default());
    let server = start(
        &source,
        TestCatalogue::new().with(0, Venue::BinanceSpot, "SHAREDBTCUSDT"),
    )
    .await;

    list(Venue::BinanceSpot, "SHAREDBTCUSDT");
    let mut first = subscribe(server.addr, 0)
        .await
        .expect("the fake source accepts every symbol");
    let mut second = subscribe(server.addr, 0)
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

/// An instrument carrying more than one pair streams *one* book, merged out of every pair's:
/// the whole point of the catalogue's pair list, and the only test that proves the merge, the
/// per-level venue suffix and the two-connector registry path together over a real socket.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_multi_pair_instrument_streams_one_merged_book() {
    let binance_spot = Arc::new(FakeSource::default());
    let bitstamp = Arc::new(FakeSource::default());
    let server = start_on(
        &binance_spot,
        &bitstamp,
        TestCatalogue::new().with_pairs(
            0,
            &[(Venue::BinanceSpot, "MULTIBTCUSDT"), (Venue::Bitstamp, "multibtcusd")],
        ),
    )
    .await;

    list(Venue::BinanceSpot, "MULTIBTCUSDT");
    list(Venue::Bitstamp, "multibtcusd");
    let mut books = subscribe(server.addr, 0)
        .await
        .expect("both venues accept the instrument's spelling");
    opening_snapshot(&mut books).await;

    binance_spot.publish("MULTIBTCUSDT", &book(&[(100.0, 1.0)], &[(99.0, 1.0)]));
    let one_venue = next_book(&mut books).await;
    assert_eq!(
        one_venue.asks.len(),
        1,
        "the venue that has not published yet contributes nothing"
    );

    bitstamp.publish("multibtcusd", &book(&[(100.5, 2.0)], &[(99.5, 2.0)]));
    let merged = next_book(&mut books).await;

    let asks: Vec<(f64, u32)> = merged
        .asks
        .iter()
        .map(|level| (level.price, level.venue_idx))
        .collect();
    assert_eq!(
        asks,
        vec![
            (100.0, venue_idx(Venue::BinanceSpot)),
            (100.5, venue_idx(Venue::Bitstamp)),
        ],
        "both pairs are in one book, best first, each level naming the venue that quoted it"
    );
    let bids: Vec<(f64, u32)> = merged
        .bids
        .iter()
        .map(|level| (level.price, level.venue_idx))
        .collect();
    assert_eq!(
        bids,
        vec![
            (99.5, venue_idx(Venue::Bitstamp)),
            (99.0, venue_idx(Venue::BinanceSpot)),
        ]
    );
    assert_eq!(
        merged.spread, 0.5,
        "the spread is the merged tops', not either venue's own"
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
    let server = start(
        &source,
        TestCatalogue::new().with(0, Venue::BinanceSpot, "REFUSEDBTCUSDT"),
    )
    .await;

    let status = subscribe(server.addr, 41)
        .await
        .expect_err("an index this server does not carry is refused");

    assert_eq!(status.code(), tonic::Code::NotFound);
    assert!(
        status.message().contains("41"),
        "the refusal names the index it did not recognise, got {:?}",
        status.message()
    );
    assert_eq!(reject_code(&status), Some(RejectCode::UnknownInstrument));
    assert!(
        !RejectCode::UnknownInstrument.retryable(),
        "the catalogue is loaded once, so an index it does not carry never will be"
    );

    stop(server).await;
}

/// A catalogue entry whose venue has not listed its symbol yet is refused as retryable, and
/// the same request succeeds once the connector has caught up.
///
/// The two `NOT_FOUND`s in this file are the case the reject-code metadata exists for: this
/// one is worth retrying and the one above never is, and `grpc-status` cannot tell them apart.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unlisted_symbol_is_retryable_and_succeeds_once_its_venue_lists_it() {
    let source = Arc::new(FakeSource::default());
    let server = start(
        &source,
        TestCatalogue::new().with(0, Venue::BinanceSpot, "NOTLISTEDYETBTC"),
    )
    .await;

    let status = subscribe(server.addr, 0)
        .await
        .expect_err("nothing has interned the symbol yet");
    assert_eq!(status.code(), tonic::Code::NotFound);
    assert_eq!(reject_code(&status), Some(RejectCode::UnlistedSymbol));
    assert!(
        RejectCode::UnlistedSymbol.retryable(),
        "a connector that has not caught up may have caught up by the next attempt"
    );
    assert!(
        source.subscribed().is_empty(),
        "an unresolved instrument must not reach the connector"
    );

    // The connector's listing refresh, and the wait for the registry's next sweep to come due.
    list(Venue::BinanceSpot, "NOTLISTEDYETBTC");
    tokio::time::sleep(AFTER_A_SWEEP).await;

    let mut books = subscribe(server.addr, 0)
        .await
        .expect("the symbol is listed now");
    opening_snapshot(&mut books).await;

    drop(books);
    stop(server).await;
}

/// A connector that turns the subscribe down is reported on the client's own stream, and is
/// marked retryable - unlike an index this server does not carry, trying again could work.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_connector_refusal_reaches_the_client_as_retryable() {
    let source = Arc::new(FakeSource::rejecting("the venue is not ready"));
    let server = start(
        &source,
        TestCatalogue::new().with(0, Venue::BinanceSpot, "CONNREFUSEDBTC"),
    )
    .await;

    list(Venue::BinanceSpot, "CONNREFUSEDBTC");
    let status = subscribe(server.addr, 0)
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
    let server = start(
        &source,
        TestCatalogue::new()
            .with(0, Venue::BinanceSpot, "twobtcusdt")
            .with(1, Venue::BinanceSpot, "twoethusdt"),
    )
    .await;

    list(Venue::BinanceSpot, "twobtcusdt");
    list(Venue::BinanceSpot, "twoethusdt");
    let mut btc = subscribe(server.addr, 0)
        .await
        .expect("the fake source accepts every symbol");
    let mut eth = subscribe(server.addr, 1)
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
    let server = start(
        &source,
        TestCatalogue::new().with(0, Venue::BinanceSpot, "shutdownbtcusdt"),
    )
    .await;

    list(Venue::BinanceSpot, "shutdownbtcusdt");
    let mut books = subscribe(server.addr, 0)
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
    let server = start(&source, TestCatalogue::new()).await;

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
    let server = start(&source, TestCatalogue::new()).await;

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
            tonic::Request::new(proto::SubscribeBookRequest { instrument_idx: 0 }),
            path,
            tonic_prost::ProstCodec::default(),
        )
        .await
        .expect_err("this server has two methods, and that is not one of them");

    assert_eq!(status.code(), tonic::Code::Unimplemented);
    assert_eq!(reject_code(&status), Some(RejectCode::NotThisService));

    stop(server).await;
}
