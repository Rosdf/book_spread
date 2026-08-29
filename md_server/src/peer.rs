//! The client half of a mock connection, driven by the broadcaster and registry tests.
//!
//! Here rather than in [`crate::test_util`] because this is the peer a [`Session`] writes to:
//! it reads the acceptance header, the opening snapshot and each book frame, and watches the
//! close. `cfg(test)` and crate-private - the end-to-end test is a real client on a real
//! socket and needs none of it.
use crate::transport::mock::{MockClient, MockStream, mock_pair, mock_pair_with_capacity};
use md_proto::md::v1 as proto;
use md_wire::framing::{self, ReadFrameError, Rejected};
use prost::Message as _;
use std::future::Future;
use std::time::Duration;
use tokio::io::AsyncWriteExt as _;

/// The client half of a mock connection, with the reads a test needs off it.
///
/// Built by [`connected`], which also hands back the server half to give to a broadcaster.
#[derive(Debug)]
pub(crate) struct Client {
    sock: MockClient,
    buf: Vec<u8>,
}

impl Client {
    /// Wraps a socket a test got some other way than [`connected`] - from
    /// [`MockConnector::connect`](crate::transport::mock::MockConnector::connect), say,
    /// where the server half went to an accept loop rather than straight to a
    /// broadcaster.
    pub(crate) fn from_socket(sock: MockClient) -> Self {
        Self {
            sock,
            buf: Vec::new(),
        }
    }

    /// Sends the subscribe request a real client opens with. Only needed by a test that
    /// goes through [`crate::framed`]; every other one is handed to a broadcaster with
    /// the handshake already done.
    pub(crate) async fn request(&mut self, venue: &str, symbol: &str) {
        framing::write_request(
            &mut self.sock,
            &proto::SubscribeBookRequest {
                pairs: vec![proto::Pair {
                    venue: venue.to_owned(),
                    symbol: symbol.to_owned(),
                }],
            },
        )
        .await
        .expect("the request is written");
    }

    /// Reads the response header, which a broadcaster writes as the first thing on a session.
    ///
    /// # Errors
    ///
    /// The server's own reason, when the subscription was turned down.
    pub(crate) async fn accepted(&mut self) -> Result<(), Rejected> {
        deadline(framing::read_response(&mut self.sock, &mut self.buf))
            .await
            .expect("a response header arrives promptly")
            .expect("the header is well formed")
    }

    /// The next book off the wire.
    pub(crate) async fn next_book(&mut self) -> proto::BookUpdate {
        self.next_frame().await;
        proto::BookUpdate::decode(self.buf.as_slice()).expect("the frame is a BookUpdate")
    }

    /// Reads the frame right after the acceptance header and asserts it is the empty book -
    /// the snapshot every session opens with. See [`crate::session`]'s module doc.
    pub(crate) async fn opening_snapshot(&mut self) {
        let snapshot = self.next_book().await;
        assert!(
            snapshot.asks.is_empty() && snapshot.bids.is_empty(),
            "a session's first frame is always its opening snapshot, empty here because \
                 nothing had been published yet, got {snapshot:?}"
        );
    }

    /// Asserts no frame arrives within a short, deterministic window.
    ///
    /// Meant for a `#[tokio::test(start_paused = true)]` test: the sleep this races against
    /// never elapses in real time, so this is instant rather than a real wait.
    pub(crate) async fn assert_quiet(&mut self) {
        let raced = tokio::time::timeout(
            Duration::from_millis(50),
            framing::read_frame(&mut self.sock, &mut self.buf),
        )
        .await;
        assert!(
            raced.is_err(),
            "expected no frame to arrive, but got {raced:?}"
        );
    }

    /// The next frame's body, left in this client's buffer and also returned.
    pub(crate) async fn next_frame(&mut self) -> Vec<u8> {
        deadline(framing::read_frame(&mut self.sock, &mut self.buf))
            .await
            .expect("a frame arrives promptly")
            .expect("the stream is healthy");
        self.buf.clone()
    }

    /// Waits for the server to close the connection, and fails if it sends anything more.
    pub(crate) async fn ended(&mut self) {
        let outcome = deadline(framing::read_frame(&mut self.sock, &mut self.buf))
            .await
            .expect("the stream ends promptly");
        assert!(
            matches!(outcome, Err(ReadFrameError::Closed)),
            "the server must close the connection rather than send more, got {outcome:?}"
        );
    }

    /// Sends bytes the protocol does not allow, which is one of the two ways a session ends.
    pub(crate) async fn misbehave(&mut self) {
        self.sock
            .write_all(b"unexpected")
            .await
            .expect("the connection is still open");
    }
}

/// Every test read is bounded: a hang here is a bug, not a slow machine.
async fn deadline<F: Future>(work: F) -> Result<F::Output, tokio::time::error::Elapsed> {
    tokio::time::timeout(Duration::from_secs(5), work).await
}

/// A connected pair: the client half a test drives, and the server half to hand to
/// the registry.
///
/// Synchronous, unlike the real-socket `connected` this replaced: a mock pair needs no accept
/// to complete.
pub(crate) fn connected() -> (Client, MockStream) {
    let (client, server) = mock_pair();
    (
        Client {
            sock: client,
            buf: Vec::new(),
        },
        server,
    )
}

/// The same, with the server's write queue capped small enough to back up.
///
/// This is what makes the backpressure path reachable in a test: a broadcaster can write a
/// good many books before a client that never reads causes a single `Pending`, so without a
/// small cap the partial write - and therefore the splice hazard `Session::inflight` exists to
/// prevent - would simply never happen.
pub(crate) fn connected_congested() -> (Client, MockStream) {
    let (client, server) = mock_pair_with_capacity(32);
    (
        Client {
            sock: client,
            buf: Vec::new(),
        },
        server,
    )
}
