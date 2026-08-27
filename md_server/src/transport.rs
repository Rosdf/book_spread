//! The seam between the accept loop and whatever actually listens.
//!
//! [`crate::server::serve`] and [`crate::framed::accept`] are generic over [`Listener`] rather
//! than hard-wired to [`TcpListener`], which is what lets [`crate::test_util`] swap in a mock
//! that never touches a real socket. `pub` rather than `pub(crate)`: [`Listener`] appears in
//! `serve`'s public signature, and `private_bounds` is denied workspace-wide.

use std::fmt::Debug;
use std::io;
use std::net::SocketAddr;
use std::task::{Context, Poll, ready};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};

/// Something that accepts connections, each yielding a byte stream and a peer identity to log.
pub trait Listener: Send + Sync + 'static {
    /// One accepted connection's byte stream.
    type Stream: AsyncRead + AsyncWrite + Unpin + Send + 'static;
    /// What identifies the peer on the other end, for logging.
    type Peer: Debug + Send + 'static;

    /// Polls for the next accepted connection.
    ///
    /// # Errors
    ///
    /// Whatever the underlying transport reports.
    fn poll_accept(&self, cx: &mut Context<'_>) -> Poll<io::Result<(Self::Stream, Self::Peer)>>;
}

impl Listener for TcpListener {
    type Stream = TcpStream;
    type Peer = SocketAddr;

    fn poll_accept(&self, cx: &mut Context<'_>) -> Poll<io::Result<(TcpStream, SocketAddr)>> {
        let (sock, peer) = ready!(Self::poll_accept(self, cx))?;
        // Nagle would hold a ~400-byte book back waiting for company, which for a market-data
        // feed is tens of milliseconds of latency bought for nothing. Set here, on the way in,
        // so no socket is ever handed onward without it.
        if let Err(err) = sock.set_nodelay(true) {
            tracing::warn!(?peer, %err, "could not disable Nagle on an accepted connection");
        }
        Poll::Ready(Ok((sock, peer)))
    }
}

/// Resolves to the next accepted connection.
///
/// A free function rather than an inherent method on [`Listener`] because a trait method
/// cannot return `impl Future` without erasing `Self`, and `poll_accept` already gives
/// `select!` everything it needs directly.
pub(crate) async fn accept<L: Listener>(listener: &L) -> io::Result<(L::Stream, L::Peer)> {
    std::future::poll_fn(|cx| listener.poll_accept(cx)).await
}
