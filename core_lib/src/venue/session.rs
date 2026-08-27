//! How a connection's session ends, and closing a socket by the book.

use crate::net::WsConnector;
use futures_util::{SinkExt as _, StreamExt as _};
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

/// Something that ends a session's socket, taking every symbol on it down together.
///
/// Kept apart from a venue's own `ReplayError` so the recovery scope is visible in each
/// signature: returning this reconnects, returning a `ReplayError` resyncs one symbol.
///
/// Generic over the leaf error type `E` directly, rather than over `W: WsConnector` with
/// `W::Error` as the field type - same reasoning as [`crate::venue::spec::SnapshotFetchErrorImpl`]:
/// a field typed via an associated-type projection forces the *declaration itself* to restate
/// the trait bound the projection needs (Rust checks a struct/enum's own well-formedness at
/// definition time, not deferred to callers), whereas a field typed `E` directly needs nothing
/// stated at all - the bound `E: Display` that `#[error]`/`#[from]` need is picked up by
/// `thiserror`'s generated impls on their own. [`SessionError`] is the ergonomic alias callers
/// actually use.
#[derive(Debug, thiserror::Error)]
pub enum SessionErrorImpl<E> {
    #[error("websocket: {0}")]
    Ws(#[from] Box<E>),

    #[error("stream closed by peer")]
    Closed,
}

pub type SessionError<W> = SessionErrorImpl<<W as WsConnector>::Error>;

/// Boxes a connector error into a [`SessionError`].
///
/// A blanket `impl<E> From<E> for SessionErrorImpl<E>` is not possible here - the coherence
/// checker cannot rule out `E` itself being `SessionErrorImpl<E>`, which would conflict with
/// the standard library's `impl<T> From<T> for T` - so call sites map explicitly with this
/// instead of `?`.
pub fn ws_err<W: WsConnector>(err: W::Error) -> SessionError<W> {
    SessionErrorImpl::Ws(Box::new(err))
}

/// How long the peer gets to answer our close frame before the socket is dropped anyway.
/// Bounded because a wedged socket must not hold up the connector's shutdown.
pub const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

/// How a session ended when it ended cleanly.
#[derive(Debug)]
pub enum SessionEnd {
    /// The peer closed, or some venue-specific routine boundary hit (Binance's 24h limit,
    /// Bitstamp's `bts:request_reconnect`). Routine; reconnect.
    Reconnect,
    /// The supervisor dropped this lane. The socket is already closed; stop.
    ShutDown,
}

/// Closes the socket by the book: send a close frame, then wait out the peer's reply.
///
/// The handshake completes from the read side, so the read half has to keep being polled;
/// frames still arriving are read and discarded. Bounded by [`CLOSE_TIMEOUT`], after which the
/// socket is simply dropped - a peer that will not answer must not stall shutdown.
pub async fn close<W: WsConnector>(stream: &mut W::Stream) {
    if stream.send(Message::Close(None)).await.is_err() {
        // A socket that cannot take the frame is already gone.
        return;
    }

    let wait = tokio::time::timeout(CLOSE_TIMEOUT, async {
        loop {
            match stream.next().await {
                None | Some(Ok(Message::Close(_)) | Err(_)) => return,
                Some(Ok(_)) => {}
            }
        }
    });

    if wait.await.is_err() {
        tracing::warn!("peer did not answer close frame in time, dropping socket");
    }
}
