//! Accepting connections and handshaking them onto a broadcaster.
//!
//! The whole of this module is the *setup* path. Once a socket has been handed to its
//! broadcaster ([`crate::broadcast`]) nothing here touches it again: there is no per-client
//! task, and no code between the encoder and the kernel. A handshake task lives for one
//! request, one lookup and one hand-off, and then ends.

use md_wire::framing;
use crate::registry::Registry;
use crate::transport::{self, Listener};
use crate::venue::Connectors;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::task::JoinSet;

/// How long a freshly accepted connection has to send its request.
///
/// A peer that connects and then says nothing costs a task and a file descriptor for as long
/// as it likes, which is a cheap thing to do a great many times. Nothing legitimate needs
/// more than a moment: the request is written immediately after the connect.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Accepts connections until `stop` resolves, then tears down every handshake still in flight.
///
/// Reaching the end of this function matters for more than tidiness: each handshake task holds
/// an `Arc<Registry>`, and [`crate::server::serve`] can only reclaim the connectors once every
/// one of them is gone.
pub(crate) async fn accept<C: Connectors, L: Listener>(
    registry: Arc<Registry<C, L::Stream>>,
    listener: L,
    stop: oneshot::Receiver<()>,
) {
    let mut handshakes = JoinSet::new();
    // Pinned once rather than awaited in the branch: `select!` polls the same future on every
    // lap, and a fresh one each time would never resolve.
    let mut stopping = std::pin::pin!(async move {
        let _ = stop.await;
    });

    loop {
        tokio::select! {
            accepted = transport::accept(&listener) => match accepted {
                Ok((sock, peer)) => {
                    handshakes.spawn(handshake(Arc::clone(&registry), sock, peer));
                }
                Err(err) => tracing::warn!(%err, "accept failed"),
            },
            () = &mut stopping => break,
            // Reaps finished handshakes as they go, so a long-lived server does not
            // accumulate their join handles. `None` - the set being empty - simply disables
            // this branch.
            Some(_) = handshakes.join_next() => {}
        }
    }

    // Stops accepting before the teardown, so nothing new joins the set.
    drop(listener);
    // Aborted rather than awaited: a handshake is waiting on a client to say something, and
    // `HANDSHAKE_TIMEOUT` is far too long to make shutdown wait for one that never will.
    // Aborting cannot tear anything: the hand-off to a broadcaster is a synchronous
    // `Registry::subscribe`, so a socket has either been queued or not, and a connection
    // dropped mid-handshake is a connection the client must handle being closed anyway.
    // `shutdown` also waits for the aborts to land, which is what releases the last
    // `Arc<Registry>`.
    handshakes.shutdown().await;
}

/// Reads one connection's request and hands its socket to the broadcaster that will serve it.
///
/// The socket leaves this function in one of three ways: attached to a broadcaster, refused
/// with a reason, or simply dropped because the client never said anything usable.
async fn handshake<C: Connectors, S: AsyncRead + AsyncWrite + Unpin + Send + 'static, P: Debug>(
    registry: Arc<Registry<C, S>>,
    mut sock: S,
    peer: P,
) {
    let mut buf = Vec::new();
    let read = tokio::time::timeout(
        HANDSHAKE_TIMEOUT,
        framing::read_request(&mut sock, &mut buf),
    )
    .await;

    let request = match read {
        Ok(Ok(request)) => request,
        Ok(Err(err)) => {
            tracing::debug!(?peer, %err, "dropping a connection that sent no usable request");
            return;
        }
        Err(_) => {
            tracing::debug!(?peer, "dropping a connection that sent no request in time");
            return;
        }
    };

    let key = match crate::request::key_of(&request) {
        Ok(key) => key,
        Err(rejection) => {
            reject(&mut sock, peer, rejection.code(), rejection.reason()).await;
            return;
        }
    };

    // From here the broadcaster owns the answer: it writes the acceptance header itself, as
    // the first thing in flight on the session, or refuses with the venue's own reason. The
    // only case that comes back is the registry declining to take the socket at all.
    if let Err(refused) = registry.subscribe(key, sock) {
        let (mut declined, why) = refused.into_parts();
        reject(
            &mut declined,
            peer,
            framing::RejectCode::Unavailable,
            why,
        )
        .await;
    }
}

/// Best-effort refusal: the client is going away either way, so a failed write is only worth
/// a line in the log.
async fn reject<S: AsyncWrite + Unpin, P: Debug>(
    sock: &mut S,
    peer: P,
    code: framing::RejectCode,
    reason: &str,
) {
    if let Err(err) = framing::write_reject(sock, code, reason).await {
        tracing::debug!(?peer, %err, "could not tell a client why it was refused");
    }
}
