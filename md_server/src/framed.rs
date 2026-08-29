//! Accepting connections and handshaking them onto a broadcaster.
//!
//! The whole of this module is the *setup* path. Once a socket has been handed to its
//! broadcaster ([`crate::broadcast`]) nothing here touches it again: there is no per-client
//! task, and no code between the encoder and the kernel. A handshake task lives for one
//! request, one lookup and one hand-off, and then ends.

use crate::registry::events::RegistryTx;
use crate::transport::{self, Listener};
use md_wire::framing;
use std::fmt::Debug;
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
/// a `RegistryTx`, and the registry task only stops - and only then hands the connectors back
/// to [`crate::server::serve`] - once every one of them is gone.
pub(crate) async fn accept<L: Listener>(
    registry: RegistryTx<L::Stream>,
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
                    handshakes.spawn(handshake(registry.clone(), sock, peer));
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
    // Aborting cannot tear anything: the hand-off is a synchronous send onto the registry's
    // queue, so a socket has either been queued or not, and a connection dropped
    // mid-handshake is a connection the client must handle being closed anyway. `shutdown`
    // also waits for the aborts to land, which is what releases the last of these
    // `RegistryTx` clones.
    handshakes.shutdown().await;
}

/// Reads one connection's request and hands its socket to the broadcaster that will serve it.
///
/// The socket leaves this function in one of three ways: attached to a broadcaster, refused
/// with a reason, or simply dropped because the client never said anything usable.
async fn handshake<S: AsyncRead + AsyncWrite + Unpin + Send + 'static, P: Debug>(
    registry: RegistryTx<S>,
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
    //
    // A reply that never arrives is the registry having gone away - or an event handler
    // having panicked - with this socket in hand. There is nothing to write on it, because
    // there is nothing left to write it *to*; it went with the reply, and dropping it is what
    // closes it.
    match registry.subscribe(key, sock).await {
        Ok(Ok(())) => {}
        Ok(Err(refused)) => {
            let (mut declined, code, why) = refused.into_parts();
            reject(&mut declined, peer, code, why).await;
        }
        Err(_) => tracing::debug!(?peer, "the registry could not answer for a connection"),
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

#[cfg(test)]
mod test {
    use crate::peer::Client;
    use crate::registry::harness::registry_for;
    use crate::test_util::FakeSource;
    use crate::transport::mock::MockListener;
    use crate::venue::Venue;
    use core_lib::venue::test_util::test_instrument_for;
    use std::sync::Arc;

    /// The accept path end to end, with no socket under it: a connection is accepted, its
    /// request read, and its socket handed to the broadcaster that answers on it.
    ///
    /// Two connections rather than one, because the second only gets served if the loop came
    /// back round after the first - which is the part a single accept would not show.
    #[tokio::test]
    async fn a_connection_is_accepted_handshaken_and_answered() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source);
        let (listener, connector) = MockListener::new();
        let (stop_accepting, stopped) = oneshot::channel();
        let accepting = tokio::spawn(super::accept(harness.registry.clone(), listener, stopped));

        for symbol in ["FRAMEDBTCUSDT", "FRAMEDETHUSDT"] {
            let _ = test_instrument_for(Venue::BinanceSpot, symbol);
            let mut client = Client::from_socket(connector.connect());
            client.request("binance_spot", symbol).await;
            client
                .accepted()
                .await
                .expect("the instrument is listed and the fake source accepts every symbol");
            client.opening_snapshot().await;
        }

        assert_eq!(
            source.subscribed(),
            vec!["FRAMEDBTCUSDT", "FRAMEDETHUSDT"],
            "each accepted connection reached the registry, under the wire's own casing - the \
             contract is case-sensitive now, so nothing normalises it"
        );

        let _ = stop_accepting.send(());
        accepting.await.expect("the accept loop does not panic");
    }

    /// Shutting the accept loop down closes the connections whose handshake is still in
    /// flight, rather than holding shutdown up for `HANDSHAKE_TIMEOUT` waiting on a client
    /// that may never speak.
    ///
    /// The silent client connects first, so the loop has taken it by the time the talkative
    /// one behind it has been answered - which is what makes this deterministic without any
    /// sleeping or yielding.
    #[tokio::test]
    async fn stopping_closes_a_handshake_still_in_flight() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source);
        let (listener, connector) = MockListener::new();
        let (stop_accepting, stopped) = oneshot::channel();
        let accepting = tokio::spawn(super::accept(harness.registry.clone(), listener, stopped));

        let _ = test_instrument_for(Venue::BinanceSpot, "STILLINFLIGHTBTCUSDT");
        let mut silent = Client::from_socket(connector.connect());
        let mut talkative = Client::from_socket(connector.connect());
        talkative
            .request("binance_spot", "STILLINFLIGHTBTCUSDT")
            .await;
        talkative
            .accepted()
            .await
            .expect("the instrument is listed and the fake source accepts every symbol");

        let _ = stop_accepting.send(());
        accepting.await.expect("the accept loop does not panic");

        silent.ended().await;
        assert_eq!(
            source.subscribed(),
            vec!["STILLINFLIGHTBTCUSDT"],
            "a connection that never sent a request must not reach the registry"
        );
    }
}
