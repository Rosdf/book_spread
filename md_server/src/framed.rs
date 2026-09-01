//! Accepting connections and handshaking them onto a broadcaster.
//!
//! The whole of this module is the *setup* path. Once a client has been handed to its
//! broadcaster ([`crate::broadcast`]) nothing here touches it again: there is no per-client
//! task, and no code between the encoder and the wire. A handshake task lives for one request,
//! one lookup and one hand-off, and then ends.
//!
//! Generic over both ends of that: [`Listener`] for what produces byte streams, and
//! [`Handshaker`] for what turns one into a client that has asked for a symbol. Neither the
//! accept loop nor anything below it names HTTP/2 - see [`crate::grpc`] for the one place that
//! does.

use crate::catalogue::AskedPair;
use crate::client::{ClientHandshake as _, HandshakeError, Handshaker, Route};
use crate::registry::events::RegistryTx;
use crate::transport::{self, Listener};
use bytes::Bytes;
use md_wire::grpc::CatalogueIdx;
use std::fmt::Debug;
use std::time::Duration;
use tokio::task::JoinSet;

/// How long a freshly accepted connection has to send its request.
///
/// A peer that connects and then says nothing costs a task and a file descriptor for as long
/// as it likes, which is a cheap thing to do a great many times. Nothing legitimate needs
/// more than a moment: the request is written immediately after the connect. The whole
/// handshake is inside this - the HTTP/2 preface, the settings exchange, the headers and the
/// one request message - because all of it is the client's to send promptly.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Accepts connections until `stop` resolves, then tears down every handshake still in flight.
///
/// Reaching the end of this function matters for more than tidiness: each handshake task holds
/// a `RegistryTx`, and the registry task only stops - and only then hands the connectors back
/// to [`crate::server::serve`] - once every one of them is gone.
///
/// `catalogue` is the encoded `GetCatalogue` response body. It is loaded once, before the
/// server starts serving, so it is a value this loop holds rather than anything it subscribes
/// to - and the clone handed to each connection is a refcount bump, the same fan-out
/// discipline a book gets.
pub(crate) async fn accept<L: Listener, H: Handshaker<L::Stream>>(
    registry: RegistryTx<H::Client>,
    listener: L,
    handshaker: &'static H,
    catalogue: Bytes,
    stop: oneshot::Receiver<()>,
) {
    let mut in_flight = JoinSet::new();
    // Pinned once rather than awaited in the branch: `select!` polls the same future on every
    // lap, and a fresh one each time would never resolve.
    let mut stopping = std::pin::pin!(async move {
        let _ = stop.await;
    });

    loop {
        tokio::select! {
            accepted = transport::accept(&listener) => match accepted {
                Ok((sock, peer)) => {
                    in_flight.spawn(handshake(
                        registry.clone(),
                        handshaker,
                        catalogue.clone(),
                        sock,
                        peer,
                    ));
                }
                Err(err) => tracing::warn!(%err, "accept failed"),
            },
            () = &mut stopping => break,
            // Reaps finished handshakes as they go, so a long-lived server does not
            // accumulate their join handles. `None` - the set being empty - simply disables
            // this branch.
            Some(_) = in_flight.join_next() => {}
        }
    }

    // Stops accepting before the teardown, so nothing new joins the set.
    drop(listener);
    // Aborted rather than awaited: a handshake is waiting on a client to say something, and
    // `HANDSHAKE_TIMEOUT` is far too long to make shutdown wait for one that never will.
    // Aborting cannot tear anything: the hand-off is a synchronous send onto the registry's
    // queue, so a client has either been queued or not, and a connection dropped
    // mid-handshake is a connection the client must handle being closed anyway. `shutdown`
    // also waits for the aborts to land, which is what releases the last of these
    // `RegistryTx` clones.
    in_flight.shutdown().await;
}

/// Reads one connection's request and answers it - with the catalogue, or by handing the
/// client to the broadcaster that will serve its instrument.
///
/// The connection leaves this function in one of four ways: answered with the catalogue,
/// attached to a broadcaster, refused with a reason, or simply dropped because the client
/// never said anything usable.
async fn handshake<S: Send + 'static, H: Handshaker<S>, P: Debug>(
    registry: RegistryTx<H::Client>,
    handshaker: &H,
    catalogue: Bytes,
    sock: S,
    peer: P,
) {
    let asked = tokio::time::timeout(HANDSHAKE_TIMEOUT, handshaker.handshake(sock)).await;

    let route = match asked {
        Ok(Ok(route)) => route,
        Ok(Err(HandshakeError::Refused(client, rejected))) => {
            tracing::debug!(?peer, %rejected, "refusing a request this server will not serve");
            client.reject(rejected).await;
            return;
        }
        Ok(Err(HandshakeError::Lost)) => {
            tracing::debug!(?peer, "dropping a connection that sent no usable request");
            return;
        }
        Err(_) => {
            tracing::debug!(?peer, "dropping a connection that sent no request in time");
            return;
        }
    };

    let (request, client) = match route {
        Route::Subscribe(request, client) => (request, client),
        // Answered from here rather than through the registry: the catalogue never changes, so
        // there is nothing to ask the registry task about, and this call cannot queue behind a
        // subscribe or a teardown.
        Route::Catalogue(client) => {
            client.respond_unary(catalogue).await;
            return;
        }
    };

    // From here the broadcaster owns the answer: it sends the response headers itself, as the
    // first thing in flight on the stream, or refuses with the venue's own reason. The only
    // case that comes back is the registry declining to take the client at all.
    //
    // A reply that never arrives is the registry having gone away - or an event handler
    // having panicked - with this client in hand. There is nothing to answer on, because
    // there is nothing left to answer *with*; it went with the reply, and dropping it is what
    // closes the connection.
    let asked: Box<[AskedPair]> = request
        .pairs
        .into_iter()
        .map(|pair| AskedPair::new(pair.venue.into(), pair.symbol.into()))
        .collect();
    match registry
        .subscribe(CatalogueIdx::new(request.instrument_idx), asked, client)
        .await
    {
        Ok(Ok(())) => {}
        Ok(Err(refused)) => {
            let (declined, rejected) = refused.into_parts();
            declined.reject(rejected).await;
        }
        Err(_) => tracing::debug!(?peer, "the registry could not answer for a connection"),
    }
}

#[cfg(test)]
mod test {
    use crate::client::mock::{MockHandshaker, scripted};
    use crate::registry::harness::registry_for;
    use crate::test_util::FakeSource;
    use crate::transport::mock::MockListener;
    use bytes::Bytes;
    use md_wire::grpc::CatalogueIdx;
    use std::sync::Arc;

    /// The two symbols the catalogue in these tests carries, at indices 0 and 1.
    const SYMBOLS: [&str; 2] = ["FRAMEDBTCUSDT", "FRAMEDETHUSDT"];

    /// Stands in for the encoded `GetCatalogue` body. What it contains is
    /// [`crate::catalogue::encode`]'s business; what matters here is that the accept loop
    /// hands the same buffer to every connection that asks.
    fn catalogue() -> Bytes {
        Bytes::from_static(b"catalogue")
    }

    /// The accept path end to end, with no transport under it: a connection is accepted, its
    /// request read, and its client handed to the broadcaster that answers on it.
    ///
    /// Two connections rather than one, because the second only gets served if the loop came
    /// back round after the first - which is the part a single accept would not show.
    #[tokio::test]
    async fn a_connection_is_accepted_handshaken_and_answered() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source, &SYMBOLS);
        let (listener, connector) = MockListener::new();
        let (handshaker, script) = scripted();
        let (stop_accepting, stopped) = oneshot::channel();
        let accepting = tokio::spawn(super::accept(
            harness.registry.clone(),
            listener,
            Box::leak(Box::new(handshaker)),
            catalogue(),
            stopped,
        ));

        for idx in 0..2 {
            script.asks_for(
                CatalogueIdx::new(idx),
                &[("binance_spot", SYMBOLS[idx as usize])],
            );
            let _connection = connector.connect();
            let peer = script.next_peer().await;
            peer.accepted()
                .await
                .expect("the instrument is listed and the fake source accepts every symbol");
            peer.opening_snapshot().await;
        }

        assert_eq!(
            source.subscribed(),
            SYMBOLS.to_vec(),
            "each accepted connection reached the registry, under the venue's own casing - the \
             catalogue is what spells a symbol, and nothing normalises it"
        );

        let _ = stop_accepting.send(());
        accepting.await.expect("the accept loop does not panic");
    }

    /// A catalogue call is answered from the buffer the accept loop holds, and never reaches
    /// the registry - which is why it cannot queue behind a subscribe or a teardown.
    #[tokio::test]
    async fn a_catalogue_call_is_answered_without_touching_the_registry() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source, &SYMBOLS);
        let (listener, connector) = MockListener::new();
        let (handshaker, script) = scripted();
        let (stop_accepting, stopped) = oneshot::channel();
        let accepting = tokio::spawn(super::accept(
            harness.registry.clone(),
            listener,
            Box::leak(Box::new(handshaker)),
            catalogue(),
            stopped,
        ));

        script.asks_for_the_catalogue();
        let _connection = connector.connect();
        let peer = script.next_peer().await;
        peer.accepted().await.expect("a catalogue call is answered");
        assert_eq!(
            peer.next_frame().await,
            catalogue(),
            "the caller is given the buffer the accept loop holds, not a re-encode"
        );
        assert!(
            source.subscribed().is_empty(),
            "asking what the server carries must not subscribe anything"
        );

        let _ = stop_accepting.send(());
        accepting.await.expect("the accept loop does not panic");
    }

    /// Shutting the accept loop down abandons the connections whose handshake is still in
    /// flight, rather than holding shutdown up for `HANDSHAKE_TIMEOUT` waiting on a client
    /// that may never speak.
    ///
    /// The silent client is scripted first, so the loop has taken it by the time the talkative
    /// one behind it has been answered - which is what makes this deterministic without any
    /// sleeping or yielding.
    #[tokio::test]
    async fn stopping_abandons_a_handshake_still_in_flight() {
        let source = Arc::new(FakeSource::default());
        let harness = registry_for(&source, &["STILLINFLIGHTBTCUSDT"]);
        let (listener, connector) = MockListener::new();
        let (handshaker, script) = scripted();
        let (stop_accepting, stopped) = oneshot::channel();
        let accepting = tokio::spawn(super::accept(
            harness.registry.clone(),
            listener,
            Box::leak(Box::new(handshaker)),
            catalogue(),
            stopped,
        ));

        script.says_nothing();
        script.asks_for(CatalogueIdx::new(0), &[("binance_spot", "STILLINFLIGHTBTCUSDT")]);
        let _silent = connector.connect();
        let _talkative = connector.connect();

        let peer = script.next_peer().await;
        peer.accepted()
            .await
            .expect("the instrument is listed and the fake source accepts every symbol");

        let _ = stop_accepting.send(());
        accepting.await.expect("the accept loop does not panic");

        assert_eq!(
            source.subscribed(),
            vec!["STILLINFLIGHTBTCUSDT"],
            "a connection that never sent a request must not reach the registry"
        );
    }

    /// `MockHandshaker` is only ever named through `scripted`; this keeps the type from
    /// looking unused to the compiler while staying honest about what it is.
    const _: fn() -> Option<MockHandshaker> = || None;
}
