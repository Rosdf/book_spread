//! The gRPC transport: the only place in this crate that knows the wire is HTTP/2.
//!
//! Everything else works against the three traits in [`crate::client`]. This module implements
//! them on top of [`h2`], which is what makes the fan-out possible at all:
//! [`h2::server::Connection::poll_closed`] is a plain poll function that drives the whole
//! connection, so a broadcaster can own its clients' connections and drive them from its own
//! `select!` rather than giving each one a task. A book therefore still crosses no channel and
//! no task boundary between the encoder and the kernel.
//!
//! # What h2 costs, honestly
//!
//! `SendStream::send_data` takes the payload by value and queues the handle, so handing it a
//! `Bytes` is a refcount bump - the same buffer reaches every client, never re-encoded. The one
//! copy left is inside h2: a DATA payload under 256 bytes is copied into the connection's write
//! buffer rather than chained for vectored I/O, so a thin book costs a few-hundred-byte memcpy
//! per client while a full-depth one does not. That is the price of not hand-rolling HTTP/2,
//! and it is a memcpy rather than an allocation or a re-encode.
//!
//! # One stream per connection
//!
//! `max_concurrent_streams(1)`. HTTP/2 would happily multiplex every symbol a client wants onto
//! one connection, but then one broadcaster could not own that connection, and books for
//! different symbols would interleave behind one flow-control window. One stream per connection
//! is what keeps a slow symbol from stalling a fast one - see [`md_wire::grpc`].

mod sink;

use crate::client::{ClientHandshake, HandshakeError, Handshaker, Route};
use bytes::{Bytes, BytesMut};
use h2::RecvStream;
use h2::server::{Connection, SendResponse};
use http::header::{CONTENT_TYPE, HeaderName, HeaderValue};
use http::{HeaderMap, Method, Response, StatusCode};
use md_proto::md::v1::{CatalogueRequest, SubscribeBookRequest};
use md_wire::grpc::{
    CATALOGUE_PATH, CONTENT_TYPE as GRPC_CONTENT_TYPE, CONTENT_TYPE_PREFIX, MAX_MESSAGE_LEN,
    MESSAGE_PREFIX, REJECT_CODE_HEADER, RejectCode, Rejected, SUBSCRIBE_PATH, Status, message_len,
};
use sink::H2Sink;
use std::fmt;
use std::future::poll_fn;
use std::task::Poll;
use tokio::io::{AsyncRead, AsyncWrite};

/// How long a client may take to read its own refusal before the connection is simply dropped.
///
/// A client that will not read its own rejection does not get to hold a shutting-down
/// broadcaster up; it sees a closed connection instead, which it has to handle anyway.
pub(crate) const REJECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

/// `grpc-status`, as a header name built once.
fn grpc_status() -> HeaderName {
    HeaderName::from_static("grpc-status")
}

/// `grpc-message`, as a header name built once.
fn grpc_message() -> HeaderName {
    HeaderName::from_static("grpc-message")
}

/// Accepts connections as `md.v1.MarketData`.
///
/// Holds the connection settings so they are stated once rather than per connection. Every
/// limit here exists to bound what one hostile peer can make this server allocate: h2's own
/// defaults are generous for a general-purpose server, and this one carries messages of a few
/// hundred bytes.
pub(crate) struct H2Handshaker {
    builder: h2::server::Builder,
}

// Hand-written because `h2::server::Builder` is `Debug` but `Connection` - which the types
// below hold - is not, and this keeps all three consistent about saying nothing useful.
impl fmt::Debug for H2Handshaker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("H2Handshaker").finish_non_exhaustive()
    }
}

impl H2Handshaker {
    pub(crate) fn new() -> Self {
        let mut builder = h2::server::Builder::new();
        builder
            // One book per connection, so a second stream is a client misunderstanding the
            // contract rather than something to serve.
            .max_concurrent_streams(1)
            // A request is a venue name and a symbol. Nothing legitimate is larger.
            .max_frame_size(16 * 1024)
            .max_header_list_size(8 * 1024);
        Self { builder }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send + 'static> Handshaker<S> for H2Handshaker {
    type Client = H2Client<S>;

    async fn handshake(
        &self,
        sock: S,
    ) -> Result<Route<Self::Client>, HandshakeError<Self::Client>> {
        let mut conn: Connection<S, Bytes> = self
            .builder
            .handshake(sock)
            .await
            .map_err(|_| HandshakeError::Lost)?;

        // `accept` drives the connection itself, so the settings exchange and the client's
        // HEADERS both happen inside this await.
        // A connection that closed before asking for anything, or failed on the way, leaves
        // nothing to answer on - so there is nothing to say about it either.
        let Some(Ok((request, respond))) = conn.accept().await else {
            return Err(HandshakeError::Lost);
        };

        let (head, mut body) = request.into_parts();
        // Checked before the body is read: a peer that is not speaking this service at all
        // should not get to stream bytes at us first.
        let call = match check_route(&head) {
            Ok(call) => call,
            Err(rejected) => {
                return Err(HandshakeError::Refused(H2Client { conn, respond }, rejected));
            }
        };

        // Both calls read their one message the same way, and a `CatalogueRequest` is empty -
        // so a catalogue call still reads a five-byte header and an empty body, which is what
        // drains the request to END_STREAM before the answer goes out.
        let asked = match call {
            Call::Subscribe => read_request::<S, SubscribeBookRequest>(&mut conn, &mut body)
                .await
                .map(Asked::Subscribe),
            Call::Catalogue => read_request::<S, CatalogueRequest>(&mut conn, &mut body)
                .await
                .map(|CatalogueRequest {}| Asked::Catalogue),
        };

        match asked {
            Ok(Asked::Subscribe(request)) => {
                Ok(Route::Subscribe(request, H2Client { conn, respond }))
            }
            Ok(Asked::Catalogue) => Ok(Route::Catalogue(H2Client { conn, respond })),
            Err(Some(rejected)) => {
                Err(HandshakeError::Refused(H2Client { conn, respond }, rejected))
            }
            Err(None) => Err(HandshakeError::Lost),
        }
    }
}

/// One request, decoded. The same two cases as [`Route`], before there is a client to attach
/// to them - which there is not until the body has been read off `conn`.
#[derive(Debug)]
enum Asked {
    Subscribe(SubscribeBookRequest),
    Catalogue,
}

/// Which of this service's two methods a request is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Call {
    Subscribe,
    Catalogue,
}

/// Checks the request is one of this service's methods, spoken as gRPC.
fn check_route(head: &http::request::Parts) -> Result<Call, Rejected> {
    let unknown = || {
        Rejected::new(
            RejectCode::NotThisService,
            format!("unknown method {} {}", head.method, head.uri.path()).into_boxed_str(),
        )
    };
    if head.method != Method::POST {
        return Err(unknown());
    }
    let call = match head.uri.path() {
        SUBSCRIBE_PATH => Call::Subscribe,
        CATALOGUE_PATH => Call::Catalogue,
        _ => return Err(unknown()),
    };

    // A `content-type` of `application/grpc` may carry a sub-type (`+proto`, `+json`) and
    // parameters after it, so this is a prefix test rather than an equality one. Only proto is
    // actually served, but a client asking for `+json` gets the same answer as one asking for
    // nothing in particular: the response says `+proto` and it can make of that what it will.
    let grpc = head
        .headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with(CONTENT_TYPE_PREFIX));
    if grpc {
        Ok(call)
    } else {
        Err(Rejected::new(
            RejectCode::NotThisService,
            Box::from("content-type must be application/grpc"),
        ))
    }
}

/// Reads the one length-prefixed message the client sends.
///
/// `conn` is polled alongside the body because [`RecvStream::poll_data`] does not drive the
/// connection - h2 expects a task to be doing that, and here there is none. Polling the body
/// first means a frame already buffered is taken without another trip through the connection.
///
/// # Errors
///
/// `Err(Some(_))` for a request that arrived and cannot be served, which is still answerable;
/// `Err(None)` for a connection that ended with nothing to answer on.
async fn read_request<S: AsyncRead + AsyncWrite + Unpin, M: prost::Message + Default>(
    conn: &mut Connection<S, Bytes>,
    body: &mut RecvStream,
) -> Result<M, Option<Rejected>> {
    let malformed = |why: &str| Some(Rejected::new(RejectCode::MalformedRequest, Box::from(why)));

    let mut buf = BytesMut::new();
    let mut announced = None;
    loop {
        // The length is known as soon as the header is in, and is checked against the bound
        // before another byte is accepted - so a five-byte header cannot make this allocate
        // without limit.
        if announced.is_none() && buf.len() >= MESSAGE_PREFIX {
            let header = buf[..MESSAGE_PREFIX]
                .try_into()
                .expect("checked to be at least MESSAGE_PREFIX bytes");
            announced = Some(
                message_len(&header)
                    .ok_or_else(|| malformed("the message is compressed or over the size bound"))?,
            );
        }
        if let Some(body_len) = announced
            && buf.len() >= MESSAGE_PREFIX + body_len
        {
            return M::decode(&buf[MESSAGE_PREFIX..MESSAGE_PREFIX + body_len])
                .map_err(|_| malformed("the body is not a message of this method's type"));
        }

        let received = poll_fn(|cx| match body.poll_data(cx) {
            Poll::Ready(chunk) => Poll::Ready(chunk),
            // Only reached when the body has nothing buffered: driving the connection is what
            // makes the next frame arrive. `Ready` here is the connection being over.
            Poll::Pending => match conn.poll_closed(cx) {
                Poll::Ready(_) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            },
        })
        .await;

        let Some(Ok(chunk)) = received else {
            // The stream ended, or failed, before a whole message arrived. An empty body is
            // the ordinary shape of a peer that hung up; anything else is a truncated one, and
            // neither is answerable in a way the client would read.
            return Err(None);
        };
        if buf.len() + chunk.len() > MESSAGE_PREFIX + MAX_MESSAGE_LEN {
            return Err(malformed("the request is over the size bound"));
        }
        // Releasing as it is consumed keeps the client's window open, which matters only for a
        // request large enough to be split - but costs nothing for one that is not.
        let _ = body.flow_control().release_capacity(chunk.len());
        buf.extend_from_slice(&chunk);
    }
}

/// A client that has asked for a symbol and has not been answered yet.
pub(crate) struct H2Client<S> {
    conn: Connection<S, Bytes>,
    respond: SendResponse<Bytes>,
}

// Hand-written: `h2::server::Connection` is not `Debug`, and the fields worth naming in a log
// line - which symbol, which peer - live on the broadcaster rather than here.
impl<S> fmt::Debug for H2Client<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("H2Client").finish_non_exhaustive()
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send + 'static> ClientHandshake for H2Client<S> {
    type Sink = H2Sink<S>;

    fn accept(mut self) -> Self::Sink {
        let mut response = Response::new(());
        *response.status_mut() = StatusCode::OK;
        response
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static(GRPC_CONTENT_TYPE));

        // `send_response` only queues; the broadcaster's first `poll_progress` flushes it,
        // ahead of the opening snapshot it queues in the same lap. An error here means the
        // client reset the stream between the handshake and now, which the sink reports as
        // `Ended` on its first poll rather than needing a variant of its own.
        let send = self.respond.send_response(response, false).ok();
        H2Sink::new(self.conn, send)
    }

    async fn reject(mut self, rejected: Rejected) {
        // A Trailers-Only response: gRPC's shape for a call that fails before it starts. The
        // status is in the *headers*, and END_STREAM is set on them, so there is no body and
        // no trailers to follow.
        let mut response = Response::new(());
        *response.status_mut() = StatusCode::OK;
        let headers = response.headers_mut();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static(GRPC_CONTENT_TYPE));
        put_status(headers, &rejected);

        if self.respond.send_response(response, true).is_err() {
            return;
        }
        // Queued, not sent: the connection has to be driven for the frame to reach the wire,
        // and this is the only thing that will ever drive it.
        let flushed = tokio::time::timeout(REJECT_TIMEOUT, poll_fn(|cx| self.conn.poll_closed(cx)));
        if flushed.await.is_err() {
            tracing::debug!("could not tell a client why it was refused");
        }
    }

    async fn respond_unary(mut self, body: Bytes) {
        let mut response = Response::new(());
        *response.status_mut() = StatusCode::OK;
        response
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static(GRPC_CONTENT_TYPE));

        let Ok(mut send) = self.respond.send_response(response, false) else {
            return;
        };
        // No reservation dance: one message, sent whole. A caller's initial window is far
        // larger than a catalogue, and one that shrinks it below that is a peer this server
        // owes nothing beyond the timeout below.
        if send.send_data(body, false).is_err() {
            return;
        }
        let mut trailers = HeaderMap::new();
        trailers.insert(
            grpc_status(),
            HeaderValue::from(u16::from(Status::Ok.as_code())),
        );
        if send.send_trailers(trailers).is_err() {
            return;
        }

        // Queued, not sent - the same as a refusal: nothing else will ever drive this
        // connection, so this is what puts the answer on the wire.
        let flushed = tokio::time::timeout(REJECT_TIMEOUT, poll_fn(|cx| self.conn.poll_closed(cx)));
        if flushed.await.is_err() {
            tracing::debug!("could not hand a client the catalogue it asked for");
        }
    }
}

/// Writes a status - and, when it is a refusal, the exact reason - into `headers`.
///
/// Used for both a Trailers-Only refusal and the trailers that end a running stream, because
/// the two carry the same three fields and differ only in where they sit.
fn put_status(headers: &mut HeaderMap, rejected: &Rejected) {
    let status = rejected.code().status();
    headers.insert(
        grpc_status(),
        HeaderValue::from(u16::from(status.as_code())),
    );
    if let Ok(value) = HeaderValue::from_str(&percent_encode(rejected.reason())) {
        headers.insert(grpc_message(), value);
    }
    // The canonical status above is what a client that knows only gRPC sees. This is the
    // detail it cannot carry - in particular whether retrying could ever work.
    headers.insert(
        HeaderName::from_static(REJECT_CODE_HEADER),
        HeaderValue::from(u16::from(rejected.code().as_byte())),
    );
}

/// Percent-encodes a `grpc-message`, as the gRPC wire specification requires.
///
/// Only printable ASCII travels literally; everything else - including the `%` that introduces
/// an escape - goes as `%XX`. Reasons here are built from venue and symbol names, so this
/// almost never has anything to do, but "almost never" is not "never": a symbol reaches this
/// straight off the wire.
fn percent_encode(reason: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";

    let mut out = String::with_capacity(reason.len());
    for byte in reason.bytes() {
        if (0x20..=0x7E).contains(&byte) && byte != b'%' {
            out.push(char::from(byte));
        } else {
            out.push('%');
            out.push(char::from(HEX[usize::from(byte >> 4)]));
            out.push(char::from(HEX[usize::from(byte & 0x0F)]));
        }
    }
    out
}

#[cfg(test)]
mod test {
    use super::{H2Handshaker, REJECT_TIMEOUT};
    use crate::client::{ClientHandshake as _, ClientSink as _, HandshakeError, Handshaker as _, Sent, State};
    use crate::transport::mock::{MockClient, MockStream, mock_pair};
    use bytes::{BufMut as _, Bytes, BytesMut};
    use h2::client::SendRequest;
    use http::header::CONTENT_TYPE;
    use http::{HeaderMap, Method, Request};
    use md_proto::md::v1::SubscribeBookRequest;
    use md_wire::grpc::{
        CATALOGUE_PATH, MESSAGE_PREFIX, REJECT_CODE_HEADER, RejectCode, SUBSCRIBE_PATH,
        put_message_prefix,
    };
    use std::future::{Future, poll_fn};
    use std::task::Poll;
    use std::time::Duration;

    /// A window small enough that one book fills it, so the stalled path is reachable without
    /// publishing a hundred and sixty of them.
    ///
    /// This is the client's *receive* window, which is the client's to choose - exactly as it
    /// is in production. Making it a knob here is what turns "a client that fell behind" into
    /// something a test can arrange in one line.
    const TINY_WINDOW: u32 = 40;

    fn request() -> SubscribeBookRequest {
        SubscribeBookRequest {
            instrument_idx: 7,
            pairs: vec![md_proto::md::v1::SubscribePair {
                venue: "binance_spot".to_owned(),
                symbol: "BTCUSDT".to_owned(),
            }],
        }
    }

    /// One gRPC length-prefixed message, the way the encoder produces one.
    fn framed(message: &impl prost::Message) -> Bytes {
        let body = prost::Message::encode_to_vec(message);
        let mut framed = BytesMut::with_capacity(MESSAGE_PREFIX + body.len());
        framed.put_bytes(0, MESSAGE_PREFIX);
        framed.put_slice(&body);
        let len = u32::try_from(body.len()).expect("a test message is small");
        put_message_prefix(&mut framed[..MESSAGE_PREFIX], len);
        framed.freeze()
    }

    /// An HTTP/2 client on the other end of `io`, with its connection driven by a task of its
    /// own - which is what a real client does, and what this server deliberately does not.
    async fn client_on(io: MockClient, window: u32) -> SendRequest<Bytes> {
        let (send_request, connection) = h2::client::Builder::new()
            .initial_window_size(window)
            .handshake::<_, Bytes>(io)
            .await
            .expect("the server completes the preface");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        send_request
    }

    /// Sends one request and returns the response, whatever it turns out to be.
    fn ask(
        send_request: &mut SendRequest<Bytes>,
        method: Method,
        path: &str,
        content_type: &str,
        body: Bytes,
    ) -> h2::client::ResponseFuture {
        let asked = Request::builder()
            .method(method)
            .uri(path)
            .header(CONTENT_TYPE, content_type)
            .header("te", "trailers")
            .body(())
            .expect("a well-formed request");
        let (response, mut send_body) = send_request
            .send_request(asked, false)
            .expect("the stream opens");
        send_body
            .send_data(body, true)
            .expect("the request body fits the initial window");
        response
    }

    /// The ordinary case: a well-formed `SubscribeBook`.
    fn subscribe(send_request: &mut SendRequest<Bytes>) -> h2::client::ResponseFuture {
        ask(
            send_request,
            Method::POST,
            SUBSCRIBE_PATH,
            "application/grpc+proto",
            framed(&request()),
        )
    }

    /// The subscribe half of a handshake's answer, for a test that expects one.
    fn subscribed(
        handshaken: Result<
            super::Route<super::H2Client<MockStream>>,
            HandshakeError<super::H2Client<MockStream>>,
        >,
    ) -> (SubscribeBookRequest, super::H2Client<MockStream>) {
        match handshaken.expect("the request is well formed") {
            super::Route::Subscribe(request, client) => (request, client),
            super::Route::Catalogue(_) => panic!("this request named the streaming method"),
        }
    }

    /// Runs the server's handshake on `server` while `work` drives the client half.
    ///
    /// Both halves are futures on this one task, so neither makes progress unless the other is
    /// being polled too - which is exactly the arrangement the broadcaster is in.
    async fn handshaking<T>(
        server: MockStream,
        work: impl Future<Output = T>,
    ) -> (
        Result<
            super::Route<super::H2Client<MockStream>>,
            HandshakeError<super::H2Client<MockStream>>,
        >,
        T,
    ) {
        let handshaker = H2Handshaker::new();
        tokio::join!(handshaker.handshake(server), work)
    }

    /// Drives `sink` the way a broadcaster's loop does while `work` runs.
    ///
    /// Nothing on the server side moves unless something polls it: this stands in for the
    /// `poll_sessions` branch of the broadcaster's `select!`.
    async fn while_driving<T>(
        sink: &mut super::sink::H2Sink<MockStream>,
        work: impl Future<Output = T>,
    ) -> T {
        tokio::select! {
            biased;
            out = work => out,
            () = poll_fn(|cx| match sink.poll_progress(cx) {
                State::Running => Poll::Pending,
                State::Ended => Poll::Ready(()),
            }) => panic!("the sink ended before the work finished"),
        }
    }

    /// Every test wait is bounded: a hang here is a bug, not a slow machine.
    async fn deadline<F: Future>(work: F) -> F::Output {
        tokio::time::timeout(Duration::from_secs(5), work)
            .await
            .expect("the exchange completes promptly")
    }

    // The request future is deliberately *not* awaited inside the block: the response only
    // arrives once the server side is being driven, which is what happens after this
    // returns.
    #[expect(
        clippy::async_yields_async,
        reason = "the response future is handed back unawaited on purpose - see above"
    )]
    #[tokio::test]
    async fn a_well_formed_request_is_read_and_the_stream_answered() {
        let (io, server) = mock_pair();
        let (handshaken, response) = deadline(handshaking(server, async {
            let mut send_request = client_on(io, 65_535).await;
            subscribe(&mut send_request)
        }))
        .await;

        let (asked, client) = subscribed(handshaken);
        assert_eq!(asked, request(), "the request survives the wire intact");

        let mut sink = client.accept();
        let payload = Bytes::from_static(b"\x00\x00\x00\x00\x03abc");
        let sent = poll_fn(|cx| Poll::Ready(sink.poll_send(cx, &payload))).await;
        assert_eq!(sent, Sent::Queued);

        let mut body = while_driving(&mut sink, deadline(response))
            .await
            .expect("the response headers arrive")
            .into_body();
        assert_eq!(
            while_driving(&mut sink, deadline(body.data()))
                .await
                .expect("a message arrives")
                .expect("the stream is healthy"),
            payload,
            "what reaches the client is the buffer the broadcaster offered, byte for byte"
        );
    }

    /// The unary half of this service: one message, then `grpc-status: 0` trailers, on a
    /// connection nothing but `respond_unary` will ever drive.
    // The request future is deliberately *not* awaited inside the block: the response only
    // arrives once the server side is being driven, which is what happens after this
    // returns.
    #[expect(
        clippy::async_yields_async,
        reason = "the response future is handed back unawaited on purpose - see above"
    )]
    #[tokio::test]
    async fn a_catalogue_call_is_answered_with_one_message_and_an_ok_status() {
        let (io, server) = mock_pair();
        let (handshaken, response) = deadline(handshaking(server, async {
            let mut send_request = client_on(io, 65_535).await;
            ask(
                &mut send_request,
                Method::POST,
                CATALOGUE_PATH,
                "application/grpc+proto",
                framed(&md_proto::md::v1::CatalogueRequest {}),
            )
        }))
        .await;

        let client = match handshaken.expect("the request is well formed") {
            super::Route::Catalogue(client) => client,
            super::Route::Subscribe(..) => panic!("this request named the unary method"),
        };

        let body = Bytes::from_static(b"\x00\x00\x00\x00\x03abc");
        let (answered, ()) = tokio::join!(deadline(response), client.respond_unary(body.clone()));
        let mut answer = answered.expect("the response headers arrive").into_body();

        assert_eq!(
            deadline(answer.data())
                .await
                .expect("a message arrives")
                .expect("the stream is healthy"),
            body,
            "the caller is given the buffer it was answered with, byte for byte"
        );
        let trailers = deadline(answer.trailers())
            .await
            .expect("the trailers arrive")
            .expect("a unary call ends with a status");
        assert_eq!(
            trailers.get("grpc-status").and_then(|v| v.to_str().ok()),
            Some("0"),
            "a successful unary call ends OK, not merely by closing"
        );
    }

    /// A peer speaking HTTP/2 to the wrong method, or not speaking gRPC at all, is turned away
    /// before it gets to send a body.
    // The request future is deliberately *not* awaited inside the block: the response only
    // arrives once the server side is being driven, which is what happens after this
    // returns.
    #[expect(
        clippy::async_yields_async,
        reason = "the response future is handed back unawaited on purpose - see above"
    )]
    #[tokio::test]
    async fn a_request_this_service_does_not_serve_is_refused() {
        for (method, path, content_type) in [
            (Method::POST, "/md.v1.MarketData/NoSuchMethod", "application/grpc"),
            (Method::GET, SUBSCRIBE_PATH, "application/grpc"),
            (Method::POST, SUBSCRIBE_PATH, "text/plain"),
            (Method::GET, CATALOGUE_PATH, "application/grpc"),
        ] {
            let (io, server) = mock_pair();
            let (handshaken, response) = deadline(handshaking(server, async {
                let mut send_request = client_on(io, 65_535).await;
                ask(
                    &mut send_request,
                    method.clone(),
                    path,
                    content_type,
                    framed(&request()),
                )
            }))
            .await;

            let HandshakeError::Refused(client, rejected) = handshaken.expect_err("refused")
            else {
                panic!("a request that arrived and cannot be served is still answerable");
            };
            assert_eq!(
                rejected.code(),
                RejectCode::NotThisService,
                "for {method} {path} ({content_type})"
            );

            let (headers, ()) = tokio::join!(deadline(response), client.reject(rejected));
            let refusal = headers.expect("a refusal is a response, not a connection error");
            assert_status(refusal.headers(), RejectCode::NotThisService);
            assert!(
                refusal.into_body().is_end_stream(),
                "a Trailers-Only refusal has no body at all"
            );
        }
    }

    /// A body that is not a `SubscribeBookRequest` is a refusal rather than a dropped
    /// connection: the client asked something answerable, it was just wrong.
    // The request future is deliberately *not* awaited inside the block: the response only
    // arrives once the server side is being driven, which is what happens after this
    // returns.
    #[expect(
        clippy::async_yields_async,
        reason = "the response future is handed back unawaited on purpose - see above"
    )]
    #[tokio::test]
    async fn a_malformed_request_body_is_refused_with_a_reason() {
        let (io, server) = mock_pair();
        let (handshaken, _response) = deadline(handshaking(server, async {
            let mut send_request = client_on(io, 65_535).await;
            // A five-byte header announcing three bytes, then three bytes that are not a
            // protobuf message of this shape.
            let mut body = BytesMut::from(&[0u8; MESSAGE_PREFIX][..]);
            put_message_prefix(&mut body[..MESSAGE_PREFIX], 3);
            body.put_slice(&[0xFF, 0xFF, 0xFF]);
            ask(
                &mut send_request,
                Method::POST,
                SUBSCRIBE_PATH,
                "application/grpc",
                body.freeze(),
            )
        }))
        .await;

        let HandshakeError::Refused(_, rejected) = handshaken.expect_err("refused") else {
            panic!("a malformed body is still answerable");
        };
        assert_eq!(rejected.code(), RejectCode::MalformedRequest);
    }

    /// A client that hangs up mid-handshake leaves nothing to answer on, which is not a
    /// refusal - there is nowhere to write one.
    #[tokio::test]
    async fn a_connection_that_says_nothing_is_lost_rather_than_refused() {
        let (io, server) = mock_pair();
        let handshaker = H2Handshaker::new();
        let outcome = deadline(async {
            let (result, ()) = tokio::join!(handshaker.handshake(server), async {
                let send_request = client_on(io, 65_535).await;
                // Connects, completes the preface, and then goes away without asking for
                // anything.
                drop(send_request);
            });
            result
        })
        .await;

        assert!(
            matches!(outcome, Err(HandshakeError::Lost)),
            "a connection with nothing on it is lost, not refused"
        );
    }

    /// The backpressure contract, at the transport: a client whose window is full is told
    /// `Full` rather than blocked or buffered, and gets whatever is current once it reads.
    ///
    /// Buffering instead would be the bug that matters - `send_data` without a reservation
    /// grows without bound - so this asserts the *offer* was refused, not merely that the
    /// delivery was late.
    // The request future is deliberately *not* awaited inside the block: the response only
    // arrives once the server side is being driven, which is what happens after this
    // returns.
    #[expect(
        clippy::async_yields_async,
        reason = "the response future is handed back unawaited on purpose - see above"
    )]
    #[tokio::test]
    async fn a_full_window_refuses_the_offer_and_takes_the_next_one() {
        let (io, server) = mock_pair();
        let (handshaken, response) = deadline(handshaking(server, async {
            let mut send_request = client_on(io, TINY_WINDOW).await;
            subscribe(&mut send_request)
        }))
        .await;

        let (_, client) = subscribed(handshaken);
        let mut sink = client.accept();

        // Fills the window exactly, so the next offer has nowhere to go.
        let first = Bytes::from(vec![b'a'; TINY_WINDOW as usize]);
        assert_eq!(
            poll_fn(|cx| Poll::Ready(sink.poll_send(cx, &first))).await,
            Sent::Queued
        );

        let mut body = while_driving(&mut sink, deadline(response))
            .await
            .expect("the response headers arrive")
            .into_body();
        let delivered = while_driving(&mut sink, deadline(body.data()))
            .await
            .expect("a message arrives")
            .expect("the stream is healthy");
        assert_eq!(delivered.len(), TINY_WINDOW as usize);

        // Read but not released: the client has the bytes, and the server's window for it is
        // still shut.
        let stale = Bytes::from(vec![b'b'; TINY_WINDOW as usize]);
        assert_eq!(
            poll_fn(|cx| Poll::Ready(sink.poll_send(cx, &stale))).await,
            Sent::Full,
            "a shut window must refuse the offer outright rather than buffer it"
        );

        // The client catches up, which is what reopens the window.
        body.flow_control()
            .release_capacity(delivered.len())
            .expect("the capacity was consumed");
        // The broadcaster's lap, in one closure: drive the connection - which is what the
        // peer's WINDOW_UPDATE arrives on - and re-offer the book that is current now.
        let fresh = Bytes::from(vec![b'c'; TINY_WINDOW as usize]);
        let taken = deadline(poll_fn(|cx| {
            assert!(
                !sink.poll_progress(cx).is_ended(),
                "the stream must survive a client that merely fell behind"
            );
            match sink.poll_send(cx, &fresh) {
                Sent::Full => Poll::Pending,
                other => Poll::Ready(other),
            }
        }))
        .await;
        assert_eq!(
            taken,
            Sent::Queued,
            "the book offered once the window reopened is the current one, not the stale one"
        );

        let caught_up = while_driving(&mut sink, deadline(body.data()))
            .await
            .expect("a message arrives")
            .expect("the stream is healthy");
        assert_eq!(
            caught_up, fresh,
            "the client is given what was current when it could take something"
        );
    }

    /// Ending a stream says why, in trailers, rather than dropping the connection.
    // The request future is deliberately *not* awaited inside the block: the response only
    // arrives once the server side is being driven, which is what happens after this
    // returns.
    #[expect(
        clippy::async_yields_async,
        reason = "the response future is handed back unawaited on purpose - see above"
    )]
    #[tokio::test]
    async fn finishing_ends_the_stream_with_its_status() {
        let (io, server) = mock_pair();
        let (handshaken, response) = deadline(handshaking(server, async {
            let mut send_request = client_on(io, 65_535).await;
            subscribe(&mut send_request)
        }))
        .await;

        let (_, client) = subscribed(handshaken);
        let mut sink = client.accept();
        let mut body = while_driving(&mut sink, deadline(response))
            .await
            .expect("the response headers arrive")
            .into_body();

        let why = md_wire::grpc::Rejected::new(RejectCode::ShuttingDown, Box::from("bye now"));
        sink.begin_finish(&why);

        // Flushed by polling, not by `begin_finish` - which is what lets a broadcaster close
        // every client at once under one deadline.
        deadline(poll_fn(|cx| match sink.poll_progress(cx) {
            State::Ended => Poll::Ready(()),
            State::Running => Poll::Pending,
        }))
        .await;

        let trailers = deadline(body.trailers())
            .await
            .expect("the trailers arrive")
            .expect("a stream that ended with a status has trailers");
        assert_status(&trailers, RejectCode::ShuttingDown);
        assert_eq!(
            trailers
                .get("grpc-message")
                .and_then(|value| value.to_str().ok()),
            Some("bye now")
        );
    }

    /// A reason with bytes gRPC will not carry literally has to survive as an escape, not be
    /// dropped - a symbol reaches a refusal straight off the wire.
    #[test]
    fn a_reason_is_percent_encoded() {
        assert_eq!(super::percent_encode("unknown venue \"kraken\""), "unknown venue \"kraken\"");
        assert_eq!(super::percent_encode("100% off"), "100%25 off");
        assert_eq!(super::percent_encode("a\nb"), "a%0Ab");
        assert_eq!(super::percent_encode("é"), "%C3%A9");
    }

    /// Both halves of a status: the canonical code a gRPC-only client reads, and the exact
    /// one only this metadata carries.
    fn assert_status(headers: &HeaderMap, expected: RejectCode) {
        assert_eq!(
            headers.get("grpc-status").and_then(|v| v.to_str().ok()),
            Some(expected.status().as_code().to_string().as_str()),
        );
        assert_eq!(
            headers.get(REJECT_CODE_HEADER).and_then(|v| v.to_str().ok()),
            Some(expected.as_byte().to_string().as_str()),
        );
    }

    /// Named so `REJECT_TIMEOUT` is not merely dead to this module's tests.
    const _: Duration = REJECT_TIMEOUT;
}
