use bytes::Bytes;
use futures_util::{Sink, Stream};
use reqwest::IntoUrl;
use tokio_tungstenite::tungstenite::Message;

pub trait Response: Sized + Send + 'static {
    type Error: std::error::Error + Send + 'static;

    /// Turns an HTTP error status into an `Err`, leaving the response otherwise untouched.
    ///
    /// # Errors
    ///
    /// Whatever the client reports for a non-success status.
    fn error_for_status(self) -> Result<Self, Self::Error>;
    fn bytes(self) -> impl Future<Output = Result<Bytes, Self::Error>> + Send + 'static;
}

pub trait RequestBuilder: Send + 'static {
    type Response: Response;
    type Error: std::error::Error + Send + 'static;

    fn send(self) -> impl Future<Output = Result<Self::Response, Self::Error>> + Send + 'static;
}

pub trait RestClient: Clone + Send + Sync + 'static {
    type Builder: RequestBuilder;

    fn get(&self, url: impl IntoUrl) -> Self::Builder;
}

impl Response for reqwest::Response {
    type Error = reqwest::Error;

    fn error_for_status(self) -> Result<Self, Self::Error> {
        self.error_for_status()
    }

    fn bytes(self) -> impl Future<Output = Result<Bytes, Self::Error>> + Send + 'static {
        self.bytes()
    }
}

impl RequestBuilder for reqwest::RequestBuilder {
    type Response = reqwest::Response;
    type Error = reqwest::Error;

    fn send(self) -> impl Future<Output = Result<Self::Response, Self::Error>> + Send + 'static {
        self.send()
    }
}

impl RestClient for reqwest::Client {
    type Builder = reqwest::RequestBuilder;

    fn get(&self, url: impl IntoUrl) -> Self::Builder {
        self.get(url)
    }
}

/// A connector that opens a new WebSocket stream on demand.
///
/// Mirrors [`RestClient`], collapsed to one trait: unlike an HTTP request, connecting has no
/// separate "builder" phase to send later - `connect` is the whole operation.
pub trait WsConnector: Clone + Send + Sync + 'static {
    /// A single duplex connection, already usable with `futures_util::StreamExt::split`.
    type Stream: Sink<Message, Error = Self::Error>
        + Stream<Item = Result<Message, Self::Error>>
        + Unpin
        + Send
        + 'static;

    type Error: std::error::Error + Send + 'static;

    fn connect(&self, url: &str) -> impl Future<Output = Result<Self::Stream, Self::Error>> + Send;
}

/// The real transport: opens a TLS websocket via `tokio-tungstenite`.
#[derive(Debug, Clone, Copy, Default)]
pub struct TungsteniteConnector;

impl WsConnector for TungsteniteConnector {
    type Stream = tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >;
    type Error = tokio_tungstenite::tungstenite::Error;

    async fn connect(&self, url: &str) -> Result<Self::Stream, Self::Error> {
        let (stream, _response) = tokio_tungstenite::connect_async(url).await?;
        Ok(stream)
    }
}
