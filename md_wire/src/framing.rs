//! The length-prefixed framing that carries `md.v1` over a plain TCP connection.
//!
//! # The protocol
//!
//! - **Request**: a `u32` little-endian length, then a [`SubscribeBookRequest`].
//! - **Response header**: a `u32` little-endian length, then a [`RejectCode`] byte and a
//!   UTF-8 reason. A *zero* length means accepted, and nothing follows it.
//! - **Stream**: a `u32` little-endian length, then a `BookUpdate`, repeating.
//! - **End of stream**: the server closes the socket. Both sides of a book being empty
//!   already means "the connector is resyncing" (see `SmallBook::is_empty`), so a close is
//!   unambiguously the stream being over rather than a book with nothing in it.
//!
//! # One book per connection
//!
//! A request names its pairs, and today only the first is served; a client that wants three
//! symbols still opens three connections. That is what lets a broadcaster own the write half
//! of each socket outright, with no mutex, no interleaving between symbols and no
//! head-of-line blocking between them - which is the point of the whole exercise. It is also
//! ordinary for a market-data feed.
//!
//! [`SubscribeBookRequest`]: md_proto::md::v1::SubscribeBookRequest

use md_proto::md::v1::SubscribeBookRequest;
use prost::Message as _;
use std::io;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};

/// Bytes of little-endian `u32` length in front of every message.
///
/// A book's prefix is written into the same buffer as its body by
/// [`BookEncoder::encode`](crate::encode::BookEncoder::encode), so a session hands the whole
/// frame to one `write` rather than assembling it per client.
pub const LENGTH_PREFIX: usize = 4;

/// The largest frame either side will read.
///
/// Every message this protocol carries is tiny - a request is a venue name and a symbol, a
/// book is at most twenty levels - so this is not a capacity limit but a bound on what a
/// hostile peer can make the other end allocate off a four-byte header.
pub const MAX_FRAME_LEN: usize = 4 * 1024;

/// Why a subscription was turned down. The wire form is the discriminant byte.
///
/// Two codes rather than a copy of gRPC's status set: the client's only real decision is
/// whether retrying could ever help.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RejectCode {
    /// The request itself is wrong - an unknown venue, a malformed symbol. Retrying it
    /// verbatim will fail the same way.
    InvalidArgument = 1,
    /// The request is well formed but cannot be served: the venue does not list the symbol,
    /// the connector is gone, or the server is shutting down. Retrying may work later.
    Unavailable = 2,
}

impl RejectCode {
    fn as_byte(self) -> u8 {
        self as u8
    }

    fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            1 => Some(Self::InvalidArgument),
            2 => Some(Self::Unavailable),
            _ => None,
        }
    }
}

/// A server's refusal, as the client sees it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{reason}")]
pub struct Rejected {
    code: RejectCode,
    reason: Box<str>,
}

impl Rejected {
    pub fn new(code: RejectCode, reason: Box<str>) -> Self {
        Self { code, reason }
    }

    pub fn code(&self) -> RejectCode {
        self.code
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }
}

/// Why one frame could not be read.
///
/// [`Closed`](ReadFrameError::Closed) is separated out because it is the ordinary end of a
/// connection - the peer hung up - rather than a fault worth logging as one.
#[derive(Debug, thiserror::Error)]
pub enum ReadFrameError {
    #[error("the peer closed the connection")]
    Closed,

    #[error("reading a frame: {0}")]
    Io(#[from] io::Error),

    #[error("a frame of {len} bytes exceeds the {MAX_FRAME_LEN} byte limit")]
    TooLarge { len: usize },
}

/// Why a response header could not be read.
#[derive(Debug, thiserror::Error)]
pub enum ReadResponseError {
    #[error(transparent)]
    Frame(#[from] ReadFrameError),

    #[error("the response header is empty of everything but its status byte")]
    Truncated,

    #[error("unknown status byte {byte}")]
    UnknownCode { byte: u8 },

    #[error("the rejection reason is not UTF-8")]
    Reason(#[from] std::string::FromUtf8Error),
}

/// Reads one length-prefixed frame, replacing whatever `body` held.
///
/// `body` is the caller's buffer so a connection can read frame after frame without
/// allocating for each one.
///
/// # Errors
///
/// [`ReadFrameError::Closed`] when the peer hung up between frames, and the other variants
/// for a short, oversized or failed read.
pub async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    body: &mut Vec<u8>,
) -> Result<(), ReadFrameError> {
    let mut prefix = [0u8; LENGTH_PREFIX];
    if let Err(err) = reader.read_exact(&mut prefix).await {
        return Err(if err.kind() == io::ErrorKind::UnexpectedEof {
            ReadFrameError::Closed
        } else {
            ReadFrameError::Io(err)
        });
    }

    let len = usize::try_from(u32::from_le_bytes(prefix))
        .map_err(|_| ReadFrameError::TooLarge { len: usize::MAX })?;
    if len > MAX_FRAME_LEN {
        return Err(ReadFrameError::TooLarge { len });
    }

    body.clear();
    body.resize(len, 0);
    reader.read_exact(body.as_mut_slice()).await?;
    Ok(())
}

/// Writes `body` behind its length prefix, as a single write.
///
/// One buffer rather than a write per part: with Nagle disabled, two writes are two packets,
/// and the whole point of this transport is not to add latency of its own.
///
/// # Errors
///
/// Whatever the underlying stream reports.
pub async fn write_frame<W: AsyncWrite + Unpin>(writer: &mut W, body: &[u8]) -> io::Result<()> {
    let len = u32::try_from(body.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "a frame longer than u32::MAX cannot be length-prefixed",
        )
    })?;

    let mut framed = Vec::with_capacity(LENGTH_PREFIX + body.len());
    framed.extend_from_slice(&len.to_le_bytes());
    framed.extend_from_slice(body);
    writer.write_all(&framed).await
}

/// Sends a subscription request.
///
/// # Errors
///
/// Whatever the underlying stream reports.
pub async fn write_request<W: AsyncWrite + Unpin>(
    writer: &mut W,
    request: &SubscribeBookRequest,
) -> io::Result<()> {
    write_frame(writer, &request.encode_to_vec()).await
}

/// Reads a subscription request.
///
/// # Errors
///
/// A read failure, or a body that is not a `SubscribeBookRequest`.
pub async fn read_request<R: AsyncRead + Unpin>(
    reader: &mut R,
    body: &mut Vec<u8>,
) -> Result<SubscribeBookRequest, ReadRequestError> {
    read_frame(reader, body).await?;
    Ok(SubscribeBookRequest::decode(body.as_slice())?)
}

/// Why a subscription request could not be read.
#[derive(Debug, thiserror::Error)]
pub enum ReadRequestError {
    #[error(transparent)]
    Frame(#[from] ReadFrameError),

    #[error("the request is not a SubscribeBookRequest: {0}")]
    Decode(#[from] prost::DecodeError),
}

/// Accepts the subscription: an empty frame, and then books.
///
/// # Errors
///
/// Whatever the underlying stream reports.
pub async fn write_accept<W: AsyncWrite + Unpin>(writer: &mut W) -> io::Result<()> {
    write_frame(writer, &[]).await
}

/// Turns the subscription down and says why. Nothing follows on the connection.
///
/// # Errors
///
/// Whatever the underlying stream reports.
pub async fn write_reject<W: AsyncWrite + Unpin>(
    writer: &mut W,
    code: RejectCode,
    reason: &str,
) -> io::Result<()> {
    let mut body = Vec::with_capacity(1 + reason.len());
    body.push(code.as_byte());
    body.extend_from_slice(reason.as_bytes());
    write_frame(writer, &body).await
}

/// Reads the response header. `Ok(())` means the stream is about to start.
///
/// # Errors
///
/// [`ReadResponseError`] for a malformed header, or the server's own [`Rejected`] reason.
pub async fn read_response<R: AsyncRead + Unpin>(
    reader: &mut R,
    body: &mut Vec<u8>,
) -> Result<Result<(), Rejected>, ReadResponseError> {
    read_frame(reader, body).await?;

    let Some((&byte, reason)) = body.split_first() else {
        return Ok(Ok(()));
    };
    let code = RejectCode::from_byte(byte).ok_or(ReadResponseError::UnknownCode { byte })?;
    if reason.is_empty() {
        return Err(ReadResponseError::Truncated);
    }

    Ok(Err(Rejected::new(code, String::from_utf8(reason.to_vec())?.into_boxed_str())))
}

#[cfg(test)]
mod test {
    use super::{
        MAX_FRAME_LEN, ReadFrameError, RejectCode, read_frame, read_request, read_response,
        write_accept, write_reject, write_request,
    };
    use md_proto::md::v1::{Pair, SubscribeBookRequest};

    fn request() -> SubscribeBookRequest {
        SubscribeBookRequest {
            pairs: vec![Pair {
                venue: "binance_spot".to_owned(),
                symbol: "BTCUSDT".to_owned(),
            }],
        }
    }

    /// Both halves of the handshake, over a duplex pipe: what one side writes is what the
    /// other reads back.
    #[tokio::test]
    async fn a_request_and_an_acceptance_round_trip() {
        let (mut client, mut server) = tokio::io::duplex(64);
        let mut buf = Vec::new();

        write_request(&mut client, &request())
            .await
            .expect("the pipe accepts the request");
        assert_eq!(
            read_request(&mut server, &mut buf)
                .await
                .expect("the request decodes"),
            request()
        );

        write_accept(&mut server)
            .await
            .expect("the pipe accepts the header");
        read_response(&mut client, &mut buf)
            .await
            .expect("the header is well formed")
            .expect("an empty header is an acceptance");
    }

    #[tokio::test]
    async fn a_rejection_carries_its_code_and_reason() {
        let (mut client, mut server) = tokio::io::duplex(64);
        let mut buf = Vec::new();

        write_reject(&mut server, RejectCode::InvalidArgument, "unknown venue \"kraken\"")
            .await
            .expect("the pipe accepts the header");

        let rejected = read_response(&mut client, &mut buf)
            .await
            .expect("the header is well formed")
            .expect_err("a non-empty header is a rejection");
        assert_eq!(rejected.code(), RejectCode::InvalidArgument);
        assert_eq!(rejected.reason(), "unknown venue \"kraken\"");
    }

    /// A peer hanging up between frames is the ordinary end of a stream, and has to be
    /// distinguishable from a frame that was cut short.
    #[tokio::test]
    async fn a_closed_connection_reads_as_closed_not_as_an_error() {
        let (client, mut server) = tokio::io::duplex(64);
        drop(client);

        let err = read_frame(&mut server, &mut Vec::new())
            .await
            .expect_err("there is nothing to read");
        assert!(matches!(err, ReadFrameError::Closed), "got {err:?}");
    }

    /// The bound exists so a four-byte header cannot make the other end allocate without
    /// limit; it has to be enforced before the body is read, not after.
    #[tokio::test]
    async fn an_oversized_length_is_refused_before_anything_is_allocated() {
        let (mut client, mut server) = tokio::io::duplex(64);
        let announced = u32::try_from(MAX_FRAME_LEN + 1).expect("the bound fits a u32");
        tokio::io::AsyncWriteExt::write_all(&mut client, &announced.to_le_bytes())
            .await
            .expect("the pipe accepts four bytes");

        let err = read_frame(&mut server, &mut Vec::new())
            .await
            .expect_err("the announced length is over the bound");
        assert!(
            matches!(err, ReadFrameError::TooLarge { len } if len == MAX_FRAME_LEN + 1),
            "got {err:?}"
        );
    }
}
