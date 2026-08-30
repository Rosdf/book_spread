//! The gRPC contract for `md.v1.MarketData`, shared by the server and its clients.
//!
//! # The protocol
//!
//! Ordinary gRPC over HTTP/2, so most of it is not spelled out here - h2 on the server side
//! and tonic on the client side own the frames. What this module holds is the handful of
//! constants and mappings both ends have to agree on:
//!
//! - **Paths**: [`SUBSCRIBE_PATH`] and [`CATALOGUE_PATH`], `POST`, with a `content-type`
//!   starting [`CONTENT_TYPE_PREFIX`].
//! - **Request**: one length-prefixed `md.v1.SubscribeBookRequest` - or
//!   `md.v1.CatalogueRequest`, which is empty - then `END_STREAM`.
//! - **Stream**: one length-prefixed `BookUpdate` per DATA payload, repeating. A catalogue is
//!   a single message, followed by `grpc-status: 0` trailers.
//! - **End of stream**: trailers carrying `grpc-status`.
//! - **Refusal**: a Trailers-Only response - `grpc-status`, `grpc-message`, and the exact
//!   [`RejectCode`] in [`REJECT_CODE_HEADER`].
//!
//! # Identity is numeric
//!
//! A client asks [`CATALOGUE_PATH`] what this server carries and gets back two tables: venue
//! index -> venue name, and instrument index -> the pairs under it. Everything after that
//! travels as an index - a [`CatalogueIdx`] on a subscribe, a [`VenueIdx`] on every level -
//! so no venue name and no symbol is spelled out on the hot path.
//!
//! # One book per stream
//!
//! A request names one instrument, and every pair under it is validated while only the first
//! is served; a client that wants three symbols opens three streams. HTTP/2 would happily
//! multiplex them onto one connection, but the server keeps one stream per connection so that
//! a broadcaster owns its clients outright, with no mutex, no interleaving between symbols and
//! no head-of-line blocking between them - which is the point of the whole exercise. It is
//! also ordinary for a market-data feed.
//!
//! # Why the reject codes are not just gRPC status codes
//!
//! The eight [`RejectCode`]s say more than the canonical status codes do - in particular
//! [`RejectCode::retryable`] is not derivable from the status alone, since `NOT_FOUND` for an
//! instrument this server does not carry is permanent while `NOT_FOUND` for one whose venue
//! has not listed its symbol yet is not. So both travel: the canonical code in `grpc-status`,
//! for clients that only understand gRPC, and the exact one in [`REJECT_CODE_HEADER`], for
//! clients that want the detail.

/// An instrument's index in the catalogue.
///
/// Assigned by the server's catalogue - a config file, not this process - and unrelated to
/// `core_lib`'s `InstrumentId`, which is issued by the instrument registry as symbols are
/// interned. A type of its own rather than a bare `u32` so it cannot be swapped with a
/// [`VenueIdx`] at a call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CatalogueIdx(u32);

impl CatalogueIdx {
    #[must_use]
    pub const fn new(idx: u32) -> Self {
        Self(idx)
    }

    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// A venue's index in the catalogue's venue table.
///
/// What a `Level` carries instead of a venue name, and what a catalogue pair names its venue
/// by. See [`CatalogueIdx`] for why this is a type rather than a `u32`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct VenueIdx(u32);

impl VenueIdx {
    #[must_use]
    pub const fn new(idx: u32) -> Self {
        Self(idx)
    }

    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// Bytes of gRPC message header in front of every message: one compression flag, then a
/// big-endian `u32` length.
///
/// A book's header is written into the same buffer as its body by `md_server`'s `BookEncoder`,
/// so a session hands h2 one whole length-prefixed message rather than assembling it per
/// client.
pub const MESSAGE_PREFIX: usize = 5;

/// The compression flag for a message that is not compressed. This server never compresses:
/// a book is a few hundred bytes and the latency of a compressor is worth more than the
/// bandwidth it would save.
pub const UNCOMPRESSED: u8 = 0;

/// The largest message either side will read.
///
/// A request is one index and a book is at most twenty levels, so this is not a capacity limit
/// but a bound on what a hostile peer can make the other end allocate off a five-byte header.
/// A catalogue is the one message that can approach it, since it spells out every instrument
/// this server carries.
pub const MAX_MESSAGE_LEN: usize = 4 * 1024;

/// The streaming method: one book per stream.
pub const SUBSCRIBE_PATH: &str = "/md.v1.MarketData/SubscribeBook";

/// The unary method: what this server carries.
pub const CATALOGUE_PATH: &str = "/md.v1.MarketData/GetCatalogue";

/// What the server sends back. Clients may send this or the bare `application/grpc`, or
/// either with parameters after it, so requests are matched against
/// [`CONTENT_TYPE_PREFIX`] rather than against this.
pub const CONTENT_TYPE: &str = "application/grpc+proto";

/// What a request's `content-type` must start with to be gRPC at all.
pub const CONTENT_TYPE_PREFIX: &str = "application/grpc";

/// Carries the exact [`RejectCode`] alongside the canonical `grpc-status`.
///
/// Lower-case because HTTP/2 header names are, and h2 rejects an upper-case one outright.
pub const REJECT_CODE_HEADER: &str = "md-reject-code";

/// The gRPC status codes this server sends. The wire form is the decimal discriminant, in
/// the `grpc-status` trailer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Status {
    Ok = 0,
    InvalidArgument = 3,
    NotFound = 5,
    FailedPrecondition = 9,
    Unimplemented = 12,
    Unavailable = 14,
}

impl Status {
    /// The decimal `grpc-status` value.
    pub fn as_code(self) -> u8 {
        self as u8
    }
}

/// Why a subscription was turned down. The wire form is the decimal discriminant, in the
/// [`REJECT_CODE_HEADER`] metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RejectCode {
    /// The request named an instrument index the catalogue does not carry. Nothing about it
    /// will change: the catalogue is loaded once, at startup.
    UnknownInstrument = 1,
    /// The catalogue carries the instrument, but the venue's connector has not interned its
    /// symbol yet - so there is nothing to subscribe on. A connector that has not caught up,
    /// rather than a symbol that does not exist.
    UnlistedSymbol = 2,
    /// The server is shutting down.
    ShuttingDown = 3,
    /// The connector answered the subscribe with a refusal.
    ConnectorRefused = 4,
    /// The connector went away before it could answer.
    ConnectorGone = 5,
    /// The broadcaster's book stream ended while joins were still queued.
    StreamEnded = 6,
    /// The request was not one of this service's methods, or not gRPC at all.
    NotThisService = 7,
    /// The request body was not a well-formed message of the method's type.
    MalformedRequest = 8,
}

impl RejectCode {
    pub fn as_byte(self) -> u8 {
        self as u8
    }

    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            1 => Some(Self::UnknownInstrument),
            2 => Some(Self::UnlistedSymbol),
            3 => Some(Self::ShuttingDown),
            4 => Some(Self::ConnectorRefused),
            5 => Some(Self::ConnectorGone),
            6 => Some(Self::StreamEnded),
            7 => Some(Self::NotThisService),
            8 => Some(Self::MalformedRequest),
            _ => None,
        }
    }

    /// Whether retrying the same request verbatim could ever succeed.
    pub fn retryable(self) -> bool {
        matches!(
            self,
            Self::UnlistedSymbol
                | Self::ShuttingDown
                | Self::ConnectorRefused
                | Self::ConnectorGone
                | Self::StreamEnded
        )
    }

    /// The canonical gRPC status a client that knows nothing of [`REJECT_CODE_HEADER`] sees.
    pub fn status(self) -> Status {
        match self {
            Self::MalformedRequest => Status::InvalidArgument,
            Self::UnknownInstrument | Self::UnlistedSymbol => Status::NotFound,
            Self::ConnectorRefused => Status::FailedPrecondition,
            Self::ShuttingDown | Self::ConnectorGone | Self::StreamEnded => Status::Unavailable,
            Self::NotThisService => Status::Unimplemented,
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

    /// Splits into the parts a Trailers-Only response is built from.
    pub fn into_parts(self) -> (RejectCode, Box<str>) {
        (self.code, self.reason)
    }
}

/// Writes a message header for a body of `len` bytes into the first [`MESSAGE_PREFIX`] bytes
/// of `at`.
///
/// Separate from the encoder so the two places that produce a message - the hot-path book
/// encoder, which reserves the header and patches it once the body's length is known, and
/// anything encoding a one-off - cannot disagree about the byte order. gRPC's length is
/// *big*-endian, unlike protobuf's own varints, which is exactly the kind of detail worth
/// having in one place.
///
/// # Panics
///
/// If `at` is shorter than [`MESSAGE_PREFIX`].
pub fn put_message_prefix(at: &mut [u8], len: u32) {
    at[0] = UNCOMPRESSED;
    at[1..MESSAGE_PREFIX].copy_from_slice(&len.to_be_bytes());
}

/// Reads a message header, returning the body length that follows it.
///
/// `None` when the peer set the compression flag - nothing here ever compresses, so a set
/// flag is a message this end cannot read - or when the announced length is over
/// [`MAX_MESSAGE_LEN`].
pub fn message_len(header: &[u8; MESSAGE_PREFIX]) -> Option<usize> {
    if header[0] != UNCOMPRESSED {
        return None;
    }
    let announced = u32::from_be_bytes([header[1], header[2], header[3], header[4]]);
    let len = usize::try_from(announced).ok()?;
    (len <= MAX_MESSAGE_LEN).then_some(len)
}

#[cfg(test)]
mod test {
    use super::{
        CatalogueIdx, MAX_MESSAGE_LEN, MESSAGE_PREFIX, RejectCode, Status, VenueIdx, message_len,
        put_message_prefix,
    };

    const ALL_CODES: [RejectCode; 8] = [
        RejectCode::UnknownInstrument,
        RejectCode::UnlistedSymbol,
        RejectCode::ShuttingDown,
        RejectCode::ConnectorRefused,
        RejectCode::ConnectorGone,
        RejectCode::StreamEnded,
        RejectCode::NotThisService,
        RejectCode::MalformedRequest,
    ];

    /// The metadata header is the only thing carrying the exact code, so it has to survive a
    /// round trip through its byte for every variant - a client reads `retryable()` off the
    /// far side of it.
    #[test]
    fn every_reject_code_round_trips_through_its_byte() {
        for code in ALL_CODES {
            assert_eq!(RejectCode::from_byte(code.as_byte()), Some(code));
        }
        assert_eq!(RejectCode::from_byte(0), None);
        assert_eq!(RejectCode::from_byte(9), None);
    }

    /// A refusal is never `Ok`: a client that only reads `grpc-status` must not see a
    /// rejection as a stream that simply ended.
    #[test]
    fn no_reject_code_maps_to_ok() {
        for code in ALL_CODES {
            assert_ne!(code.status(), Status::Ok, "for {code:?}");
        }
    }

    /// Retryability is the part `grpc-status` cannot express on its own, which is why
    /// [`super::REJECT_CODE_HEADER`] exists at all - `FAILED_PRECONDITION` for a connector
    /// that was not ready is worth retrying and `INVALID_ARGUMENT` for a body that is not a
    /// message of this method's type never is.
    ///
    /// What must still hold is that the two never contradict each other: a code worth
    /// retrying must not arrive as a status every gRPC client reads as permanent. `NOT_FOUND`
    /// is deliberately not on that list: the two codes carrying it - an instrument the
    /// catalogue does not have and one whose symbol its venue has not listed yet - differ in
    /// exactly this, which is the case that makes the metadata worth carrying.
    #[test]
    fn no_retryable_code_arrives_as_a_permanent_status() {
        const PERMANENT: [Status; 2] = [Status::InvalidArgument, Status::Unimplemented];

        for code in ALL_CODES {
            assert!(
                !(code.retryable() && PERMANENT.contains(&code.status())),
                "{code:?} is worth retrying but arrives as {:?}, which a client that reads \
                 only `grpc-status` will treat as final",
                code.status()
            );
        }
    }

    /// gRPC's length is big-endian, unlike every other length in protobuf. Asserting the
    /// exact bytes is what stops that being "fixed" back to little-endian - a two-byte length
    /// is enough to tell the two apart, and stays inside [`MAX_MESSAGE_LEN`].
    #[test]
    fn a_message_prefix_is_a_flag_byte_and_a_big_endian_length() {
        let mut header = [0xFFu8; MESSAGE_PREFIX];
        put_message_prefix(&mut header, 0x0102);
        assert_eq!(header, [0x00, 0x00, 0x00, 0x01, 0x02]);
        assert_eq!(message_len(&header), Some(0x0102));
    }

    #[test]
    fn a_compressed_or_oversized_message_is_refused_before_anything_is_allocated() {
        let mut header = [0u8; MESSAGE_PREFIX];
        put_message_prefix(&mut header, 1);
        header[0] = 1;
        assert_eq!(message_len(&header), None, "the compression flag is set");

        let over = u32::try_from(MAX_MESSAGE_LEN + 1).expect("the bound fits a u32");
        put_message_prefix(&mut header, over);
        assert_eq!(message_len(&header), None, "over the length bound");
    }

    /// Both indices are `u32` on the wire and mean entirely different things. This is what
    /// they are for: the value goes in and comes back, and the two types never mix.
    #[test]
    fn an_index_carries_its_value_and_nothing_else() {
        assert_eq!(CatalogueIdx::new(7).get(), 7);
        assert_eq!(VenueIdx::new(0).get(), 0);
        assert_ne!(CatalogueIdx::new(1), CatalogueIdx::new(2));
    }
}
