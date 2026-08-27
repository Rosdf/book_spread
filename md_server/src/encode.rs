//! Turns a book into the bytes that go on the wire, with no intermediate model and no
//! allocation per book.
//!
//! The obvious way to produce an `md.v1.BookUpdate` is to fill a [`BookUpdate`] and
//! call `encode_to_vec`. That costs a malloc/free pair per book, two traversals (one for
//! `encoded_len`, one for the encode), a copy of every level out of the reader's slot into
//! the scratch, and a re-encode of the `venue` and `symbol` strings, which never change for
//! the life of a broadcaster.
//!
//! None of that is necessary, because the message is completely deterministic. Every field
//! tag is `<= 15`, so every key is a single byte; a `Level` is two `double`s, so its body is
//! at most 18 bytes and its length is always a one-byte varint; and a book carries at most
//! [`MAX_DEPTH`] levels a side. So the encoder holds the encoded `venue`/`symbol` fields once
//! as a prefix, and appends the levels straight out of the reader's slot into storage it
//! already owns.
//!
//! That storage is a small pool of whole buffers, not a fresh allocation per book. A frame is
//! a `Bytes` that owns the buffer it was frozen from, so a client that is behind pins the
//! buffer it is holding a frame from; recycling a buffer once every session has released it is
//! what keeps the steady state at zero allocations even while a client is stalled. See
//! [`crate::broadcast`]'s `BufferPool`.
//!
//! # Staying byte-identical to prost
//!
//! This is a second implementation of an encoding prost already implements, so it can drift.
//! What keeps it honest is [`test::the_hand_encoder_agrees_with_prost`], which encodes a
//! spread of books both ways and asserts the bytes are equal. The one rule that is easy to
//! miss is proto3 default elision: prost omits a scalar field equal to its default, so a
//! `double` field that is `0.0` is not written at all. [`PositiveF64`] permits `+0.0`
//! (`is_sign_positive` is true for it), so a level with a zero price or size is reachable and
//! the rule has to be replicated - see [`put_side`].
//!
//! [`BookUpdate`]: md_proto::md::v1::BookUpdate
//! [`PositiveF64`]: core_lib::positive_f64::PositiveF64

use md_wire::framing::LENGTH_PREFIX;
use bytes::{BufMut as _, Bytes, BytesMut};
use core_lib::incremental_book::Level;

/// Levels a side, matching `SmallBook`'s depth. Bounds the size of the reusable buffer, and
/// checked against in [`BookEncoder::encode`] - that bound is what the `expect` there relies
/// on.
const MAX_DEPTH: usize = 10;

/// `BookUpdate.venue`, field 1, wire type 2.
const VENUE_FIELD: u32 = 1;
/// `BookUpdate.symbol`, field 2, wire type 2.
const SYMBOL_FIELD: u32 = 2;
/// `BookUpdate.asks`, field 3, wire type 2: `(3 << 3) | 2`.
const ASKS_KEY: u8 = 0x1A;
/// `BookUpdate.bids`, field 4, wire type 2: `(4 << 3) | 2`.
const BIDS_KEY: u8 = 0x22;
/// `Level.price`, field 1, wire type 1 (64-bit): `(1 << 3) | 1`.
const PRICE_KEY: u8 = 0x09;
/// `Level.size`, field 2, wire type 1 (64-bit): `(2 << 3) | 1`.
const SIZE_KEY: u8 = 0x11;

/// A key byte plus eight little-endian bytes, for each of `price` and `size`.
const MAX_LEVEL_BODY: u8 = 18;
/// What one `Level` costs inside `asks`/`bids`: its key, its one-byte length, and its body.
const MAX_LEVEL_LEN: usize = 2 + MAX_LEVEL_BODY as usize;

/// The levels in the largest possible frame: full depth on both sides. Only the tests need
/// this, to size the buffer for [`BookEncoder::max_frame_len`].
#[cfg(test)]
const MAX_LEVELS_LEN: usize = MAX_LEVEL_LEN * 2 * MAX_DEPTH;

pub(crate) trait BufferProvider {
    fn get_buffer(&mut self, capacity: usize) -> BytesMut;
}

/// Encodes one symbol's books, recycling its storage across every one of them.
///
/// One per broadcaster: the prefix is that broadcaster's identity, so this cannot be shared
/// between symbols.
#[derive(Debug)]
pub(crate) struct BookEncoder {
    /// The encoded `venue` and `symbol` fields, built once. They are the same in every book
    /// this broadcaster ever sends.
    prefix: Box<[u8]>,
}

impl BookEncoder {
    /// `venue` and `symbol` are the identity every book from this broadcaster carries; they
    /// come from its [`Key`](crate::registry::Key), since a book carries no identity itself.
    pub(crate) fn new(venue: &str, symbol: &str) -> Self {
        // Built with prost rather than by hand: this runs once per broadcaster, so there is
        // nothing to win by open-coding it, and borrowing prost's own varint keeps the
        // length prefix of a long symbol correct by construction.
        let mut prefix = Vec::new();
        put_string(&mut prefix, VENUE_FIELD, venue);
        put_string(&mut prefix, SYMBOL_FIELD, symbol);

        Self {
            prefix: prefix.into_boxed_slice(),
        }
    }

    /// Encodes one book into a length-prefixed frame, best level first on each side.
    ///
    /// Both sides empty is the resync signal, and encodes to a frame carrying nothing but the
    /// prefix - which is exactly what prost produces for a `BookUpdate` with two empty
    /// `repeated` fields, so the signal survives as "no levels" rather than as a short read.
    pub(crate) fn encode(&self, asks: &[Level], bids: &[Level], buffers: &mut impl BufferProvider) -> Bytes {
        debug_assert!(asks.len() <= MAX_DEPTH && bids.len() <= MAX_DEPTH, "a book is at most MAX_DEPTH levels a side");

        // An upper bound, not the exact length: only a level with a zero price or size comes
        // out shorter, and asking for a few bytes too many is cheaper than a traversal to find
        // out. The exact length is known once the body is written, and patched in below.
        let needed = LENGTH_PREFIX + self.prefix.len() + MAX_LEVEL_LEN * (asks.len() + bids.len());
        let mut buf = buffers.get_buffer(needed);
        debug_assert!(
            buf.is_empty(),
            "a pooled buffer must be cleared before it is handed back, so a frame always starts at offset 0"
        );

        buf.put_bytes(0, LENGTH_PREFIX);
        buf.put_slice(&self.prefix);
        put_side(&mut buf, ASKS_KEY, asks);
        put_side(&mut buf, BIDS_KEY, bids);

        let body_len = u32::try_from(buf.len() - LENGTH_PREFIX)
            .expect("a book is at most 20 levels, so a frame never approaches 4 GiB");
        buf[..LENGTH_PREFIX].copy_from_slice(&body_len.to_le_bytes());

        buf.freeze()
    }

    /// The largest frame this encoder can produce. Only the tests ask; nothing on the write
    /// path needs to know, since a frame carries its own length.
    #[cfg(test)]
    fn max_frame_len(&self) -> usize {
        LENGTH_PREFIX + self.prefix.len() + MAX_LEVELS_LEN
    }
}

/// Appends one `repeated Level` field, one entry per level.
///
/// The body length is `MAX_LEVEL_BODY` minus 9 for each component prost would elide. That
/// subtraction never fires on a real book - a venue does not quote a zero price or a zero
/// size - so it predicts perfectly, and it is what keeps these bytes equal to prost's.
fn put_side(buf: &mut BytesMut, key: u8, levels: &[Level]) {
    for level in levels {
        let price = level.price().get();
        let size = level.size().get();
        let body_len = MAX_LEVEL_BODY - 9 * u8::from(price == 0.0) - 9 * u8::from(size == 0.0);

        buf.put_u8(key);
        // A one-byte variant: the body is at most 18 bytes, well under the 0x80 continuation
        // threshold.
        buf.put_u8(body_len);
        if price != 0.0 {
            buf.put_u8(PRICE_KEY);
            buf.put_f64_le(price);
        }
        if size != 0.0 {
            buf.put_u8(SIZE_KEY);
            buf.put_f64_le(size);
        }
    }
}

/// Appends one `string` field, skipping it entirely when empty - which is what prost's
/// generated code does for a proto3 scalar holding its default.
fn put_string(out: &mut Vec<u8>, field: u32, value: &str) {
    if value.is_empty() {
        return;
    }
    prost::encoding::string::encode(field, &value.to_owned(), out);
}

#[cfg(test)]
mod test {
    use bytes::BytesMut;
    use super::{BookEncoder, BufferProvider};
    use md_wire::framing::LENGTH_PREFIX;
    use core_lib::incremental_book::Level;
    use core_lib::positive_f64::PositiveF64;
    use md_proto::md::v1 as proto;
    use prost::Message as _;

    struct TestBufferProvider;

    impl BufferProvider for TestBufferProvider {
        fn get_buffer(&mut self, capacity: usize) -> BytesMut {
            BytesMut::with_capacity(capacity)
        }
    }

    fn level(price: f64, size: f64) -> Level {
        Level::new(
            PositiveF64::new(price).expect("test prices are positive"),
            PositiveF64::new(size).expect("test sizes are positive"),
        )
    }

    fn as_proto(levels: &[Level]) -> Vec<proto::Level> {
        levels
            .iter()
            .map(|l| proto::Level {
                price: l.price().get(),
                size: l.size().get(),
            })
            .collect()
    }

    /// The guard on the whole hand-rolled encoder: for every shape of book, the bytes it
    /// produces are the bytes prost would have produced.
    #[test]
    fn the_hand_encoder_agrees_with_prost() {
        let deep: Vec<Level> = (1..=10)
            .map(|i| level(f64::from(i) * 1.5, f64::from(i) / 8.0))
            .collect();
        // Bound before the table so the borrows outlive it: one symbol whose length is the
        // largest one-byte varint, one past it.
        let longest_short = "a".repeat(127);
        let two_byte_len = "b".repeat(200);
        let cases: Vec<(&str, &str, Vec<Level>, Vec<Level>)> = vec![
            // The resync signal: both sides empty.
            ("binance_spot", "btcusdt", Vec::new(), Vec::new()),
            // One side only, which is what a half-populated book looks like.
            (
                "binance_spot",
                "btcusdt",
                vec![level(100.5, 1.25)],
                Vec::new(),
            ),
            (
                "bitstamp",
                "btcusd",
                Vec::new(),
                vec![level(99.5, 2.0), level(99.0, 4.0)],
            ),
            // Full depth both sides: the largest frame.
            ("bitstamp", "ethusd", deep.clone(), deep),
            // `PositiveF64` permits `+0.0`, so prost's default elision is reachable.
            (
                "binance_spot",
                "ethusdt",
                vec![level(0.0, 1.0), level(1.0, 0.0), level(0.0, 0.0)],
                vec![level(f64::MIN_POSITIVE, f64::MAX)],
            ),
            // A symbol long enough that its length is still a one-byte varint, and one past
            // that - the prefix is built by prost, but only this pins that down.
            (
                "binance_spot",
                &longest_short,
                vec![level(1.0, 1.0)],
                Vec::new(),
            ),
            (
                "binance_spot",
                &two_byte_len,
                vec![level(1.0, 1.0)],
                Vec::new(),
            ),
        ];

        for (venue, symbol, asks, bids) in cases {
            let expected = proto::BookUpdate {
                venue: venue.to_owned(),
                symbol: symbol.to_owned(),
                asks: as_proto(&asks),
                bids: as_proto(&bids),
            }
            .encode_to_vec();

            let encoder = BookEncoder::new(venue, symbol);
            let frame = encoder.encode(&asks, &bids, &mut TestBufferProvider);

            let declared = u32::from_le_bytes(
                frame[..LENGTH_PREFIX]
                    .try_into()
                    .expect("the prefix is four bytes"),
            );
            assert_eq!(
                usize::try_from(declared).expect("a frame length fits a usize"),
                frame.len() - LENGTH_PREFIX,
                "the length prefix must describe the body that follows it ({venue}/{symbol})"
            );
            assert_eq!(
                &frame[LENGTH_PREFIX..],
                expected.as_slice(),
                "the hand encoder must produce prost's bytes ({venue}/{symbol})"
            );
        }
    }

    /// Encoding is stateful - one buffer, split repeatedly - so a second book has to come out
    /// the same as if it were the first.
    #[test]
    fn a_reused_encoder_keeps_producing_whole_frames() {
        let encoder = BookEncoder::new("binance_spot", "btcusdt");
        let asks = [level(100.5, 1.25)];
        let bids = [level(99.5, 2.0)];

        let first = encoder.encode(&asks, &bids, &mut TestBufferProvider);
        // Held on to on purpose: a live frame is what keeps the buffer from being reclaimed
        // in place, which is the case where `reserve` has to allocate a fresh chunk.
        let mut held = vec![first.clone()];
        for _ in 0..2_000 {
            held.push(encoder.encode(&asks, &bids, &mut TestBufferProvider));
        }

        assert!(
            held.iter().all(|frame| *frame == first),
            "every frame from one book must be identical, whatever the buffer did underneath"
        );
        assert!(
            first.len() <= encoder.max_frame_len(),
            "the advertised maximum must bound a real frame"
        );
    }
}
