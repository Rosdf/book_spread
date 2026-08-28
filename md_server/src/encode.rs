//! Turns a book into the bytes that go on the wire, with no intermediate model and no
//! allocation per book.
//!
//! The obvious way to produce an `md.v1.BookUpdate` is to fill a [`BookUpdate`] and
//! call `encode_to_vec`. That costs a malloc/free pair per book, two traversals (one for
//! `encoded_len`, one for the encode), a copy of every level out of the reader's slot into
//! the scratch, and a re-encode of the `venue` string on every level, which never changes for
//! the life of a broadcaster.
//!
//! None of that is necessary, because the message is completely deterministic. Every field
//! tag is `<= 15`, so every key is a single byte; a book carries at most [`MAX_DEPTH`] levels
//! a side; and this broadcaster's venue never changes, so its encoded `Level.venue` suffix can
//! be built once and appended after each level's two doubles. So the encoder holds that suffix
//! once and appends the levels straight out of the reader's slot into storage it already owns.
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
//! the rule has to be replicated - see [`put_side`]. The same rule applies to `spread`: a
//! locked book, where the best ask equals the best bid, produces a `0.0` spread that prost
//! would elide.
//!
//! [`BookUpdate`]: md_proto::md::v1::BookUpdate
//! [`PositiveF64`]: core_lib::positive_f64::PositiveF64

use bytes::{BufMut as _, Bytes, BytesMut};
use core_lib::incremental_book::Level;
use md_wire::framing::LENGTH_PREFIX;

/// Levels a side, matching `SmallBook`'s depth. Bounds the size of the reusable buffer, and
/// checked against in [`BookEncoder::encode`] - that bound is what the `expect` there relies
/// on.
const MAX_DEPTH: usize = 10;

/// `BookUpdate.asks`, field 1, wire type 2: `(1 << 3) | 2`.
const ASKS_KEY: u8 = 0x0A;
/// `BookUpdate.bids`, field 2, wire type 2: `(2 << 3) | 2`.
const BIDS_KEY: u8 = 0x12;
/// `BookUpdate.spread`, field 3, wire type 1 (64-bit): `(3 << 3) | 1`.
const SPREAD_KEY: u8 = 0x19;
/// `Level.price`, field 1, wire type 1 (64-bit): `(1 << 3) | 1`.
const PRICE_KEY: u8 = 0x09;
/// `Level.size`, field 2, wire type 1 (64-bit): `(2 << 3) | 1`.
const SIZE_KEY: u8 = 0x11;
/// `Level.venue`, field 3, wire type 2.
const LEVEL_VENUE_FIELD: u32 = 3;

/// `spread`'s key plus its eight little-endian bytes.
const SPREAD_LEN: usize = 9;

pub(crate) trait BufferProvider {
    fn get_buffer(&mut self, capacity: usize) -> BytesMut;
}

/// Encodes one venue's books, recycling its storage across every one of them.
///
/// One per broadcaster: the venue suffix cached here is that broadcaster's identity, so this
/// cannot be shared between venues.
#[derive(Debug)]
pub(crate) struct BookEncoder {
    /// The encoded `Level.venue` field - key, length varint, bytes - appended after every
    /// level's price and size. The same suffix for every level this broadcaster ever sends,
    /// since a book carries one venue today.
    venue_suffix: Box<[u8]>,
    /// The largest a level's body (`price` + `size` + `venue_suffix`) can be. Bounds the
    /// one-byte length varint every level's key is followed by - see the `debug_assert!` in
    /// [`BookEncoder::new`].
    level_body_max: u8,
}

impl BookEncoder {
    /// `venue` is the identity every level from this broadcaster carries; it comes from its
    /// [`Key`](crate::registry::Key), since a book carries no identity itself.
    pub(crate) fn new(venue: &str) -> Self {
        // Built with prost rather than by hand: this runs once per broadcaster, so there is
        // nothing to win by open-coding it, and borrowing prost's own varint keeps the
        // length prefix of a long venue name correct by construction.
        let mut venue_suffix = Vec::new();
        put_string(&mut venue_suffix, LEVEL_VENUE_FIELD, venue);

        let level_body_max = 18 + venue_suffix.len();
        debug_assert!(
            level_body_max < 0x80,
            "a level's body must stay a one-byte length varint - Venue::as_str is a closed \
             set of short names, so this holds for every venue this server carries"
        );

        Self {
            venue_suffix: venue_suffix.into_boxed_slice(),
            level_body_max: u8::try_from(level_body_max)
                .expect("checked above: level_body_max < 0x80"),
        }
    }

    /// Encodes one book into a length-prefixed frame, best level first on each side.
    ///
    /// Both sides empty is the resync signal, and encodes to a frame carrying nothing but a
    /// `NaN` spread - which is exactly what prost produces for a `BookUpdate` with two empty
    /// `repeated` fields and `spread` computed the same way, so the signal survives as "no
    /// levels" rather than as a short read.
    pub(crate) fn encode(
        &self,
        asks: &[Level],
        bids: &[Level],
        buffers: &mut impl BufferProvider,
    ) -> Bytes {
        debug_assert!(
            asks.len() <= MAX_DEPTH && bids.len() <= MAX_DEPTH,
            "a book is at most MAX_DEPTH levels a side"
        );

        // An upper bound, not the exact length: only a level with a zero price or size, or a
        // locked book's elided spread, comes out shorter, and asking for a few bytes too many
        // is cheaper than a traversal to find out. The exact length is known once the body is
        // written, and patched in below.
        let needed = LENGTH_PREFIX
            + (2 + self.level_body_max as usize) * (asks.len() + bids.len())
            + SPREAD_LEN;
        let mut buf = buffers.get_buffer(needed);
        debug_assert!(
            buf.is_empty(),
            "a pooled buffer must be cleared before it is handed back, so a frame always starts at offset 0"
        );

        buf.put_bytes(0, LENGTH_PREFIX);
        self.put_side(&mut buf, ASKS_KEY, asks);
        self.put_side(&mut buf, BIDS_KEY, bids);

        let spread = asks
            .first()
            .zip(bids.first())
            .map_or(f64::NAN, |(ask, bid)| ask.price().get() - bid.price().get());
        // proto3 default elision: `0.0 != 0.0` is false, so this only ever fires on a locked
        // book. `NaN != 0.0` is true, so an empty-sided book's spread is always written.
        if spread != 0.0 {
            buf.put_u8(SPREAD_KEY);
            buf.put_f64_le(spread);
        }

        let body_len = u32::try_from(buf.len() - LENGTH_PREFIX)
            .expect("a book is at most 20 levels, so a frame never approaches 4 GiB");
        buf[..LENGTH_PREFIX].copy_from_slice(&body_len.to_le_bytes());

        buf.freeze()
    }

    /// Appends one `repeated Level` field, one entry per level.
    ///
    /// The body length is `level_body_max` minus 9 for each of `price`/`size` prost would
    /// elide. That subtraction never fires on a real book - a venue does not quote a zero
    /// price or a zero size - so it predicts perfectly, and it is what keeps these bytes equal
    /// to prost's.
    fn put_side(&self, buf: &mut BytesMut, key: u8, levels: &[Level]) {
        for level in levels {
            let price = level.price().get();
            let size = level.size().get();
            let body_len =
                self.level_body_max - 9 * u8::from(price == 0.0) - 9 * u8::from(size == 0.0);

            buf.put_u8(key);
            // A one-byte varint: checked at construction to be under the 0x80 continuation
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
            buf.put_slice(&self.venue_suffix);
        }
    }

    /// The largest frame this encoder can produce. Only the tests ask; nothing on the write
    /// path needs to know, since a frame carries its own length.
    #[cfg(test)]
    fn max_frame_len(&self) -> usize {
        LENGTH_PREFIX + (2 + self.level_body_max as usize) * 2 * MAX_DEPTH + SPREAD_LEN
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
    use super::{BookEncoder, BufferProvider};
    use bytes::BytesMut;
    use core_lib::incremental_book::Level;
    use core_lib::positive_f64::PositiveF64;
    use md_proto::md::v1 as proto;
    use md_wire::framing::LENGTH_PREFIX;
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

    fn as_proto(levels: &[Level], venue: &str) -> Vec<proto::Level> {
        levels
            .iter()
            .map(|l| proto::Level {
                price: l.price().get(),
                size: l.size().get(),
                venue: venue.to_owned(),
            })
            .collect()
    }

    fn spread_of(asks: &[Level], bids: &[Level]) -> f64 {
        asks.first()
            .zip(bids.first())
            .map_or(f64::NAN, |(ask, bid)| ask.price().get() - bid.price().get())
    }

    /// The guard on the whole hand-rolled encoder: for every shape of book, the bytes it
    /// produces are the bytes prost would have produced.
    #[test]
    fn the_hand_encoder_agrees_with_prost() {
        let deep: Vec<Level> = (1..=10)
            .map(|i| level(f64::from(i) * 1.5, f64::from(i) / 8.0))
            .collect();
        let cases: Vec<(&str, Vec<Level>, Vec<Level>)> = vec![
            // The resync signal: both sides empty. NaN spread, always written.
            ("binance_spot", Vec::new(), Vec::new()),
            // One side only: the other side's absence also makes the spread NaN.
            ("binance_spot", vec![level(100.5, 1.25)], Vec::new()),
            (
                "bitstamp",
                Vec::new(),
                vec![level(99.5, 2.0), level(99.0, 4.0)],
            ),
            // Full depth both sides: the largest frame.
            ("bitstamp", deep.clone(), deep),
            // `PositiveF64` permits `+0.0`, so prost's default elision is reachable.
            (
                "binance_spot",
                vec![level(0.0, 1.0), level(1.0, 0.0), level(0.0, 0.0)],
                vec![level(f64::MIN_POSITIVE, f64::MAX)],
            ),
            // A locked book: best ask equals best bid, so the spread is 0.0 and prost elides it.
            (
                "binance_spot",
                vec![level(100.0, 1.0)],
                vec![level(100.0, 1.0)],
            ),
            // A crossed book: a negative spread, which is never elided.
            (
                "binance_spot",
                vec![level(99.0, 1.0)],
                vec![level(100.0, 1.0)],
            ),
        ];

        for (venue, asks, bids) in cases {
            let expected = proto::BookUpdate {
                asks: as_proto(&asks, venue),
                bids: as_proto(&bids, venue),
                spread: spread_of(&asks, &bids),
            }
            .encode_to_vec();

            let encoder = BookEncoder::new(venue);
            let frame = encoder.encode(&asks, &bids, &mut TestBufferProvider);

            let declared = u32::from_le_bytes(
                frame[..LENGTH_PREFIX]
                    .try_into()
                    .expect("the prefix is four bytes"),
            );
            assert_eq!(
                usize::try_from(declared).expect("a frame length fits a usize"),
                frame.len() - LENGTH_PREFIX,
                "the length prefix must describe the body that follows it ({venue})"
            );
            assert_eq!(
                &frame[LENGTH_PREFIX..],
                expected.as_slice(),
                "the hand encoder must produce prost's bytes ({venue})"
            );
        }
    }

    /// Encoding is stateful - one buffer, split repeatedly - so a second book has to come out
    /// the same as if it were the first.
    #[test]
    fn a_reused_encoder_keeps_producing_whole_frames() {
        let encoder = BookEncoder::new("binance_spot");
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
