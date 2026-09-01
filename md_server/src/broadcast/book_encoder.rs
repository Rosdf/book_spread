//! Turns a book into the bytes that go on the wire, with no intermediate model and no
//! allocation per book.
//!
//! The obvious way to produce an `md.v1.BookUpdate` is to fill a [`BookUpdate`] and
//! call `encode_to_vec`. That costs a malloc/free pair per book, two traversals (one for
//! `encoded_len`, one for the encode) and a copy of every level out of the reader's slot into
//! an intermediate.
//!
//! None of that is necessary, because the message is completely deterministic. Every field
//! tag is `<= 15`, so every key is a single byte; a book carries at most [`MAX_DEPTH`] levels
//! a side; and a `Level.venue_idx` is one byte of key and one of varint, whatever the venue -
//! [`crate::encode::venue_idx`] numbers venues from one and the build refuses to grow past
//! what a single varint byte holds. So a level's body has one length, known at compile time,
//! and the encoder appends the levels straight out of the merge into storage it already owns.
//!
//! That storage is a small pool of whole buffers, not a fresh allocation per book. A frame is
//! a `Bytes` that owns the buffer it was frozen from, so a client that is behind pins the
//! buffer it is holding a frame from; recycling a buffer once every session has released it is
//! what keeps the steady state at zero allocations even while a client is stalled. See
//! [`super::broadcaster`]'s `BufferPool`, which is the [`BufferProvider`] in production.
//!
//! This is the book half of the server's hand-rolled protobuf; the field-level pieces it
//! shares with the catalogue's encoder live in [`crate::encode`].
//!
//! # Staying byte-identical to prost
//!
//! This is a second implementation of an encoding prost already implements, so it can drift.
//! What keeps it honest is this module's `the_hand_encoder_agrees_with_prost` test, which encodes a
//! spread of books both ways and asserts the bytes are equal. The one rule that is easy to
//! miss is proto3 default elision: prost omits a scalar field equal to its default, so a
//! `double` field that is `0.0` is not written at all. A `venue_idx` is never one of these -
//! that is what numbering venues from one buys. [`PositiveF64`] permits `+0.0`
//! (`is_sign_positive` is true for it), so a level with a zero price or size is reachable and
//! the rule has to be replicated - see [`BookEncoder::put_side`]. The same rule applies to
//! `spread`: a locked book, where the best ask equals the best bid, produces a `0.0` spread
//! that prost would elide.
//!
//! [`BookUpdate`]: md_proto::md::v1::BookUpdate
//! [`PositiveF64`]: core_lib::positive_f64::PositiveF64

use super::book_merger::MergedLevel;
use crate::encode::MAX_VENUE_IDX_ENCODED_LEN;
use bytes::{BufMut as _, Bytes, BytesMut};
use core_lib::positive_f64::PositiveF64;
use core_lib::small_book::SmallBook;
use md_wire::grpc::{MESSAGE_PREFIX, put_message_prefix};

/// Levels a side, matching `SmallBook`'s depth. Bounds the size of the reusable buffer, and
/// checked against in [`BookEncoder::encode`] - that bound is what the `expect` there relies
/// on.
const MAX_DEPTH: usize = SmallBook::LEVELS;

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
/// `Level.venue_idx`, field 3, wire type 0 (varint).
const LEVEL_VENUE_FIELD: u32 = 3;

/// `spread`'s key plus its eight little-endian bytes.
const SPREAD_LEN: usize = 9;

/// A level's two scalars at full width: each a one-byte key and eight little-endian bytes.
/// What a level's body is, before its `venue_idx` and before proto3 elides a zero.
const LEVEL_SCALARS: usize = 18;

pub(super) trait BufferProvider {
    fn get_buffer(&mut self, capacity: usize) -> BytesMut;
}

/// Encodes one symbol's merged books into buffers it borrows from a [`BufferProvider`].
///
/// Stateless: a level's venue travels on the level itself, and every venue encodes to the
/// same width, so there is nothing per-broadcaster left to hold.
#[derive(Debug)]
pub(super) struct BookEncoder;

impl BookEncoder {
    /// A level's body at full width: `price`, `size` and `venue_idx`, before proto3 elides a
    /// zero price or size. The same for every level of every book, which is what lets the
    /// one-byte length varint be written before the body rather than patched in after it.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "the assert above the cast is what rules the truncation out"
    )]
    const LEVEL_BODY: u8 = {
        let body = LEVEL_SCALARS + MAX_VENUE_IDX_ENCODED_LEN;
        assert!(
            body < 0x80,
            "a level's body must stay under the varint continuation bit, so its length is the \
             single byte `put_side` writes in front of it"
        );
        body as u8
    };

    /// Encodes one book into a whole gRPC length-prefixed message, best level first on each
    /// side.
    ///
    /// Both sides empty is the resync signal, and encodes to a frame carrying nothing but a
    /// `NaN` spread - which is exactly what prost produces for a `BookUpdate` with two empty
    /// `repeated` fields and `spread` computed the same way, so the signal survives as "no
    /// levels" rather than as a short read.
    pub(super) fn encode<A, B>(asks: A, bids: B, buffers: &mut impl BufferProvider) -> Bytes
    where
        A: ExactSizeIterator<Item = MergedLevel>,
        B: ExactSizeIterator<Item = MergedLevel>,
    {
        let asks_len = asks.len();
        let bids_len = bids.len();
        debug_assert!(
            asks_len <= MAX_DEPTH && bids_len <= MAX_DEPTH,
            "a book is at most MAX_DEPTH levels a side"
        );

        // An upper bound, not the exact length: only a level with a zero price or size, or a
        // locked book's elided spread, comes out shorter, and asking for a few bytes too many
        // is cheaper than a traversal to find out. The exact length is known once the body is
        // written, and patched in below.
        let needed = MESSAGE_PREFIX
            + (2 + usize::from(Self::LEVEL_BODY)) * (asks_len + bids_len)
            + SPREAD_LEN;
        let mut buf = buffers.get_buffer(needed);
        debug_assert!(
            buf.is_empty(),
            "a pooled buffer must be cleared before it is handed back, so a frame always starts at offset 0"
        );

        buf.put_bytes(0, MESSAGE_PREFIX);
        let best_ask = Self::put_side(&mut buf, ASKS_KEY, asks);
        let best_bid = Self::put_side(&mut buf, BIDS_KEY, bids);

        let spread = best_ask
            .zip(best_bid)
            .map_or(f64::NAN, |(ask, bid)| ask.get() - bid.get());
        // proto3 default elision: `0.0 != 0.0` is false, so this only ever fires on a locked
        // book. `NaN != 0.0` is true, so an empty-sided book's spread is always written.
        if spread != 0.0 {
            buf.put_u8(SPREAD_KEY);
            buf.put_f64_le(spread);
        }

        let body_len = u32::try_from(buf.len() - MESSAGE_PREFIX)
            .expect("a book is at most 20 levels, so a message never approaches 4 GiB");
        put_message_prefix(&mut buf[..MESSAGE_PREFIX], body_len);

        buf.freeze()
    }

    /// Appends one `repeated Level` field, one entry per level, and reports the best (first)
    /// level it saw - the only thing `encode` still needs off a side once the levels
    /// themselves are consumed on the way through, which is what lets the spread survive a
    /// consuming iterator instead of being read back off a slice.
    ///
    /// The body length is [`Self::LEVEL_BODY`] minus 9 for each of `price`/`size` prost would
    /// elide. That subtraction never fires on a real book - a venue does not quote a zero
    /// price or a zero size - so it predicts perfectly, and it is what keeps these bytes equal
    /// to prost's.
    fn put_side(
        buf: &mut BytesMut,
        key: u8,
        levels: impl Iterator<Item = MergedLevel>,
    ) -> Option<PositiveF64> {
        let mut best = None;
        for level in levels {
            best.get_or_insert_with(|| level.price());

            let price = level.price().get();
            let size = level.size().get();

            buf.put_u8(key);
            // A one-byte varint: `LEVEL_BODY` is asserted under the 0x80 continuation
            // threshold, and this is no larger.
            buf.put_u8(Self::LEVEL_BODY);

            buf.put_u8(PRICE_KEY);
            buf.put_f64_le(price);

            buf.put_u8(SIZE_KEY);
            buf.put_f64_le(size);

            crate::encode::put_venue_idx(buf, LEVEL_VENUE_FIELD, level.venue());
        }
        best
    }

    /// The largest frame this encoder can produce. Only the tests ask; nothing on the write
    /// path needs to know, since a frame carries its own length.
    #[cfg(test)]
    fn max_frame_len() -> usize {
        MESSAGE_PREFIX + (2 + usize::from(Self::LEVEL_BODY)) * 2 * MAX_DEPTH + SPREAD_LEN
    }
}

#[cfg(test)]
mod test {
    use super::{BookEncoder, BufferProvider};
    use crate::broadcast::book_merger::{MergedLevel, tagged};
    use crate::encode::venue_idx;
    use bytes::BytesMut;
    use core_lib::Venue;
    use core_lib::incremental_book::Level as BookLevel;
    use core_lib::positive_f64::PositiveF64;
    use md_proto::md::v1 as proto;
    use md_wire::grpc::{MESSAGE_PREFIX, message_len};
    use prost::Message as _;

    struct TestBufferProvider;

    impl BufferProvider for TestBufferProvider {
        fn get_buffer(&mut self, capacity: usize) -> BytesMut {
            BytesMut::with_capacity(capacity)
        }
    }

    fn level(price: f64, size: f64, venue: Venue) -> MergedLevel {
        MergedLevel::new(
            PositiveF64::new(price).expect("test prices are positive"),
            PositiveF64::new(size).expect("test sizes are positive"),
            venue,
        )
    }

    fn book_level(price: f64, size: f64) -> BookLevel {
        BookLevel::new(
            PositiveF64::new(price).expect("test prices are positive"),
            PositiveF64::new(size).expect("test sizes are positive"),
        )
    }

    fn as_proto(levels: &[MergedLevel]) -> Vec<proto::Level> {
        levels
            .iter()
            .map(|l| proto::Level {
                price: l.price().get(),
                size: l.size().get(),
                venue_idx: venue_idx(l.venue()).get(),
            })
            .collect()
    }

    fn spread_of(asks: &[MergedLevel], bids: &[MergedLevel]) -> f64 {
        asks.first()
            .zip(bids.first())
            .map_or(f64::NAN, |(ask, bid)| ask.price().get() - bid.price().get())
    }

    /// The guard on the whole hand-rolled encoder: for every shape of book, the bytes it
    /// produces are the bytes prost would have produced.
    #[test]
    fn the_hand_encoder_agrees_with_prost() {
        let deep: Vec<MergedLevel> = (1..=10)
            .map(|i| level(f64::from(i) * 1.5, f64::from(i) / 8.0, Venue::BinanceSpot))
            .collect();
        let cases: Vec<(&str, Vec<MergedLevel>, Vec<MergedLevel>)> = vec![
            // The resync signal: both sides empty. NaN spread, always written.
            ("empty", Vec::new(), Vec::new()),
            // One side only: the other side's absence also makes the spread NaN.
            (
                "asks only",
                vec![level(100.5, 1.25, Venue::Bitstamp)],
                Vec::new(),
            ),
            (
                "bids only",
                Vec::new(),
                vec![
                    level(99.5, 2.0, Venue::BinanceSpot),
                    level(99.0, 4.0, Venue::Bitstamp),
                ],
            ),
            // Full depth both sides: the largest frame.
            ("full depth", deep.clone(), deep),
            // The highest venue index this build carries, on both sides.
            (
                "widest index",
                vec![level(100.5, 1.25, Venue::Bitstamp)],
                vec![level(99.5, 2.0, Venue::Bitstamp)],
            ),
            // A merged book, which is the whole point of tagging each level: one side
            // carrying levels from both venues, so a venue read per level and one assumed for
            // the whole side cannot produce the same bytes.
            (
                "two venues on one side",
                vec![
                    level(100.0, 1.0, Venue::BinanceSpot),
                    level(100.0, 2.0, Venue::Bitstamp),
                    level(101.0, 3.0, Venue::BinanceSpot),
                ],
                vec![
                    level(99.0, 1.0, Venue::BinanceSpot),
                    level(99.0, 2.0, Venue::Bitstamp),
                ],
            ),
            // `PositiveF64` permits `+0.0`, so prost's default elision is reachable.
            (
                "bound price and size",
                vec![],
                vec![level(f64::MIN_POSITIVE, f64::MAX, Venue::BinanceSpot)],
            ),
            // A locked book: best ask equals best bid, so the spread is 0.0 and prost elides it.
            (
                "locked",
                vec![level(100.0, 1.0, Venue::BinanceSpot)],
                vec![level(100.0, 1.0, Venue::Bitstamp)],
            ),
            // A crossed book: a negative spread, which is never elided.
            (
                "crossed",
                vec![level(99.0, 1.0, Venue::Bitstamp)],
                vec![level(100.0, 1.0, Venue::BinanceSpot)],
            ),
        ];

        for (case, asks, bids) in cases {
            let expected = proto::BookUpdate {
                asks: as_proto(&asks),
                bids: as_proto(&bids),
                spread: spread_of(&asks, &bids),
            }
            .encode_to_vec();

            let frame = BookEncoder::encode(
                asks.iter().copied(),
                bids.iter().copied(),
                &mut TestBufferProvider,
            );

            let header = frame[..MESSAGE_PREFIX]
                .try_into()
                .expect("the header is five bytes");
            assert_eq!(
                message_len(&header),
                Some(frame.len() - MESSAGE_PREFIX),
                "the gRPC header must be uncompressed and describe the body that follows it \
                 ({case})"
            );
            assert_eq!(
                &frame[MESSAGE_PREFIX..],
                expected.as_slice(),
                "the hand encoder must produce prost's bytes ({case})"
            );
        }
    }

    /// `tagged` is a second way to produce the levels a single-venue book encodes to - a
    /// straight walk instead of a one-run merge - so it needs its own guard that the two
    /// produce identical bytes, on the same shapes [`the_hand_encoder_agrees_with_prost`]
    /// already checks against prost.
    #[test]
    fn the_single_venue_fast_path_agrees_with_the_merge() {
        let deep: Vec<BookLevel> = (1..=10)
            .map(|i| book_level(f64::from(i) * 1.5, f64::from(i) / 8.0))
            .collect();
        let venue = Venue::BinanceSpot;
        let cases: Vec<(&str, Vec<BookLevel>, Vec<BookLevel>)> = vec![
            ("empty", Vec::new(), Vec::new()),
            ("asks only", vec![book_level(100.5, 1.25)], Vec::new()),
            (
                "bids only",
                Vec::new(),
                vec![book_level(99.5, 2.0), book_level(99.0, 4.0)],
            ),
            ("full depth", deep.clone(), deep),
            (
                "inf and max",
                vec![],
                vec![book_level(f64::MIN_POSITIVE, f64::MAX)],
            ),
            (
                "locked",
                vec![book_level(100.0, 1.0)],
                vec![book_level(100.0, 1.0)],
            ),
            (
                "crossed",
                vec![book_level(99.0, 1.0)],
                vec![book_level(100.0, 1.0)],
            ),
        ];

        for (case, asks, bids) in cases {
            let merged_asks: Vec<MergedLevel> = asks
                .iter()
                .map(|l| MergedLevel::new(l.price(), l.size(), venue))
                .collect();
            let merged_bids: Vec<MergedLevel> = bids
                .iter()
                .map(|l| MergedLevel::new(l.price(), l.size(), venue))
                .collect();

            let via_merge = BookEncoder::encode(
                merged_asks.iter().copied(),
                merged_bids.iter().copied(),
                &mut TestBufferProvider,
            );
            let via_tagged = BookEncoder::encode(
                tagged(venue, &asks),
                tagged(venue, &bids),
                &mut TestBufferProvider,
            );

            assert_eq!(
                via_tagged, via_merge,
                "the single-venue fast path must produce the same bytes as the merged path \
                 ({case})"
            );
        }
    }

    /// Encoding is stateful - one buffer, split repeatedly - so a second book has to come out
    /// the same as if it were the first.
    #[test]
    fn a_reused_encoder_keeps_producing_whole_frames() {
        let asks = [level(100.5, 1.25, Venue::Bitstamp)];
        let bids = [level(99.5, 2.0, Venue::Bitstamp)];

        let first = BookEncoder::encode(
            asks.iter().copied(),
            bids.iter().copied(),
            &mut TestBufferProvider,
        );
        // Held on to on purpose: a live frame is what keeps the buffer from being reclaimed
        // in place, which is the case where `reserve` has to allocate a fresh chunk.
        let mut held = vec![first.clone()];
        for _ in 0..2_000 {
            held.push(BookEncoder::encode(
                asks.iter().copied(),
                bids.iter().copied(),
                &mut TestBufferProvider,
            ));
        }

        assert!(
            held.iter().all(|frame| *frame == first),
            "every frame from one book must be identical, whatever the buffer did underneath"
        );
        assert!(
            first.len() <= BookEncoder::max_frame_len(),
            "the advertised maximum must bound a real frame"
        );
    }
}
