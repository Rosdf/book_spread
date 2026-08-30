//! Turns a book into the bytes that go on the wire, with no intermediate model and no
//! allocation per book.
//!
//! The obvious way to produce an `md.v1.BookUpdate` is to fill a [`BookUpdate`] and
//! call `encode_to_vec`. That costs a malloc/free pair per book, two traversals (one for
//! `encoded_len`, one for the encode), a copy of every level out of the reader's slot into
//! the scratch, and a re-encode of the level's venue on every level, which never changes for
//! the life of a broadcaster.
//!
//! None of that is necessary, because the message is completely deterministic. Every field
//! tag is `<= 15`, so every key is a single byte; a book carries at most [`MAX_DEPTH`] levels
//! a side; and this broadcaster's venue never changes, so its encoded `Level.venue_idx` suffix can
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

use crate::broadcast::book_merger::MergedLevel;
use bytes::{BufMut, Bytes, BytesMut};
use core_lib::Venue;
use core_lib::heapless_linear_map::HeaplessLinearMap;
use core_lib::small_book::SmallBook;
use md_wire::grpc::{MESSAGE_PREFIX, VenueIdx, put_message_prefix};

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
/// What a level's body is, before its venue suffix and before proto3 elides a zero.
const LEVEL_SCALARS: usize = 18;

pub(crate) trait BufferProvider {
    fn get_buffer(&mut self, capacity: usize) -> BytesMut;
}

/// Encodes one symbol's merged books, recycling its storage across every one of them.
///
/// One per broadcaster, holding the suffix of every venue that broadcaster can quote: a
/// merged book's levels no longer share one venue, so the identity is looked up per level
/// rather than being the encoder's own.
#[derive(Debug)]
pub(crate) struct BookEncoder {
    /// The encoded `Level.venue_idx` field - key, then the index as a varint - appended after
    /// each level's price and size, one entry per venue this broadcaster serves. A suffix is
    /// *empty* for venue index zero: proto3 elides a scalar equal to its default, so writing
    /// anything there would stop these bytes being prost's.
    ///
    /// A linear map rather than a hash map for the same reason the catalogue's venue table is
    /// one - at most [`Venue::COUNT`] entries, known at compile time, so a scan over two keys
    /// on the per-level path beats hashing and allocates nothing.
    venue_suffix: HeaplessLinearMap<VenueIdx, Box<[u8]>, { Venue::COUNT }>,
    /// The largest a level's body (`price` + `size` + `venue_suffix`) can be, taken over the
    /// *widest* venue suffix. Bounds the one-byte length varint every level's key is followed
    /// by - see the `debug_assert!` in [`BookEncoder::new`] - and, through
    /// [`BookEncoder::encode`], the capacity a frame asks the pool for, which is an upper
    /// bound rather than an exact size once venues of different widths share a book.
    level_body_max: u8,
}

impl BookEncoder {
    /// `venue_ids` is every venue whose levels this encoder will be asked to write - the
    /// venues' indices in the catalogue, which is the only thing that knows them, since a
    /// book carries no identity itself and neither does an `Instrument`. Passing one that is
    /// never quoted costs an unused entry; leaving one out that is quoted is a panic in
    /// [`BookEncoder::encode`].
    pub(crate) fn new(venue_ids: &[VenueIdx]) -> Self {
        let mut map = HeaplessLinearMap::new();
        let mut max_suffix: usize = 0;

        for idx in venue_ids {
            // Built with prost rather than by hand: this runs once per broadcaster, so there is
            // nothing to win by open-coding it, and borrowing prost's own varint keeps a large
            // index correct by construction - including the elided zero.
            let mut venue_suffix = Vec::new();
            put_uint32(&mut venue_suffix, LEVEL_VENUE_FIELD, idx.get());
            max_suffix = usize::max(max_suffix, venue_suffix.len());

            map.insert(*idx, venue_suffix.into_boxed_slice())
                // Only reachable with more distinct venues than this build carries, which the
                // catalogue's own venue table - the source of every `VenueIdx` - cannot name.
                .map_err(|_| ())
                .expect("a catalogue names at most one index per venue");
        }

        let level_body_max = LEVEL_SCALARS + max_suffix;
        debug_assert!(
            level_body_max < 0x80,
            "a level's body must stay a one-byte length varint - a venue index is at most a \
             five-byte varint, so this holds for every catalogue this server can load"
        );

        Self {
            venue_suffix: map,
            level_body_max: u8::try_from(level_body_max)
                .expect("checked above: level_body_max < 0x80"),
        }
    }

    /// Encodes one book into a whole gRPC length-prefixed message, best level first on each
    /// side.
    ///
    /// Both sides empty is the resync signal, and encodes to a frame carrying nothing but a
    /// `NaN` spread - which is exactly what prost produces for a `BookUpdate` with two empty
    /// `repeated` fields and `spread` computed the same way, so the signal survives as "no
    /// levels" rather than as a short read.
    pub(crate) fn encode(
        &self,
        asks: &[MergedLevel],
        bids: &[MergedLevel],
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
        let needed = MESSAGE_PREFIX
            + (2 + self.level_body_max as usize) * (asks.len() + bids.len())
            + SPREAD_LEN;
        let mut buf = buffers.get_buffer(needed);
        debug_assert!(
            buf.is_empty(),
            "a pooled buffer must be cleared before it is handed back, so a frame always starts at offset 0"
        );

        buf.put_bytes(0, MESSAGE_PREFIX);
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

        let body_len = u32::try_from(buf.len() - MESSAGE_PREFIX)
            .expect("a book is at most 20 levels, so a message never approaches 4 GiB");
        put_message_prefix(&mut buf[..MESSAGE_PREFIX], body_len);

        buf.freeze()
    }

    /// Appends one `repeated Level` field, one entry per level.
    ///
    /// The body length is this level's own suffix plus its two scalars, minus 9 for each of
    /// `price`/`size` prost would elide. That subtraction never fires on a real book - a venue
    /// does not quote a zero price or a zero size - so it predicts perfectly, and it is what
    /// keeps these bytes equal to prost's.
    ///
    /// Per level rather than per encoder: two venues on one side can have suffixes of
    /// different widths, and a level written to `level_body_max` when its own suffix is
    /// narrower is a length that does not describe what follows it.
    fn put_side(&self, buf: &mut BytesMut, key: u8, levels: &[MergedLevel]) {
        for level in levels {
            let price = level.price().get();
            let size = level.size().get();
            let suffix = self
                .venue_suffix
                .get(&level.venue())
                .expect("every venue a level can carry was named to `BookEncoder::new`");
            let full = u8::try_from(LEVEL_SCALARS + suffix.len())
                .expect("no wider than `level_body_max`, checked under 0x80 at construction");
            let body_len = full - 9 * u8::from(price == 0.0) - 9 * u8::from(size == 0.0);

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
            buf.put_slice(suffix);
        }
    }

    /// The largest frame this encoder can produce. Only the tests ask; nothing on the write
    /// path needs to know, since a frame carries its own length.
    #[cfg(test)]
    fn max_frame_len(&self) -> usize {
        MESSAGE_PREFIX + (2 + self.level_body_max as usize) * 2 * MAX_DEPTH + SPREAD_LEN
    }
}

/// Appends one `string` field, skipping it entirely when empty - which is what prost's
/// generated code does for a proto3 scalar holding its default.
pub(crate) fn put_string(out: &mut Vec<u8>, field: u32, value: &str) {
    if value.is_empty() {
        return;
    }
    prost::encoding::string::encode(field, &value.to_owned(), out);
}

/// Appends one `uint32` field, skipping it entirely when zero - the same proto3 default
/// elision [`put_string`] applies to an empty string.
pub(crate) fn put_uint32(out: &mut Vec<u8>, field: u32, value: u32) {
    if value == 0 {
        return;
    }
    prost::encoding::uint32::encode(field, &value, out);
}

/// Appends one length-delimited submessage field, whose body is already encoded.
///
/// Unlike the scalars above, a repeated message entry is written even when its body is empty:
/// proto3's default elision is about scalars, and an empty entry in a `repeated` field is a
/// present entry.
pub(crate) fn put_message(out: &mut impl BufMut, field: u32, body: &[u8]) {
    prost::encoding::encode_key(field, prost::encoding::WireType::LengthDelimited, out);
    prost::encoding::encode_varint(body.len() as u64, out);
    out.put_slice(body);
}

#[cfg(test)]
mod test {
    use super::{BookEncoder, BufferProvider};
    use crate::broadcast::book_merger::MergedLevel;
    use bytes::BytesMut;
    use core_lib::positive_f64::PositiveF64;
    use md_proto::md::v1 as proto;
    use md_wire::grpc::{MESSAGE_PREFIX, VenueIdx, message_len};
    use prost::Message as _;

    struct TestBufferProvider;

    impl BufferProvider for TestBufferProvider {
        fn get_buffer(&mut self, capacity: usize) -> BytesMut {
            BytesMut::with_capacity(capacity)
        }
    }

    fn level(price: f64, size: f64, venue_idx: u32) -> MergedLevel {
        MergedLevel::new(
            PositiveF64::new(price).expect("test prices are positive"),
            PositiveF64::new(size).expect("test sizes are positive"),
            VenueIdx::new(venue_idx),
        )
    }

    fn as_proto(levels: &[MergedLevel]) -> Vec<proto::Level> {
        levels
            .iter()
            .map(|l| proto::Level {
                price: l.price().get(),
                size: l.size().get(),
                venue_idx: l.venue().get(),
            })
            .collect()
    }

    /// Every venue any level of `asks` or `bids` carries, which is what the encoder has to be
    /// told about up front.
    fn venues_of(asks: &[MergedLevel], bids: &[MergedLevel]) -> Vec<VenueIdx> {
        let mut all: Vec<VenueIdx> = asks.iter().chain(bids).map(MergedLevel::venue).collect();
        all.sort_unstable();
        all.dedup();
        all
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
            .map(|i| level(f64::from(i) * 1.5, f64::from(i) / 8.0, 1))
            .collect();
        // Venue index zero is the elision case - proto3 writes nothing for a scalar equal to
        // its default - and 300 is the one that needs a two-byte varint, which a venue name
        // never did.
        let cases: Vec<(&str, Vec<MergedLevel>, Vec<MergedLevel>)> = vec![
            // The resync signal: both sides empty. NaN spread, always written.
            ("empty", Vec::new(), Vec::new()),
            // One side only: the other side's absence also makes the spread NaN.
            ("asks only", vec![level(100.5, 1.25, 0)], Vec::new()),
            ("bids only", Vec::new(), vec![level(99.5, 2.0, 1), level(99.0, 4.0, 1)]),
            // Full depth both sides: the largest frame.
            ("full depth", deep.clone(), deep),
            // An index past one byte of varint, which the length prefix has to account for.
            ("wide index", vec![level(100.5, 1.25, 300)], vec![level(99.5, 2.0, 300)]),
            // A merged book, which is the whole point of the per-level suffix: one side
            // carrying both the elided zero and the two-byte varint, so a suffix looked up
            // per level and one cached per encoder cannot produce the same bytes.
            (
                "two venues on one side",
                vec![level(100.0, 1.0, 0), level(100.0, 2.0, 300), level(101.0, 3.0, 0)],
                vec![level(99.0, 1.0, 300), level(99.0, 2.0, 0)],
            ),
            // `PositiveF64` permits `+0.0`, so prost's default elision is reachable.
            (
                "zero price and size",
                vec![level(0.0, 1.0, 0), level(1.0, 0.0, 0), level(0.0, 0.0, 0)],
                vec![level(f64::MIN_POSITIVE, f64::MAX, 0)],
            ),
            // A locked book: best ask equals best bid, so the spread is 0.0 and prost elides it.
            ("locked", vec![level(100.0, 1.0, 0)], vec![level(100.0, 1.0, 0)]),
            // A crossed book: a negative spread, which is never elided.
            ("crossed", vec![level(99.0, 1.0, 1)], vec![level(100.0, 1.0, 1)]),
        ];

        for (case, asks, bids) in cases {
            let expected = proto::BookUpdate {
                asks: as_proto(&asks),
                bids: as_proto(&bids),
                spread: spread_of(&asks, &bids),
            }
            .encode_to_vec();

            let encoder = BookEncoder::new(&venues_of(&asks, &bids));
            let frame = encoder.encode(&asks, &bids, &mut TestBufferProvider);

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

    /// Encoding is stateful - one buffer, split repeatedly - so a second book has to come out
    /// the same as if it were the first.
    #[test]
    fn a_reused_encoder_keeps_producing_whole_frames() {
        let encoder = BookEncoder::new(&[VenueIdx::new(0)]);
        let asks = [level(100.5, 1.25, 0)];
        let bids = [level(99.5, 2.0, 0)];

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
