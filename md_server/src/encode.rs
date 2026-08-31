//! The field-level pieces of the server's hand-rolled protobuf, shared by everything that
//! writes a message by hand.
//!
//! Two encoders sit on top of these: `broadcast::book_encoder`, which writes a
//! `BookUpdate` per book on the fan-out path, and [`crate::catalogue::encode`], which writes
//! the `GetCatalogue` response once at startup. Neither builds a prost message first - see
//! the book encoder's module doc for why that is worth doing at all - so both need the same
//! thing from here: one field appended to a buffer, exactly as prost would have written it.
//!
//! The rule that makes "exactly as prost would have written it" more than a wrapper is proto3
//! default elision: prost omits a scalar field equal to its default, so an empty `string` and
//! a zero `uint32` are not written at all. Each function below carries that elision, which is
//! why a caller appends through them rather than through `prost::encoding` directly.

use bytes::BufMut;
use core_lib::Venue;
use md_wire::grpc::VenueIdx;

/// Appends one `string` field, skipping it entirely when empty - which is what prost's
/// generated code does for a proto3 scalar holding its default.
pub(crate) fn put_string(out: &mut Vec<u8>, field: u32, value: &str) {
    if value.is_empty() {
        return;
    }
    prost::encoding::encode_key(field, prost::encoding::WireType::LengthDelimited, out);
    prost::encoding::encode_varint(value.len() as u64, out);
    out.put_slice(value.as_bytes());
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

/// The index the wire carries for `venue`, in place of its name.
///
/// The server's own numbering rather than the catalogue file's: a catalogue names its venues
/// by name, and this is the one place that turns a name into the number every `Level`,
/// `Pair.venue_idx` and `VenueEntry.idx` is stamped with. Changing one of these renumbers
/// every level already shipped, so they are a client-visible contract.
///
/// Numbered from one, not zero. Zero is a `uint32`'s proto3 default, which prost elides, so a
/// venue holding it would encode to nothing at all - see [`put_venue_idx`], which is free to
/// write unconditionally only because of this.
pub(crate) const fn venue_idx(venue: Venue) -> VenueIdx {
    match venue {
        Venue::BinanceSpot => VenueIdx::new(1),
        Venue::Bitstamp => VenueIdx::new(2),
    }
}

/// The two facts the encoders rely on: the mapping is exactly `Venue::ALL`'s order numbered
/// from one - so a venue added to the enum without a number here stops the build - and every
/// index fits a single varint byte, which is what [`MAX_VENUE_IDX_ENCODED_LEN`] states.
const _: () = {
    let mut position = 0;
    while position < Venue::ALL.len() {
        let idx = venue_idx(Venue::ALL[position]).get();
        assert!(
            idx as usize == position + 1,
            "venue indices must be Venue::ALL's order numbered from one"
        );
        assert!(idx < 128, "a venue index must fit one varint byte");
        position += 1;
    }
};

/// Appends one `Level.venue_idx`/`Pair.venue_idx`/`VenueEntry.idx` field.
///
/// Unconditional, unlike [`put_uint32`]: [`venue_idx`] numbers from one, so no venue can hold
/// the default prost would elide.
pub(crate) fn put_venue_idx(out: &mut impl BufMut, field: u32, venue: Venue) {
    prost::encoding::uint32::encode(field, &venue_idx(venue).get(), out);
}

/// What [`put_venue_idx`] appends, at its widest: a one-byte key - every field it is written
/// to has a tag `<= 15` - and a one-byte varint, which the assertion above holds it to.
///
/// The book encoder's per-level length prefix is written from this before the level's body
/// is, so it has to be the exact width for the single-byte case rather than merely an upper
/// bound - see `broadcast::book_encoder::BookEncoder::LEVEL_BODY`.
pub(crate) const MAX_VENUE_IDX_ENCODED_LEN: usize = 2;
