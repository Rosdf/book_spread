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
