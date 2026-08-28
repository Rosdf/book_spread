//! Decoders that apply Binance depth payloads straight into an [`IncrementalBook`], plus the
//! two entry points [`on_frame`] and [`seed_and_replay`] that back `impl Venue for
//! BinanceSpot`.
//!
//! Nothing here builds an intermediate model for a *live* frame. `serde`'s [`DeserializeSeed`]
//! lets a decoder carry `&mut IncrementalBook` into the visit, so each price level is applied
//! at the moment it is parsed and then forgotten - see [`core_lib::venue::levels`], which owns
//! the parts of that every venue shares.
//!
//! A frame for a symbol that has no book yet cannot do that, so it is staged instead: [`Buffered`]
//! holds every buffered diff's ids and levels, parsed once on arrival, and [`seed_and_replay`]
//! walks it after the snapshot lands. This costs parsing levels for diffs the snapshot may
//! later turn out to cover and discard, where the old code skipped them with `IgnoredAny` and
//! re-parsed the raw bytes later - but re-parsing was never actually sound, since simd-json
//! unescapes into its own input buffer; see [`core_lib::venue::pending`].
//!
//! The `/stream` endpoint is used rather than `/ws` deliberately: it wraps every event as
//! `{"stream": "...", "data": {...}}`, giving an unambiguous demux key as the first field.

use bytes::Bytes;
use core_lib::incremental_book::{IncrementalBook, UpdateResult};
use core_lib::venue::levels::{
    BookSink, LevelSink, LevelsSeed, Side, apply_level, worth_publishing,
};
use core_lib::venue::{
    Decoder, FrameAction, FrameCtx, Generations, PendingDiffs, Retry, Scratch, Slot, SlotState,
    Symbol,
};
use serde::Deserialize;
use serde::de::{
    DeserializeSeed, Deserializer, Error as _, IgnoredAny, MapAccess, SeqAccess, Visitor,
};
use std::collections::HashSet;
use std::fmt::{self, Formatter};
use std::time::Instant;

/// The half of the slot state machine that is not `Bootstrapping`.
#[derive(Debug)]
pub enum Ready {
    /// Seeded from the REST snapshot, but no diff has been applied on top of it yet.
    ///
    /// A snapshot is frequently *ahead* of the websocket, since REST reflects the book now
    /// while the socket still has queued diffs. Every event at or below `last_update_id` is
    /// therefore already in the book and must be dropped rather than sequence-checked, until
    /// one straddles the boundary and takes the slot `Live`.
    Seeded { last_update_id: u64 },
    /// Book is current; `prev_u` is the `u` of the last applied event.
    Live { prev_u: u64 },
}

type BinanceSlot = Slot<Ready, Buffered>;
type BinanceTable = core_lib::venue::SlotTable<Ready, Buffered>;

/// Something that invalidates one symbol's book. The socket and its other symbols are fine.
#[derive(Debug, thiserror::Error)]
pub enum BootstrapError {
    #[error(transparent)]
    Fetch(#[from] core_lib::venue::SnapshotFetchError<reqwest::RequestBuilder>),

    #[error("decoding snapshot: {0}")]
    Decode(#[from] simd_json::Error),

    #[error("snapshot lastUpdateId {snapshot} does not reach buffered diffs starting at {first}")]
    SnapshotGap { snapshot: u64, first: u64 },

    #[error("sequence gap while replaying: expected first update id {expected}, got {got}")]
    Gap { expected: u64, got: u64 },
}

/// Which of the two recoveries each failure needs.
///
/// The split is "the snapshot was unusable" versus "what we buffered was". Only [`Gap`] is the
/// second: it is a hole *between* two buffered diffs, i.e. a frame the socket dropped, and no
/// snapshot can fill that in. Everything else is the snapshot's problem - it never arrived, it
/// did not decode, or it did not reach back far enough - and the diffs already buffered are
/// still exactly what the next one should be replayed onto.
///
/// [`Gap`]: BootstrapError::Gap
impl core_lib::venue::BootstrapRetry for BootstrapError {
    fn retry(&self) -> Retry {
        match self {
            Self::Fetch(_) | Self::Decode(_) | Self::SnapshotGap { .. } => Retry::Refetch,
            Self::Gap { .. } => Retry::Resync,
        }
    }
}

/// Why `GET /api/v3/exchangeInfo` could not be read.
///
/// Its own type rather than a variant of [`BootstrapError`]: the two share no caller and no
/// recovery - a listing failure is retried by the connector, a bootstrap failure resyncs one
/// symbol.
#[derive(Debug, thiserror::Error)]
#[error("decoding the exchangeInfo listing: {0}")]
pub struct SymbolsError(#[from] simd_json::Error);

/// Ways a Binance payload can be unusable.
///
/// These are raised from inside `serde` visitors, where the only channel back to the caller
/// is `serde::de::Error::custom`, which takes something `Display`. So they never propagate
/// as themselves - they end up as the message inside a `simd_json::Error`. They still get
/// their own type so each condition is written once and the tests can match on the wording.
///
/// A frame naming a stream this connection does not carry is deliberately *not* here: it is a
/// routine outcome ([`FrameAction::Ignored`]), not a malformed payload.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
enum MalformedPayload {
    /// Binance documents the key order as `e, E, s, U, u, b, a` (and `stream` before `data`
    /// in the envelope). If that ever changes, a level array could be reached before the ids
    /// that decide whether to apply it, so this fails loudly instead.
    #[error("field {0:?} arrived before the fields it depends on")]
    FieldOrder(&'static str),

    #[error("sequence gap: expected first update id {expected}, got {got}")]
    Gap { expected: u64, got: u64 },

    #[error("depthUpdate missing U or u")]
    MissingUpdateIds,

    #[error("depth snapshot missing lastUpdateId")]
    MissingLastUpdateId,

    #[error("frame contained no fields")]
    EmptyFrame,

    #[error("envelope named a stream but carried no data")]
    MissingData,
}

// ---------------------------------------------------------------------------------------
// The buffered-diff arena
// ---------------------------------------------------------------------------------------

/// Where one buffered diff's levels sit inside [`Buffered::levels`], plus the ids that decide
/// whether it is replayed at all.
#[derive(Debug, Clone, Copy)]
struct DiffMeta {
    /// `U` - first update id covered by this event.
    first_id: u64,
    /// `u` - last update id covered by this event.
    last_id: u64,
    start: usize,
    /// End of this diff's bids, and the start of its asks.
    bid_end: usize,
    end: usize,
}

/// One bootstrapping symbol's diffs, parsed on arrival.
///
/// One flat `Vec` of levels for every buffered diff rather than a `Vec` per diff: a diff is
/// tiny (a handful of levels) and there can be hundreds of them, so per-diff allocation would
/// dominate. [`DiffMeta`] carries the ranges.
#[derive(Debug, Default)]
pub struct Buffered {
    levels: Vec<(f64, f64)>,
    meta: Vec<DiffMeta>,
}

/// One buffered diff, resolved back out of the arena.
#[derive(Debug, Clone, Copy)]
struct BufferedDiff<'a> {
    first_id: u64,
    last_id: u64,
    bids: &'a [(f64, f64)],
    asks: &'a [(f64, f64)],
}

impl Buffered {
    /// Every buffered diff in arrival order.
    fn diffs(&self) -> impl ExactSizeIterator<Item = BufferedDiff<'_>> {
        self.meta.iter().map(|meta| BufferedDiff {
            first_id: meta.first_id,
            last_id: meta.last_id,
            bids: &self.levels[meta.start..meta.bid_end],
            asks: &self.levels[meta.bid_end..meta.end],
        })
    }
}

impl PendingDiffs for Buffered {
    fn buffered(&self) -> usize {
        self.meta.len()
    }

    fn clear(&mut self) {
        self.levels.clear();
        self.meta.clear();
    }
}

impl LevelSink for Buffered {
    fn push_level(&mut self, price: f64, qty: f64) {
        self.levels.push((price, qty));
    }
}

// ---------------------------------------------------------------------------------------
// depthUpdate
// ---------------------------------------------------------------------------------------

/// What one `depthUpdate` did to the book.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DiffOutcome {
    /// `U` - first update id covered by this event.
    first_id: u64,
    /// `u` - last update id covered by this event.
    last_id: u64,
    /// False when the event predated the snapshot and was skipped wholesale.
    applied: bool,
    /// True when the top of book moved, i.e. the merged result was `Close` or `Both`.
    publish: bool,
}

impl DiffOutcome {
    pub(crate) fn first_id(&self) -> u64 {
        self.first_id
    }

    pub(crate) fn last_id(&self) -> u64 {
        self.last_id
    }

    pub(crate) fn applied(&self) -> bool {
        self.applied
    }

    pub(crate) fn publish(&self) -> bool {
        self.publish
    }

    /// Builds an outcome directly, bypassing `DiffSeed`. Production code only ever gets one
    /// out of a decode; this exists so tests can drive `on_seeded` without a JSON round trip.
    #[cfg(test)]
    pub(crate) const fn for_test(first_id: u64, last_id: u64, applied: bool) -> Self {
        Self {
            first_id,
            last_id,
            applied,
            publish: applied,
        }
    }
}

/// Applies one `depthUpdate` body into `book` during deserialization.
///
/// Events whose `u` is at or below `min_id` are already covered by the snapshot and are
/// skipped without touching the book. `min_id` is zero in steady state.
///
/// `expect_first` is the `U` this event must carry to continue the sequence. Binance emits
/// `U` before the level arrays, so checking it here rejects a gapped event *before* a single
/// level has been applied - the book is never left half-updated by a gap.
#[derive(Debug)]
struct DiffSeed<'a> {
    book: &'a mut IncrementalBook,
    min_id: u64,
    expect_first: Option<u64>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(field_identifier)]
enum DiffField {
    #[serde(rename = "U")]
    First,
    #[serde(rename = "u")]
    Last,
    #[serde(rename = "b")]
    Bids,
    #[serde(rename = "a")]
    Asks,
    #[serde(other)]
    Skip,
}

impl DiffField {
    /// The wire name, for a [`MalformedPayload::FieldOrder`] complaint.
    const fn name(self) -> &'static str {
        match self {
            Self::First => "U",
            Self::Last => "u",
            Self::Bids => "b",
            Self::Asks => "a",
            Self::Skip => "?",
        }
    }
}

impl<'de> DeserializeSeed<'de> for DiffSeed<'_> {
    type Value = DiffOutcome;

    fn deserialize<D: Deserializer<'de>>(self, de: D) -> Result<DiffOutcome, D::Error> {
        de.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for DiffSeed<'_> {
    type Value = DiffOutcome;

    fn expecting(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("a depthUpdate object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<DiffOutcome, A::Error> {
        let mut first_id_opt: Option<u64> = None;
        let mut last_id_opt: Option<u64> = None;
        let mut merged: Option<UpdateResult> = None;
        let mut applied = false;

        while let Some(field) = map.next_key::<DiffField>()? {
            match field {
                DiffField::First => {
                    let got: u64 = map.next_value()?;
                    if let Some(expected) = self.expect_first
                        && got != expected
                    {
                        return Err(A::Error::custom(MalformedPayload::Gap { expected, got }));
                    }
                    first_id_opt = Some(got);
                }
                DiffField::Last => last_id_opt = Some(map.next_value()?),
                DiffField::Bids | DiffField::Asks => {
                    // Binance documents the key order as e, E, s, U, u, b, a, so `u` is
                    // known by now and decides whether these levels are already covered by
                    // the snapshot. Failing loudly beats silently applying stale levels.
                    let Some(last) = last_id_opt else {
                        return Err(A::Error::custom(MalformedPayload::FieldOrder(field.name())));
                    };

                    if last > self.min_id {
                        applied = true;
                        let side = if matches!(field, DiffField::Bids) {
                            Side::Bid
                        } else {
                            Side::Ask
                        };
                        let mut sink = BookSink::new(&mut *self.book, side, &mut merged);
                        map.next_value_seed(LevelsSeed::new(&mut sink))?;
                    } else {
                        map.next_value::<IgnoredAny>()?;
                    }
                }
                DiffField::Skip => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }

        let (Some(first_id), Some(last_id)) = (first_id_opt, last_id_opt) else {
            return Err(A::Error::custom(MalformedPayload::MissingUpdateIds));
        };

        Ok(DiffOutcome {
            first_id,
            last_id,
            applied,
            publish: applied && worth_publishing(merged),
        })
    }
}

/// Stages one `depthUpdate` into `pending`, for a symbol that has no book to apply it to yet.
///
/// Reads `U` and `u` and appends both level arrays to the arena in one pass, returning `U` -
/// the cursor the core needs, since `U` of the first buffered event is what the snapshot's
/// `lastUpdateId` has to reach.
#[derive(Debug)]
struct BufferSeed<'p> {
    pending: &'p mut Buffered,
}

impl<'de> DeserializeSeed<'de> for BufferSeed<'_> {
    type Value = u64;

    fn deserialize<D: Deserializer<'de>>(self, de: D) -> Result<u64, D::Error> {
        de.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for BufferSeed<'_> {
    type Value = u64;

    fn expecting(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("a depthUpdate object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<u64, A::Error> {
        let start = self.pending.levels.len();
        let mut first_id: Option<u64> = None;
        let mut last_id: Option<u64> = None;
        // `None` until `b` has been read: bids own the arena's first stretch, asks the rest,
        // so asks arriving first would silently split it in the wrong place.
        let mut bid_end: Option<usize> = None;

        while let Some(field) = map.next_key::<DiffField>()? {
            match field {
                DiffField::First => first_id = Some(map.next_value()?),
                DiffField::Last => last_id = Some(map.next_value()?),
                DiffField::Bids => {
                    map.next_value_seed(LevelsSeed::new(&mut *self.pending))?;
                    bid_end = Some(self.pending.levels.len());
                }
                DiffField::Asks => {
                    if bid_end.is_none() {
                        return Err(A::Error::custom(MalformedPayload::FieldOrder(field.name())));
                    }
                    map.next_value_seed(LevelsSeed::new(&mut *self.pending))?;
                }
                DiffField::Skip => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }

        let (Some(first), Some(last)) = (first_id, last_id) else {
            return Err(A::Error::custom(MalformedPayload::MissingUpdateIds));
        };

        self.pending.meta.push(DiffMeta {
            first_id: first,
            last_id: last,
            start,
            bid_end: bid_end.unwrap_or(start),
            end: self.pending.levels.len(),
        });
        Ok(first)
    }
}

// ---------------------------------------------------------------------------------------
// Combined-stream wrapper
// ---------------------------------------------------------------------------------------

/// The result of decoding one frame off the socket.
#[derive(Debug)]
enum DataFrame<'a> {
    /// A diff was applied into `slot`'s book, which was already `Live`.
    Applied {
        slot: &'a mut BinanceSlot,
        outcome: DiffOutcome,
    },
    /// A diff for `slot`, which is seeded but not yet live. The caller must check that it
    /// straddles `last_update_id` before promoting the slot.
    Seeded {
        slot: &'a mut BinanceSlot,
        outcome: DiffOutcome,
        last_update_id: u64,
    },
    /// `slot` is still bootstrapping, so the diff was staged into its arena. `first_id` (`U`)
    /// is the only id `on_buffered` needs, so `u` is not carried out.
    Buffer {
        slot: &'a mut BinanceSlot,
        first_id: u64,
    },
}

/// A control response: a SUBSCRIBE ack, or a rejection.
#[derive(Debug, PartialEq, Eq)]
struct ControlFrame {
    id: Option<u64>,
    /// The venue's numeric reason, when one could be read. `None` on an ack, and also on a
    /// rejection whose shape [`ErrorValue`] did not recognize - which is why this is not what
    /// decides whether the reply was a rejection.
    code: Option<i64>,
    /// Whether the reply carried a rejection at all. The *presence* of `error` (or a top-level
    /// `code`) is the signal, not a code that parsed: a rejection reported without its code is
    /// still a rejection, and reporting it is the point.
    rejected: bool,
}

/// One decoded frame: a symbol's diff carrying the slot it was decoded into, a control reply
/// carrying none, or an envelope for a stream this connection does not carry. The first two
/// are told apart by the frame's first key - see [`MuxSeed`].
#[derive(Debug)]
enum Frame<'a> {
    Data(DataFrame<'a>),
    Control(ControlFrame),
    /// Well-formed, and for somebody else. Carries the stream name for the log line.
    Unknown(Box<str>),
}

/// Decodes one frame off the socket: either a combined-stream envelope
/// `{"stream": "...", "data": {...}}` or a control reply such as `{"result":null,"id":7}`.
///
/// Which of the two it is gets decided by the *first* key. Binance always puts `stream`
/// first in an envelope, so reading it first makes "the stream is resolved before the body
/// is touched" a fact of the parse rather than something checked afterwards, and it keeps
/// the two unrelated message shapes in two separate functions.
///
/// `processed_slot` is the error-attribution out-param: it starts `None` and stays `None`
/// unless an envelope body was entered and then failed partway through. Levels are applied
/// as they parse, so a mid-body failure can leave the book half-updated; the caller reads
/// this back out of a failed decode to know which slot needs a resync, without the table
/// itself having to remember a mark that could go stale.
#[derive(Debug)]
struct MuxSeed<'a, 'b> {
    table: &'a mut BinanceTable,
    processed_slot: &'b mut Option<&'a mut BinanceSlot>,
}

/// The first key of a frame, which is what decides the frame's shape.
///
/// Only ever read once, at the head of the map. Every subsequent key is read as an
/// [`EnvelopeField`] or a [`ControlField`], neither of which can express the keys that
/// belong to the other shape.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(field_identifier, rename_all = "lowercase")]
enum FirstField {
    Stream,
    Data,
    Id,
    Code,
    Error,
    #[serde(other)]
    Skip,
}

/// Keys of a combined-stream envelope, after `stream` has been consumed.
///
/// A reply's `id`/`code` cannot appear here, so they are not variants: they fold into
/// `Skip` like any other key the envelope does not care about.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(field_identifier, rename_all = "lowercase")]
enum EnvelopeField {
    Data,
    #[serde(other)]
    Skip,
}

/// Keys of a control reply.
///
/// `stream` is not a variant: a frame that starts with `stream` takes the envelope path, so
/// one appearing here is just noise and folds into `Skip`.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(field_identifier, rename_all = "lowercase")]
enum ControlField {
    Id,
    Code,
    /// The stream API nests its rejections as `{"error":{"code":..,"msg":..},"id":N}`, rather
    /// than putting `code` at the top level the way the (separate) WebSocket API does. Without
    /// this variant such a reply folded into `Skip` and got logged as an acknowledgement.
    Error,
    /// A body with no `stream` ahead of it, which there is no way to route.
    Data,
    #[serde(other)]
    Skip,
}

/// Keys of a rejection's nested body. `msg` is not read: it repeats what `code` already says.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(field_identifier, rename_all = "lowercase")]
enum ErrorField {
    Code,
    #[serde(other)]
    Skip,
}

/// What a control reply's `error` (or top-level `code`) key carried.
///
/// Decoded through `deserialize_any` and deliberately *total*: every JSON shape produces a
/// value, none produces an error. The exact envelope Binance's stream API uses for a rejection
/// has not been confirmed against a live socket - the symbol listing now filters out the easy
/// way to provoke one - so the decode must not depend on that guess being right. A shape this
/// does not recognize costs the numeric code in the log line and nothing else, rather than
/// failing the whole frame and producing an `undecodable frame` warning per rejection.
///
/// Null is [`Self::Absent`] rather than a rejection, so an API that spells "no error" as
/// `{"error":null,...}` is not misreported as one - the error that would fire on every
/// successful control frame, which is worse noise than the one this exists to fix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ErrorValue {
    /// The key was present but null: not a rejection.
    Absent,
    /// A rejection. `code` is `None` when the value's shape yielded no integer one.
    Rejected { code: Option<i64> },
}

impl<'de> Deserialize<'de> for ErrorValue {
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        de.deserialize_any(ErrorValueVisitor)
    }
}

struct ErrorValueVisitor;

impl<'de> Visitor<'de> for ErrorValueVisitor {
    type Value = ErrorValue;

    fn expecting(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("any JSON value")
    }

    fn visit_bool<E: serde::de::Error>(self, _v: bool) -> Result<ErrorValue, E> {
        Ok(ErrorValue::Rejected { code: None })
    }

    fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<ErrorValue, E> {
        Ok(ErrorValue::Rejected { code: Some(v) })
    }

    fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<ErrorValue, E> {
        Ok(ErrorValue::Rejected {
            code: i64::try_from(v).ok(),
        })
    }

    fn visit_f64<E: serde::de::Error>(self, _v: f64) -> Result<ErrorValue, E> {
        Ok(ErrorValue::Rejected { code: None })
    }

    /// A code quoted as a string still yields a number; a prose message does not.
    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<ErrorValue, E> {
        Ok(ErrorValue::Rejected {
            code: v.parse().ok(),
        })
    }

    fn visit_none<E: serde::de::Error>(self) -> Result<ErrorValue, E> {
        Ok(ErrorValue::Absent)
    }

    fn visit_some<D: Deserializer<'de>>(self, de: D) -> Result<ErrorValue, D::Error> {
        de.deserialize_any(self)
    }

    fn visit_unit<E: serde::de::Error>(self) -> Result<ErrorValue, E> {
        Ok(ErrorValue::Absent)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<ErrorValue, A::Error> {
        while seq.next_element::<IgnoredAny>()?.is_some() {}
        Ok(ErrorValue::Rejected { code: None })
    }

    /// `{"code":..,"msg":..}` - the shape this is written for. The value of `code` recurses
    /// through this same visitor, so a `code` that is null, a string, or anything else is as
    /// survivable there as it is here.
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<ErrorValue, A::Error> {
        let mut code = None;

        while let Some(field) = map.next_key::<ErrorField>()? {
            match field {
                ErrorField::Code => {
                    if let ErrorValue::Rejected { code: found } = map.next_value()? {
                        code = code.or(found);
                    }
                }
                ErrorField::Skip => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }

        Ok(ErrorValue::Rejected { code })
    }
}

/// Resolves the stream name's symbol prefix to a `&mut BinanceSlot`, borrowing nothing from
/// the frame.
///
/// A name nobody carries is not an error: it comes back as the name itself, so the caller can
/// skip the body and report [`Frame::Unknown`]. That happens routinely for a symbol
/// unsubscribed a moment ago, whose diffs keep arriving until Binance acts on the control
/// frame - every 100ms.
struct StreamLookup<'a> {
    table: &'a mut BinanceTable,
}

impl<'de, 'a> DeserializeSeed<'de> for StreamLookup<'a> {
    type Value = Result<&'a mut BinanceSlot, Box<str>>;

    fn deserialize<D: Deserializer<'de>>(self, de: D) -> Result<Self::Value, D::Error> {
        de.deserialize_str(self)
    }
}

impl<'a> Visitor<'_> for StreamLookup<'a> {
    type Value = Result<&'a mut BinanceSlot, Box<str>>;

    fn expecting(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("a stream name")
    }

    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
        // Symbols are ASCII alphanumeric, so the first `@` is unambiguously the suffix
        // boundary. No allocation on the hit path: this is a borrow of the frame's own bytes.
        let symbol = v.split('@').next().unwrap_or(v);
        Ok(self.table.get_mut(symbol).ok_or_else(|| v.into()))
    }
}

/// Applies or stages one `depthUpdate` body according to the slot's state.
///
/// It only picks a delegate, so it needs no `Visitor` of its own.
struct SlotSeed<'a> {
    slot: &'a mut BinanceSlot,
}

impl<'de, 'a> DeserializeSeed<'de> for SlotSeed<'a> {
    type Value = DataFrame<'a>;

    fn deserialize<D: Deserializer<'de>>(self, de: D) -> Result<DataFrame<'a>, D::Error> {
        match self.slot.state {
            // No book to apply into yet, so the ids and both level arrays go into this
            // slot's own arena for `seed_and_replay` to walk once the snapshot lands.
            SlotState::Bootstrapping(ref mut boot) => {
                let first_id = BufferSeed {
                    pending: &mut boot.pending,
                }
                .deserialize(de)?;
                Ok(DataFrame::Buffer {
                    slot: self.slot,
                    first_id,
                })
            }
            // Seeded but not live: events the snapshot already covers get skipped, and no
            // contiguity can be demanded yet because nothing has been applied to compare to.
            SlotState::Ready(Ready::Seeded { last_update_id }) => {
                let outcome = DiffSeed {
                    book: &mut self.slot.book,
                    min_id: last_update_id,
                    expect_first: None,
                }
                .deserialize(de)?;
                Ok(DataFrame::Seeded {
                    slot: self.slot,
                    outcome,
                    last_update_id,
                })
            }
            SlotState::Ready(Ready::Live { prev_u }) => {
                let outcome = DiffSeed {
                    book: &mut self.slot.book,
                    min_id: 0,
                    expect_first: Some(prev_u + 1),
                }
                .deserialize(de)?;
                Ok(DataFrame::Applied {
                    slot: self.slot,
                    outcome,
                })
            }
        }
    }
}

/// An envelope body that failed to decode, plus whether it was ever entered.
struct EnvelopeError<E> {
    deser_error: E,
    /// True once the `data` key was seen and its seed started running, i.e. levels may
    /// already have been applied to the slot's book - or staged into its arena. Decides
    /// whether the failure gets to blame a slot: a frame that never entered the body left
    /// nothing half-updated.
    had_processing: bool,
}

/// Reads the remainder of an envelope whose stream has already been resolved to `slot`.
fn envelope<'de, 'a, A: MapAccess<'de>>(
    slot: &'a mut BinanceSlot,
    map: A,
) -> Result<DataFrame<'a>, EnvelopeError<A::Error>> {
    fn inner<'de, 'a, A: MapAccess<'de>>(
        slot: &'a mut BinanceSlot,
        mut map: A,
        had_processing: &mut bool,
    ) -> Result<DataFrame<'a>, A::Error> {
        let mut frame = None;

        while let Some(field) = map.next_key::<EnvelopeField>()? {
            match field {
                EnvelopeField::Data => {
                    *had_processing = true;
                    frame = Some(map.next_value_seed(SlotSeed { slot })?);
                }
                EnvelopeField::Skip => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }

        frame.ok_or_else(move || A::Error::custom(MalformedPayload::MissingData))
    }

    let mut had_processing = false;

    inner(slot, map, &mut had_processing).map_err(|err| EnvelopeError {
        deser_error: err,
        had_processing,
    })
}

/// Reads a control reply: `{"result":null,"id":N}`, `{"error":{"code":..,"msg":..},"id":N}`,
/// or the flat `{"code":-1121,"msg":...,"id":N}`.
///
/// `first` is the key already consumed to decide this is not an envelope.
fn control<'de, A: MapAccess<'de>>(
    first: ControlField,
    mut map: A,
) -> Result<ControlFrame, A::Error> {
    let mut id = None;
    let mut code = None;
    let mut rejected = false;
    let mut field = first;

    loop {
        match field {
            ControlField::Id => id = map.next_value()?,
            // Both spellings go through the same total decode. A top-level `code` only ever
            // appears on a rejection - an ack is `{"result":null,"id":N}`, which carries
            // neither key - so either one marks the reply rejected.
            ControlField::Code | ControlField::Error => {
                if let ErrorValue::Rejected { code: found } = map.next_value()? {
                    rejected = true;
                    code = code.or(found);
                }
            }
            ControlField::Data => {
                return Err(A::Error::custom(MalformedPayload::FieldOrder("data")));
            }
            ControlField::Skip => {
                map.next_value::<IgnoredAny>()?;
            }
        }

        match map.next_key::<ControlField>()? {
            Some(next) => field = next,
            None => {
                return Ok(ControlFrame { id, code, rejected });
            }
        }
    }
}

impl<'de, 'a> DeserializeSeed<'de> for MuxSeed<'a, '_> {
    type Value = Frame<'a>;

    fn deserialize<D: Deserializer<'de>>(self, de: D) -> Result<Frame<'a>, D::Error> {
        de.deserialize_map(self)
    }
}

impl<'de, 'a> Visitor<'de> for MuxSeed<'a, '_> {
    type Value = Frame<'a>;

    fn expecting(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("a combined-stream envelope or a control response")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Frame<'a>, A::Error> {
        let Some(first) = map.next_key::<FirstField>()? else {
            return Err(A::Error::custom(MalformedPayload::EmptyFrame));
        };

        match first {
            FirstField::Stream => match map.next_value_seed(StreamLookup { table: self.table })? {
                Ok(slot) => match envelope(slot, map) {
                    Ok(ok) => Ok(Frame::Data(ok)),
                    Err(er) => {
                        if er.had_processing {
                            *self.processed_slot = Some(slot);
                        }
                        Err(er.deser_error)
                    }
                },
                Err(name) => {
                    // Nobody to hand the body to, so skip the rest of the envelope wholesale
                    // rather than parsing levels into a book that does not exist.
                    while map.next_key::<EnvelopeField>()?.is_some() {
                        map.next_value::<IgnoredAny>()?;
                    }
                    Ok(Frame::Unknown(name))
                }
            },
            FirstField::Id => control(ControlField::Id, map).map(Frame::Control),
            FirstField::Code => control(ControlField::Code, map).map(Frame::Control),
            FirstField::Error => control(ControlField::Error, map).map(Frame::Control),
            FirstField::Data => control(ControlField::Data, map).map(Frame::Control),
            FirstField::Skip => control(ControlField::Skip, map).map(Frame::Control),
        }
    }
}

// ---------------------------------------------------------------------------------------
// REST depth snapshot
// ---------------------------------------------------------------------------------------

/// Seeds a *cleared* book from `GET /api/v3/depth`, returning `lastUpdateId`.
#[derive(Debug)]
struct SnapshotSeed<'a> {
    book: &'a mut IncrementalBook,
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq)]
#[serde(field_identifier)]
enum SnapshotField {
    #[serde(rename = "lastUpdateId")]
    LastUpdateId,
    #[serde(rename = "bids")]
    Bids,
    #[serde(rename = "asks")]
    Asks,
    #[serde(other)]
    Skip,
}

impl<'de> DeserializeSeed<'de> for SnapshotSeed<'_> {
    type Value = u64;

    fn deserialize<D: Deserializer<'de>>(self, de: D) -> Result<u64, D::Error> {
        de.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for SnapshotSeed<'_> {
    type Value = u64;

    fn expecting(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("a depth snapshot object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<u64, A::Error> {
        let mut last_update_id: Option<u64> = None;
        let mut merged = None;

        while let Some(field) = map.next_key::<SnapshotField>()? {
            match field {
                SnapshotField::LastUpdateId => last_update_id = Some(map.next_value()?),
                SnapshotField::Bids | SnapshotField::Asks => {
                    let side = if field == SnapshotField::Bids {
                        Side::Bid
                    } else {
                        Side::Ask
                    };
                    let mut sink = BookSink::new(&mut *self.book, side, &mut merged);
                    map.next_value_seed(LevelsSeed::new(&mut sink))?;
                }
                SnapshotField::Skip => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }

        last_update_id.ok_or_else(|| A::Error::custom(MalformedPayload::MissingLastUpdateId))
    }
}

// ---------------------------------------------------------------------------------------
// GET /api/v3/exchangeInfo
// ---------------------------------------------------------------------------------------

/// One entry of `exchangeInfo`'s `symbols` array. Every other key is skipped by serde's
/// default handling of unknown fields.
#[derive(Debug, Deserialize)]
struct SymbolInfo {
    symbol: Box<str>,
    status: Box<str>,
}

#[derive(Debug, Deserialize)]
struct ExchangeInfo {
    symbols: Vec<SymbolInfo>,
}

/// The only `status` Binance uses for a pair that can actually be traded right now; `HALT`,
/// `BREAK`, `PRE_TRADING` and the rest all mean "not now".
const TRADING: &str = "TRADING";

/// Decodes `GET /api/v3/exchangeInfo` into the tradable symbols - `Venue::parse_symbols` for
/// Binance.
///
/// Deserialized into an owned intermediate rather than seeded straight into the set, unlike
/// everything else in this file: this runs once an hour, not once per frame, and the readable
/// version is worth more here than the allocations it saves.
///
/// # Errors
/// [`SymbolsError`] if the body does not decode.
pub(crate) fn parse_symbols(body: Bytes) -> Result<HashSet<Symbol>, SymbolsError> {
    let mut scratch = Scratch::default();
    let info: ExchangeInfo = scratch.with_owned_bytes(body, |data| simd_json::from_slice(data))?;

    Ok(info
        .symbols
        .into_iter()
        .filter(|entry| &*entry.status == TRADING)
        .filter_map(|entry| Symbol::new(entry.symbol).ok())
        .collect())
}

// ---------------------------------------------------------------------------------------
// Entry points consumed by `impl Venue for BinanceSpot`
// ---------------------------------------------------------------------------------------

/// A live event for a slot seeded from the snapshot with nothing applied on top yet.
///
/// Binance's rule, which applies to live events exactly as it does to replayed ones:
/// anything at or below `lastUpdateId` is already in the snapshot and is dropped, and the
/// first event to apply must straddle the boundary.
fn on_seeded(
    slot: &mut BinanceSlot,
    outcome: &DiffOutcome,
    last_update_id: u64,
    generations: &mut Generations,
) {
    if !outcome.applied() {
        // Already covered by the snapshot; `DiffSeed` skipped it without touching the book.
        // Common right after bootstrap, because REST often runs ahead of the feed.
        tracing::trace!(
            symbol = %slot.symbol,
            u = outcome.last_id(),
            last_update_id,
            "event already covered by the snapshot"
        );
        return;
    }

    let boundary = last_update_id + 1;
    if outcome.first_id() > boundary || boundary > outcome.last_id() {
        tracing::warn!(
            symbol = %slot.symbol,
            last_update_id,
            first_id = outcome.first_id(),
            "no event straddles the snapshot, resyncing"
        );
        slot.reset(generations.take());
        return;
    }

    slot.state = SlotState::Ready(Ready::Live {
        prev_u: outcome.last_id(),
    });
    slot.publisher.publish(&slot.book);
    tracing::info!(symbol = %slot.symbol, prev_u = outcome.last_id(), "book live");
}

/// Decodes one frame and acts on it - `Venue::on_frame` for Binance.
pub(crate) fn on_frame<'t>(
    ctx: FrameCtx<'t, '_, '_, Ready, (), Buffered>,
    bytes: Bytes,
) -> FrameAction<'t, Ready, Buffered> {
    let FrameCtx {
        table,
        dec,
        generations,
    } = ctx;
    let (scratch, bufs, _stage) = dec.parts();

    let decoded = decode_frame(bytes, scratch, bufs, table);
    match decoded {
        Ok(Frame::Data(DataFrame::Applied { slot, outcome })) => {
            // `expect_first` already rejected a gapped event before any level was applied,
            // so reaching here means the sequence is intact.
            slot.state = SlotState::Ready(Ready::Live {
                prev_u: outcome.last_id(),
            });
            slot.last_frame = Instant::now();
            if outcome.publish() {
                slot.publisher.publish(&slot.book);
                tracing::trace!(
                    symbol = %slot.symbol,
                    u = outcome.last_id(),
                    "published top of book"
                );
            }
            FrameAction::Handled
        }
        Ok(Frame::Data(DataFrame::Seeded {
            slot,
            outcome,
            last_update_id,
        })) => {
            slot.last_frame = Instant::now();
            on_seeded(slot, &outcome, last_update_id, generations);
            FrameAction::Handled
        }
        Ok(Frame::Data(DataFrame::Buffer { slot, first_id })) => FrameAction::Buffer {
            slot,
            cursor: first_id,
        },
        Ok(Frame::Control(ControlFrame { id, code, rejected })) => {
            if rejected {
                FrameAction::ControlRejected { id, code }
            } else {
                tracing::debug!(?id, "control request acknowledged");
                FrameAction::Handled
            }
        }
        Ok(Frame::Unknown(name)) => FrameAction::Ignored { name },
        Err(DecodeError { slot, err }) => FrameAction::Undecodable { slot, err },
    }
}

/// Seeds `slot.book` from the snapshot body and replays the buffered diffs onto it, returning
/// the [`Ready`] state the slot lands in - `Seeded` if nothing buffered reached past the
/// snapshot, `Live` if one already did. `Venue::seed_and_replay` for Binance.
pub(crate) fn seed_and_replay(
    slot: &mut BinanceSlot,
    pending: &Buffered,
    first_buffered: Option<u64>,
    body: Bytes,
    dec: &mut Decoder<()>,
) -> Result<Ready, BootstrapError> {
    slot.book.clear();
    let (scratch, bufs, _stage) = dec.parts();

    let last_update_id = scratch.with_owned_bytes(body, |data| -> Result<_, _> {
        let mut de = simd_json::Deserializer::from_slice_with_buffers(data, bufs)?;
        SnapshotSeed {
            book: &mut slot.book,
        }
        .deserialize(&mut de)
    })?;

    // The snapshot must reach at least as far as the earliest event we buffered, or there
    // is a hole between them that no amount of replaying can close. Binance's rule is that
    // there is a gap only when the buffered `U` is more than one past `lastUpdateId`:
    // `U == lastUpdateId + 1` is perfectly contiguous, and the boundary check below - which
    // is the same comparison - is what actually decides whether a diff straddles.
    if let Some(first) = first_buffered
        && first > last_update_id + 1
    {
        return Err(BootstrapError::SnapshotGap {
            snapshot: last_update_id,
            first,
        });
    }

    let mut prev_u = last_update_id;
    let mut first_applied = true;

    for diff in pending.diffs() {
        // Everything at or below `lastUpdateId` is already in the snapshot.
        if diff.last_id <= last_update_id {
            continue;
        }

        if first_applied {
            // Binance's own check: the first event kept must straddle the snapshot.
            let boundary = last_update_id + 1;
            if diff.first_id > boundary || boundary > diff.last_id {
                return Err(BootstrapError::SnapshotGap {
                    snapshot: last_update_id,
                    first: diff.first_id,
                });
            }
            first_applied = false;
        } else if diff.first_id != prev_u + 1 {
            return Err(BootstrapError::Gap {
                expected: prev_u + 1,
                got: diff.first_id,
            });
        }

        for &(price, qty) in diff.bids {
            apply_level(&mut slot.book, Side::Bid, price, qty);
        }
        for &(price, qty) in diff.asks {
            apply_level(&mut slot.book, Side::Ask, price, qty);
        }
        prev_u = diff.last_id;
    }

    Ok(if first_applied {
        // Nothing buffered reached past the snapshot. The snapshot may well be ahead of the
        // socket, so the boundary check has to carry over to the first live event.
        Ready::Seeded { last_update_id }
    } else {
        Ready::Live { prev_u }
    })
}

/// A frame that failed to decode, plus the slot to blame - if any. `slot` is `Some` only when
/// the failure happened partway through an envelope body already entered, i.e. the book it
/// points at may be half-updated and needs a resync; see [`MuxSeed`]'s `processed_slot`.
struct DecodeError<'a> {
    slot: Option<&'a mut BinanceSlot>,
    err: simd_json::Error,
}

/// Decodes one frame, handing back the slot to blame alongside a decode error.
fn decode_frame<'a>(
    bytes: Bytes,
    scratch: &mut Scratch,
    bufs: &mut simd_json::Buffers,
    table: &'a mut BinanceTable,
) -> Result<Frame<'a>, DecodeError<'a>> {
    scratch.with_owned_bytes(bytes, |data| -> Result<_, _> {
        let mut de = simd_json::Deserializer::from_slice_with_buffers(data, bufs)
            .map_err(|err| DecodeError { err, slot: None })?;

        let mut error_slot: Option<&'a mut BinanceSlot> = None;

        MuxSeed {
            table,
            processed_slot: &mut error_slot,
        }
        .deserialize(&mut de)
        .map_err(|err| DecodeError {
            slot: error_slot,
            err,
        })
    })
}

#[cfg(test)]
mod test {
    use super::{
        BootstrapError, BufferSeed, Buffered, DiffSeed, Frame, MuxSeed, Ready, SnapshotSeed,
        on_seeded, parse_symbols, seed_and_replay,
    };
    use core_lib::connector::book_publisher::make_book_publisher_pair;
    use core_lib::incremental_book::{IncrementalBook, UpdateResult};
    use core_lib::positive_f64::PositiveF64;
    use core_lib::venue::levels::{Side, apply_level};
    use core_lib::venue::{
        BootstrapRetry as _, Decoder, Generations, PendingDiffs as _, Retry, Slot, SlotState,
        SlotTable, Symbol,
    };
    use serde::de::DeserializeSeed;
    use std::time::Instant;

    /// A genuine `GET /api/v3/depth?symbol=BTCUSDT&limit=5` response.
    const SNAPSHOT: &str = include_str!("../tests/data/depth_snapshot.json");
    /// Its `lastUpdateId`.
    const LAST: u64 = 98_619_652_905;
    /// A `GET /api/v3/exchangeInfo` response, trimmed to four pairs.
    const EXCHANGE_INFO: &str = include_str!("../tests/data/exchange_info.json");

    const BTC: &str = "btcusdt";
    const ETH: &str = "ethusdt";

    fn test_slot(raw_symbol: &str, state: SlotState<Ready, Buffered>) -> Slot<Ready, Buffered> {
        let symbol = Symbol::new(raw_symbol.into()).unwrap();
        let wire_name = crate::symbol::stream_name(&symbol, crate::symbol::DepthSpeed::Fast);
        let (publisher, _reader) = make_book_publisher_pair();
        Slot {
            symbol,
            wire_name,
            book: IncrementalBook::new(),
            publisher,
            state,
            generation: 0,
            last_frame: Instant::now(),
        }
    }

    fn bytes(json: &str) -> Vec<u8> {
        json.as_bytes().to_vec()
    }

    fn run<'a, S>(buf: &'a mut [u8], seed: S) -> Result<S::Value, simd_json::Error>
    where
        S: DeserializeSeed<'a>,
    {
        let mut de = simd_json::Deserializer::from_slice(buf)?;
        seed.deserialize(&mut de)
    }

    fn mux<'t>(
        buf: &mut [u8],
        table: &'t mut SlotTable<Ready, Buffered>,
    ) -> (
        Result<Frame<'t>, simd_json::Error>,
        Option<&'t mut Slot<Ready, Buffered>>,
    ) {
        let mut processed_slot: Option<&'t mut Slot<Ready, Buffered>> = None;
        let result = match simd_json::Deserializer::from_slice(buf) {
            Ok(mut de) => MuxSeed {
                table,
                processed_slot: &mut processed_slot,
            }
            .deserialize(&mut de),
            Err(err) => Err(err),
        };
        (result, processed_slot)
    }

    /// A combined-stream envelope around a `depthUpdate` covering `first..=last`, named for
    /// the one stream [`buffer`] puts in its table.
    fn frame(first: u64, last: u64, bid: &str) -> String {
        format!(
            r#"{{"stream":"btcusdt@depth@100ms","data":{{"e":"depthUpdate","E":1,"s":"X","U":{first},"u":{last},"b":[["{bid}","1.00000000"]],"a":[]}}}}"#
        )
    }

    /// Stages `frames` into an arena the way the live path does, through the real
    /// envelope-and-slot decode rather than by building `Buffered` by hand.
    fn buffer(frames: &[String]) -> Buffered {
        let mut table: SlotTable<Ready, Buffered> = SlotTable::default();
        table
            .insert(test_slot(
                BTC,
                SlotState::bootstrapping(Buffered::default()),
            ))
            .unwrap();

        for json in frames {
            let mut buf = bytes(json);
            let (result, _) = mux(&mut buf, &mut table);
            assert!(matches!(
                result,
                Ok(Frame::Data(super::DataFrame::Buffer { .. }))
            ));
        }

        let slot = table.get_mut(BTC).unwrap();
        let SlotState::Bootstrapping(boot) = &mut slot.state else {
            unreachable!("the slot was inserted bootstrapping")
        };
        std::mem::take(&mut boot.pending)
    }

    fn replay(pending: &Buffered, first_buffered: Option<u64>) -> Result<Ready, BootstrapError> {
        let mut slot = test_slot("BTCUSDT", SlotState::bootstrapping(Buffered::default()));
        let mut dec: Decoder<()> = Decoder::default();
        seed_and_replay(
            &mut slot,
            pending,
            first_buffered,
            SNAPSHOT.into(),
            &mut dec,
        )
    }

    fn pos(v: f64) -> PositiveF64 {
        PositiveF64::new(v).unwrap()
    }

    #[test]
    fn snapshot_seed_reads_a_real_depth_response() {
        let mut book = IncrementalBook::new();
        let mut buf = bytes(SNAPSHOT);
        let last_update_id = run(&mut buf, SnapshotSeed { book: &mut book }).unwrap();

        assert_eq!(last_update_id, LAST);
        assert_eq!(book.first_bids().len(), 5);
        assert_eq!(book.first_asks().len(), 5);
        assert_eq!(book.first_bids().next().unwrap().price(), pos(64_437.42));
    }

    const DIFF: &str = r#"{"e":"depthUpdate","E":1672515782136,"s":"BTCUSDT","U":157,"u":160,
        "b":[["64437.42000000","2.00000000"]],"a":[["64437.43000000","0.00000000"]]}"#;

    #[test]
    fn diff_seed_applies_both_sides_and_reports_ids() {
        let mut book = IncrementalBook::new();
        let mut buf = bytes(DIFF);
        let outcome = run(
            &mut buf,
            DiffSeed {
                book: &mut book,
                min_id: 0,
                expect_first: None,
            },
        )
        .unwrap();

        assert_eq!(outcome.first_id, 157);
        assert_eq!(outcome.last_id, 160);
        assert!(outcome.applied);
        assert!(outcome.publish);
    }

    #[test]
    fn diff_seed_rejects_a_gap_before_applying_any_level() {
        let mut book = IncrementalBook::new();
        apply_level(&mut book, Side::Bid, 100.0, 1.0);

        let mut buf = bytes(DIFF);
        let err = run(
            &mut buf,
            DiffSeed {
                book: &mut book,
                min_id: 0,
                expect_first: Some(99),
            },
        )
        .unwrap_err();

        assert!(err.to_string().contains("sequence gap"), "{err}");
        assert_eq!(book.first_bids().len(), 1);
    }

    #[test]
    fn buffer_seed_reads_the_ids_and_stages_both_sides() {
        let mut pending = Buffered::default();
        let mut buf = bytes(DIFF);
        let first_id = run(
            &mut buf,
            BufferSeed {
                pending: &mut pending,
            },
        )
        .unwrap();

        assert_eq!(first_id, 157);
        assert_eq!(pending.buffered(), 1);

        let diff = pending.diffs().next().unwrap();
        assert_eq!((diff.first_id, diff.last_id), (157, 160));
        assert_eq!(diff.bids, &[(64_437.42, 2.0)]);
        assert_eq!(diff.asks, &[(64_437.43, 0.0)]);
    }

    /// The arena is one flat `Vec` for every diff, so the offsets are what keep two diffs
    /// from bleeding into each other.
    #[test]
    fn a_second_staged_diff_does_not_reach_into_the_first_ones_levels() {
        let mut pending = buffer(&[frame(1, 2, "100.00000000"), frame(3, 4, "200.00000000")]);

        let staged: Vec<_> = pending
            .diffs()
            .map(|diff| (diff.first_id, diff.bids.to_vec(), diff.asks.len()))
            .collect();
        assert_eq!(
            staged,
            vec![(1, vec![(100.0, 1.0)], 0), (3, vec![(200.0, 1.0)], 0)]
        );

        pending.clear();
        assert_eq!(pending.buffered(), 0);
        assert!(pending.diffs().next().is_none());
    }

    /// A staged diff must land on the book exactly as the same diff applied inline does.
    #[test]
    fn replaying_a_staged_diff_matches_applying_it_inline() {
        let mut inline = IncrementalBook::new();
        let mut buf = bytes(DIFF);
        run(
            &mut buf,
            DiffSeed {
                book: &mut inline,
                min_id: 0,
                expect_first: None,
            },
        )
        .unwrap();

        let mut staged = IncrementalBook::new();
        let mut pending = Buffered::default();
        let mut ids = bytes(DIFF);
        run(
            &mut ids,
            BufferSeed {
                pending: &mut pending,
            },
        )
        .unwrap();
        for diff in pending.diffs() {
            for &(price, qty) in diff.bids {
                apply_level(&mut staged, Side::Bid, price, qty);
            }
            for &(price, qty) in diff.asks {
                apply_level(&mut staged, Side::Ask, price, qty);
            }
        }

        let seen = |book: &IncrementalBook| {
            (
                book.first_bids()
                    .map(|l| (l.price(), l.size()))
                    .collect::<Vec<_>>(),
                book.first_asks()
                    .map(|l| (l.price(), l.size()))
                    .collect::<Vec<_>>(),
            )
        };
        assert_eq!(seen(&staged), seen(&inline));
    }

    fn envelope(stream: &str, body: &str) -> String {
        format!(r#"{{"stream":"{stream}","data":{body}}}"#)
    }

    #[test]
    fn mux_routes_a_live_stream_to_its_slot() {
        let mut table: SlotTable<Ready, Buffered> = SlotTable::default();
        table
            .insert(test_slot(
                ETH,
                SlotState::bootstrapping(Buffered::default()),
            ))
            .unwrap();
        table
            .insert(test_slot(
                BTC,
                SlotState::Ready(Ready::Live { prev_u: 156 }),
            ))
            .unwrap();

        let mut buf = bytes(&envelope("btcusdt@depth@100ms", DIFF));
        let (result, processed_slot) = mux(&mut buf, &mut table);
        let Frame::Data(super::DataFrame::Applied { slot, outcome }) = result.unwrap() else {
            panic!("expected an applied diff");
        };
        assert_eq!(outcome.last_id(), 160);
        assert_eq!(slot.book.first_bids().len(), 1);
        assert!(processed_slot.is_none());
    }

    #[test]
    fn mux_stages_into_a_bootstrapping_slot_and_reads_its_first_id() {
        let mut table: SlotTable<Ready, Buffered> = SlotTable::default();
        table
            .insert(test_slot(
                BTC,
                SlotState::bootstrapping(Buffered::default()),
            ))
            .unwrap();

        let mut buf = bytes(&envelope("btcusdt@depth@100ms", DIFF));
        let (result, _processed_slot) = mux(&mut buf, &mut table);

        let Frame::Data(super::DataFrame::Buffer { slot, first_id }) = result.unwrap() else {
            panic!("expected a buffered frame");
        };
        assert_eq!(first_id, 157);
        assert_eq!(
            slot.book.first_bids().len(),
            0,
            "nothing may be applied before the snapshot"
        );

        let SlotState::Bootstrapping(boot) = &slot.state else {
            unreachable!("still bootstrapping")
        };
        assert_eq!(
            boot.pending.buffered(),
            1,
            "the diff must be staged in the slot's own arena"
        );
    }

    #[test]
    fn a_failed_body_hands_back_the_slot_it_was_writing_into() {
        let mut table: SlotTable<Ready, Buffered> = SlotTable::default();
        table
            .insert(test_slot(ETH, SlotState::Ready(Ready::Live { prev_u: 0 })))
            .unwrap();
        table
            .insert(test_slot(
                BTC,
                SlotState::Ready(Ready::Live { prev_u: 156 }),
            ))
            .unwrap();

        let broken = r#"{"U":157,"u":160,"b":[["100.0","1.0"],["oops","1.0"]],"a":[]}"#;
        let mut buf = bytes(&envelope("btcusdt@depth@100ms", broken));
        let (result, processed_slot) = mux(&mut buf, &mut table);

        let err = result.unwrap_err();
        assert!(err.to_string().contains("malformed decimal"), "{err}");
        let slot = processed_slot.expect("a body entered partway through must hand back its slot");
        assert_eq!(slot.wire_name.as_ref(), "btcusdt@depth@100ms");
    }

    /// Diffs for a symbol unsubscribed a moment ago keep arriving every 100ms until Binance
    /// acts on the control frame. That is routine, so it must not surface as a decode error
    /// - which is what produced one `warn!` per frame.
    #[test]
    fn a_stream_this_connection_does_not_carry_is_ignored_rather_than_failed() {
        let mut table: SlotTable<Ready, Buffered> = SlotTable::default();
        table
            .insert(test_slot(
                BTC,
                SlotState::Ready(Ready::Live { prev_u: 156 }),
            ))
            .unwrap();

        let mut buf = bytes(&envelope("dogeusdt@depth@100ms", DIFF));
        let (result, processed_slot) = mux(&mut buf, &mut table);

        let Frame::Unknown(name) = result.unwrap() else {
            panic!("expected the frame to be ignored, not decoded or failed");
        };
        assert_eq!(name.as_ref(), "dogeusdt@depth@100ms");
        assert!(processed_slot.is_none());
    }

    /// Decodes `json` as a control reply.
    fn control(json: &str) -> super::ControlFrame {
        let mut table: SlotTable<Ready, Buffered> = SlotTable::default();
        let mut buf = bytes(json);
        let (result, _processed_slot) = mux(&mut buf, &mut table);

        match result {
            Ok(Frame::Control(control)) => control,
            Ok(other) => panic!("expected a control frame, got {other:?}"),
            Err(err) => panic!("a control reply must always decode: {err}"),
        }
    }

    #[test]
    fn mux_reads_control_acknowledgements() {
        assert_eq!(
            control(r#"{"result":null,"id":7}"#),
            super::ControlFrame {
                id: Some(7),
                code: None,
                rejected: false,
            }
        );
    }

    /// The stream API nests its rejections. Without an `error` variant this folded into
    /// `Skip` and got logged as an acknowledgement, which is how a rejected SUBSCRIBE went
    /// unnoticed.
    #[test]
    fn mux_surfaces_a_nested_stream_api_rejection() {
        assert_eq!(
            control(r#"{"error":{"code":2,"msg":"Invalid request: invalid stream"},"id":3}"#),
            super::ControlFrame {
                id: Some(3),
                code: Some(2),
                rejected: true,
            }
        );
    }

    /// Nothing may depend on `error` arriving first: it decides the frame's shape through
    /// `FirstField`, but `id` ahead of it takes the same path.
    #[test]
    fn a_rejection_is_read_whichever_key_comes_first() {
        assert_eq!(
            control(r#"{"id":3,"error":{"code":2,"msg":"x"}}"#),
            super::ControlFrame {
                id: Some(3),
                code: Some(2),
                rejected: true,
            }
        );
    }

    #[test]
    fn mux_surfaces_a_flat_rejection_too() {
        assert_eq!(
            control(r#"{"code":-1121,"msg":"Invalid symbol.","id":3}"#),
            super::ControlFrame {
                id: Some(3),
                code: Some(-1121),
                rejected: true,
            }
        );
    }

    /// The exact rejection envelope is not confirmed against a live socket, so every shape
    /// `error` might carry has to decode rather than fail. Failing would cost one
    /// `undecodable frame` warning per rejection - worse than the silent swallow this
    /// replaced - and would do it on every reconnect if the venue rejects on every reconnect.
    #[test]
    fn a_rejection_whose_code_cannot_be_read_is_still_a_rejection() {
        for (json, expected) in [
            // A code quoted as a string is still a number.
            (r#"{"error":{"code":"2","msg":"x"},"id":3}"#, Some(2)),
            (r#"{"error":{"msg":"no code at all"},"id":3}"#, None),
            (r#"{"error":{"code":null},"id":3}"#, None),
            (r#"{"error":"something went wrong","id":3}"#, None),
            (r#"{"error":{},"id":3}"#, None),
            (r#"{"error":[1,2],"id":3}"#, None),
            (r#"{"error":true,"id":3}"#, None),
            // A whole extra layer of envelope around it.
            (
                r#"{"status":400,"error":{"code":-1121,"msg":"x"},"id":3}"#,
                Some(-1121),
            ),
        ] {
            assert_eq!(
                control(json),
                super::ControlFrame {
                    id: Some(3),
                    code: expected,
                    rejected: true,
                },
                "{json}"
            );
        }
    }

    /// The other direction: an API that spells "no error" as an explicit null must not have
    /// every successful control frame reported as a rejection.
    #[test]
    fn an_explicit_null_error_is_an_acknowledgement_not_a_rejection() {
        for json in [
            r#"{"error":null,"result":null,"id":3}"#,
            r#"{"result":null,"error":null,"id":3}"#,
            r#"{"code":null,"id":3}"#,
        ] {
            assert_eq!(
                control(json),
                super::ControlFrame {
                    id: Some(3),
                    code: None,
                    rejected: false,
                },
                "{json}"
            );
        }
    }

    #[test]
    fn bootstrap_discards_covered_frames_and_replays_the_rest() {
        let pending = buffer(&[
            frame(LAST - 5, LAST - 1, "64000.00000000"),
            frame(LAST, LAST + 2, "64437.42000000"),
            frame(LAST + 3, LAST + 4, "64437.41000000"),
        ]);

        let state = replay(&pending, Some(LAST - 5)).unwrap();
        assert!(
            matches!(state, Ready::Live { prev_u } if prev_u == LAST + 4),
            "{state:?}"
        );
    }

    #[test]
    fn a_snapshot_ahead_of_every_buffered_diff_leaves_the_slot_seeded() {
        let pending = buffer(&[
            frame(LAST - 200, LAST - 100, "64000.00000000"),
            frame(LAST - 99, LAST - 1, "64001.00000000"),
        ]);

        let state = replay(&pending, Some(LAST - 200)).unwrap();
        assert!(
            matches!(state, Ready::Seeded { last_update_id } if last_update_id == LAST),
            "{state:?}"
        );
    }

    /// The gap check is `U > lastUpdateId + 1`, not `lastUpdateId < U`: a diff whose `U` is
    /// exactly one past the snapshot is the contiguous case Binance's own procedure names, and
    /// rejecting it restarted the bootstrap of a perfectly good pair.
    #[test]
    fn a_snapshot_one_short_of_the_first_buffered_diff_is_contiguous() {
        let pending = buffer(&[frame(LAST + 1, LAST + 2, "64437.42000000")]);

        let state = replay(&pending, Some(LAST + 1)).unwrap();
        assert!(
            matches!(state, Ready::Live { prev_u } if prev_u == LAST + 2),
            "{state:?}"
        );
    }

    #[test]
    fn a_snapshot_older_than_the_buffered_diffs_is_rejected() {
        let pending = buffer(&[frame(LAST + 10, LAST + 11, "64437.42000000")]);

        let err = replay(&pending, Some(LAST + 10)).unwrap_err();
        assert!(
            matches!(err, BootstrapError::SnapshotGap { snapshot, first } if snapshot == LAST && first == LAST + 10),
            "{err}"
        );
    }

    #[test]
    fn the_first_kept_frame_must_straddle_the_snapshot() {
        let pending = buffer(&[frame(LAST + 5, LAST + 6, "64437.42000000")]);

        let err = replay(&pending, Some(LAST)).unwrap_err();
        assert!(matches!(err, BootstrapError::SnapshotGap { .. }), "{err}");
    }

    #[test]
    fn a_hole_between_buffered_frames_is_rejected() {
        let pending = buffer(&[
            frame(LAST, LAST + 2, "64437.42000000"),
            frame(LAST + 9, LAST + 10, "64437.41000000"),
        ]);

        let err = replay(&pending, Some(LAST)).unwrap_err();
        assert!(
            matches!(err, BootstrapError::Gap { expected, got } if expected == LAST + 3 && got == LAST + 9),
            "{err}"
        );
    }

    #[test]
    fn on_seeded_drops_an_event_the_snapshot_already_covers() {
        let mut slot = test_slot(
            BTC,
            SlotState::Ready(Ready::Seeded {
                last_update_id: 500,
            }),
        );
        let mut generations = Generations::default();

        let outcome = super::DiffOutcome::for_test(400, 450, false);
        on_seeded(&mut slot, &outcome, 500, &mut generations);

        assert!(
            matches!(
                slot.state,
                SlotState::Ready(Ready::Seeded {
                    last_update_id: 500
                })
            ),
            "an event the snapshot already covers must not change the slot's state"
        );
        assert_eq!(slot.book.first_bids().len(), 0);
    }

    #[test]
    fn on_seeded_promotes_on_the_straddling_event() {
        let mut slot = test_slot(
            BTC,
            SlotState::Ready(Ready::Seeded {
                last_update_id: 500,
            }),
        );
        slot.book.update_bid(pos(100.0), pos(1.0));
        let mut generations = Generations::default();

        let outcome = super::DiffOutcome::for_test(499, 505, true);
        on_seeded(&mut slot, &outcome, 500, &mut generations);

        let SlotState::Ready(Ready::Live { prev_u }) = slot.state else {
            panic!("a straddling event must promote the slot to live");
        };
        assert_eq!(prev_u, 505);
    }

    #[test]
    fn on_seeded_resyncs_when_nothing_straddles_the_boundary() {
        let mut slot = test_slot(
            BTC,
            SlotState::Ready(Ready::Seeded {
                last_update_id: 500,
            }),
        );
        slot.book.update_bid(pos(100.0), pos(1.0));
        let mut generations = Generations::default();

        let outcome = super::DiffOutcome::for_test(600, 610, true);
        on_seeded(&mut slot, &outcome, 500, &mut generations);

        assert!(
            matches!(slot.state, SlotState::Bootstrapping(_)),
            "a genuine hole must still force a resync"
        );
        assert_eq!(slot.book.first_bids().len(), 0);
    }

    #[test]
    fn zero_quantity_deletes_instead_of_inserting_a_zero_size_level() {
        let mut book = IncrementalBook::new();

        assert_eq!(apply_level(&mut book, Side::Bid, 100.0, 0.0), None);
        apply_level(&mut book, Side::Bid, 100.0, 5.0);
        assert_eq!(
            apply_level(&mut book, Side::Bid, 100.0, 0.0),
            Some(UpdateResult::Close)
        );
        assert_eq!(book.first_bids().len(), 0);
    }

    #[test]
    fn the_listing_keeps_only_pairs_that_are_actually_trading() {
        let listed = parse_symbols(EXCHANGE_INFO.into()).unwrap();

        let mut names: Vec<&str> = listed.iter().map(Symbol::as_str).collect();
        names.sort_unstable();
        assert_eq!(
            names,
            vec!["btcusdt", "ethusdt"],
            "HALT and BREAK pairs must not be subscribable"
        );
    }

    /// Only a hole *between* buffered diffs discredits the buffer; every other failure is the
    /// snapshot's, and retrying against the same diffs is what keeps `first_cursor` from
    /// climbing out of reach.
    #[test]
    fn only_a_sequence_hole_throws_the_buffered_diffs_away() {
        assert_eq!(
            BootstrapError::SnapshotGap {
                snapshot: 1,
                first: 9
            }
            .retry(),
            Retry::Refetch
        );
        assert_eq!(
            BootstrapError::Gap {
                expected: 1,
                got: 9
            }
            .retry(),
            Retry::Resync
        );

        let decode = BootstrapError::Decode(
            simd_json::Deserializer::from_slice(&mut bytes("!")).unwrap_err(),
        );
        assert_eq!(decode.retry(), Retry::Refetch);
    }
}
