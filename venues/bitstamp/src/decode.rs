//! Decoders for Bitstamp's `diff_order_book` payloads, plus the two entry points [`on_frame`]
//! and [`seed_and_replay`] that back `impl VenueSpec for Bitstamp`.
//!
//! Binance's decoder ([`binance_spot`]'s `decode.rs`) applies every price level straight into
//! the book it belongs to, with zero intermediate model, because its envelope names the
//! stream *first* - `{"stream": "...", "data": {...}}` - so the target book is known before a
//! single level is parsed. Bitstamp's envelope is `{"data": {...}, "channel": "...",
//! "event": "data"}`: the levels arrive before the channel name that says which book they
//! belong to.
//!
//! That forces a two-phase decode. [`LevelStage`] is a reusable buffer - owned by the
//! connection as `VenueSpec::Stage`, cleared and refilled every frame rather than reallocated -
//! that the level arrays are parsed into during the first phase. [`FrameSeed`] then resolves
//! `channel` to a slot and hands the caller back a [`Frame`] naming that slot; the caller
//! applies the already-staged levels onto the resolved book (see [`on_frame`]), or - for a
//! slot that is still bootstrapping - copies them out of the stage into that slot's own
//! [`Buffered`] arena, to be replayed once the snapshot lands. This is still allocation-free
//! per frame in steady state - the stage's `Vec` is reused and diffs are tiny - but it is a
//! real intermediate model where Binance has none, and a mid-frame decode failure here can
//! never leave a book half-updated, since nothing was ever applied to one: `on_frame` always
//! reports `FrameAction::Undecodable { slot: None, .. }` for that reason.
//!
//! The two staging buffers are not the same thing and cannot be collapsed into one.
//! [`LevelStage`] is connection-wide and lives for exactly one frame, because the channel is
//! not known until after the data; [`Buffered`] is one bootstrapping slot's own and lives until
//! its snapshot arrives.
//!
//! There is also no sequence number: Bitstamp gives only a `microtimestamp` per frame, not a
//! Binance-style `U`/`u` pair. So [`LevelStage`] holds one `Vec` plus a split index rather
//! than two `Vec`s or a "which side came first" flag - side order is fixed (bids, then asks),
//! and [`MalformedPayload::FieldOrder`] rejects a payload that breaks it, rather than the
//! decoder trying to cope with either order.

use bytes::Bytes;
use core_lib::connector::InstrumentRegistrar;
use core_lib::incremental_book::{IncrementalBook, UpdateResult};
use core_lib::instrument::InstrumentId;
use core_lib::map::InternalHashSet;
use core_lib::venue::levels::{
    BookSink, LevelSink, LevelsSeed, Side, apply_level, merge, worth_publishing,
};
use core_lib::venue::{
    Decoder, FrameAction, FrameCtx, PendingDiffs, Retry, Scratch, Slot, SlotState,
};
use serde::Deserialize;
use serde::de::{DeserializeSeed, Deserializer, Error as _, IgnoredAny, MapAccess, Visitor};
use std::fmt::{self, Formatter};

type BitstampSlot = Slot<Ready, Buffered>;
type BitstampTable = core_lib::venue::SlotTable<Ready, Buffered>;

/// The half of the slot state machine that is not `Bootstrapping`. Only one shape, unlike
/// Binance: Bitstamp carries no sequence numbers, so once a book is seeded any diff past the
/// snapshot simply applies - there is no phase waiting for one to straddle a boundary.
#[derive(Debug)]
pub struct Ready {
    /// Microtimestamp of the last applied diff.
    last_micro: u64,
}

/// Something that invalidates one symbol's book. The socket and its other symbols are fine.
///
/// No `Gap` variant, unlike Binance: there is no sequence id to gap on here, only
/// [`BootstrapError::SnapshotGap`] against the diffs buffered before the snapshot arrived.
#[derive(Debug, thiserror::Error)]
pub enum BootstrapError {
    #[error(transparent)]
    Fetch(#[from] core_lib::venue::SnapshotFetchError<reqwest::RequestBuilder>),

    #[error("decoding snapshot or buffered diff: {0}")]
    Decode(#[from] simd_json::Error),

    #[error("snapshot microtimestamp {snapshot} does not reach buffered diffs starting at {first}")]
    SnapshotGap { snapshot: u64, first: u64 },
}

/// Which of the two recoveries each failure needs.
///
/// Every variant is the snapshot's problem - it never arrived, it did not decode, or its
/// microtimestamp did not reach the diffs already buffered - so all three retry against those
/// same diffs. There is no counterpart to Binance's `Gap`: Bitstamp carries no sequence ids, so
/// nothing here can ever discredit the buffer.
///
/// The [`BootstrapError::SnapshotGap`] case is the one that matters. Bitstamp serves
/// `order_book` from a cache that advances about once a second while its diff stream advances
/// continuously, so restarting the bootstrap instead - which re-arms `first_cursor` from the
/// next diff - left lower-volume pairs cycling without ever getting a book.
impl core_lib::venue::BootstrapRetry for BootstrapError {
    fn retry(&self) -> Retry {
        match self {
            Self::Fetch(_) | Self::Decode(_) | Self::SnapshotGap { .. } => Retry::Refetch,
        }
    }
}

/// Why `GET /api/v2/trading-pairs-info/` could not be read.
///
/// Its own type rather than a variant of [`BootstrapError`]: the two share no caller and no
/// recovery - a listing failure is retried by the connector, a bootstrap failure resyncs one
/// symbol.
#[derive(Debug, thiserror::Error)]
#[error("decoding the trading-pairs listing: {0}")]
pub struct SymbolsError(#[from] simd_json::Error);

/// Ways a Bitstamp payload can be unusable.
///
/// These are raised from inside `serde` visitors, where the only channel back to the caller is
/// `serde::de::Error::custom`, which takes something `Display`. So they never propagate as
/// themselves - they end up as the message inside a `simd_json::Error`. They still get their
/// own type so each condition is written once and the tests can match on the wording.
///
/// There is no `Gap` variant, unlike Binance's decoder: Bitstamp carries no sequence ids, so a
/// dropped frame is simply undetectable here - see the per-symbol idle resync in
/// `core_lib::venue::connection` for the partial mitigation. A frame naming a channel this
/// connection does not carry is not here either: it is a routine outcome
/// ([`FrameAction::Ignored`]), not a malformed payload.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
enum MalformedPayload {
    #[error("malformed microtimestamp {0:?}")]
    Timestamp(Box<str>),

    /// Bitstamp sends `bids` before `asks` in every `data` object observed live. If that ever
    /// changes, the shared stage's split index would be built from the wrong prefix, so this
    /// fails loudly instead - `asks` arriving before `bids` is the only ordering this covers.
    #[error("field {0:?} arrived before the fields it depends on")]
    FieldOrder(&'static str),

    #[error("data frame missing its channel name")]
    MissingChannel,

    #[error("data frame missing its microtimestamp")]
    MissingMicrotimestamp,

    #[error("frame contained no fields")]
    EmptyFrame,
}

// ---------------------------------------------------------------------------------------
// The staging buffer
// ---------------------------------------------------------------------------------------

/// One frame's parsed levels, staged because Bitstamp names the channel *after* the data.
///
/// Both sides share one buffer: `[..split]` is bids, `[split..]` is asks. Side order is fixed
/// rather than recorded - see the module doc - so there is no field saying which side came
/// first, only the split point between them.
#[derive(Debug, Default)]
pub struct LevelStage {
    levels: Vec<(f64, f64)>,
    split: usize,
}

impl LevelStage {
    /// Empties the buffer while keeping its allocation, so steady-state decoding never
    /// reallocates.
    fn clear(&mut self) {
        self.levels.clear();
        self.split = 0;
    }

    fn push(&mut self, price: f64, qty: f64) {
        self.levels.push((price, qty));
    }

    /// Marks every level pushed so far as a bid. Levels pushed after this call are asks.
    fn close_bids(&mut self) {
        self.split = self.levels.len();
    }

    pub(crate) fn bids(&self) -> &[(f64, f64)] {
        &self.levels[..self.split]
    }

    pub(crate) fn asks(&self) -> &[(f64, f64)] {
        &self.levels[self.split..]
    }
}

/// Applies every level currently staged in `stage` onto `book`, in bid-then-ask order.
///
/// Shared by the live-apply path ([`on_frame`]) and the bootstrap replay path
/// ([`seed_and_replay`]), which is exactly why the levels live in `LevelStage` rather than
/// being applied inline as they are parsed - both callers need the same staged levels applied
/// to two different books at two different times.
fn apply_stage(book: &mut IncrementalBook, stage: &LevelStage) -> Option<UpdateResult> {
    let mut merged = None;
    for &(price, qty) in stage.bids() {
        merge(&mut merged, apply_level(book, Side::Bid, price, qty));
    }
    for &(price, qty) in stage.asks() {
        merge(&mut merged, apply_level(book, Side::Ask, price, qty));
    }
    merged
}

/// Whether `apply_stage`'s result is worth publishing - the top of book moved.
const fn stage_worth_publishing(merged: Option<UpdateResult>) -> bool {
    worth_publishing(merged)
}

impl LevelSink for LevelStage {
    fn push_level(&mut self, price: f64, qty: f64) {
        self.push(price, qty);
    }
}

// ---------------------------------------------------------------------------------------
// The buffered-diff arena
// ---------------------------------------------------------------------------------------

/// Where one buffered diff's levels sit inside [`Buffered::levels`], plus the microtimestamp
/// that decides whether it is replayed at all.
#[derive(Debug, Clone, Copy)]
struct DiffMeta {
    micro: u64,
    start: usize,
    /// End of this diff's bids, and the start of its asks.
    bid_end: usize,
    end: usize,
}

/// One bootstrapping symbol's diffs, copied out of [`LevelStage`] once the frame's channel has
/// resolved to that symbol's slot.
///
/// One flat `Vec` of levels for every buffered diff rather than a `Vec` per diff: a diff is
/// tiny and there can be hundreds of them, so per-diff allocation would dominate.
#[derive(Debug, Default)]
pub struct Buffered {
    levels: Vec<(f64, f64)>,
    meta: Vec<DiffMeta>,
}

/// One buffered diff, resolved back out of the arena.
#[derive(Debug, Clone, Copy)]
struct BufferedDiff<'a> {
    micro: u64,
    bids: &'a [(f64, f64)],
    asks: &'a [(f64, f64)],
}

impl Buffered {
    /// Copies everything currently staged in `stage` in as one more buffered diff.
    ///
    /// A copy rather than a move, because `stage` is connection-wide and the very next frame -
    /// for any symbol on this socket - overwrites it.
    fn push_staged(&mut self, stage: &LevelStage, micro: u64) {
        let start = self.levels.len();
        self.levels.extend_from_slice(stage.bids());
        let bid_end = self.levels.len();
        self.levels.extend_from_slice(stage.asks());
        self.meta.push(DiffMeta {
            micro,
            start,
            bid_end,
            end: self.levels.len(),
        });
    }

    /// Every buffered diff in arrival order.
    fn diffs(&self) -> impl ExactSizeIterator<Item = BufferedDiff<'_>> {
        self.meta.iter().map(|meta| BufferedDiff {
            micro: meta.micro,
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

// ---------------------------------------------------------------------------------------
// The `data` object
// ---------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(field_identifier, rename_all = "lowercase")]
enum DataField {
    Timestamp,
    Microtimestamp,
    Bids,
    Asks,
    /// Present only on a `bts:error` reply's `data` object; every other event's `data` never
    /// carries it, so this falls out as `None` for them.
    Message,
    #[serde(other)]
    Skip,
}

/// Parses a `data` object into `stage`, returning its microtimestamp and, if present, its
/// `message` (only a `bts:error` reply carries one).
struct DataSeed<'s> {
    stage: &'s mut LevelStage,
}

impl<'de> DeserializeSeed<'de> for DataSeed<'_> {
    type Value = (Option<u64>, Option<Box<str>>);

    fn deserialize<D: Deserializer<'de>>(self, de: D) -> Result<Self::Value, D::Error> {
        de.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for DataSeed<'_> {
    type Value = (Option<u64>, Option<Box<str>>);

    fn expecting(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("a data object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut micro: Option<u64> = None;
        let mut message: Option<Box<str>> = None;
        let mut seen_bids = false;

        while let Some(field) = map.next_key::<DataField>()? {
            match field {
                DataField::Microtimestamp => {
                    let raw: &str = map.next_value()?;
                    let parsed = raw
                        .parse()
                        .map_err(|_| A::Error::custom(MalformedPayload::Timestamp(raw.into())))?;
                    micro = Some(parsed);
                }
                DataField::Bids => {
                    seen_bids = true;
                    map.next_value_seed(LevelsSeed::new(&mut *self.stage))?;
                    self.stage.close_bids();
                }
                DataField::Asks => {
                    if !seen_bids {
                        return Err(A::Error::custom(MalformedPayload::FieldOrder("asks")));
                    }
                    map.next_value_seed(LevelsSeed::new(&mut *self.stage))?;
                }
                DataField::Message => {
                    let raw: Option<&str> = map.next_value()?;
                    message = raw.map(Into::into);
                }
                DataField::Timestamp | DataField::Skip => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }

        Ok((micro, message))
    }
}

// ---------------------------------------------------------------------------------------
// The top-level frame
// ---------------------------------------------------------------------------------------

/// A control reply: `bts:subscription_succeeded`, `bts:unsubscription_succeeded`, or
/// `bts:error`. `bts:request_reconnect` is not one of these - it is significant enough to its
/// own caller that [`FrameSeed`] surfaces it as [`Frame::Reconnect`] instead.
#[derive(Debug, PartialEq, Eq)]
struct ControlFrame {
    event: Box<str>,
    /// Empty for a `bts:error` reply - see the module doc on why that reply cannot be
    /// attributed to the subscription that caused it.
    channel: Box<str>,
    message: Option<Box<str>>,
}

/// One decoded frame off the socket.
#[derive(Debug)]
enum Frame<'a> {
    /// A diff for a slot with a live book. The stage (owned by the caller, not by this frame)
    /// is ready to apply.
    Data {
        slot: &'a mut BitstampSlot,
        micro: u64,
    },
    /// The slot is bootstrapping, so the caller should copy the staged levels into its arena -
    /// which this seed cannot do itself, since the stage is the caller's, not the frame's.
    Buffer {
        slot: &'a mut BitstampSlot,
        micro: u64,
    },
    Control(ControlFrame),
    /// `bts:request_reconnect`: the caller should end the session cleanly and reconnect.
    Reconnect,
    /// A well-formed data frame for a channel this connection does not carry, or one not
    /// shaped like `diff_order_book_<pair>` at all. Routine, not an error: diffs for a
    /// just-unsubscribed symbol keep arriving until Bitstamp acts on the control frame.
    Unknown(Box<str>),
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(field_identifier, rename_all = "lowercase")]
enum TopField {
    Data,
    Channel,
    Event,
    #[serde(other)]
    Skip,
}

/// Decodes one frame off the socket, resolving `channel` to a slot for a `data` event.
///
/// Every key is read regardless of arrival order - unlike Binance's envelope, Bitstamp does
/// not put the demux key first, so nothing here can assume `data`, `channel`, and `event`
/// arrive in any particular sequence relative to each other. The one order this decoder does
/// depend on is internal to the `data` object: `bids` before `asks` - see the module doc.
struct FrameSeed<'a, 's> {
    table: &'a mut BitstampTable,
    stage: &'s mut LevelStage,
}

impl<'de, 'a> DeserializeSeed<'de> for FrameSeed<'a, '_> {
    type Value = Frame<'a>;

    fn deserialize<D: Deserializer<'de>>(self, de: D) -> Result<Frame<'a>, D::Error> {
        de.deserialize_map(self)
    }
}

impl<'de, 'a> Visitor<'de> for FrameSeed<'a, '_> {
    type Value = Frame<'a>;

    fn expecting(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("a Bitstamp data frame or control reply")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Frame<'a>, A::Error> {
        self.stage.clear();

        let Some(mut field) = map.next_key::<TopField>()? else {
            return Err(A::Error::custom(MalformedPayload::EmptyFrame));
        };

        let mut micro: Option<u64> = None;
        let mut message: Option<Box<str>> = None;
        // Borrowed, not owned. Both are read back here and here only - `event` against two
        // literals, `channel` straight against the table - and simd-json unescapes in place,
        // so `next_value` hands back a `&str` into the frame buffer at no cost. Owning them
        // would be two mallocs and two copies on every diff frame. The only paths that need
        // an owned copy are the cold ones below, which take it there.
        let mut channel: Option<&'de str> = None;
        let mut event: Option<&'de str> = None;

        loop {
            match field {
                TopField::Data => {
                    let (m, msg) = map.next_value_seed(DataSeed { stage: self.stage })?;
                    micro = m;
                    message = msg;
                }
                TopField::Channel => {
                    channel = Some(map.next_value()?);
                }
                TopField::Event => {
                    event = Some(map.next_value()?);
                }
                TopField::Skip => {
                    map.next_value::<IgnoredAny>()?;
                }
            }

            match map.next_key::<TopField>()? {
                Some(next) => field = next,
                None => break,
            }
        }

        if event == Some("bts:request_reconnect") {
            return Ok(Frame::Reconnect);
        }

        if event == Some("data") {
            let data_channel =
                channel.ok_or_else(|| A::Error::custom(MalformedPayload::MissingChannel))?;
            let data_micro =
                micro.ok_or_else(|| A::Error::custom(MalformedPayload::MissingMicrotimestamp))?;

            // The table is keyed by the whole channel name now, the same string Bitstamp
            // echoes back here.
            let resolved = self.table.get_mut(data_channel);

            let Some(slot) = resolved else {
                return Ok(Frame::Unknown(data_channel.into()));
            };

            return Ok(if matches!(slot.state, SlotState::Bootstrapping(_)) {
                Frame::Buffer {
                    slot,
                    micro: data_micro,
                }
            } else {
                Frame::Data {
                    slot,
                    micro: data_micro,
                }
            });
        }

        Ok(Frame::Control(ControlFrame {
            event: event.unwrap_or_default().into(),
            channel: channel.unwrap_or_default().into(),
            message,
        }))
    }
}

// ---------------------------------------------------------------------------------------
// REST order book snapshot
// ---------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(field_identifier, rename_all = "lowercase")]
enum SnapshotField {
    Timestamp,
    Microtimestamp,
    Bids,
    Asks,
    #[serde(other)]
    Skip,
}

/// Seeds a *cleared* book from `GET /api/v2/order_book/{pair}/`, returning its microtimestamp.
///
/// Unlike the staged `data` object, this applies straight into the book: the book it belongs
/// to is never in doubt for a REST fetch, so there is nothing to stage for.
#[derive(Debug)]
struct SnapshotSeed<'a> {
    book: &'a mut IncrementalBook,
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
        f.write_str("a Bitstamp order book snapshot")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<u64, A::Error> {
        let mut micro: Option<u64> = None;
        let mut merged = None;

        while let Some(field) = map.next_key::<SnapshotField>()? {
            match field {
                SnapshotField::Microtimestamp => {
                    let raw: &str = map.next_value()?;
                    let parsed = raw
                        .parse()
                        .map_err(|_| A::Error::custom(MalformedPayload::Timestamp(raw.into())))?;
                    micro = Some(parsed);
                }
                SnapshotField::Bids => {
                    let mut sink = BookSink::new(&mut *self.book, Side::Bid, &mut merged);
                    map.next_value_seed(LevelsSeed::new(&mut sink))?;
                }
                SnapshotField::Asks => {
                    let mut sink = BookSink::new(&mut *self.book, Side::Ask, &mut merged);
                    map.next_value_seed(LevelsSeed::new(&mut sink))?;
                }
                SnapshotField::Timestamp | SnapshotField::Skip => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }

        micro.ok_or_else(|| A::Error::custom(MalformedPayload::MissingMicrotimestamp))
    }
}

// ---------------------------------------------------------------------------------------
// Entry points consumed by `impl VenueSpec for Bitstamp`
// ---------------------------------------------------------------------------------------

/// Logs one control reply, or reports a rejection for the caller to attribute.
///
/// `bts:error` carries neither an id nor a channel, which is why the attribution has to come
/// from the pacer - see `ControlPacer::names_for`. There is no code either, only a message, so
/// the message is logged here and the rejection carries none.
fn on_control(control: &ControlFrame) -> FrameAction<'static, Ready, Buffered> {
    match control.event.as_ref() {
        "bts:subscription_succeeded" => tracing::debug!(channel = %control.channel, "subscribed"),
        "bts:unsubscription_succeeded" => {
            tracing::debug!(channel = %control.channel, "unsubscribed");
        }
        "bts:error" => {
            tracing::error!(message = ?control.message, "control request rejected by the venue");
            return FrameAction::ControlRejected {
                id: None,
                code: None,
            };
        }
        other => tracing::debug!(event = other, "unrecognized control reply"),
    }
    FrameAction::Handled
}

/// Decodes one frame and acts on it - `VenueSpec::on_frame` for Bitstamp.
pub(crate) fn on_frame<'t>(
    ctx: FrameCtx<'t, '_, '_, Ready, LevelStage, Buffered>,
    bytes: Bytes,
) -> FrameAction<'t, Ready, Buffered> {
    let FrameCtx {
        table,
        dec,
        generations: _,
        received,
    } = ctx;
    let (scratch, bufs, stage) = dec.parts();

    let decoded = scratch.with_owned_bytes(bytes, |data| -> Result<_, _> {
        let mut de = simd_json::Deserializer::from_slice_with_buffers(data, bufs)?;
        FrameSeed { table, stage }.deserialize(&mut de)
    });

    match decoded {
        Ok(Frame::Data { slot, micro }) => {
            slot.last_frame = received;
            let SlotState::Ready(Ready { last_micro }) = slot.state else {
                unreachable!("FrameSeed only returns Frame::Data for a ready slot")
            };
            if micro <= last_micro {
                // Only reachable via duplicate delivery - TCP preserves order on one socket,
                // so this is not a gap, just a repeat.
                tracing::trace!(instrument = %slot.instrument, micro, last_micro, "duplicate diff dropped");
            } else {
                let merged = apply_stage(&mut slot.book, stage);
                slot.state = SlotState::Ready(Ready { last_micro: micro });
                if stage_worth_publishing(merged) {
                    slot.publisher.publish(&slot.book);
                    tracing::trace!(instrument = %slot.instrument, micro, "published top of book");
                }
            }
            FrameAction::Handled
        }
        Ok(Frame::Buffer { slot, micro }) => {
            // The channel resolves only after the data, so the levels are still sitting in the
            // connection-wide stage. Copy them into this slot's own arena now that it is known
            // - the next frame, for any symbol on this socket, overwrites the stage.
            if let SlotState::Bootstrapping(boot) = &mut slot.state {
                boot.pending.push_staged(stage, micro);
            }
            FrameAction::Buffer {
                slot,
                cursor: micro,
            }
        }
        Ok(Frame::Control(control)) => on_control(&control),
        Ok(Frame::Reconnect) => {
            tracing::info!("server requested reconnect");
            FrameAction::Reconnect
        }
        Ok(Frame::Unknown(channel)) => FrameAction::Ignored { name: channel },
        Err(err) => {
            // Nothing to resync: the staged decode never touches a book before the channel
            // resolves, so no half-updated state is possible here - see the module doc.
            FrameAction::Undecodable { slot: None, err }
        }
    }
}

/// Seeds `slot.book` from the snapshot body and replays the buffered diffs onto it, returning
/// the microtimestamp of the last diff applied - the snapshot's own, if nothing buffered
/// reached past it. `VenueSpec::seed_and_replay` for Bitstamp.
///
/// The sync rule, since there is no sequence id to straddle a boundary with: the snapshot's
/// microtimestamp must reach at least as far as the earliest buffered diff, or there is a
/// window whose changes were never seen; every buffered diff at or before the snapshot is
/// already included and is dropped; the rest apply in order, and any diff at all is enough -
/// unlike Binance, nothing here has to straddle a boundary once the snapshot is known good.
pub(crate) fn seed_and_replay(
    slot: &mut BitstampSlot,
    pending: &Buffered,
    first_buffered: Option<u64>,
    body: Bytes,
    dec: &mut Decoder<LevelStage>,
) -> Result<Ready, BootstrapError> {
    slot.book.clear();
    let (scratch, bufs, _stage) = dec.parts();

    let snapshot_micro = scratch.with_owned_bytes(body, |data| -> Result<_, _> {
        let mut de = simd_json::Deserializer::from_slice_with_buffers(data, bufs)?;
        SnapshotSeed {
            book: &mut slot.book,
        }
        .deserialize(&mut de)
    })?;

    if let Some(first) = first_buffered
        && snapshot_micro < first
    {
        return Err(BootstrapError::SnapshotGap {
            snapshot: snapshot_micro,
            first,
        });
    }

    let mut last_micro = snapshot_micro;

    for diff in pending.diffs() {
        if diff.micro <= snapshot_micro {
            // Already included in the snapshot.
            continue;
        }
        for &(price, qty) in diff.bids {
            apply_level(&mut slot.book, Side::Bid, price, qty);
        }
        for &(price, qty) in diff.asks {
            apply_level(&mut slot.book, Side::Ask, price, qty);
        }
        last_micro = diff.micro;
    }

    Ok(Ready { last_micro })
}

// ---------------------------------------------------------------------------------------
// GET /api/v2/trading-pairs-info/
// ---------------------------------------------------------------------------------------

/// One entry of the trading-pairs listing. Every other key - `name`, `description`,
/// `minimum_order`, the decimal precisions - is skipped by serde's default handling of unknown
/// fields.
#[derive(Debug, Deserialize)]
struct PairInfo {
    /// The pair as it appears in a channel name and a REST path, e.g. `btcusd`.
    url_symbol: Box<str>,
    trading: Box<str>,
}

/// The only `trading` value Bitstamp uses for a pair that can be traded right now.
const ENABLED: &str = "Enabled";

/// Decodes `GET /api/v2/trading-pairs-info/` into the tradable symbols -
/// `VenueSpec::parse_symbols` for Bitstamp.
///
/// Deserialized into an owned intermediate rather than seeded straight into the set, unlike
/// everything else in this file: this runs once an hour, not once per frame, and the readable
/// version is worth more here than the allocations it saves.
///
/// # Errors
/// [`SymbolsError`] if the body does not decode.
pub(crate) fn parse_symbols<R: InstrumentRegistrar>(
    body: Bytes,
    reg: &R,
) -> Result<InternalHashSet<InstrumentId>, SymbolsError> {
    let mut scratch = Scratch::default();
    let pairs: Vec<PairInfo> =
        scratch.with_owned_bytes(body, |data| simd_json::from_slice(data))?;

    Ok(pairs
        .into_iter()
        .filter(|pair| &*pair.trading == ENABLED)
        .map(|pair| reg.register(&pair.url_symbol).id())
        .collect())
}

#[cfg(test)]
mod test {
    use super::{
        Buffered, ControlFrame, Frame, FrameSeed, LevelStage, Ready, SnapshotSeed, apply_stage,
        parse_symbols, seed_and_replay,
    };
    use core_lib::Venue;
    use core_lib::connector::VenueGuard;
    use core_lib::connector::book_publisher::make_book_publisher_pair;
    use core_lib::incremental_book::{IncrementalBook, UpdateResult};
    use core_lib::instrument::Instrument;
    use core_lib::positive_f64::PositiveF64;
    use core_lib::venue::levels::{Side, apply_level};
    use core_lib::venue::test_util::test_instrument_for;
    use core_lib::venue::{
        BootstrapRetry as _, Decoder, PendingDiffs as _, Retry, Slot, SlotState, SlotTable,
    };
    use serde::de::DeserializeSeed as _;
    use std::time::Instant;

    /// A real REST body, trimmed to 20 levels per side. The untrimmed one is ~155 KB and does
    /// not belong in git.
    const SNAPSHOT: &str = include_str!("../tests/data/order_book_snapshot.json");
    /// A real `diff_order_book_btcusd` envelope, captured whole.
    const DIFF: &str = include_str!("../tests/data/diff_frame.json");
    /// Its `microtimestamp`.
    const DIFF_MICRO: u64 = 1_787_579_231_798_113;
    /// The snapshot fixture's `microtimestamp`, which is *older* than `DIFF_MICRO`.
    const SNAPSHOT_MICRO: u64 = 1_787_579_216_806_095;
    /// A `GET /api/v2/trading-pairs-info/` response, trimmed to four pairs.
    const PAIRS: &str = include_str!("../tests/data/trading_pairs_info.json");

    fn pos(v: f64) -> PositiveF64 {
        PositiveF64::new(v).unwrap()
    }

    fn bytes(json: &str) -> Vec<u8> {
        json.as_bytes().to_vec()
    }

    fn slot(raw_symbol: &str, state: SlotState<Ready, Buffered>) -> Slot<Ready, Buffered> {
        let instrument = test_instrument_for(Venue::Bitstamp, raw_symbol);
        let wire_name = crate::symbol::channel_name(instrument);
        let (publisher, _reader) = make_book_publisher_pair();
        Slot {
            instrument,
            wire_name,
            book: IncrementalBook::new(),
            publisher,
            state,
            generation: 0,
            last_frame: Instant::now(),
        }
    }

    fn frame<'a>(
        table: &'a mut SlotTable<Ready, Buffered>,
        stage: &mut LevelStage,
        json: &str,
    ) -> Result<Frame<'a>, simd_json::Error> {
        let mut buf = bytes(json);
        let mut de = simd_json::Deserializer::from_slice(&mut buf)?;
        FrameSeed { table, stage }.deserialize(&mut de)
    }

    /// The whole live path for one frame: decode, then do what `on_frame` does for a
    /// bootstrapping slot - copy the staged levels into that slot's own arena.
    fn stage_into_slot(table: &mut SlotTable<Ready, Buffered>, stage: &mut LevelStage, json: &str) {
        let Frame::Buffer { slot, micro } = frame(table, stage, json).unwrap() else {
            panic!("expected a buffered frame");
        };
        let SlotState::Bootstrapping(boot) = &mut slot.state else {
            unreachable!("Frame::Buffer only comes back for a bootstrapping slot")
        };
        boot.pending.push_staged(stage, micro);
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
    fn staged_decode_splits_bids_from_asks_and_applies_both_including_deletes() {
        let mut table: SlotTable<Ready, Buffered> = SlotTable::default();
        table
            .insert(slot("btcusd", SlotState::Ready(Ready { last_micro: 0 })))
            .unwrap();
        let mut stage = LevelStage::default();

        let Frame::Data { slot, micro } = frame(&mut table, &mut stage, DIFF).unwrap() else {
            panic!("expected a live data frame");
        };
        assert_eq!(micro, DIFF_MICRO);

        let merged = apply_stage(&mut slot.book, &stage);
        assert!(merged.is_some(), "a real diff frame must touch the book");
        assert!(
            slot.book.first_bids().len() > 0 || slot.book.first_asks().len() > 0,
            "some level must have survived the deletes"
        );
    }

    #[test]
    fn asks_before_bids_is_rejected() {
        let mut table: SlotTable<Ready, Buffered> = SlotTable::default();
        table
            .insert(slot("btcusd", SlotState::Ready(Ready { last_micro: 0 })))
            .unwrap();
        let mut stage = LevelStage::default();

        let broken = r#"{"data":{"timestamp":"1","microtimestamp":"1","asks":[["1.0","1.0"]],"bids":[["1.0","1.0"]]},"channel":"diff_order_book_btcusd","event":"data"}"#;
        let err = frame(&mut table, &mut stage, broken).unwrap_err();
        assert!(err.to_string().contains("arrived before"), "{err}");
    }

    #[test]
    fn a_bootstrapping_slot_gets_the_staged_levels_copied_into_its_own_arena() {
        let mut table: SlotTable<Ready, Buffered> = SlotTable::default();
        table
            .insert(slot(
                "btcusd",
                SlotState::bootstrapping(Buffered::default()),
            ))
            .unwrap();
        let mut stage = LevelStage::default();

        stage_into_slot(&mut table, &mut stage, DIFF);

        let target = table.get_mut("diff_order_book_btcusd").unwrap();
        assert_eq!(
            target.book.first_bids().len(),
            0,
            "nothing may be applied before the snapshot"
        );
        let SlotState::Bootstrapping(boot) = &target.state else {
            unreachable!("still bootstrapping")
        };
        assert_eq!(boot.pending.buffered(), 1);
        let buffered = boot.pending.diffs().next().unwrap();
        assert_eq!(buffered.micro, DIFF_MICRO);
        assert_eq!(buffered.bids, stage.bids());
        assert_eq!(buffered.asks, stage.asks());
    }

    /// The arena is one flat `Vec` shared by every buffered diff, so the offsets are what keep
    /// two of them from bleeding into each other.
    #[test]
    fn a_second_staged_diff_does_not_reach_into_the_first_ones_levels() {
        let mut table: SlotTable<Ready, Buffered> = SlotTable::default();
        table
            .insert(slot(
                "btcusd",
                SlotState::bootstrapping(Buffered::default()),
            ))
            .unwrap();
        let mut stage = LevelStage::default();

        let second = r#"{"data":{"timestamp":"1","microtimestamp":"1787579231798999","bids":[["100.0","1.0"]],"asks":[["101.0","2.0"]]},"channel":"diff_order_book_btcusd","event":"data"}"#;
        stage_into_slot(&mut table, &mut stage, DIFF);
        stage_into_slot(&mut table, &mut stage, second);

        let target = table.get_mut("diff_order_book_btcusd").unwrap();
        let SlotState::Bootstrapping(boot) = &mut target.state else {
            unreachable!("still bootstrapping")
        };
        let staged: Vec<_> = boot
            .pending
            .diffs()
            .map(|diff| (diff.micro, diff.bids.len(), diff.asks.to_vec()))
            .collect();
        assert_eq!(staged.len(), 2);
        assert_eq!(staged[1], (1_787_579_231_798_999, 1, vec![(101.0, 2.0)]));

        boot.pending.clear();
        assert_eq!(boot.pending.buffered(), 0);
    }

    /// A staged diff must land on the book exactly as the same diff applied inline does.
    #[test]
    fn replaying_a_staged_diff_matches_applying_it_inline() {
        let mut inline_table: SlotTable<Ready, Buffered> = SlotTable::default();
        inline_table
            .insert(slot("btcusd", SlotState::Ready(Ready { last_micro: 0 })))
            .unwrap();
        let mut stage = LevelStage::default();
        let Frame::Data { slot: live, .. } = frame(&mut inline_table, &mut stage, DIFF).unwrap()
        else {
            panic!("expected a live data frame");
        };
        apply_stage(&mut live.book, &stage);

        let mut staged_table: SlotTable<Ready, Buffered> = SlotTable::default();
        staged_table
            .insert(slot(
                "btcusd",
                SlotState::bootstrapping(Buffered::default()),
            ))
            .unwrap();
        let mut staged_stage = LevelStage::default();
        stage_into_slot(&mut staged_table, &mut staged_stage, DIFF);

        let mut replayed = IncrementalBook::new();
        let boot_slot = staged_table.get_mut("diff_order_book_btcusd").unwrap();
        let SlotState::Bootstrapping(boot) = &boot_slot.state else {
            unreachable!("still bootstrapping")
        };
        for diff in boot.pending.diffs() {
            for &(price, qty) in diff.bids {
                apply_level(&mut replayed, Side::Bid, price, qty);
            }
            for &(price, qty) in diff.asks {
                apply_level(&mut replayed, Side::Ask, price, qty);
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
        let inline_slot = inline_table.get_mut("diff_order_book_btcusd").unwrap();
        assert_eq!(seen(&replayed), seen(&inline_slot.book));
    }

    /// Diffs for a symbol unsubscribed a moment ago keep arriving until Bitstamp acts on the
    /// control frame. That is routine, so it must not surface as a decode error.
    #[test]
    fn a_channel_this_connection_does_not_carry_is_ignored_rather_than_failed() {
        let mut table: SlotTable<Ready, Buffered> = SlotTable::default();
        table
            .insert(slot("ethusd", SlotState::Ready(Ready { last_micro: 0 })))
            .unwrap();
        let mut stage = LevelStage::default();

        let Frame::Unknown(channel) = frame(&mut table, &mut stage, DIFF).unwrap() else {
            panic!("expected the frame to be ignored, not decoded or failed");
        };
        assert_eq!(channel.as_ref(), "diff_order_book_btcusd");
    }

    #[test]
    fn mux_reads_a_subscription_succeeded_reply() {
        let mut table: SlotTable<Ready, Buffered> = SlotTable::default();
        let mut stage = LevelStage::default();
        let json = r#"{"event":"bts:subscription_succeeded","channel":"diff_order_book_btcusd","data":{}}"#;

        let Frame::Control(control) = frame(&mut table, &mut stage, json).unwrap() else {
            panic!("expected a control frame");
        };
        assert_eq!(
            control,
            ControlFrame {
                event: "bts:subscription_succeeded".into(),
                channel: "diff_order_book_btcusd".into(),
                message: None,
            }
        );
    }

    #[test]
    fn mux_reads_an_unsubscription_succeeded_reply() {
        let mut table: SlotTable<Ready, Buffered> = SlotTable::default();
        let mut stage = LevelStage::default();
        let json = r#"{"event":"bts:unsubscription_succeeded","channel":"diff_order_book_btcusd","data":{}}"#;

        let Frame::Control(control) = frame(&mut table, &mut stage, json).unwrap() else {
            panic!("expected a control frame");
        };
        assert_eq!(control.event.as_ref(), "bts:unsubscription_succeeded");
    }

    #[test]
    fn mux_reads_an_error_reply_with_no_channel_but_a_message() {
        let mut table: SlotTable<Ready, Buffered> = SlotTable::default();
        let mut stage = LevelStage::default();
        let json = r#"{"event":"bts:error","channel":"","data":{"code":null,"message":"Invalid channel provided."}}"#;

        let Frame::Control(control) = frame(&mut table, &mut stage, json).unwrap() else {
            panic!("expected a control frame");
        };
        assert_eq!(control.channel.as_ref(), "");
        assert_eq!(
            control.message.as_deref(),
            Some("Invalid channel provided.")
        );
    }

    #[test]
    fn mux_reads_a_request_reconnect_as_its_own_variant() {
        let mut table: SlotTable<Ready, Buffered> = SlotTable::default();
        let mut stage = LevelStage::default();
        let json = r#"{"event":"bts:request_reconnect","channel":"","data":{}}"#;

        assert!(matches!(
            frame(&mut table, &mut stage, json).unwrap(),
            Frame::Reconnect
        ));
    }

    #[test]
    fn empty_frame_is_rejected() {
        let mut table: SlotTable<Ready, Buffered> = SlotTable::default();
        let mut stage = LevelStage::default();
        let err = frame(&mut table, &mut stage, "{}").unwrap_err();
        assert!(err.to_string().contains("no fields"), "{err}");
    }

    #[test]
    fn snapshot_seed_reads_a_real_order_book_response() {
        let mut book = IncrementalBook::new();
        let mut buf = bytes(SNAPSHOT);
        let mut de = simd_json::Deserializer::from_slice(&mut buf).unwrap();
        let micro = SnapshotSeed { book: &mut book }
            .deserialize(&mut de)
            .unwrap();

        assert_eq!(micro, SNAPSHOT_MICRO);
        assert_eq!(
            book.first_bids().len(),
            20,
            "the fixture carries 20 levels per side"
        );
        assert!(book.first_asks().len() > 0);
        assert_eq!(book.first_bids().next().unwrap().price(), pos(78_130.25));
    }

    /// Buffered diffs at or before the snapshot are already in it and are dropped; the ones
    /// past it apply, in order, and the last one's microtimestamp is what the slot goes live
    /// on.
    #[test]
    fn replay_drops_covered_diffs_and_applies_the_rest() {
        let mut table: SlotTable<Ready, Buffered> = SlotTable::default();
        table
            .insert(slot(
                "btcusd",
                SlotState::bootstrapping(Buffered::default()),
            ))
            .unwrap();
        let mut stage = LevelStage::default();

        // Older than the snapshot, so it must leave no trace: a level nothing else touches,
        // asserted absent below.
        let covered = format!(
            r#"{{"data":{{"timestamp":"1","microtimestamp":"{}","bids":[["1.00","5.00000000"]],"asks":[]}},"channel":"diff_order_book_btcusd","event":"data"}}"#,
            SNAPSHOT_MICRO - 1000
        );
        stage_into_slot(&mut table, &mut stage, &covered);
        stage_into_slot(&mut table, &mut stage, DIFF);

        let target = table.get_mut("diff_order_book_btcusd").unwrap();
        let SlotState::Bootstrapping(boot) = &mut target.state else {
            unreachable!("still bootstrapping")
        };
        let pending = std::mem::take(&mut boot.pending);
        let mut dec: Decoder<LevelStage> = Decoder::default();

        let ready = seed_and_replay(
            target,
            &pending,
            Some(SNAPSHOT_MICRO - 1000),
            SNAPSHOT.into(),
            &mut dec,
        )
        .unwrap();

        assert_eq!(ready.last_micro, DIFF_MICRO);
        assert!(target.book.first_bids().len() > 0);
        assert!(
            !target
                .book
                .first_bids()
                .any(|level| level.price() == pos(1.00)),
            "a diff the snapshot already covers must not be replayed"
        );
    }

    #[test]
    fn a_snapshot_older_than_the_first_buffered_diff_is_rejected() {
        let mut target = slot("btcusd", SlotState::bootstrapping(Buffered::default()));
        let pending = Buffered::default();
        let mut dec: Decoder<LevelStage> = Decoder::default();

        let err = seed_and_replay(
            &mut target,
            &pending,
            Some(u64::MAX),
            SNAPSHOT.into(),
            &mut dec,
        )
        .unwrap_err();

        assert!(
            matches!(err, super::BootstrapError::SnapshotGap { .. }),
            "{err}"
        );
    }

    #[test]
    fn the_listing_keeps_only_pairs_that_are_actually_trading() {
        let listed = parse_symbols(PAIRS.into(), &VenueGuard::new(Venue::Bitstamp)).unwrap();

        let mut names: Vec<&str> = listed
            .iter()
            .copied()
            .map(|inst| Instrument::by_id(inst).name())
            .collect();
        names.sort_unstable();
        assert_eq!(
            names,
            vec!["btcusd", "ethusd"],
            "disabled pairs must not be subscribable"
        );
    }

    /// Bitstamp serves `order_book` from a cache that advances about once a second while its
    /// diff stream advances continuously, so a snapshot that has not reached the buffered diffs
    /// is the ordinary case rather than a rare one. Restarting the bootstrap instead re-arms
    /// `first_cursor` from the next diff, which is what left lower-volume pairs cycling without
    /// ever getting a book.
    #[test]
    fn every_failure_retries_against_the_diffs_already_buffered() {
        assert_eq!(
            super::BootstrapError::SnapshotGap {
                snapshot: 1,
                first: 9
            }
            .retry(),
            Retry::Refetch
        );

        let decode = super::BootstrapError::Decode(
            simd_json::Deserializer::from_slice(&mut bytes("!")).unwrap_err(),
        );
        assert_eq!(decode.retry(), Retry::Refetch);
    }
}
