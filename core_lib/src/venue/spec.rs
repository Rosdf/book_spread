//! The trait a venue implements to plug into the generic connection/supervisor machinery in
//! [`crate::venue::connection`] and [`crate::venue::supervisor`].
//!
//! Deliberately absent: `R: RestClient` and `W: WsConnector` appear nowhere in [`VenueSpec`]
//! itself. That is what keeps a venue's decode and sequencing logic - the part worth unit
//! testing on its own - free of transport generics and independently testable against plain
//! JSON fixtures, with no socket or HTTP client in sight.
//!
//! Absent for the same reason: the shared tuning. [`VenueSpec::Config`] is a venue's own extras
//! alone, and the connection loop keeps [`crate::venue::config::CoreConfig`] to itself rather
//! than routing it back through this trait - so there is no way for venue code to read a knob
//! that is not about its wire format.

use crate::connector::InstrumentRegistrar;
use crate::instrument::{Instrument, InstrumentId};
use crate::map::InternalHashSet;
use crate::net::RequestBuilder;
use crate::shared_string::SharedString;
use crate::venue::pending::PendingDiffs;
use crate::venue::scratch::Scratch;
use crate::venue::table::{Slot, SlotTable};
use bytes::Bytes;
use std::fmt::Debug;
use std::time::Instant;

/// A control frame's method: subscribe to a symbol's stream, or drop it.
///
/// Building the actual wire payload is a venue's own job - see
/// [`crate::venue::spec::ControlPacer`] - since the shape of that frame is different for every
/// venue; this only names which of the two operations is meant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    Subscribe,
    Unsubscribe,
}

/// Source of [`Slot::generation`] stamps for one connection.
///
/// Monotonic and connection-wide, so a reconnect sweep that resets every slot invalidates every
/// fetch left over from the dead session exactly as a single slot's own reset invalidates that
/// slot's own outstanding fetch. See [`SlotTable`]'s module doc for why the stamp exists at all.
#[derive(Debug, Default)]
pub struct Generations {
    next: u64,
}

impl Generations {
    /// Hands out the next generation, so the caller can stamp a fresh bootstrap attempt with
    /// one nothing else has used.
    pub fn take(&mut self) -> u64 {
        let generation = self.next;
        self.next += 1;
        generation
    }
}

/// The reusable parse state one connection owns, passed as one argument to
/// [`VenueSpec::on_frame`]/[`VenueSpec::seed_and_replay`].
///
/// Generic directly over `Stage` (a venue's [`VenueSpec::Stage`]) rather than over the whole
/// venue, same as [`crate::venue::table::SlotState`] is over `Ready` - see that type's doc.
/// Unbounded as a result: nothing here needs `Stage` to be anything but a plain value.
///
/// Not `Debug`: `simd_json::Buffers` is not, and nothing needs to log this - it is pure decode
/// scratch, never a value worth printing.
pub struct Decoder<Stage> {
    scratch: Scratch,
    bufs: simd_json::Buffers,
    stage: Stage,
}

impl<Stage: Debug> std::fmt::Debug for Decoder<Stage> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Decoder")
            .field("scratch", &self.scratch)
            .field("stage", &self.stage)
            .finish_non_exhaustive()
    }
}

impl<Stage: Default> Default for Decoder<Stage> {
    fn default() -> Self {
        Self {
            scratch: Scratch::default(),
            bufs: simd_json::Buffers::default(),
            stage: Stage::default(),
        }
    }
}

impl<Stage> Decoder<Stage> {
    /// Splits every field out at once, since a venue's decode always needs more than one of
    /// them borrowed independently - `scratch` mutably while also writing into `stage`, say.
    /// There are no single-field accessors for that reason: nothing ever wanted just one.
    pub fn parts(&mut self) -> (&mut Scratch, &mut simd_json::Buffers, &mut Stage) {
        (&mut self.scratch, &mut self.bufs, &mut self.stage)
    }
}

/// Everything [`VenueSpec::on_frame`] needs: the slot table to resolve a frame's symbol against,
/// the reusable decode scratch, and the generation counter for a slot that needs resetting.
///
/// Generic directly over `Ready`, `Stage` and `P` rather than over the whole venue - same
/// reasoning as [`Decoder`] and [`crate::venue::table::SlotState`] - so, again, unbounded.
/// `P` is here only because [`SlotTable`] carries it; nothing in this type touches it.
///
/// Three independent lifetimes, not one shared across all three fields: [`FrameAction`] borrows
/// a slot out of `table` for `'t` and hands it back to the caller, so `'t` has to outlive the
/// call to [`VenueSpec::on_frame`] - but `dec` and `generations` must not be forced to outlive it
/// too, or the caller could never touch `self.handler` again (which owns both) while still
/// holding the returned slot. Naming `'d` and `'g` separately is what lets those two borrows
/// end when the call returns, same as if they had been ordinary by-value arguments.
pub struct FrameCtx<'t, 'd, 'g, Ready, Stage, P> {
    pub table: &'t mut SlotTable<Ready, P>,
    pub dec: &'d mut Decoder<Stage>,
    pub generations: &'g mut Generations,
}

impl<Ready, Stage, P> std::fmt::Debug for FrameCtx<'_, '_, '_, Ready, Stage, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrameCtx").finish_non_exhaustive()
    }
}

/// What decoding and acting on one frame produced.
///
/// Borrows out of [`FrameCtx::table`] for `'t` rather than naming an [`Instrument`] - see
/// [`FrameCtx`]'s doc for why that borrow is free to outlive the call to [`VenueSpec::on_frame`]
/// while `dec`/`generations` are not. This is why the `Handled` path (which needs no slot back)
/// and the `Buffer`/`Undecodable` paths (which do) all cost nothing: no clone, no re-lookup.
#[derive(Debug)]
pub enum FrameAction<'t, Ready, P> {
    /// Applied, a control reply, or deliberately ignored - `VenueSpec::on_frame` has already done
    /// everything there is to do, including publishing and stamping [`Slot::last_frame`].
    Handled,
    /// No book yet for this slot: the diff has already been staged into the slot's own pending
    /// arena, and the core starts the snapshot fetch if it is the first one buffered for this
    /// bootstrap attempt.
    Buffer {
        slot: &'t mut Slot<Ready, P>,
        cursor: u64,
    },
    /// The venue itself asked for a reconnect (e.g. Bitstamp's `bts:request_reconnect`).
    Reconnect,
    /// A well-formed frame naming a stream or channel this connection does not carry.
    ///
    /// Routine rather than an error: a symbol unsubscribed a moment ago keeps arriving until
    /// the venue acts on the control frame - every 100ms, for Binance - and one warning per
    /// such frame was pure noise. This is a first-class outcome rather than a `serde` failure
    /// raised from inside a visitor for exactly that reason.
    Ignored { name: Box<str> },
    /// The venue rejected a control request. `id` is the request id it echoed back, if any;
    /// the core resolves that to the symbols the request named via
    /// [`ControlPacer::names_for`].
    ControlRejected { id: Option<u64>, code: Option<i64> },
    /// The frame could not be decoded.
    ///
    /// `slot` is `Some` only when the failure could have left a half-updated book - i.e. an
    /// envelope for a known slot was entered and a level may already have been applied before
    /// the failure. `None` covers everything else: a malformed control reply, or a failure
    /// before any book was ever touched.
    Undecodable {
        slot: Option<&'t mut Slot<Ready, P>>,
        err: simd_json::Error,
    },
}

/// What a failed bootstrap should do next.
///
/// The two failures look alike from the outside and need opposite recoveries: either the
/// *snapshot* was unusable and everything the slot buffered still is, or the buffered diffs
/// themselves cannot be trusted. Treating the first as the second is what livelocks a symbol -
/// see [`Retry::Refetch`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Retry {
    /// Fetch another snapshot against the diffs already buffered, leaving the arena and
    /// `first_cursor` exactly as they are.
    ///
    /// This is the recovery for a snapshot that did not reach far enough back, and it has to
    /// leave `first_cursor` alone: restarting the bootstrap instead re-arms it from the *next*
    /// diff to arrive, so the bar the next snapshot has to clear moves up every attempt. On a
    /// venue that serves its snapshot from a cache advancing more slowly than its diff stream -
    /// Bitstamp's advances about once a second - the bar outruns the snapshot indefinitely and
    /// the symbol never gets a book at all.
    Refetch,
    /// Throw the whole bootstrap away and start over under a fresh generation: the buffered
    /// diffs are the problem, so another snapshot cannot help.
    Resync,
}

/// Lets a [`VenueSpec::ReplayError`] say which of [`Retry`]'s two recoveries it needs.
///
/// A one-method trait rather than a `VenueSpec` method, because this is a property of the error
/// value and nothing else: the connection already owns everything else the decision needs.
pub trait BootstrapRetry {
    fn retry(&self) -> Retry;
}

/// The venue-specific half of a connector: wire shapes, sequencing rules, and bootstrap
/// procedure.
///
/// Everything transport-shaped - the socket, the REST client, reconnect/backoff, the slot
/// table - is generic and lives in [`crate::venue::connection`] and
/// [`crate::venue::supervisor`].
pub trait VenueSpec: Debug + Sized + Send + Sync + 'static {
    /// This venue's own extras and nothing else - its endpoints, its wire-format knobs.
    ///
    /// Deliberately *not* the whole connector config: the shared tuning lives in
    /// [`crate::venue::config::CoreConfig`], which only the generic machinery ever acts on, and
    /// a caller pairs the two through [`crate::venue::config::ConnectorConfig`]. Every method
    /// below is handed
    /// `&Self::Config` alone, so a venue cannot read - or come to depend on -
    /// `max_pending_frames`, the backoff cap, or anything else that is not its business.
    type Config: Clone + Debug + Send + Sync + 'static;
    /// The half of the slot state machine that is not `Bootstrapping`. Binance's is
    /// `Seeded { last_update_id } | Live { prev_u }`; Bitstamp's is `Live { last_micro }`.
    type Ready: Debug + Send;
    /// Per-connection decode scratch beyond [`Decoder`]'s own `Scratch`/`simd_json::Buffers`.
    /// Bitstamp's is `LevelStage`; Binance needs none, so its is `()`.
    type Stage: Default + Debug + Send;
    /// Where one bootstrapping symbol's diffs are staged, parsed, until its snapshot lands.
    type Pending: PendingDiffs;
    /// Why seeding or replaying a bootstrap failed. Always one symbol's problem, never the
    /// socket's - the caller recovers that slot alone and moves on, along whichever of
    /// [`Retry`]'s two paths the error names.
    type ReplayError: std::error::Error + BootstrapRetry + Send + 'static;
    /// Why a symbol listing could not be decoded. Its own type rather than a variant of
    /// [`Self::ReplayError`]: the two share no caller, no recovery, and no lifetime - a
    /// listing failure is the connector's, a replay failure is one symbol's.
    type SymbolsError: std::error::Error + Send + 'static;
    type Pacer: ControlPacer + Debug;

    fn stream_url(cfg: &Self::Config) -> String;

    /// The REST URL listing every symbol this venue trades.
    fn symbols_url(cfg: &Self::Config) -> String;

    /// Decodes a [`Self::symbols_url`] response into the instruments that are both listed and
    /// currently tradable - anything halted, delisted or not yet trading is left out, so a
    /// subscribe for it is rejected up front rather than discovered from a control rejection.
    ///
    /// Registers every decoded name through `reg` as it goes, under the venue's own spelling -
    /// this is the only place a raw name from the wire becomes an [`Instrument`].
    ///
    /// # Errors
    /// `Self::SymbolsError` if the listing does not decode.
    fn parse_symbols<R: InstrumentRegistrar>(
        body: Bytes,
        reg: &R,
    ) -> Result<InternalHashSet<InstrumentId>, Self::SymbolsError>;

    /// The REST URL to fetch `instrument`'s bootstrap snapshot from.
    fn snapshot_url(cfg: &Self::Config, instrument: Instrument) -> String;

    /// The name this venue's control frames and data frames use for `instrument` on the wire (a
    /// Binance stream name, a Bitstamp channel name).
    fn wire_name(cfg: &Self::Config, instrument: Instrument) -> SharedString;

    /// Decodes one frame and acts on it - the only venue-specific hot path. See [`FrameAction`]
    /// for what each outcome means.
    ///
    /// `bytes` is taken by value: simd-json rewrites its input in place, so there is nothing
    /// worth handing back, and a diff for a bootstrapping symbol is staged into that slot's
    /// [`Self::Pending`] here rather than kept as raw JSON for a second parse later.
    fn on_frame<'t>(
        ctx: FrameCtx<'t, '_, '_, Self::Ready, Self::Stage, Self::Pending>,
        bytes: Bytes,
    ) -> FrameAction<'t, Self::Ready, Self::Pending>;

    /// Seeds `slot`'s book from a fetched REST snapshot and replays `pending`'s already-parsed
    /// diffs onto it, returning the [`Self::Ready`] state the slot lands in.
    ///
    /// `pending` is passed separately rather than read back out of `slot`: the caller lifts it
    /// out with `mem::take` so the arena and the book it replays into can be borrowed at the
    /// same time. Shared rather than mutable, since replaying only reads it - the caller drops
    /// it either way.
    ///
    /// `first_buffered` is the cursor (Binance's `U`, Bitstamp's `microtimestamp`) of the
    /// earliest buffered diff, which the snapshot must reach or there is a hole no amount of
    /// replaying can close.
    ///
    /// # Errors
    /// `Self::ReplayError` if the snapshot fails to decode, or does not reach far enough to
    /// close the gap since bootstrapping started.
    fn seed_and_replay(
        slot: &mut Slot<Self::Ready, Self::Pending>,
        pending: &Self::Pending,
        first_buffered: Option<u64>,
        body: Bytes,
        dec: &mut Decoder<Self::Stage>,
    ) -> Result<Self::Ready, Self::ReplayError>;
}

/// Why a bootstrap snapshot could not be fetched over REST - shared by every venue, since the
/// failure modes of an HTTP GET are not venue-specific.
///
/// Generic over the two leaf error types directly, rather than over `R: RequestBuilder` with
/// associated-type projections as field types: `#[derive(Debug)]` adds a bound for every
/// generic parameter that textually appears in a field, even one reached only through a
/// projection, so a field typed `R::Error` would force a needless `R: Debug` that
/// `R: RequestBuilder` does not provide. Parameterizing over the leaf types instead lets each
/// bound resolve to the `Debug` the corresponding `std::error::Error` supertrait already
/// guarantees. [`SnapshotFetchError`] is the ergonomic alias callers actually use.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotFetchErrorImpl<T, U> {
    #[error("http request: {0}")]
    HttpRequest(T),

    #[error("http response: {0}")]
    HttpResponse(U),

    /// The concurrency semaphore was closed, which only happens on shutdown.
    #[error("connector is shutting down")]
    ShuttingDown,
}

pub type SnapshotFetchError<R> = SnapshotFetchErrorImpl<
    <R as RequestBuilder>::Error,
    <<R as RequestBuilder>::Response as crate::net::Response>::Error,
>;

/// A completed snapshot fetch, routed back to the connection that asked for it. Generic over
/// the leaf error types for the same reason as [`SnapshotFetchErrorImpl`].
#[derive(Debug)]
pub struct SnapshotResultImpl<E1, E2> {
    pub instrument: Instrument,
    pub body: Result<Bytes, SnapshotFetchErrorImpl<E1, E2>>,
    /// The bootstrap attempt this fetch was spawned for. Compared against the slot's current
    /// [`Slot::generation`] on arrival, so a result that outran an unsubscribe/resubscribe or a
    /// reset racing it is discarded rather than applied to a slot it was never meant for.
    pub generation: u64,
}

pub type SnapshotResult<R> = SnapshotResultImpl<
    <R as RequestBuilder>::Error,
    <<R as RequestBuilder>::Response as crate::net::Response>::Error,
>;

impl<E1, E2> SnapshotResultImpl<E1, E2> {
    pub const fn new(
        instrument: Instrument,
        body: Result<Bytes, SnapshotFetchErrorImpl<E1, E2>>,
        generation: u64,
    ) -> Self {
        Self {
            instrument,
            body,
            generation,
        }
    }
}

/// Paces a venue's control frames (subscribe/unsubscribe) onto the wire.
///
/// Binance batches: chunk up to `SUBSCRIBE_CHUNK` names per frame and sleep inline between
/// frames, which is cheap because a `SUBSCRIBE` names many streams at once. Bitstamp cannot
/// batch - each control frame names exactly one channel - so blocking the read half on a sleep
/// per admitted symbol would stall reading for `N * control_gap`; instead it queues and drains
/// one frame per tick of a timer, never blocking the read half. Both meet at this trait so the
/// generic session loop has one code path for either strategy.
pub trait ControlPacer: Default + Send {
    /// Queues one symbol's control frame. Never sends by itself.
    fn enqueue(&mut self, method: Method, name: SharedString);

    /// Sends whatever is due to go out right now, right after a burst of commands has been
    /// admitted. Binance's batching pacer does its whole chunk-and-sleep dance here; Bitstamp's
    /// queueing pacer is a no-op, since it only ever sends on its own timer.
    fn on_admitted<W: crate::net::WsConnector>(
        &mut self,
        stream: &mut W::Stream,
    ) -> impl Future<Output = Result<(), crate::venue::session::SessionError<W>>> + Send;

    /// When the session should next wake to send more, or `None` when nothing is queued.
    fn next_deadline(&self) -> Option<Instant>;

    /// Sends whatever is due at the deadline named by [`Self::next_deadline`].
    fn on_deadline<W: crate::net::WsConnector>(
        &mut self,
        stream: &mut W::Stream,
    ) -> impl Future<Output = Result<(), crate::venue::session::SessionError<W>>> + Send;

    /// The wire names the control request `id` asked about, for attributing a rejection back
    /// to the symbols that caused it. Empty when nothing is known about `id`.
    ///
    /// This lives on the pacer rather than on [`FrameCtx`] because it is transport state - who
    /// was told what, and when - which that context deliberately carries none of. What "known"
    /// means is each pacer's own business: Binance echoes the id back, so its batching pacer
    /// keeps a bounded map of the ids still in flight; Bitstamp's `bts:error` carries neither
    /// an id nor a channel, so its queueing pacer can only name the frame it last sent.
    fn names_for(&self, id: Option<u64>) -> &[SharedString];
}
