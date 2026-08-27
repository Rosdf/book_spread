//! Tuning shared by every venue connector, and the generic container - [`ConnectorConfig`] -
//! that pairs it with a venue's own extras.
//!
//! The two halves stay apart all the way down. A venue's [`crate::venue::spec::Venue::Config`]
//! is *only* its extras - endpoints, wire-format knobs - and every `Venue` method is handed
//! that alone, so venue code cannot read or depend on the shared tuning. [`CoreConfig`] belongs
//! to the generic machinery instead, which is the only thing that acts on it. `ConnectorConfig`
//! is where the two meet, and it is what a caller builds and what
//! [`crate::connector::ConnectorHandle::new`] takes.
//!
//! [`CoreConfig`] deliberately carries no stream/REST endpoint: every venue's default endpoint
//! differs, and this type has exactly one [`Default`] impl - so an endpoint would either force
//! one venue's default onto the other or need per-venue overrides for a value venue code
//! already has to turn into a URL anyway. Endpoints stay in a venue's own `Inner`, next to the
//! [`crate::venue::spec::Venue::stream_url`]/[`crate::venue::spec::Venue::snapshot_url`] that
//! consume them.

use serde::Deserialize;
use std::time::Duration;

/// Tuning every venue connector needs, regardless of wire format.
///
/// Private fields behind getters, and built by overriding [`Default`] one named field at a
/// time. A positional constructor is the wrong shape here: six arguments, three of them
/// `Duration` and two of them `usize`, are trivially swappable at a call site and every venue
/// was passing values it did not actually mean to change. `CoreConfig::default().with_*()`
/// names each override and leaves the rest alone.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CoreConfig {
    /// Kept well under any venue's hard per-socket cap, so one socket failure resyncs a
    /// bounded number of symbols.
    max_symbols_per_connection: usize,
    /// Bounds the REST burst when a whole connection rebootstraps at once.
    max_concurrent_snapshots: usize,
    /// How many frames a symbol may buffer while waiting for its snapshot.
    max_pending_frames: usize,
    /// Deserialized through serde's own `Duration` impl, so a config file spells this
    /// `{ "secs": 30, "nanos": 0 }`.
    max_backoff: Duration,
    /// A symbol with no frame for this long is resynced on its own, without touching the
    /// socket or its neighbours. `None` disables the sweep entirely.
    idle_symbol_timeout: Option<Duration>,
    /// How often the idle sweep runs. Irrelevant when `idle_symbol_timeout` is `None`.
    idle_scan_interval: Duration,
}

impl CoreConfig {
    #[must_use]
    pub const fn with_max_symbols_per_connection(mut self, symbols: usize) -> Self {
        self.max_symbols_per_connection = symbols;
        self
    }

    #[must_use]
    pub const fn with_max_concurrent_snapshots(mut self, snapshots: usize) -> Self {
        self.max_concurrent_snapshots = snapshots;
        self
    }

    #[must_use]
    pub const fn with_max_pending_frames(mut self, frames: usize) -> Self {
        self.max_pending_frames = frames;
        self
    }

    #[must_use]
    pub const fn with_max_backoff(mut self, backoff: Duration) -> Self {
        self.max_backoff = backoff;
        self
    }

    #[must_use]
    pub const fn with_idle_symbol_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.idle_symbol_timeout = timeout;
        self
    }

    #[must_use]
    pub const fn with_idle_scan_interval(mut self, interval: Duration) -> Self {
        self.idle_scan_interval = interval;
        self
    }

    pub const fn max_symbols_per_connection(&self) -> usize {
        self.max_symbols_per_connection
    }

    pub const fn max_concurrent_snapshots(&self) -> usize {
        self.max_concurrent_snapshots
    }

    pub const fn max_pending_frames(&self) -> usize {
        self.max_pending_frames
    }

    pub const fn max_backoff(&self) -> Duration {
        self.max_backoff
    }

    pub const fn idle_symbol_timeout(&self) -> Option<Duration> {
        self.idle_symbol_timeout
    }

    pub const fn idle_scan_interval(&self) -> Duration {
        self.idle_scan_interval
    }
}

/// The baseline every venue starts from, and what a config file that specifies `"core": {...}`
/// partially - or omits it - fills the gaps with. A venue overrides only the fields it has a
/// reason to differ on, through the `with_*` methods above.
impl Default for CoreConfig {
    fn default() -> Self {
        Self {
            max_symbols_per_connection: 200,
            max_concurrent_snapshots: 8,
            max_pending_frames: 512,
            max_backoff: Duration::from_secs(30),
            idle_symbol_timeout: Some(Duration::from_secs(60)),
            idle_scan_interval: Duration::from_secs(10),
        }
    }
}

/// Whole-connector config: [`CoreConfig`] paired with `Inner`, a venue's own extras (its
/// endpoints and whatever else its wire format needs - see
/// [`crate::venue::spec::Venue::Config`]).
///
/// The generic machinery holds one of these and never hands the whole thing to a venue: it
/// reads [`Self::core`] for its own tuning and passes [`Self::inner`] to every `Venue` method.
///
/// `inner` is flattened, so a config file spells this `{"core": {...}, "stream_endpoint":
/// "...", ...}` - `inner`'s own fields sit next to `core`, not nested under an `"inner"` key.
/// `deny_unknown_fields` cannot live here - serde forbids combining it with `flatten` - so it
/// lives on `Inner` itself instead (see e.g. `binance_spot::subscription::Extra`); an unknown
/// top-level key still gets rejected there, since flatten hands every field it does not
/// recognize itself straight to `Inner`'s own deserializer.
///
/// `core` defaults to [`CoreConfig::default`] when absent; `inner`'s fields default however
/// `Inner`'s own `#[serde(default)]` says. A venue wanting different `CoreConfig` values than
/// that baseline overrides them in [`Defaults::default_core`].
#[derive(Debug, Clone, Deserialize)]
pub struct ConnectorConfig<Inner> {
    #[serde(default)]
    core: CoreConfig,
    #[serde(flatten)]
    inner: Inner,
}

impl<Inner> ConnectorConfig<Inner> {
    pub const fn new(core: CoreConfig, inner: Inner) -> Self {
        Self { core, inner }
    }

    pub const fn core(&self) -> &CoreConfig {
        &self.core
    }

    pub const fn inner(&self) -> &Inner {
        &self.inner
    }
}

/// Lets a venue's `Inner` type supply its own [`CoreConfig`] defaults, so
/// [`ConnectorConfig<Inner>`] can implement [`Default`] without every venue writing that impl
/// itself.
///
/// A venue crate cannot `impl Default for core_lib::venue::ConnectorConfig<Extra>` directly -
/// `Default` has no generic parameters of its own for `Extra` to occupy, so Rust's orphan rules
/// reject an impl of a foreign zero-parameter trait for a foreign generic type instantiated
/// with a local one. Implementing this trait for `Extra` instead is a plain
/// local-trait-for-local-type impl, and the blanket impl below turns it into
/// `ConnectorConfig<Extra>: Default`. That is what makes `ConnectorConfig::default()` at a call
/// site pick up a venue's overrides for *both* halves at once.
///
/// [`ConnectorConfig<Inner>`]: ConnectorConfig
pub trait Defaults: Default {
    /// Falls back to [`CoreConfig::default`] when a venue has no reason to differ from it.
    fn default_core() -> CoreConfig {
        CoreConfig::default()
    }
}

impl<Inner: Defaults> Default for ConnectorConfig<Inner> {
    fn default() -> Self {
        Self::new(Inner::default_core(), Inner::default())
    }
}
