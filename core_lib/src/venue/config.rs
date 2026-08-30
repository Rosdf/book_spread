//! Tuning shared by every venue connector, and the generic container - [`ConnectorConfig`] -
//! that pairs it with a venue's own extras.
//!
//! The two halves stay apart all the way down. A venue's [`crate::venue::spec::VenueSpec::Config`]
//! is *only* its extras - endpoints, wire-format knobs - and every `VenueSpec` method is handed
//! that alone, so venue code cannot read or depend on the shared tuning. [`CoreConfig`] belongs
//! to the generic machinery instead, which is the only thing that acts on it. `ConnectorConfig`
//! is where the two meet, and it is what a caller builds and what
//! [`crate::connector::ConnectorHandle::new`] takes.
//!
//! [`CoreConfig`] deliberately carries no stream/REST endpoint: every venue's default endpoint
//! differs, and this type has exactly one [`Default`] impl - so an endpoint would either force
//! one venue's default onto the other or need per-venue overrides for a value venue code
//! already has to turn into a URL anyway. Endpoints stay in a venue's own `Inner`, next to the
//! [`crate::venue::spec::VenueSpec::stream_url`]/[`crate::venue::spec::VenueSpec::snapshot_url`] that
//! consume them.

use serde::Deserialize;
use std::time::Duration;

/// Reads a [`Duration`] written the way a human writes one: `"30s"`, `"500ms"`, `"2m"`.
///
/// serde's own `Duration` impl spells a duration as a struct - `{ secs = 30, nanos = 0 }` in
/// TOML - which is a poor thing to ask of a config file for values that are all plainly "a few
/// seconds". Used through `#[serde(with = "duration_str")]` on the fields below, and through
/// [`duration_str::option`] for the one that is optional.
pub mod duration_str {
    use serde::de::Unexpected;
    use serde::{Deserialize as _, Deserializer};
    use std::borrow::Cow;
    use std::time::Duration;

    /// Suffix, and how many nanoseconds one of it is. Longest first, so `ms` is matched
    /// before `s` would take its `s`.
    const UNITS: [(&str, u64); 4] = [
        ("ms", 1_000_000),
        ("s", 1_000_000_000),
        ("m", 60 * 1_000_000_000),
        ("h", 3_600 * 1_000_000_000),
    ];

    /// # Errors
    ///
    /// A string with no unit suffix this understands, or a count that is not a whole number,
    /// or one whose nanoseconds overflow a `u64` - about 584 years.
    pub fn parse<E: serde::de::Error>(raw: &str) -> Result<Duration, E> {
        let invalid = || E::invalid_value(Unexpected::Str(raw), &"a duration like \"30s\", \"500ms\" or \"2m\"");

        let (written, nanos_per) = UNITS
            .iter()
            .find_map(|&(suffix, nanos)| raw.strip_suffix(suffix).map(|count| (count, nanos)))
            .ok_or_else(invalid)?;
        let count: u64 = written.trim().parse().map_err(|_| invalid())?;
        count
            .checked_mul(nanos_per)
            .map(Duration::from_nanos)
            .ok_or_else(invalid)
    }

    /// # Errors
    ///
    /// Whatever `deserializer` reports, or [`parse`]'s own rejection of the string it read.
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
        // `Cow` rather than `&str`: a format that unescapes, or one that buffers the document
        // (which is what `#[serde(flatten)]` does to everything under it), hands over an owned
        // string, and refusing that would make `"30s"` valid in one place and not another.
        let raw = <Cow<'_, str>>::deserialize(deserializer)?;
        parse(&raw)
    }

    /// The same, for a field that may be absent. `#[serde(with)]` names the module, so an
    /// `Option<Duration>` needs one of its own rather than the `deserialize` above.
    pub mod option {
        use serde::{Deserialize as _, Deserializer};
        use std::borrow::Cow;
        use std::time::Duration;

        /// # Errors
        ///
        /// Whatever `deserializer` reports, or [`super::parse`]'s rejection of the string.
        pub fn deserialize<'de, D: Deserializer<'de>>(
            deserializer: D,
        ) -> Result<Option<Duration>, D::Error> {
            let raw = <Option<Cow<'_, str>>>::deserialize(deserializer)?;
            raw.as_deref().map(super::parse).transpose()
        }
    }
}

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
    /// Spelled the way a human writes a duration - `"30s"` - see [`duration_str`].
    #[serde(with = "duration_str")]
    max_backoff: Duration,
    /// A symbol with no frame for this long is resynced on its own, without touching the
    /// socket or its neighbours. `None` disables the sweep entirely.
    #[serde(with = "duration_str::option")]
    idle_symbol_timeout: Option<Duration>,
    /// How often the idle sweep runs. Irrelevant when `idle_symbol_timeout` is `None`.
    #[serde(with = "duration_str")]
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
/// [`crate::venue::spec::VenueSpec::Config`]).
///
/// The generic machinery holds one of these and never hands the whole thing to a venue: it
/// reads [`Self::core`] for its own tuning and passes [`Self::inner`] to every `VenueSpec` method.
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
