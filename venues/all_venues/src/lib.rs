//! The one enum that names every venue this workspace carries.
//!
//! A crate of its own, and not part of `core_lib`, because it has to sit *below* `core_lib`
//! in the dependency graph: `core_lib` names a `Venue` on every [`Instrument`] it interns, and
//! each venue crate names its own [`Venue`] variant on its `Connector` impl. Putting the enum
//! in `core_lib` itself would work for the first of those but not the second - a venue crate
//! already depends on `core_lib`, so `core_lib` cannot depend back on a venue crate for its
//! identity. This crate depends on nothing, so both directions are free to depend on it.
//!
//! [`Instrument`]: https://docs.rs/core_lib (core_lib::instrument::Instrument)

/// One market this workspace connects to.
///
/// Not `#[non_exhaustive]`: every venue this build carries is listed here, and a `match` over
/// it - [`Venue::as_str`], a server's per-venue connector table - is meant to stop compiling
/// the day a variant is added, not to silently fall through.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Venue {
    BinanceSpot,
    Bitstamp,
}

impl Venue {
    /// The name a client puts in a subscribe request, and the one echoed back on every update.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BinanceSpot => "binance_spot",
            Self::Bitstamp => "bitstamp",
        }
    }

    /// Case-insensitive. `None` for a venue this build does not carry.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        if raw.eq_ignore_ascii_case("binance_spot") {
            Some(Self::BinanceSpot)
        } else if raw.eq_ignore_ascii_case("bitstamp") {
            Some(Self::Bitstamp)
        } else {
            None
        }
    }
}
