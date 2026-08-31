//! What this server advertises: every instrument it will serve, and which venue quotes each
//! side of it.
//!
//! Loaded once, before the server starts serving, and then split in two by
//! [`crate::server::serve`] - the encoded `GetCatalogue` response the accept loop hands out,
//! and the instrument entries the registry resolves against connectors as they intern their
//! symbols. Nothing reloads it: there is no refresh task and no publish channel, so everything
//! downstream holds a value rather than a subscription.
//!
//! Identity on the wire is numeric, but only half of that numbering comes from here. An
//! instrument's [`CatalogueIdx`] - what a client names on a subscribe - is this file's, and it
//! is the entry's *position*: the first `[[instruments]]` table is index zero, the second is
//! one, and so on, so the file never spells an index out and cannot contradict itself about
//! one. A venue's index is not this file's at all; a catalogue names its venues by name, and
//! [`crate::encode::venue_idx`] is what turns that into the number a `Level` carries.
//!
//! An entry whose venue this build does not carry still consumes its position, so dropping one
//! never renumbers the entries after it.
//!
//! Nothing here validates the shape of a symbol. A catalogue entry naming a symbol no venue
//! lists is advertised as written and simply never resolves, which is the same state as a
//! connector that has not refreshed its listing yet - and the connectors are the authority on
//! which of the two it is.

pub(crate) mod encode;
pub(crate) mod source;

use core_lib::Venue;
use core_lib::map::{InternalHashMap, new_internal_map};
use core_lib::shared_string::SharedString;
use md_wire::grpc::CatalogueIdx;
use serde::Deserialize;

/// One venue's spelling of one instrument.
#[derive(Debug, Clone)]
pub(crate) struct CataloguePair {
    venue: Venue,
    /// The venue's own spelling, case-sensitive and never normalised - the same contract
    /// `core_lib`'s instrument registry keeps.
    symbol: SharedString,
}

impl CataloguePair {
    pub(crate) fn venue(&self) -> Venue {
        self.venue
    }

    pub(crate) fn symbol(&self) -> &str {
        &self.symbol
    }
}

/// What the server advertises, as loaded.
#[derive(Debug, Default)]
pub(crate) struct Catalogue {
    /// Instrument indices are the catalogue file's own and may be sparse, so this one stays a
    /// map.
    instruments: InternalHashMap<CatalogueIdx, Box<[CataloguePair]>>,
}

impl Catalogue {
    pub(crate) fn instruments(&self) -> &InternalHashMap<CatalogueIdx, Box<[CataloguePair]>> {
        &self.instruments
    }

    /// Hands the instrument entries to the registry, which is what resolves them.
    pub(crate) fn into_instruments(self) -> InternalHashMap<CatalogueIdx, Box<[CataloguePair]>> {
        self.instruments
    }

    /// Builds a catalogue directly, for a test that needs one without a file.
    ///
    /// Indices are named rather than positional here, unlike a loaded file: a test that wants
    /// a sparse or out-of-order catalogue says so.
    ///
    /// Nothing here is validated - the caller owes it the two rules [`TryFrom<RawCatalogue>`]
    /// enforces: one venue at most once per instrument, and one `(venue, symbol)` pair across
    /// the whole catalogue.
    #[cfg(any(test, feature = "test-util"))]
    pub(crate) fn for_test(instruments: &[(u32, &[(Venue, &str)])]) -> Self {
        let mut built = new_internal_map();
        for &(idx, pairs) in instruments {
            let carried: Vec<CataloguePair> = pairs
                .iter()
                .map(|&(venue, symbol)| CataloguePair {
                    venue,
                    symbol: SharedString::from(symbol),
                })
                .collect();
            built.insert(CatalogueIdx::new(idx), carried.into_boxed_slice());
        }

        Self { instruments: built }
    }
}

/// A catalogue file, as written.
///
/// ```toml
/// [[instruments]]
/// pairs = [{ venue = "binance_spot", symbol = "BTCUSDT" }]
/// ```
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawCatalogue {
    #[serde(default)]
    instruments: Box<[RawInstrument]>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawInstrument {
    pairs: Box<[RawPair]>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPair {
    venue: SharedString,
    symbol: SharedString,
}

/// Why a catalogue file could be read and parsed but is not a catalogue.
///
/// A venue name this build does not carry is *not* here: it is dropped with a `warn!`, along
/// with every instrument entry that names it, because there is nothing this build could ever
/// do with it. These are the ones that make the file self-contradictory instead.
#[derive(Debug, thiserror::Error)]
pub(crate) enum BuildCatalogueError {
    /// One broadcaster serves one instrument's whole pair list, holding one book reader per
    /// venue in it. Two pairs on one venue would want two readers on the same connector,
    /// which is a contradiction rather than a deeper book.
    #[error("instrument idx {} names venue {} twice", .idx.get(), .venue.as_str())]
    DuplicateVenueInInstrument { idx: CatalogueIdx, venue: Venue },
    /// A symbol has exactly one book reader: the connector hard-errors a second subscribe for
    /// one it already carries, and `BookReader` is not `Clone`. Two instruments naming the
    /// same pair would each try to open it.
    #[error(transparent)]
    DuplicatePair(Box<DuplicatePair>),
}

/// The payload of [`BuildCatalogueError::DuplicatePair`], boxed because spelling a pair out
/// takes several times what the other variants carry and a startup error is never hot.
#[derive(Debug, thiserror::Error)]
#[error(
    "{}/{symbol} is named by instruments {} and {}",
    .venue.as_str(),
    .first.get(),
    .second.get()
)]
pub(crate) struct DuplicatePair {
    venue: Venue,
    symbol: SharedString,
    first: CatalogueIdx,
    second: CatalogueIdx,
}

impl TryFrom<RawCatalogue> for Catalogue {
    type Error = BuildCatalogueError;

    fn try_from(raw: RawCatalogue) -> Result<Self, Self::Error> {
        let mut instruments = new_internal_map();
        // Every pair carried so far and the instrument that claimed it, so a symbol named by
        // two instruments is caught here rather than as a connector refusal on the first
        // client to ask for the second one. A `Vec` scanned linearly: a catalogue is tens of
        // entries read once at startup.
        let mut claimed: Vec<(Venue, SharedString, CatalogueIdx)> = Vec::new();

        for (position, entry) in raw.instruments.into_iter().enumerate() {
            // The entry's position *is* its index, and an entry dropped below still consumes
            // one, so an unservable venue never renumbers the entries after it.
            let idx = CatalogueIdx::new(
                u32::try_from(position)
                    .expect("a catalogue file is tens of entries, not four billion"),
            );
            let mut pairs: Vec<CataloguePair> = Vec::with_capacity(entry.pairs.len());
            let mut carried = true;

            for pair in entry.pairs {
                let Some(venue) = Venue::parse(pair.venue.as_str()) else {
                    tracing::warn!(
                        instrument = idx.get(),
                        venue = pair.venue.as_str(),
                        symbol = pair.symbol.as_str(),
                        "dropping a catalogue instrument whose venue this build does not carry"
                    );
                    carried = false;
                    break;
                };
                if pairs.iter().any(|carried_pair| carried_pair.venue == venue) {
                    return Err(BuildCatalogueError::DuplicateVenueInInstrument { idx, venue });
                }
                if let Some((_, _, first)) = claimed
                    .iter()
                    .find(|(known, symbol, _)| *known == venue && **symbol == *pair.symbol)
                {
                    return Err(BuildCatalogueError::DuplicatePair(Box::new(DuplicatePair {
                        venue,
                        symbol: pair.symbol,
                        first: *first,
                        second: idx,
                    })));
                }
                claimed.push((venue, pair.symbol.clone(), idx));
                pairs.push(CataloguePair {
                    venue,
                    symbol: pair.symbol,
                });
            }
            if !carried {
                continue;
            }
            instruments.insert(idx, pairs.into_boxed_slice());
        }

        Ok(Self { instruments })
    }
}

#[cfg(test)]
mod test {
    use super::{BuildCatalogueError, Catalogue, RawCatalogue};
    use core_lib::Venue;
    use md_wire::grpc::CatalogueIdx;

    fn parse(toml: &str) -> Result<Catalogue, BuildCatalogueError> {
        let raw: RawCatalogue = toml::from_str(toml).expect("the fixture is well-formed TOML");
        Catalogue::try_from(raw)
    }

    /// One broadcaster holds one book reader per venue in its instrument, so a venue named
    /// twice under one instrument is a contradiction rather than a deeper book.
    #[test]
    fn a_venue_named_twice_under_one_instrument_is_refused() {
        let err = parse(
            r#"
            [[instruments]]
            pairs = [{ venue = "bitstamp", symbol = "btcusd" }]

            [[instruments]]
            pairs = [
                { venue = "binance_spot", symbol = "BTCUSDT" },
                { venue = "binance_spot", symbol = "ETHUSDT" },
            ]
            "#,
        )
        .expect_err("one instrument cannot be quoted twice on one venue");

        assert!(
            matches!(
                err,
                BuildCatalogueError::DuplicateVenueInInstrument {
                    idx,
                    venue: Venue::BinanceSpot
                } if idx == CatalogueIdx::new(1)
            ),
            "got {err:?}"
        );
    }

    /// A symbol has exactly one book reader - the connector hard-errors a second subscribe -
    /// so two instruments cannot both name it.
    #[test]
    fn one_pair_named_by_two_instruments_is_refused() {
        let err = parse(
            r#"
            [[instruments]]
            pairs = [{ venue = "binance_spot", symbol = "BTCUSDT" }]

            [[instruments]]
            pairs = [
                { venue = "bitstamp", symbol = "btcusd" },
                { venue = "binance_spot", symbol = "BTCUSDT" },
            ]
            "#,
        )
        .expect_err("two instruments cannot both carry binance_spot/BTCUSDT");

        assert!(
            matches!(err, BuildCatalogueError::DuplicatePair(_)),
            "got {err:?}"
        );
        assert_eq!(
            err.to_string(),
            "binance_spot/BTCUSDT is named by instruments 0 and 1"
        );
    }

    /// The same spelling on two *different* venues is not a duplicate: that is the ordinary
    /// shape of an instrument quoted in two places.
    #[test]
    fn one_spelling_shared_by_two_venues_is_carried() {
        let catalogue = parse(
            r#"
            [[instruments]]
            pairs = [
                { venue = "binance_spot", symbol = "BTCUSD" },
                { venue = "bitstamp", symbol = "BTCUSD" },
            ]
            "#,
        )
        .expect("a symbol is only claimed per venue");

        assert_eq!(
            catalogue
                .instruments()
                .get(&CatalogueIdx::new(0))
                .expect("the entry is carried")
                .len(),
            2
        );
    }

    /// The ordinary shape: instruments filed under their position in the file, each pair
    /// naming its venue by name and keeping that venue's own spelling of the symbol.
    #[test]
    fn a_catalogue_files_instruments_under_their_position() {
        let catalogue = parse(
            r#"
            [[instruments]]
            pairs = [{ venue = "bitstamp", symbol = "ethusd" }]

            [[instruments]]
            pairs = [
                { venue = "binance_spot", symbol = "BTCUSDT" },
                { venue = "bitstamp", symbol = "btcusd" },
            ]
            "#,
        )
        .expect("the fixture is a valid catalogue");

        assert_eq!(catalogue.instruments().len(), 2);
        let pairs = catalogue
            .instruments()
            .get(&CatalogueIdx::new(1))
            .expect("the second entry is index one");
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].symbol(), "BTCUSDT");
        assert_eq!(pairs[0].venue(), Venue::BinanceSpot);
        assert_eq!(pairs[1].symbol(), "btcusd");
        assert_eq!(pairs[1].venue(), Venue::Bitstamp);
    }

    /// A venue this build does not carry cannot be served, so it is dropped - and so is every
    /// instrument that names it, since a surviving entry must always have a `Venue` to probe
    /// with. The dropped entry keeps its position, so the entries after it are not renumbered
    /// out from under a client that already read the catalogue.
    #[test]
    fn a_venue_this_build_does_not_carry_takes_its_instruments_with_it() {
        let catalogue = parse(
            r#"
            [[instruments]]
            pairs = [{ venue = "binance_spot", symbol = "BTCUSDT" }]
            [[instruments]]
            pairs = [{ venue = "kraken", symbol = "XBTUSD" }]
            [[instruments]]
            pairs = [{ venue = "bitstamp", symbol = "btcusd" }]
            "#,
        )
        .expect("an unknown venue is dropped rather than fatal");

        assert_eq!(catalogue.instruments().len(), 2);
        assert!(catalogue.instruments().contains_key(&CatalogueIdx::new(0)));
        assert!(
            !catalogue.instruments().contains_key(&CatalogueIdx::new(1)),
            "the kraken entry is dropped"
        );
        assert!(
            catalogue.instruments().contains_key(&CatalogueIdx::new(2)),
            "but it still consumes index one, so the entry after it keeps index two"
        );
    }
}
