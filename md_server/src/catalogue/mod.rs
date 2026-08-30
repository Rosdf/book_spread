//! What this server advertises: the venue index table, and every instrument it will serve.
//!
//! Loaded once, before the server starts serving, and then split in two by
//! [`crate::server::serve`] - the encoded `GetCatalogue` response the accept loop hands out,
//! and the instrument entries the registry resolves against connectors as they intern their
//! symbols. Nothing reloads it: there is no refresh task and no publish channel, so everything
//! downstream holds a value rather than a subscription.
//!
//! Identity on the wire is numeric because of this file. A client reads the two tables once,
//! and everything after that is a [`CatalogueIdx`] on a subscribe and a [`VenueIdx`] on every
//! level.
//!
//! Nothing here validates the shape of a symbol. A catalogue entry naming a symbol no venue
//! lists is advertised as written and simply never resolves, which is the same state as a
//! connector that has not refreshed its listing yet - and the connectors are the authority on
//! which of the two it is.

pub(crate) mod encode;
pub(crate) mod source;

use core_lib::Venue;
use core_lib::heapless_linear_map::HeaplessLinearMap;
use core_lib::map::{InternalHashMap, new_internal_map};
use core_lib::shared_string::SharedString;
use md_wire::grpc::{CatalogueIdx, VenueIdx};
use serde::Deserialize;

/// One venue's spelling of one instrument.
#[derive(Debug, Clone)]
pub(crate) struct CataloguePair {
    venue: Venue,
    venue_idx: VenueIdx,
    /// The venue's own spelling, case-sensitive and never normalised - the same contract
    /// `core_lib`'s instrument registry keeps.
    symbol: SharedString,
}

impl CataloguePair {
    pub(crate) fn venue(&self) -> Venue {
        self.venue
    }

    pub(crate) fn venue_idx(&self) -> VenueIdx {
        self.venue_idx
    }

    pub(crate) fn symbol(&self) -> &str {
        &self.symbol
    }
}

/// The venue table: at most one entry per [`Venue`] this build carries.
///
/// A linear map rather than a hash map because the size is a compile-time fact - two entries
/// today - so this allocates nothing and hashes nothing, and a scan over two keys beats both.
type VenueTable = HeaplessLinearMap<VenueIdx, Venue, { Venue::COUNT }>;

/// What the server advertises, as loaded.
#[derive(Debug, Default)]
pub(crate) struct Catalogue {
    venues: VenueTable,
    /// Instrument indices are the catalogue file's own and may be sparse, so this one stays a
    /// map.
    instruments: InternalHashMap<CatalogueIdx, Box<[CataloguePair]>>,
}

impl Catalogue {
    /// The venue table, for the one thing that has to write it out.
    pub(crate) fn venues(&self) -> &VenueTable {
        &self.venues
    }

    pub(crate) fn instruments(&self) -> &InternalHashMap<CatalogueIdx, Box<[CataloguePair]>> {
        &self.instruments
    }

    /// Hands the instrument entries to the registry, which is what resolves them.
    pub(crate) fn into_instruments(self) -> InternalHashMap<CatalogueIdx, Box<[CataloguePair]>> {
        self.instruments
    }

    /// Builds a catalogue directly, for a test that needs one without a file.
    ///
    /// The venue table is every [`Venue`] this build carries, at its position in
    /// [`Venue::ALL`], so a test and the server agree on which index means which venue
    /// without either spelling it out.
    ///
    /// Nothing here is validated - the caller owes it the two rules [`TryFrom<RawCatalogue>`]
    /// enforces: one venue at most once per instrument, and one `(venue, symbol)` pair across
    /// the whole catalogue.
    #[cfg(any(test, feature = "test-util"))]
    pub(crate) fn for_test(instruments: &[(u32, &[(Venue, &str)])]) -> Self {
        let mut venues = VenueTable::new();
        for (position, venue) in Venue::ALL.into_iter().enumerate() {
            let idx = u32::try_from(position).expect("this build carries a handful of venues");
            venues
                .insert(VenueIdx::new(idx), venue)
                .map_err(|_| ())
                .expect("the table is sized for every venue this build carries");
        }

        let mut built = new_internal_map();
        for &(idx, pairs) in instruments {
            let carried: Vec<CataloguePair> = pairs
                .iter()
                .map(|&(venue, symbol)| CataloguePair {
                    venue,
                    venue_idx: venue_idx_of(&venues, venue),
                    symbol: SharedString::from(symbol),
                })
                .collect();
            built.insert(CatalogueIdx::new(idx), carried.into_boxed_slice());
        }

        Self {
            venues,
            instruments: built,
        }
    }
}

/// A catalogue file, as written.
///
/// ```toml
/// [[venues]]
/// idx  = 0
/// name = "binance_spot"
///
/// [[instruments]]
/// idx   = 0
/// pairs = [{ venue = "binance_spot", symbol = "BTCUSDT" }]
/// ```
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawCatalogue {
    #[serde(default)]
    venues: Box<[RawVenue]>,
    #[serde(default)]
    instruments: Box<[RawInstrument]>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawVenue {
    idx: u32,
    name: SharedString,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawInstrument {
    idx: u32,
    pairs: Box<[RawPair]>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPair {
    venue: u32,
    symbol: SharedString,
}

/// Why a catalogue file could be read and parsed but is not a catalogue.
///
/// A venue name this build does not carry is *not* here: it is dropped with a `warn!`, along
/// with every instrument entry that names it, because there is nothing this build could ever
/// do with it. These are the ones that make the file self-contradictory instead.
#[derive(Debug, thiserror::Error)]
pub(crate) enum BuildCatalogueError {
    #[error("venue idx {} is used by two venues", .0.get())]
    DuplicateVenueIdx(VenueIdx),
    #[error("venue {} appears twice", .0.as_str())]
    DuplicateVenue(Venue),
    #[error(
        "the venue table has more entries than this build has venues ({})",
        Venue::COUNT
    )]
    TooManyVenues,
    #[error("instrument idx {} is used by two instruments", .0.get())]
    DuplicateInstrumentIdx(CatalogueIdx),
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
        let mut venues = VenueTable::new();
        for entry in raw.venues {
            let idx = VenueIdx::new(entry.idx);
            let Some(venue) = Venue::parse(&entry.name) else {
                // Advertising it would promise a stream this build cannot open, so it - and
                // every instrument under it, below - is dropped rather than carried.
                tracing::warn!(
                    venue = entry.name.as_str(),
                    idx = entry.idx,
                    "dropping a catalogue venue this build does not carry"
                );
                continue;
            };
            if venues.get(&idx).is_some() {
                return Err(BuildCatalogueError::DuplicateVenueIdx(idx));
            }
            if venues.iter().any(|(_, known)| *known == venue) {
                return Err(BuildCatalogueError::DuplicateVenue(venue));
            }
            venues
                .insert(idx, venue)
                .map_err(|_| BuildCatalogueError::TooManyVenues)?;
        }

        let mut instruments = new_internal_map();
        // Every pair carried so far, so a symbol named by two instruments is caught here
        // rather than as a connector refusal on the first client to ask for the second one.
        // A `Vec` scanned linearly: a catalogue is tens of entries read once at startup.
        let mut claimed: Vec<(Venue, SharedString, CatalogueIdx)> = Vec::new();
        for entry in raw.instruments {
            let idx = CatalogueIdx::new(entry.idx);
            let mut pairs: Vec<CataloguePair> = Vec::with_capacity(entry.pairs.len());
            let mut carried = true;
            for pair in entry.pairs {
                let venue_idx = VenueIdx::new(pair.venue);
                let Some(&venue) = venues.get(&venue_idx) else {
                    tracing::warn!(
                        instrument = entry.idx,
                        venue_idx = pair.venue,
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
                    venue_idx,
                    symbol: pair.symbol,
                });
            }
            if !carried {
                continue;
            }
            if instruments.insert(idx, pairs.into_boxed_slice()).is_some() {
                return Err(BuildCatalogueError::DuplicateInstrumentIdx(idx));
            }
        }

        Ok(Self {
            venues,
            instruments,
        })
    }
}

/// The index `venue` sits at in `venues`.
///
/// The table is small and keyed the other way round - by index, which is what the encoder and
/// a pair's own `venue_idx` need - so this is the scan that answers the reverse question.
#[cfg(any(test, feature = "test-util"))]
fn venue_idx_of(venues: &VenueTable, venue: Venue) -> VenueIdx {
    venues
        .iter()
        .find_map(|(idx, known)| (*known == venue).then_some(*idx))
        .expect("every venue this build carries is in the table")
}

#[cfg(test)]
mod test {
    use super::{BuildCatalogueError, Catalogue, RawCatalogue};
    use core_lib::Venue;
    use md_wire::grpc::{CatalogueIdx, VenueIdx};

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
            [[venues]]
            idx = 0
            name = "binance_spot"

            [[instruments]]
            idx = 1
            pairs = [{ venue = 0, symbol = "BTCUSDT" }, { venue = 0, symbol = "ETHUSDT" }]
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
            [[venues]]
            idx = 0
            name = "binance_spot"
            [[venues]]
            idx = 1
            name = "bitstamp"

            [[instruments]]
            idx = 1
            pairs = [{ venue = 0, symbol = "BTCUSDT" }]

            [[instruments]]
            idx = 2
            pairs = [{ venue = 1, symbol = "btcusd" }, { venue = 0, symbol = "BTCUSDT" }]
            "#,
        )
        .expect_err("two instruments cannot both carry binance_spot/BTCUSDT");

        assert!(
            matches!(err, BuildCatalogueError::DuplicatePair(_)),
            "got {err:?}"
        );
        assert_eq!(
            err.to_string(),
            "binance_spot/BTCUSDT is named by instruments 1 and 2"
        );
    }

    /// The same spelling on two *different* venues is not a duplicate: that is the ordinary
    /// shape of an instrument quoted in two places.
    #[test]
    fn one_spelling_shared_by_two_venues_is_carried() {
        let catalogue = parse(
            r#"
            [[venues]]
            idx = 0
            name = "binance_spot"
            [[venues]]
            idx = 1
            name = "bitstamp"

            [[instruments]]
            idx = 0
            pairs = [{ venue = 0, symbol = "BTCUSD" }, { venue = 1, symbol = "BTCUSD" }]
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

    /// The ordinary shape: a venue table and instruments filed under the file's own indices,
    /// which need not be dense.
    #[test]
    fn a_catalogue_carries_the_files_own_indices() {
        let catalogue = parse(
            r#"
            [[venues]]
            idx = 0
            name = "binance_spot"
            [[venues]]
            idx = 3
            name = "bitstamp"

            [[instruments]]
            idx = 7
            pairs = [{ venue = 0, symbol = "BTCUSDT" }, { venue = 3, symbol = "btcusd" }]
            "#,
        )
        .expect("the fixture is a valid catalogue");

        assert_eq!(
            catalogue.venues().get(&VenueIdx::new(3)),
            Some(&Venue::Bitstamp)
        );
        let pairs = catalogue
            .instruments()
            .get(&CatalogueIdx::new(7))
            .expect("the instrument is carried");
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].symbol(), "BTCUSDT");
        assert_eq!(pairs[0].venue(), Venue::BinanceSpot);
        assert_eq!(pairs[1].venue_idx(), VenueIdx::new(3));
    }

    /// A venue this build does not carry cannot be served, so it is dropped - and so is every
    /// instrument that names it, since a surviving entry must always have a `Venue` to probe
    /// with.
    #[test]
    fn a_venue_this_build_does_not_carry_takes_its_instruments_with_it() {
        let catalogue = parse(
            r#"
            [[venues]]
            idx = 0
            name = "binance_spot"
            [[venues]]
            idx = 1
            name = "kraken"

            [[instruments]]
            idx = 0
            pairs = [{ venue = 0, symbol = "BTCUSDT" }]
            [[instruments]]
            idx = 1
            pairs = [{ venue = 1, symbol = "XBTUSD" }]
            "#,
        )
        .expect("an unknown venue is dropped rather than fatal");

        assert_eq!(catalogue.venues().len(), 1);
        assert_eq!(catalogue.instruments().len(), 1);
        assert!(catalogue.instruments().contains_key(&CatalogueIdx::new(0)));
    }

    /// A file that contradicts itself is refused rather than silently half-loaded: two
    /// entries under one index means a client's subscribe would name something ambiguous.
    #[test]
    fn a_repeated_index_is_refused() {
        let venue = parse(
            r#"
            [[venues]]
            idx = 0
            name = "binance_spot"
            [[venues]]
            idx = 0
            name = "bitstamp"
            "#,
        );
        assert!(matches!(
            venue,
            Err(BuildCatalogueError::DuplicateVenueIdx(_))
        ));

        let instrument = parse(
            r#"
            [[venues]]
            idx = 0
            name = "binance_spot"

            [[instruments]]
            idx = 4
            pairs = [{ venue = 0, symbol = "BTCUSDT" }]
            [[instruments]]
            idx = 4
            pairs = [{ venue = 0, symbol = "ETHUSDT" }]
            "#,
        );
        assert!(matches!(
            instrument,
            Err(BuildCatalogueError::DuplicateInstrumentIdx(_))
        ));
    }

    /// One venue under two indices is the same contradiction from the other side: a level
    /// would have two right answers for which venue quoted it.
    #[test]
    fn one_venue_under_two_indices_is_refused() {
        let repeated = parse(
            r#"
            [[venues]]
            idx = 0
            name = "binance_spot"
            [[venues]]
            idx = 1
            name = "BINANCE_SPOT"
            "#,
        );
        assert!(matches!(
            repeated,
            Err(BuildCatalogueError::DuplicateVenue(Venue::BinanceSpot))
        ));
    }
}
