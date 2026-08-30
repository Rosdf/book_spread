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
        for entry in raw.instruments {
            let idx = CatalogueIdx::new(entry.idx);
            let mut pairs = Vec::with_capacity(entry.pairs.len());
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
