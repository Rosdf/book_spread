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
//! never renumbers the entries after it - which is [`Instruments`]'s whole reason for being a
//! slice of `Option`s rather than a map. It is also why an index alone is not a safe thing for
//! a subscribe to carry across a restart: editing this file - adding or removing an
//! `[[instruments]]` table - renumbers every entry after the edit, silently, from the next
//! process's point of view. A client that read the catalogue before the edit and subscribes by
//! index after it would get a different instrument's book with no way to notice, so a subscribe
//! names its pairs too - see [`pairs_match`] and `crate::registry::Registry::subscribe`.
//!
//! Nothing here validates the shape of a symbol. A catalogue entry naming a symbol no venue
//! lists is advertised as written and simply never resolves, which is the same state as a
//! connector that has not refreshed its listing yet - and the connectors are the authority on
//! which of the two it is.

pub(crate) mod encode;
pub(crate) mod source;

use core_lib::Venue;
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

/// One pair a subscribe claims the catalogue listed under its instrument index.
///
/// The venue stays as the client spelled it rather than being parsed into a [`Venue`] on the
/// way in: a name this build does not carry is a pair that matches nothing, which is the same
/// answer a wrong symbol gets, so there is one way for a subscribe to be wrong instead of two.
#[derive(Debug, Clone)]
pub(crate) struct AskedPair {
    venue: SharedString,
    symbol: SharedString,
}

impl AskedPair {
    pub(crate) fn new(venue: SharedString, symbol: SharedString) -> Self {
        Self { venue, symbol }
    }

    pub(crate) fn venue(&self) -> &str {
        &self.venue
    }

    pub(crate) fn symbol(&self) -> &str {
        &self.symbol
    }
}

/// Whether `asked` is exactly what the catalogue lists, pair for pair and in order.
///
/// Order matters: it is the order the merge breaks ties in, and the catalogue encoder writes
/// it deterministically, so a client echoing what it read always matches.
pub(crate) fn pairs_match(asked: &[AskedPair], carried: &[CataloguePair]) -> bool {
    asked.len() == carried.len()
        && asked.iter().zip(carried).all(|(asked_pair, carried_pair)| {
            Venue::parse(asked_pair.venue()) == Some(carried_pair.venue())
                && asked_pair.symbol() == carried_pair.symbol()
        })
}

/// Every slot the catalogue file named, by index.
///
/// A slice rather than a map: an instrument's index *is* its position in the file, so the
/// lookup a subscribe needs is a bounds check and an offset rather than a hash of a dense
/// counter. Built once at startup and never touched again.
///
/// `None` is a slot the file named but this build cannot serve - a venue it does not carry.
/// Keeping the slot is what stops a dropped entry renumbering the ones after it, and the
/// distinction is load-bearing: a `None` is `UnknownInstrument` ("this server never had it"),
/// while a `Some` whose pairs do not match is `InstrumentChanged` ("it moved").
#[derive(Debug, Default)]
pub(crate) struct Instruments(Box<[Option<Box<[CataloguePair]>>]>);

impl Instruments {
    /// What `idx` names, or `None` for a slot past the end or one this build cannot serve.
    pub(crate) fn get(&self, idx: CatalogueIdx) -> Option<&[CataloguePair]> {
        let position = usize::try_from(idx.get()).ok()?;
        self.0.get(position)?.as_deref()
    }

    /// Every servable entry with its index, in index order.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (CatalogueIdx, &[CataloguePair])> {
        self.0.iter().enumerate().filter_map(|(position, slot)| {
            let idx = CatalogueIdx::new(
                u32::try_from(position).expect("a catalogue file is tens of entries, not four billion"),
            );
            slot.as_deref().map(|pairs| (idx, pairs))
        })
    }

    /// How many entries are servable - not how many slots there are.
    pub(crate) fn len(&self) -> usize {
        self.0.iter().filter(|pairs| pairs.is_some()).count()
    }
}

/// What the server advertises, as loaded.
#[derive(Debug, Default)]
pub(crate) struct Catalogue {
    instruments: Instruments,
}

impl Catalogue {
    pub(crate) fn instruments(&self) -> &Instruments {
        &self.instruments
    }

    /// Hands the instrument entries to the registry, which is what resolves them.
    pub(crate) fn into_instruments(self) -> Instruments {
        self.instruments
    }

    /// Builds a catalogue directly, for a test that needs one without a file.
    ///
    /// Indices are named rather than positional here, unlike a loaded file: a test that wants
    /// a sparse or out-of-order catalogue says so. Sized to the largest index named, with every
    /// unnamed slot below it left `None` - the same shape a loaded file's dropped entries take.
    ///
    /// Nothing here is validated - the caller owes it the two rules [`TryFrom<RawCatalogue>`]
    /// enforces: one venue at most once per instrument, and one `(venue, symbol)` pair across
    /// the whole catalogue.
    #[cfg(any(test, feature = "test-util"))]
    pub(crate) fn for_test(instruments: &[(u32, &[(Venue, &str)])]) -> Self {
        let len = instruments
            .iter()
            .map(|&(idx, _)| usize::try_from(idx).expect("a test index fits a usize") + 1)
            .max()
            .unwrap_or(0);
        let mut slots: Vec<Option<Box<[CataloguePair]>>> = vec![None; len];
        for &(idx, pairs) in instruments {
            let carried: Vec<CataloguePair> = pairs
                .iter()
                .map(|&(venue, symbol)| CataloguePair {
                    venue,
                    symbol: SharedString::from(symbol),
                })
                .collect();
            slots[usize::try_from(idx).expect("a test index fits a usize")] =
                Some(carried.into_boxed_slice());
        }

        Self {
            instruments: Instruments(slots.into_boxed_slice()),
        }
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
        let mut instruments: Vec<Option<Box<[CataloguePair]>>> =
            Vec::with_capacity(raw.instruments.len());
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
            // Pushed either way: the slot is what keeps this entry's index from shifting the
            // ones after it, whether or not it turned out to be servable.
            instruments.push(carried.then(|| pairs.into_boxed_slice()));
        }

        Ok(Self {
            instruments: Instruments(instruments.into_boxed_slice()),
        })
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
                .get(CatalogueIdx::new(0))
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
            .get(CatalogueIdx::new(1))
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
        assert!(catalogue.instruments().get(CatalogueIdx::new(0)).is_some());
        assert!(
            catalogue.instruments().get(CatalogueIdx::new(1)).is_none(),
            "the kraken entry is dropped, and its slot is None rather than simply absent"
        );
        assert!(
            catalogue.instruments().get(CatalogueIdx::new(2)).is_some(),
            "but it still consumes index one, so the entry after it keeps index two"
        );
    }

    /// [`Instruments::iter`] is what lets the encoder drop its own sort: it must yield in
    /// index order even when a dropped entry leaves a gap in the middle.
    #[test]
    fn instruments_iterate_in_index_order_over_a_sparse_catalogue() {
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

        let indices: Vec<u32> = catalogue
            .instruments()
            .iter()
            .map(|(idx, _)| idx.get())
            .collect();
        assert_eq!(
            indices,
            vec![0, 2],
            "the dropped middle entry leaves a gap rather than shifting index two down"
        );
    }
}
