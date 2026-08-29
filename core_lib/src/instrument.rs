//! A symbol, interned once and reached everywhere else as a cheap [`Instrument`] handle.
//!
//! A symbol used to be a string at every layer: validated and lowercased in [`crate::venue`],
//! then again a layer up, cloned into every map key that named it. [`Instrument`] replaces all
//! of that with a `Copy` handle to a record this module leaks exactly once - so identity is the
//! record's address, equality and hashing are address operations, and passing a symbol around
//! costs a pointer copy.
//!
//! Nothing outside this module can build an [`Instrument`] that is not in the registry: [`Inner`]
//! is private with no public constructor, and the only way in is [`InstrumentRegistrar::register`], which
//! is itself reachable only through the sealed guard a connector is handed at spawn - see
//! [`crate::connector::ConnectorHandle::new`]. That is what makes the address-based `Hash`/`Eq`
//! below sound: two handles are equal iff they name the same instrument, because this registry
//! is the only source of [`Inner`] values and it interns by `(venue, name)`.

use crate::connector::InstrumentRegistrar;
use crate::map::InternalHashMap;
use crate::shared_string::SharedString;
use all_venues::Venue;
use hashbrown::Equivalent;
use std::cmp::Ordering;
use std::fmt::{self, Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::num::NonZeroU32;
use std::ptr;
use std::sync::RwLock;

/// An [`Instrument`]'s numeric identity, for compact storage where a pointer would not fit (a
/// wire encoding, say). Never issued twice by one process, and never reused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct InstrumentId(NonZeroU32);

/// The interned record. Only ever reached through an [`Instrument`], and only ever leaked - see
/// [`Registry::intern`].
struct Inner {
    id: InstrumentId,
    venue: Venue,
    /// Exactly as the venue's own API spells it. Never normalised: Binance's `exchangeInfo`
    /// says `BTCUSDT`, Bitstamp's `trading-pairs-info` says `btcusd`, and both are stored as
    /// given.
    name: SharedString,
}

impl Debug for Inner {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Instrument")
            .field("id", &self.id)
            .field("venue", &self.venue)
            .field("name", &self.name.as_str())
            .finish()
    }
}

/// A handle to one interned symbol, `Copy` and cheap to hash/compare: both operations touch
/// only the handle's own address, never the record it points to.
///
/// # Ordering
/// [`Hash`]/[`Eq`] are address-based rather than derived from `(venue, name)` - two instruments
/// naming the same `(venue, name)` are always the same handle, since [`Registry::intern`] is
/// idempotent, so the two notions of equality agree. What address-based equality buys is a
/// hash/compare on the per-frame path that never touches the record's cache line at all.
#[derive(Clone, Copy)]
pub struct Instrument(&'static Inner);

impl Instrument {
    #[must_use]
    pub fn id(self) -> InstrumentId {
        self.0.id
    }

    #[must_use]
    pub fn venue(self) -> Venue {
        self.0.venue
    }

    /// The venue's own spelling of this instrument's name.
    #[must_use]
    pub fn name(self) -> &'static str {
        self.0.name.as_str()
    }

    /// Resolves an [`InstrumentId`] back to its [`Instrument`].
    ///
    /// # Panics
    /// If `id` was not issued by this process's registry.
    #[must_use]
    pub fn by_id(id: InstrumentId) -> Self {
        REGISTRY.by_id(id)
    }

    /// Looks an already-interned instrument up by `(venue, name)`, allocating nothing. `None` if
    /// nothing has registered that name on that venue.
    #[must_use]
    pub fn lookup(venue: Venue, name: &str) -> Option<Self> {
        REGISTRY.lookup(venue, name)
    }

    pub fn register(
        name: impl AsRef<str> + Into<SharedString>,
        reg: &impl InstrumentRegistrar,
    ) -> Self {
        REGISTRY.intern(reg.venue(), name)
    }
}

impl Debug for Instrument {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Debug::fmt(self.0, f)
    }
}

impl fmt::Display for Instrument {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{}|{}", self.name(), self.venue().as_str()))
    }
}

impl Hash for Instrument {
    fn hash<H: Hasher>(&self, state: &mut H) {
        ptr::from_ref(self.0).hash(state);
    }
}

impl PartialEq for Instrument {
    fn eq(&self, other: &Self) -> bool {
        ptr::eq(self.0, other.0)
    }
}

impl Eq for Instrument {}

impl PartialOrd for Instrument {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Instrument {
    fn cmp(&self, other: &Self) -> Ordering {
        ptr::from_ref(self.0).cmp(&ptr::from_ref(other.0))
    }
}

/// How the registry finds an instrument it has already interned.
///
/// Deliberately separate from [`Instrument`]'s own address-based `Hash`/`Eq` - this is the one
/// place a lookup goes by `(venue, name)` rather than by identity.
#[derive(PartialEq, Eq)]
struct InstrumentKey {
    venue: Venue,
    name: SharedString,
}

/// The borrowed form of [`InstrumentKey`], so a lookup with a caller's `&str` allocates nothing.
///
/// Two types because [`SharedString`] and `&'a str` are different types, and
/// [`Equivalent`] is hashbrown's escape hatch that lets the borrowed one probe for the owned
/// one. Its `Hash` has to feed the hasher exactly what [`InstrumentKey`]'s derive does -
/// `venue` then `name` - and [`SharedString`]'s `Hash` forwards to `str::hash`, so the two
/// agree by construction.
#[derive(Hash, PartialEq, Eq)]
struct BorrowedKey<'a> {
    venue: Venue,
    name: &'a str,
}

impl Hash for InstrumentKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let key = BorrowedKey {
            venue: self.venue,
            name: self.name.as_str(),
        };
        key.hash(state);
    }
}

impl Equivalent<InstrumentKey> for BorrowedKey<'_> {
    fn equivalent(&self, key: &InstrumentKey) -> bool {
        let key_ref = BorrowedKey {
            venue: key.venue,
            name: key.name.as_str(),
        };
        *self == key_ref
    }
}

struct Table {
    by_key: InternalHashMap<InstrumentKey, Instrument>,
    /// Index `id.get() - 1`. Every id ever issued stays valid forever - instruments are never
    /// unregistered - so this only ever grows.
    by_id: Vec<Instrument>,
}

struct Registry {
    table: RwLock<Table>,
}

impl Registry {
    const fn new() -> Self {
        Self {
            table: RwLock::new(Table {
                by_key: crate::map::new_internal_map(),
                by_id: Vec::new(),
            }),
        }
    }

    fn lookup(&self, venue: Venue, name: &str) -> Option<Instrument> {
        let table = self.table.read().expect("registry lock poisoned");
        table.by_key.get(&BorrowedKey { venue, name }).copied()
    }

    /// Interns `(venue, name)`, returning the existing handle if one is already registered.
    ///
    /// A read-lock lookup first - the hit path, which every refresh after the first takes for
    /// every symbol - and a write lock only on a real miss, re-checked once acquired in case
    /// another registration raced this one to it.
    fn intern(&self, venue: Venue, name: impl AsRef<str> + Into<SharedString>) -> Instrument {
        if let Some(found) = self.lookup(venue, name.as_ref()) {
            return found;
        }

        let mut table = self.table.write().expect("registry lock poisoned");
        if let Some(found) = table.by_key.get(&BorrowedKey {
            venue,
            name: name.as_ref(),
        }) {
            return *found;
        }

        let next = u32::try_from(table.by_id.len() + 1).expect("instrument id space exhausted");
        let id = InstrumentId(NonZeroU32::new(next).expect("id space starts at 1"));
        // Leaked twice over: the name goes through `leaked` so no name in the registry is ever
        // refcounted - cloning it into the key below is a plain 16-byte copy, no atomic - and
        // the record itself is then `Box::leak`ed, which is what makes `Instrument`'s lifetime
        // genuinely `'static`.
        let leaked_name = SharedString::leaked(name.into());
        let instrument = Instrument(Box::leak(Box::new(Inner {
            id,
            venue,
            name: leaked_name.clone(),
        })));

        table.by_key.insert(
            InstrumentKey {
                venue,
                name: leaked_name,
            },
            instrument,
        );
        table.by_id.push(instrument);
        instrument
    }

    /// # Panics
    /// If `id` was not issued by this registry. An [`InstrumentId`] is proof the instrument
    /// exists, so a miss here is a bug in the caller, not a case to handle.
    fn by_id(&self, id: InstrumentId) -> Instrument {
        let table = self.table.read().expect("registry lock poisoned");
        let index = usize::try_from(id.0.get() - 1).expect("id fits usize");
        table.by_id[index]
    }
}

static REGISTRY: Registry = Registry::new();

#[cfg(test)]
mod test {
    use super::*;
    use crate::connector::VenueGuard;

    #[test]
    fn interning_the_same_name_twice_returns_the_same_handle() {
        let guard = VenueGuard::new(Venue::BinanceSpot);
        let a = guard.register("btcusdt");
        let b = guard.register("btcusdt");
        assert_eq!(a, b);
        assert!(std::ptr::eq(a.0, b.0));
    }

    #[test]
    fn the_same_name_on_two_venues_is_two_instruments() {
        let binance = VenueGuard::new(Venue::BinanceSpot).register("btcusd");
        let bitstamp = VenueGuard::new(Venue::Bitstamp).register("btcusd");
        assert_ne!(binance, bitstamp);
    }

    #[test]
    fn name_keeps_the_venues_own_casing() {
        let inst = VenueGuard::new(Venue::BinanceSpot).register("BTCUSDT");
        assert_eq!(inst.name(), "BTCUSDT");
    }

    #[test]
    fn lookup_finds_what_was_registered_and_nothing_else() {
        let guard = VenueGuard::new(Venue::Bitstamp);
        let registered = guard.register("ethusd-lookup-test");
        assert_eq!(guard.lookup("ethusd-lookup-test"), Some(registered));
        assert_eq!(guard.lookup("never-registered-xyz"), None);
    }

    #[test]
    fn by_id_resolves_back_to_the_same_instrument() {
        let inst = VenueGuard::new(Venue::BinanceSpot).register("by-id-roundtrip");
        assert_eq!(Instrument::by_id(inst.id()), inst);
    }
}
