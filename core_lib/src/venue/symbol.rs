//! The symbol type shared by every venue.
//!
//! Validation and lowercase storage are venue-agnostic - every venue this connector talks to
//! rejects the same things (empty, non-ASCII-alphanumeric) and wants the same lowercase form
//! as its primary key. What is *not* here is any wire-name construction: a stream name, a
//! channel name, a REST path segment are all venue-specific and live in
//! [`crate::venue::spec::Venue::wire_name`] and [`crate::venue::spec::Venue::snapshot_url`]
//! instead.

use std::borrow::Borrow;
use std::fmt::{self, Display, Formatter};

/// The only way [`Symbol::new`] can fail, so it is a struct rather than a one-variant enum.
///
/// Carries the rejected name back to the caller rather than a copy of it: [`Symbol::new`] takes
/// the buffer by value, so this is where it ends up when validation fails.
#[derive(Debug, thiserror::Error)]
#[error("invalid symbol {0:?}: expected a non-empty ASCII alphanumeric name")]
pub struct InvalidSymbol(Box<str>);

impl InvalidSymbol {
    /// The name that was rejected, for a structured log field.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A venue symbol, stored once in lowercase.
///
/// Some venues (Binance) also want an uppercase form for REST calls; [`Symbol::with_upper`]
/// produces it on demand by casing the one buffer in place, rather than storing a second copy.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Symbol(Box<str>);

impl Symbol {
    /// Takes ownership of a raw symbol name, validates it, and lowercases it in place.
    ///
    /// Casing happens in `raw`'s own buffer, so a valid name costs no allocation and an
    /// invalid one is handed straight back inside the error. Lowercasing is done only after
    /// validation, so the name in that error is the one the caller passed in.
    ///
    /// # Errors
    /// [`InvalidSymbol`] if `raw` is empty or holds anything but ASCII alphanumerics.
    pub fn new(mut raw: Box<str>) -> Result<Self, InvalidSymbol> {
        if raw.is_empty() || !raw.bytes().all(|b| b.is_ascii_alphanumeric()) {
            return Err(InvalidSymbol(raw));
        }
        // ASCII-only, so the length is unchanged and UTF-8 cannot break.
        raw.make_ascii_lowercase();
        Ok(Self(raw))
    }

    /// Runs `f` with the name uppercased, then restores the lowercase form.
    ///
    /// Cases the existing buffer in place rather than allocating a second string. Both
    /// conversions are ASCII-only, so they never change the length and cannot break UTF-8 -
    /// [`Symbol::new`] already rejected anything outside ASCII alphanumerics.
    pub fn with_upper<R>(&mut self, f: impl FnOnce(&str) -> R) -> R {
        /// Restores the lowercase invariant on the way out, including during an unwind.
        struct Lowercase<'a>(&'a mut str);

        impl Drop for Lowercase<'_> {
            fn drop(&mut self) {
                self.0.make_ascii_lowercase();
            }
        }

        self.0.make_ascii_uppercase();
        let guard = Lowercase(&mut self.0);
        f(&*guard.0)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for Symbol {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Lets a `HashMap<Symbol, _>` be probed with a plain `&str`, so a decoder can look a slot up
/// straight out of a frame without building a `Symbol` per frame.
///
/// `Symbol`'s derived `Hash`/`Eq` forward to the inner `Box<str>`, which forward to `str`, so
/// the borrowed and owned forms hash and compare identically - `Borrow`'s requirement.
impl Borrow<str> for Symbol {
    fn borrow(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod test {
    use super::Symbol;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    #[test]
    fn stores_lowercase_regardless_of_input_casing() {
        assert_eq!(Symbol::new("btcUsdt".into()).unwrap().0.as_ref(), "btcusdt");
        assert_eq!(Symbol::new("BTCUSDT".into()).unwrap().0.as_ref(), "btcusdt");
    }

    #[test]
    fn with_upper_exposes_uppercase_then_restores_lowercase() {
        let mut sym = Symbol::new("BTCUSDT".into()).unwrap();

        let seen = sym.with_upper(ToOwned::to_owned);

        assert_eq!(seen, "BTCUSDT");
        assert_eq!(sym.0.as_ref(), "btcusdt", "the buffer must be restored");
    }

    #[test]
    fn with_upper_returns_the_closures_value_and_nests_safely() {
        let mut sym = Symbol::new("ethusdt".into()).unwrap();
        let url = sym.with_upper(|upper| format!("symbol={upper}&limit=100"));
        assert_eq!(url, "symbol=ETHUSDT&limit=100");
        assert_eq!(sym.0.as_ref(), "ethusdt");

        for _ in 0..3 {
            assert_eq!(sym.with_upper(str::to_owned), "ETHUSDT");
            assert_eq!(sym.0.as_ref(), "ethusdt");
        }
    }

    #[test]
    fn a_panicking_closure_still_leaves_the_symbol_lowercase() {
        let mut sym = Symbol::new("BTCUSDT".into()).unwrap();

        let caught = catch_unwind(AssertUnwindSafe(|| {
            sym.with_upper(|upper| {
                assert_eq!(upper, "BTCUSDT");
                panic!("closure blew up");
            })
        }));

        assert!(caught.is_err(), "the panic must propagate");
        assert_eq!(sym.0.as_ref(), "btcusdt");
    }

    #[test]
    fn rejects_empty_and_non_alphanumeric() {
        for bad in ["", "BTC-USDT", "BTC USDT", "BTC/USDT"] {
            assert!(Symbol::new(bad.into()).is_err(), "{bad:?} should be rejected");
        }
    }

    #[test]
    fn a_rejected_name_comes_back_exactly_as_it_was_passed_in() {
        let err = Symbol::new("BTC-Usdt".into()).unwrap_err();
        assert_eq!(err.as_str(), "BTC-Usdt");
    }
}
