//! Turning a `SubscribeBookRequest` into the [`Key`] its broadcaster is filed under, or into
//! the reason it cannot be one.
//!
//! Separate from the transport on purpose: it reports a [`RejectCode`] the wire protocol can
//! carry, rather than being entangled with how a refusal is written.

use crate::registry::Key;
use crate::venue::Venue;
use md_proto::md::v1::SubscribeBookRequest;
use md_wire::framing::{RejectCode, Rejected};

/// Longest symbol name a request may carry.
///
/// Nothing upstream imposes a length limit - `core_lib`'s `Symbol` only requires non-empty
/// ASCII alphanumerics - so this is purely a bound on what a hostile request can make the
/// server allocate and log. No venue symbol comes close.
const MAX_SYMBOL_LEN: usize = 32;

/// Validates and normalises `request` into the key its broadcaster is filed under.
///
/// # Errors
///
/// [`RejectCode::InvalidArgument`] for an unknown venue or a symbol no connector would
/// accept. Nothing here can fail any other way: whether the venue *lists* the symbol is the
/// connector's answer, not this function's.
pub fn key_of(request: &SubscribeBookRequest) -> Result<Key, Rejected> {
    let Some(venue) = Venue::parse(&request.venue) else {
        return Err(Rejected::new(
            RejectCode::InvalidArgument,
            format!("unknown venue {:?}", request.venue).into_boxed_str(),
        ));
    };
    Ok(Key::new(venue, normalise_symbol(&request.symbol)?))
}

/// Lowercases `raw` after checking it is something a connector would accept.
///
/// Lowercase is the connector's own canonical form, so normalising here is what keeps
/// `BTCUSDT` and `btcusdt` on one broadcaster instead of racing for one subscription.
fn normalise_symbol(raw: &str) -> Result<Box<str>, Rejected> {
    if raw.is_empty() || raw.len() > MAX_SYMBOL_LEN {
        return Err(Rejected::new(
            RejectCode::InvalidArgument,
            format!("symbol must be 1..={MAX_SYMBOL_LEN} bytes").into_boxed_str(),
        ));
    }
    if !raw.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
        return Err(Rejected::new(
            RejectCode::InvalidArgument,
            Box::from("symbol must be ASCII alphanumeric"),
        ));
    }
    Ok(raw.to_ascii_lowercase().into_boxed_str())
}

#[cfg(test)]
mod test {
    use super::{MAX_SYMBOL_LEN, key_of, normalise_symbol};
    use crate::venue::Venue;
    use md_proto::md::v1::SubscribeBookRequest;
    use md_wire::framing::RejectCode;

    fn asking(venue: &str, symbol: &str) -> SubscribeBookRequest {
        SubscribeBookRequest {
            venue: venue.to_owned(),
            symbol: symbol.to_owned(),
        }
    }

    /// Two clients naming the same symbol in different cases have to land on one broadcaster:
    /// every connector keys its subscriptions by the lowercase form, and a second subscribe
    /// for one symbol is rejected outright.
    #[test]
    fn a_symbol_is_normalised_to_the_form_the_connector_uses() {
        assert_eq!(
            normalise_symbol("BTCUSDT")
                .expect("a valid symbol")
                .as_ref(),
            "btcusdt"
        );
        assert_eq!(
            normalise_symbol("btcusdt")
                .expect("a valid symbol")
                .as_ref(),
            "btcusdt"
        );
    }

    #[test]
    fn a_symbol_the_connector_would_refuse_fails_the_request() {
        for raw in ["", "btc-usd", "btc usd", "btc/usd"] {
            let rejection = normalise_symbol(raw).expect_err("rejected before any lock is taken");
            assert_eq!(rejection.code(), RejectCode::InvalidArgument, "for {raw:?}");
        }

        let long = "a".repeat(MAX_SYMBOL_LEN + 1);
        assert_eq!(
            normalise_symbol(&long)
                .expect_err("over the length bound")
                .code(),
            RejectCode::InvalidArgument
        );
    }

    #[test]
    fn venues_are_named_case_insensitively() {
        assert_eq!(Venue::parse("BINANCE_SPOT"), Some(Venue::BinanceSpot));
        assert_eq!(Venue::parse("bitstamp"), Some(Venue::Bitstamp));
        assert_eq!(Venue::parse("kraken"), None);
    }

    #[test]
    fn a_whole_request_resolves_to_the_key_its_broadcaster_uses() {
        let key = key_of(&asking("BINANCE_SPOT", "BTCUSDT")).expect("a valid request");
        assert_eq!(key.venue(), Venue::BinanceSpot);
        assert_eq!(key.symbol(), "btcusdt");

        let rejection = key_of(&asking("kraken", "btcusdt")).expect_err("an unknown venue");
        assert_eq!(rejection.code(), RejectCode::InvalidArgument);
        assert!(
            rejection.reason().contains("kraken"),
            "the reason names the venue that was asked for, got {:?}",
            rejection.reason()
        );
    }
}
