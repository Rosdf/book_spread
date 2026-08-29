//! Turning a `SubscribeBookRequest` into the [`Instrument`] its broadcaster is filed under, or
//! into the reason it cannot be one.
//!
//! Separate from the transport on purpose: it reports a [`RejectCode`] the wire protocol can
//! carry, rather than being entangled with how a refusal is written.

use crate::venue::Venue;
use core_lib::instrument::Instrument;
use md_proto::md::v1::SubscribeBookRequest;
use md_wire::framing::{RejectCode, Rejected};

/// Longest symbol name a request may carry.
///
/// Nothing upstream imposes a length limit, so this is purely a bound on what a hostile request
/// can make the server allocate and log. No venue symbol comes close.
const MAX_SYMBOL_LEN: usize = 32;

/// Validates `request` and resolves it to the instrument its broadcaster is filed under.
///
/// Every pair in `request.pairs` is validated, but only the first is served today - merging
/// the rest into one book is the next stage.
///
/// # Errors
///
/// [`RejectCode::InvalidArgument`] for an empty pair list, an unknown venue, a symbol shaped
/// wrong on its own terms, a symbol no venue lists as tradable, or a pair that duplicates one
/// earlier in the list. The listing check is a plain registry lookup - no round trip to a
/// connector - so an unlisted symbol is refused right here, in the handshake.
pub(crate) fn key_of(request: &SubscribeBookRequest) -> Result<Instrument, Rejected> {
    if request.pairs.is_empty() {
        return Err(Rejected::new(
            RejectCode::InvalidArgument,
            Box::from("at least one pair is required"),
        ));
    }

    let mut seen: Vec<Instrument> = Vec::with_capacity(request.pairs.len());
    for pair in &request.pairs {
        let Some(venue) = Venue::parse(&pair.venue) else {
            return Err(Rejected::new(
                RejectCode::InvalidArgument,
                format!("unknown venue {:?}", pair.venue).into_boxed_str(),
            ));
        };
        let name = normalise_symbol(&pair.symbol)?;
        let Some(instrument) = Instrument::lookup(venue, name) else {
            return Err(Rejected::new(
                RejectCode::InvalidArgument,
                format!("{name} is not listed as tradable on {}", venue.as_str()).into_boxed_str(),
            ));
        };
        if seen.contains(&instrument) {
            return Err(Rejected::new(
                RejectCode::InvalidArgument,
                format!(
                    "duplicate pair {}/{}",
                    instrument.venue().as_str(),
                    instrument.name()
                )
                .into_boxed_str(),
            ));
        }
        seen.push(instrument);
    }

    // The rest are validated above and dropped here; merging them into one book is the next
    // stage.
    Ok(seen.into_iter().next().expect("checked non-empty above"))
}

/// Checks `raw` is something a connector would accept, without allocating: the wire contract is
/// case-sensitive now - a client sends a venue's own spelling - so there is nothing left to
/// normalise, only to probe the registry with.
fn normalise_symbol(raw: &str) -> Result<&str, Rejected> {
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
    Ok(raw)
}

#[cfg(test)]
mod test {
    use super::{MAX_SYMBOL_LEN, key_of, normalise_symbol};
    use crate::venue::Venue;
    use core_lib::venue::test_util::test_instrument_for;
    use md_proto::md::v1::{Pair, SubscribeBookRequest};
    use md_wire::framing::RejectCode;

    fn pair(venue: &str, symbol: &str) -> Pair {
        Pair {
            venue: venue.to_owned(),
            symbol: symbol.to_owned(),
        }
    }

    fn asking(venue: &str, symbol: &str) -> SubscribeBookRequest {
        SubscribeBookRequest {
            pairs: vec![pair(venue, symbol)],
        }
    }

    #[test]
    fn a_shaped_ok_symbol_probes_without_allocating() {
        assert_eq!(
            normalise_symbol("BTCUSDT").expect("a valid symbol"),
            "BTCUSDT"
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
    fn a_whole_request_resolves_to_the_instrument_its_broadcaster_uses() {
        let registered = test_instrument_for(Venue::BinanceSpot, "BTCUSDTRESOLVETEST");
        let instrument = key_of(&asking("BINANCE_SPOT", "BTCUSDTRESOLVETEST")).expect("listed");
        assert_eq!(instrument, registered);

        let rejection = key_of(&asking("kraken", "btcusdt")).expect_err("an unknown venue");
        assert_eq!(rejection.code(), RejectCode::InvalidArgument);
        assert!(
            rejection.reason().contains("kraken"),
            "the reason names the venue that was asked for, got {:?}",
            rejection.reason()
        );
    }

    /// The wire contract is case-sensitive: a client must send the venue's own spelling.
    #[test]
    fn a_symbol_in_the_wrong_case_is_refused() {
        let _ = test_instrument_for(Venue::BinanceSpot, "CASETEST");
        let rejection = key_of(&asking("binance_spot", "casetest")).expect_err("wrong-case symbol");
        assert_eq!(rejection.code(), RejectCode::InvalidArgument);
    }

    #[test]
    fn an_unlisted_symbol_is_refused_with_no_round_trip() {
        let rejection = key_of(&asking("binance_spot", "NEVERLISTEDXYZ"))
            .expect_err("nothing has ever registered this name");
        assert_eq!(rejection.code(), RejectCode::InvalidArgument);
        assert!(rejection.reason().contains("not listed"));
    }

    #[test]
    fn an_empty_pair_list_is_rejected() {
        let rejection =
            key_of(&SubscribeBookRequest { pairs: Vec::new() }).expect_err("no pairs to serve");
        assert_eq!(rejection.code(), RejectCode::InvalidArgument);
    }

    #[test]
    fn a_request_names_the_first_pairs_key() {
        let binance = test_instrument_for(Venue::BinanceSpot, "FIRSTPAIRTEST");
        let _bitstamp = test_instrument_for(Venue::Bitstamp, "firstpairtest2");
        let request = SubscribeBookRequest {
            pairs: vec![
                pair("binance_spot", "FIRSTPAIRTEST"),
                pair("bitstamp", "firstpairtest2"),
            ],
        };
        let instrument = key_of(&request).expect("a valid request");
        assert_eq!(instrument, binance);
    }

    #[test]
    fn a_duplicate_pair_rejects_the_whole_request() {
        let _ = test_instrument_for(Venue::BinanceSpot, "DUPTEST");
        let request = SubscribeBookRequest {
            pairs: vec![
                pair("binance_spot", "DUPTEST"),
                pair("binance_spot", "DUPTEST"),
            ],
        };
        let rejection = key_of(&request).expect_err("a duplicate pair");
        assert_eq!(rejection.code(), RejectCode::InvalidArgument);
    }

    #[test]
    fn an_invalid_second_pair_rejects_the_whole_request() {
        let _ = test_instrument_for(Venue::BinanceSpot, "SECONDPAIRTEST");
        let request = SubscribeBookRequest {
            pairs: vec![
                pair("binance_spot", "SECONDPAIRTEST"),
                pair("kraken", "btcusd"),
            ],
        };
        let rejection = key_of(&request).expect_err("the second pair is unusable");
        assert_eq!(rejection.code(), RejectCode::InvalidArgument);
        assert!(
            rejection.reason().contains("kraken"),
            "the reason names the venue that was asked for, got {:?}",
            rejection.reason()
        );
    }
}
