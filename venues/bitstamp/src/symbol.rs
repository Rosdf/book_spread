//! Bitstamp's own wire-name construction.

use core_lib::instrument::Instrument;
use core_lib::shared_string::SharedString;

/// Prefix of a Bitstamp diff-order-book channel name, e.g. `diff_order_book_btcusd`.
pub(crate) const DIFF_CHANNEL_PREFIX: &str = "diff_order_book_";

/// e.g. `diff_order_book_btcusd`. Allocates once per subscription, never per frame.
///
/// `instrument.name()` is kept verbatim, exactly as `trading-pairs-info` spelled it - Bitstamp's
/// own listing is already lowercase, so there is nothing to recase here.
pub(crate) fn channel_name(instrument: Instrument) -> SharedString {
    let raw = instrument.name();
    let mut name = String::with_capacity(DIFF_CHANNEL_PREFIX.len() + raw.len());
    name.push_str(DIFF_CHANNEL_PREFIX);
    name.push_str(raw);
    SharedString::from(name)
}

#[cfg(test)]
mod test {
    use super::channel_name;
    use core_lib::Venue;
    use core_lib::venue::test_util::test_instrument_for;

    #[test]
    fn builds_the_diff_channel_name() {
        let inst = test_instrument_for(Venue::Bitstamp, "btcusd");
        assert_eq!(&*channel_name(inst), "diff_order_book_btcusd");
    }
}
