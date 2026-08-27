//! Bitstamp's own wire-name construction, on top of the shared [`core_lib::venue::Symbol`].

use core_lib::venue::Symbol;

/// Prefix of a Bitstamp diff-order-book channel name, e.g. `diff_order_book_btcusd`.
pub(crate) const DIFF_CHANNEL_PREFIX: &str = "diff_order_book_";

/// e.g. `diff_order_book_btcusd`. Allocates once per subscription, never per frame.
pub(crate) fn channel_name(symbol: &Symbol) -> Box<str> {
    let raw = symbol.as_str();
    let mut name = String::with_capacity(DIFF_CHANNEL_PREFIX.len() + raw.len());
    name.push_str(DIFF_CHANNEL_PREFIX);
    name.push_str(raw);
    name.into_boxed_str()
}

/// Strips [`DIFF_CHANNEL_PREFIX`] off an incoming `channel`, so the table can be probed with
/// the bare pair name and no `Symbol` allocated per frame.
pub(crate) fn pair_of_channel(channel: &str) -> Option<&str> {
    channel.strip_prefix(DIFF_CHANNEL_PREFIX)
}

#[cfg(test)]
mod test {
    use super::{channel_name, pair_of_channel};
    use core_lib::venue::Symbol;

    #[test]
    fn builds_the_diff_channel_name() {
        let sym = Symbol::new("BTCUSD".into()).unwrap();
        assert_eq!(&*channel_name(&sym), "diff_order_book_btcusd");
    }

    #[test]
    fn pair_of_channel_strips_the_prefix() {
        assert_eq!(pair_of_channel("diff_order_book_btcusd"), Some("btcusd"));
    }

    #[test]
    fn pair_of_channel_rejects_a_channel_missing_the_prefix() {
        assert_eq!(pair_of_channel("order_book_btcusd"), None);
        assert_eq!(pair_of_channel("live_trades_btcusd"), None);
    }
}
