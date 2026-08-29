//! Binance's own wire-name construction.

use core_lib::instrument::Instrument;
use core_lib::shared_string::SharedString;

/// How often Binance coalesces depth diffs for a stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum DepthSpeed {
    /// `<symbol>@depth@100ms`.
    #[default]
    Fast,
    /// `<symbol>@depth`, which Binance pushes every 1000ms.
    Slow,
}

impl DepthSpeed {
    const fn suffix(self) -> &'static str {
        match self {
            Self::Fast => "@depth@100ms",
            Self::Slow => "@depth",
        }
    }
}

/// The lowercase stream name, e.g. `btcusdt@depth@100ms`.
///
/// `instrument.name()` keeps the venue's own casing (`exchangeInfo` says `BTCUSDT`), so this
/// lowercases it - the one place Binance's stream naming actually needs to. Allocates, because
/// the result is kept as the connection's lookup key for the whole life of the subscription.
/// Called once per symbol, never per frame.
pub(crate) fn stream_name(instrument: Instrument, speed: DepthSpeed) -> SharedString {
    let suffix = speed.suffix();
    let raw = instrument.name();
    let mut name = String::with_capacity(raw.len() + suffix.len());
    name.push_str(raw);
    name.make_ascii_lowercase();
    name.push_str(suffix);
    SharedString::from(name)
}

#[cfg(test)]
mod test {
    use super::{DepthSpeed, stream_name};
    use core_lib::Venue;
    use core_lib::venue::test_util::test_instrument_for;

    #[test]
    fn builds_lowercase_stream_names() {
        let inst = test_instrument_for(Venue::BinanceSpot, "BTCUSDT");
        assert_eq!(&*stream_name(inst, DepthSpeed::Fast), "btcusdt@depth@100ms");
        assert_eq!(&*stream_name(inst, DepthSpeed::Slow), "btcusdt@depth");
    }
}
