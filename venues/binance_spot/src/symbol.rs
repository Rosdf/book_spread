//! Binance's own wire-name construction, on top of the shared [`core_lib::venue::Symbol`].

use core_lib::venue::Symbol;

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
/// Allocates, because the result is kept as the connection's lookup key for the whole life of
/// the subscription. Called once per symbol, never per frame.
pub(crate) fn stream_name(symbol: &Symbol, speed: DepthSpeed) -> Box<str> {
    let suffix = speed.suffix();
    let raw = symbol.as_str();
    let mut name = String::with_capacity(raw.len() + suffix.len());
    name.push_str(raw);
    name.push_str(suffix);
    name.into_boxed_str()
}

#[cfg(test)]
mod test {
    use super::{DepthSpeed, stream_name};
    use core_lib::venue::Symbol;

    #[test]
    fn builds_lowercase_stream_names() {
        let sym = Symbol::new("BTCUSDT".into()).unwrap();
        assert_eq!(&*stream_name(&sym, DepthSpeed::Fast), "btcusdt@depth@100ms");
        assert_eq!(&*stream_name(&sym, DepthSpeed::Slow), "btcusdt@depth");
    }
}
