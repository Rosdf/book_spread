use core_lib::venue::{CoreConfig, Defaults};
use serde::Deserialize;

/// What [`crate::Bitstamp`] declares as its [`core_lib::venue::Venue::Config`]: Bitstamp's own
/// extras, and only those.
///
/// The shared tuning is not in here. A caller pairs this with [`CoreConfig`] by building a
/// `core_lib::venue::ConnectorConfig<Config>` - whose `Default` picks up both halves, including
/// the overrides in the [`Defaults`] impl below - and hands *that* to
/// [`ConnectorHandle::new`](core_lib::connector::ConnectorHandle::new).
pub type Config = Extra;

/// Bitstamp-specific tuning: the endpoints the `Venue` methods read.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Extra {
    /// Market-data-only endpoint. Needs no API key of any kind.
    stream_endpoint: Box<str>,
    rest_endpoint: Box<str>,
}

impl Extra {
    pub(crate) fn stream_endpoint(&self) -> &str {
        &self.stream_endpoint
    }

    pub(crate) fn rest_endpoint(&self) -> &str {
        &self.rest_endpoint
    }
}

impl Default for Extra {
    fn default() -> Self {
        Self {
            stream_endpoint: "wss://ws.bitstamp.net".into(),
            rest_endpoint: "https://www.bitstamp.net".into(),
        }
    }
}

/// Supplies the shared half of `ConnectorConfig::<Config>::default()`, via [`Defaults`]'s
/// blanket impl.
///
/// Overrides exactly the two values Bitstamp has a reason to differ on; everything else stays
/// at [`CoreConfig::default`].
impl Defaults for Extra {
    fn default_core() -> CoreConfig {
        CoreConfig::default()
            // Bitstamp documents no per-socket channel cap; 100 is a blast-radius choice,
            // since one socket failure re-snapshots every symbol on it and each snapshot is
            // ~155 KB.
            .with_max_symbols_per_connection(100)
            // Lower than Binance's 8, for the same reason: each snapshot is ~155 KB, not ~5 KB.
            .with_max_concurrent_snapshots(4)
    }
}
