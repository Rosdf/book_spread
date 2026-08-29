use crate::symbol::DepthSpeed;
use core_lib::venue::Defaults;
use serde::Deserialize;

/// What [`crate::BinanceSpot`] declares as its [`core_lib::venue::VenueSpec::Config`]: Binance's
/// own extras, and only those.
///
/// The shared tuning is not in here. A caller pairs this with
/// [`core_lib::venue::CoreConfig`] by building a
/// `core_lib::venue::ConnectorConfig<Config>` - whose `Default` picks up both halves,
/// including the overrides in the [`Defaults`] impl below - and hands *that* to
/// [`ConnectorHandle::new`](core_lib::connector::ConnectorHandle::new).
pub type Config = Extra;

/// Binance-specific tuning: the endpoints and wire-format knobs the `VenueSpec` methods read.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Extra {
    /// Market-data-only endpoint. Needs no API key of any kind.
    stream_endpoint: Box<str>,
    rest_endpoint: Box<str>,
    depth_speed: DepthSpeed,
    /// Depth of the bootstrap snapshot. Limits 1-100 all cost request weight 5, against a
    /// 6000/min IP budget; 5000 would cost 250. `SmallBook` is 10 deep and
    /// `IncrementalBook`'s hot tier is 20, so 100 buys a replenishment tail for free.
    snapshot_limit: u16,
}

impl Extra {
    pub(crate) fn stream_endpoint(&self) -> &str {
        &self.stream_endpoint
    }

    pub(crate) fn rest_endpoint(&self) -> &str {
        &self.rest_endpoint
    }

    pub(crate) fn depth_speed(&self) -> DepthSpeed {
        self.depth_speed
    }

    pub(crate) fn snapshot_limit(&self) -> u16 {
        self.snapshot_limit
    }
}

impl Default for Extra {
    fn default() -> Self {
        Self {
            stream_endpoint: "wss://data-stream.binance.vision".into(),
            rest_endpoint: "https://api.binance.com".into(),
            depth_speed: DepthSpeed::Fast,
            snapshot_limit: 100,
        }
    }
}

/// Supplies the shared half of `ConnectorConfig::<Config>::default()`, via [`Defaults`]'s
/// blanket impl.
///
/// No `default_core` override: [`core_lib::venue::CoreConfig::default`]'s own values - 200
/// symbols per socket (well under Binance's hard limit of 1024, so one socket failure resyncs a
/// bounded number), 8 concurrent snapshots, 512 buffered frames - are already exactly what this
/// venue wants.
impl Defaults for Extra {}
