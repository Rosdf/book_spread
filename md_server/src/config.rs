//! The server's configuration file, and how it is split among the things that need it.
//!
//! `MD_SERVER_CONFIG` names a TOML file:
//!
//! ```toml
//! addr = "0.0.0.0:50051"
//!
//! [catalogue]
//! path = "/etc/md/catalogue.toml"
//!
//! [venues.binance_spot]
//! rest_endpoint   = "https://api.binance.com"
//! stream_endpoint = "wss://data-stream.binance.vision"
//! depth_speed     = "fast"
//! snapshot_limit  = 100
//! [venues.binance_spot.core]
//! max_backoff         = "30s"
//! idle_symbol_timeout = "60s"
//! idle_scan_interval  = "10s"
//!
//! [venues.bitstamp]
//! rest_endpoint   = "https://www.bitstamp.net"
//! stream_endpoint = "wss://ws.bitstamp.net"
//! ```
//!
//! Every section is optional except the catalogue: a venue left out falls back to
//! `ConnectorConfig::default()`, which is what the connectors were built with before there was
//! a file at all. The catalogue is not optional because a server with nothing to advertise can
//! serve nobody.
//!
//! A venue's own extras sit directly under `[venues.<name>]` and its shared tuning under
//! `[venues.<name>.core]`, because `ConnectorConfig` flattens the two - see
//! [`core_lib::venue::ConnectorConfig`].

use core_lib::venue::ConnectorConfig;
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::Path;

/// The environment variable naming the config file.
pub const CONFIG_VAR: &str = "MD_SERVER_CONFIG";

/// The whole file.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    addr: SocketAddr,
    catalogue: CatalogueConfig,
    #[serde(default)]
    venues: VenueConfigs,
}

/// Where the catalogue comes from.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogueConfig {
    path: Box<Path>,
}

impl CatalogueConfig {
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

/// One section per venue this build carries. A missing section is that venue's own defaults,
/// which is what makes a minimal config file - an address and a catalogue - a complete one.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct VenueConfigs {
    #[serde(default)]
    binance_spot: ConnectorConfig<binance_spot::Config>,
    #[serde(default)]
    bitstamp: ConnectorConfig<bitstamp::Config>,
}

/// What [`crate::server::run`] itself needs, once the connector halves have been handed off.
#[derive(Debug)]
pub struct ServerConfig {
    addr: SocketAddr,
    catalogue: CatalogueConfig,
}

impl ServerConfig {
    pub(crate) fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub(crate) fn catalogue(&self) -> &CatalogueConfig {
        &self.catalogue
    }
}

/// Why a config file did not produce a configuration.
#[derive(Debug, thiserror::Error)]
pub enum LoadConfigError {
    #[error("{CONFIG_VAR} is not set; it must name a TOML config file")]
    NoPath,
    #[error("could not read the config at {path}")]
    Read {
        path: Box<str>,
        #[source]
        err: std::io::Error,
    },
    #[error("the config at {path} is not a well-formed md_server config")]
    Parse {
        path: Box<str>,
        #[source]
        err: toml::de::Error,
    },
}

impl AppConfig {
    /// Reads the file `CONFIG_VAR` names.
    ///
    /// # Errors
    ///
    /// [`LoadConfigError::NoPath`] when the variable is unset, and the read or parse failure
    /// otherwise. Each is fatal: there is no default address to fall back to and no catalogue
    /// to serve without one.
    pub fn from_env() -> Result<Self, LoadConfigError> {
        let path = std::env::var_os(CONFIG_VAR).ok_or(LoadConfigError::NoPath)?;
        Self::load(Path::new(&path))
    }

    /// Reads and parses `path`.
    ///
    /// # Errors
    ///
    /// [`LoadConfigError::Read`] for a file that cannot be read, and
    /// [`LoadConfigError::Parse`] for one that is not this shape - including an unknown key,
    /// which is refused rather than ignored so a typo cannot silently leave a setting at its
    /// default.
    pub fn load(path: &Path) -> Result<Self, LoadConfigError> {
        let shown: Box<str> = path.display().to_string().into_boxed_str();
        let text = std::fs::read_to_string(path).map_err(|err| LoadConfigError::Read {
            path: shown.clone(),
            err,
        })?;
        toml::from_str(&text).map_err(|err| LoadConfigError::Parse { path: shown, err })
    }

    /// Splits into the three parts that go three different ways: what the server keeps, and
    /// one connector configuration per venue.
    #[must_use]
    pub fn split(
        self,
    ) -> (
        ServerConfig,
        ConnectorConfig<binance_spot::Config>,
        ConnectorConfig<bitstamp::Config>,
    ) {
        let Self {
            addr,
            catalogue,
            venues,
        } = self;
        (
            ServerConfig { addr, catalogue },
            venues.binance_spot,
            venues.bitstamp,
        )
    }
}

#[cfg(test)]
mod test {
    use super::{AppConfig, LoadConfigError};
    use std::time::Duration;

    fn parse(toml: &str) -> Result<AppConfig, toml::de::Error> {
        toml::from_str(toml)
    }

    /// The minimal file: an address and a catalogue. Every venue then runs on its own
    /// defaults, which is what the server was hard-coded to do before there was a file.
    #[test]
    fn a_minimal_config_leaves_every_venue_at_its_defaults() {
        let config = parse(
            r#"
            addr = "0.0.0.0:50051"
            [catalogue]
            path = "/etc/md/catalogue.toml"
            "#,
        )
        .expect("the minimal file is valid");

        let (server, binance, bitstamp) = config.split();
        assert_eq!(server.addr().port(), 50051);
        assert_eq!(
            server.catalogue().path().to_str(),
            Some("/etc/md/catalogue.toml")
        );
        assert_eq!(
            binance.core().max_backoff(),
            Duration::from_secs(30),
            "an omitted venue section is that venue's own defaults"
        );
        assert_eq!(
            bitstamp.core().max_symbols_per_connection(),
            100,
            "including the overrides the venue's own `Defaults` impl makes"
        );
    }

    /// A venue's extras and its shared tuning are one flattened section, and a duration is
    /// written the way a human writes one.
    #[test]
    fn a_venue_section_carries_both_halves_of_its_config() {
        let config = parse(
            r#"
            addr = "127.0.0.1:1"
            [catalogue]
            path = "catalogue.toml"

            [venues.binance_spot]
            snapshot_limit = 20
            [venues.binance_spot.core]
            max_backoff        = "45s"
            idle_scan_interval = "500ms"
            "#,
        )
        .expect("the file is valid");

        let (_, binance, _) = config.split();
        assert_eq!(binance.core().max_backoff(), Duration::from_secs(45));
        assert_eq!(
            binance.core().idle_scan_interval(),
            Duration::from_millis(500)
        );
        assert_eq!(
            binance.core().max_concurrent_snapshots(),
            8,
            "a partially specified core section fills its gaps from the defaults"
        );
    }

    /// A typo at any level this can catch one is a failure to start rather than a setting
    /// silently left at its default - which is what `deny_unknown_fields` on each of these
    /// types is for.
    ///
    /// A key inside a `[venues.<name>]` section is the one place it cannot: `ConnectorConfig`
    /// flattens a venue's extras next to `core`, and serde's own `flatten` buffers what it
    /// does not recognise rather than refusing it, so an unknown key there is dropped however
    /// the venue's `Extra` is annotated.
    #[test]
    fn an_unknown_key_is_refused() {
        for mistyped in [
            r#"
            addr = "127.0.0.1:1"
            listen = "0.0.0.0:2"
            [catalogue]
            path = "catalogue.toml"
            "#,
            r#"
            addr = "127.0.0.1:1"
            [catalogue]
            path = "catalogue.toml"
            elsewhere = "no"
            "#,
            r#"
            addr = "127.0.0.1:1"
            [catalogue]
            path = "catalogue.toml"
            [venues.kraken]
            rest_endpoint = "https://api.kraken.com"
            "#,
        ] {
            assert!(
                parse(mistyped).is_err(),
                "an unknown key must not be ignored: {mistyped}"
            );
        }
    }

    /// A file that cannot be read at all is named in the error: the whole question at startup
    /// is which file the server was pointed at.
    #[test]
    fn a_missing_file_names_itself() {
        let err = AppConfig::load(std::path::Path::new("/nonexistent/md-server.toml"))
            .expect_err("the file does not exist");
        assert!(matches!(err, LoadConfigError::Read { .. }));
        assert!(err.to_string().contains("/nonexistent/md-server.toml"));
    }
}
