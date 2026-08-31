//! Where a [`Catalogue`] comes from.
//!
//! One trait with one implementation today - a file read at startup. The trait is what keeps
//! the shape of "fetched once, before anything is served": `load` takes `self` by value, so a
//! source cannot be asked twice, and it is `async` so that fetching one over a connection
//! later is a new impl and nothing else.

use crate::catalogue::{BuildCatalogueError, Catalogue, RawCatalogue};
use std::future::Future;
use std::path::{Path};

/// Where the server gets what it advertises.
pub(crate) trait CatalogueSource: Send + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Called once, before the server starts serving.
    fn load(self) -> impl Future<Output = Result<Catalogue, Self::Error>> + Send;
}

/// A catalogue read from a TOML file - see [`RawCatalogue`] for the shape.
#[derive(Debug)]
pub(crate) struct FileCatalogue {
    path: Box<Path>,
}

impl FileCatalogue {
    pub(crate) fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf().into_boxed_path(),
        }
    }
}

/// Why a catalogue file did not produce a catalogue.
///
/// Each variant carries the path, because this is what a startup failure prints and the whole
/// question at that point is *which* file the server was told to read.
#[derive(Debug, thiserror::Error)]
pub(crate) enum LoadCatalogueError {
    #[error("could not read the catalogue at {path}")]
    Read {
        path: Box<Path>,
        #[source]
        err: std::io::Error,
    },
    #[error("the catalogue at {path} is not well-formed TOML")]
    Parse {
        path: Box<Path>,
        #[source]
        err: toml::de::Error,
    },
    #[error("the catalogue at {path} contradicts itself")]
    Build {
        path: Box<Path>,
        #[source]
        err: BuildCatalogueError,
    },
}

impl CatalogueSource for FileCatalogue {
    type Error = LoadCatalogueError;

    async fn load(self) -> Result<Catalogue, LoadCatalogueError> {
        let text = match tokio::fs::read_to_string(&self.path).await {
            Ok(text) => text,
            Err(err) => {
                return Err(LoadCatalogueError::Read {
                    path: self.path,
                    err,
                });
            }
        };

        let raw: RawCatalogue = match toml::from_str(&text) {
            Ok(raw) => raw,
            Err(err) => {
                return Err(LoadCatalogueError::Parse {
                    path: self.path,
                    err,
                });
            }
        };

        Catalogue::try_from(raw).map_err(|err| LoadCatalogueError::Build {
            path: self.path,
            err,
        })
    }
}

#[cfg(test)]
mod test {
    use super::{CatalogueSource as _, FileCatalogue, LoadCatalogueError};
    use md_wire::grpc::CatalogueIdx;

    /// The whole of the load path, over a real file: a failure here is what stops startup, so
    /// each kind has to be distinguishable rather than one opaque error.
    #[tokio::test]
    async fn a_file_is_read_parsed_and_built() {
        let dir = std::env::temp_dir().join(format!("md-catalogue-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("a temp dir is writable");
        let path = dir.join("catalogue.toml");

        std::fs::write(
            &path,
            r#"
            [[instruments]]
            pairs = [{ venue = "bitstamp", symbol = "btcusd" }]

            [[instruments]]
            pairs = [{ venue = "binance_spot", symbol = "BTCUSDT" }]
            "#,
        )
        .expect("the temp file is writable");
        let loaded = FileCatalogue::new(&path)
            .load()
            .await
            .expect("the fixture is a valid catalogue");
        assert!(
            loaded.instruments().contains_key(&CatalogueIdx::new(1)),
            "an entry's position in the file is what a client will name"
        );

        std::fs::write(&path, "this is not toml = = =").expect("the temp file is writable");
        assert!(matches!(
            FileCatalogue::new(&path).load().await,
            Err(LoadCatalogueError::Parse { .. })
        ));

        std::fs::remove_file(&path).expect("the temp file exists");
        assert!(
            matches!(
                FileCatalogue::new(&path).load().await,
                Err(LoadCatalogueError::Read { .. })
            ),
            "a missing catalogue must fail startup rather than serve nobody"
        );
        std::fs::remove_dir_all(&dir).expect("the temp dir exists");
    }
}
