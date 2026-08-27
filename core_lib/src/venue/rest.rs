//! Fetching a bootstrap snapshot over REST.
//!
//! Only the raw bytes come back: decoding happens on the connection task via
//! [`Venue::seed_and_replay`], so every mutation of every book stays on one thread and the
//! snapshot can be seeded directly into the book the slot already owns.

use crate::net::{RequestBuilder as _, Response as _, RestClient};
use crate::venue::spec::{SnapshotFetchError, Venue};
use crate::venue::symbol::Symbol;
use bytes::Bytes;
use tokio::sync::Semaphore;

/// Fetches `symbol`'s bootstrap snapshot and returns the raw body.
///
/// # Errors
/// [`SnapshotFetchError`] if the concurrency permit cannot be acquired (only on shutdown), the
/// request fails, or the response is a non-success status.
pub async fn fetch_snapshot<V, R>(
    client: &R,
    cfg: &V::Config,
    symbol: &mut Symbol,
    permits: &Semaphore,
) -> Result<Bytes, SnapshotFetchError<R::Builder>>
where
    V: Venue,
    R: RestClient,
{
    // Bounds the burst when a whole connection rebootstraps at once.
    let _permit = permits
        .acquire()
        .await
        .map_err(|_| SnapshotFetchError::<R::Builder>::ShuttingDown)?;

    let url = V::snapshot_url(cfg, symbol);

    let body = client
        .get(url)
        .send()
        .await
        .map_err(SnapshotFetchError::<R::Builder>::HttpRequest)?
        .error_for_status()
        .map_err(SnapshotFetchError::<R::Builder>::HttpResponse)?
        .bytes()
        .await
        .map_err(SnapshotFetchError::<R::Builder>::HttpResponse)?;

    Ok(body)
}
