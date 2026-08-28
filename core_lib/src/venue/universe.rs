//! The set of symbols a venue currently lists as tradable, refreshed on the hour.
//!
//! A symbol nobody trades used to be discovered the hard way: the subscribe went out, the venue
//! rejected the control frame, and nothing decoded that rejection - the slot sat there
//! bootstrapping forever, publishing an empty book. Fetching the venue's own listing instead
//! means a bad symbol is refused at the request, before a lane is even chosen.
//!
//! Refreshed against the wall clock rather than on an interval from process start, so every
//! connector in a process lands on the same minute and a listing is never more than an hour
//! stale. A failed refresh retries with the shared [`Backoff`] and, once any listing has been
//! fetched, leaves the last good one in place: a listing endpoint having a bad minute must not
//! take every symbol down with it.

use crate::net::{RequestBuilder, Response as _, RestClient};
use crate::venue::ConnectorConfig;
use crate::venue::backoff::Backoff;
use crate::venue::spec::Venue;
use crate::venue::symbol::Symbol;
use std::collections::HashSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

/// Why one listing refresh failed.
///
/// Generic over the three leaf error types directly, rather than over `V`/`R` with
/// associated-type projections as field types - same reasoning as
/// [`crate::venue::spec::SnapshotFetchErrorImpl`], whose doc spells it out. [`ListingError`] is
/// the ergonomic alias callers actually use.
#[derive(Debug, thiserror::Error)]
pub enum ListingErrorImpl<T, U, E> {
    #[error("http request: {0}")]
    HttpRequest(T),

    #[error("http response: {0}")]
    HttpResponse(U),

    #[error("decoding the symbol listing: {0}")]
    Decode(E),
}

pub type ListingError<V, R> = ListingErrorImpl<
    <<R as RestClient>::Builder as RequestBuilder>::Error,
    <<<R as RestClient>::Builder as RequestBuilder>::Response as crate::net::Response>::Error,
    <V as Venue>::SymbolsError,
>;

/// Fetches the venue's listing once.
///
/// Takes the venue's extras alone, since building the URL and decoding the body is all this
/// does - unlike [`refresh_loop`], which also paces itself off [`CoreConfig`] and so needs the
/// whole [`ConnectorConfig`].
///
/// [`CoreConfig`]: crate::venue::config::CoreConfig
///
/// # Errors
/// [`ListingError`] if the request fails, the response is a non-success status, or the body
/// does not decode.
pub async fn fetch<V, R>(client: &R, cfg: &V::Config) -> Result<HashSet<Symbol>, ListingError<V, R>>
where
    V: Venue,
    R: RestClient,
{
    let body = client
        .get(V::symbols_url(cfg))
        .send()
        .await
        .map_err(ListingErrorImpl::HttpRequest)?
        .error_for_status()
        .map_err(ListingErrorImpl::HttpResponse)?
        .bytes()
        .await
        .map_err(ListingErrorImpl::HttpResponse)?;

    V::parse_symbols(body).map_err(ListingErrorImpl::Decode)
}

/// Fetches the listing now, then again at every wall-clock hour, sending each success to the
/// supervisor.
///
/// Returns when the supervisor drops its receiver, which is the only stop signal there is.
/// A failure never ends the loop and never sends: the supervisor keeps whatever listing it
/// already had.
#[expect(
    clippy::implicit_hasher,
    reason = "the supervisor on the other end of this channel owns the concrete set; a hasher \
              parameter here would only propagate through every caller for nothing"
)]
pub async fn refresh_loop<V, R>(
    cfg: ConnectorConfig<V::Config>,
    client: R,
    tx: mpsc::Sender<HashSet<Symbol>>,
) where
    V: Venue,
    R: RestClient,
{
    let mut backoff = Backoff::new(cfg.core().max_backoff());

    loop {
        match fetch::<V, R>(&client, cfg.inner()).await {
            Ok(listed) => {
                tracing::debug!(symbols = listed.len(), "symbol listing refreshed");
                if tx.send(listed).await.is_err() {
                    return;
                }
                backoff.reset();
                tokio::time::sleep(until_next_hour()).await;
            }
            Err(err) => {
                tracing::warn!(%err, "symbol listing refresh failed, retrying");
                tokio::time::sleep(backoff.next()).await;
            }
        }
    }
}

/// How long until the next wall-clock `:00`.
///
/// Straight off `SystemTime` rather than a calendar crate: an hour boundary is
/// `3600 - (epoch_secs % 3600)` in any timezone, since every offset in use is a whole number of
/// minutes and the modulus is over UTC seconds either way. A clock before the epoch (only
/// reachable if the system clock is badly wrong) falls back to a full hour.
fn until_next_hour() -> Duration {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs());
    Duration::from_secs(3600 - secs % 3600)
}

#[cfg(test)]
mod test {
    use super::until_next_hour;
    use std::time::Duration;

    #[test]
    fn the_next_refresh_is_always_within_the_hour_and_never_immediate() {
        let wait = until_next_hour();
        assert!(
            wait > Duration::ZERO,
            "a zero wait would hot-spin the refresh loop"
        );
        assert!(wait <= Duration::from_secs(3600), "{wait:?}");
    }
}
