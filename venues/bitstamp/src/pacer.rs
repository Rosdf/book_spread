//! Bitstamp's control-frame pacing.
//!
//! Bitstamp's `bts:subscribe`/`bts:unsubscribe` each name exactly one channel, so admitting N
//! symbols costs N frames - there is no batching to fall back on the way Binance's
//! `SUBSCRIBE_CHUNK` does (see `binance_spot::pacer`). Blocking the read half on a sleep per
//! frame would stall reading for `N * CONTROL_GAP`, so instead this queues every control frame
//! and lets the generic session's timer arm - driven by [`ControlPacer::next_deadline`] -
//! drain one per tick, without ever blocking the read half.

use core_lib::net::WsConnector;
use core_lib::venue::{ControlPacer, Method, SessionError, ws_err};
use futures_util::SinkExt as _;
use std::collections::VecDeque;
use std::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::Message;

/// Minimum spacing between `bts:subscribe`/`bts:unsubscribe` frames.
const CONTROL_GAP: Duration = Duration::from_millis(50);

#[derive(Debug)]
pub struct QueuePacer {
    queue: VecDeque<(Method, Box<str>)>,
    /// The channel of the frame most recently sent, for [`ControlPacer::names_for`]. `Option`
    /// rather than a `Vec` because exactly one channel goes out per frame.
    last_sent: Option<Box<str>>,
    /// When the front of the queue may next be sent. Advances by [`CONTROL_GAP`] every time a
    /// frame goes out, regardless of whether the queue emptied in between - see
    /// [`ControlPacer::next_deadline`].
    next_ready: Instant,
}

impl Default for QueuePacer {
    fn default() -> Self {
        Self {
            queue: VecDeque::new(),
            last_sent: None,
            next_ready: Instant::now(),
        }
    }
}

impl ControlPacer for QueuePacer {
    fn enqueue(&mut self, method: Method, name: Box<str>) {
        self.queue.push_back((method, name));
    }

    /// A no-op: Bitstamp cannot batch, so nothing is sent synchronously on admission - every
    /// frame, including a reconnect's resubscribe, drains through [`Self::on_deadline`] on the
    /// same timer.
    fn on_admitted<W: WsConnector>(
        &mut self,
        _stream: &mut W::Stream,
    ) -> impl Future<Output = Result<(), SessionError<W>>> + Send {
        std::future::ready(Ok(()))
    }

    fn next_deadline(&self) -> Option<Instant> {
        (!self.queue.is_empty()).then_some(self.next_ready)
    }

    async fn on_deadline<W: WsConnector>(&mut self, stream: &mut W::Stream) -> Result<(), SessionError<W>> {
        // The session only calls this once `next_deadline` has actually elapsed and the queue
        // was non-empty at that point; nothing else drains the queue in between.
        let Some((method, channel)) = self.queue.pop_front() else {
            self.last_sent = None;
            return Ok(());
        };
        stream
            .send(Message::Text(control_payload(method, &channel).into()))
            .await
            .map_err(ws_err::<W>)?;
        self.next_ready = Instant::now() + CONTROL_GAP;
        self.last_sent = Some(channel);
        Ok(())
    }

    /// A heuristic, not an attribution: `bts:error` carries neither a request id nor a
    /// channel, so the only thing that can be said is which channel the last frame named. It
    /// is a good guess because exactly one frame goes out per [`CONTROL_GAP`] and Bitstamp
    /// answers promptly, and it is only ever used to enrich a log line.
    fn names_for(&self, _id: Option<u64>) -> &[Box<str>] {
        self.last_sent.as_slice()
    }
}

const fn method_str(method: Method) -> &'static str {
    match method {
        Method::Subscribe => "bts:subscribe",
        Method::Unsubscribe => "bts:unsubscribe",
    }
}

/// Builds `{"event":"bts:subscribe","data":{"channel":"..."}}`.
///
/// Channel names are ASCII alphanumerics plus `_`, so none of them needs JSON escaping and
/// this can be assembled directly instead of going through a serializer.
fn control_payload(method: Method, channel: &str) -> String {
    let mut out = String::from(r#"{"event":""#);
    out.push_str(method_str(method));
    out.push_str(r#"","data":{"channel":""#);
    out.push_str(channel);
    out.push_str(r#""}}"#);
    out
}

#[cfg(test)]
mod test {
    use super::{Method, control_payload};

    #[test]
    fn control_payload_names_the_method_and_the_channel() {
        assert_eq!(
            control_payload(Method::Subscribe, "diff_order_book_btcusd"),
            r#"{"event":"bts:subscribe","data":{"channel":"diff_order_book_btcusd"}}"#
        );
        assert_eq!(
            control_payload(Method::Unsubscribe, "diff_order_book_btcusd"),
            r#"{"event":"bts:unsubscribe","data":{"channel":"diff_order_book_btcusd"}}"#
        );
    }
}
