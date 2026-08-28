//! Binance's control-frame pacing: chunk up to [`SUBSCRIBE_CHUNK`] names per frame and sleep
//! [`MIN_CONTROL_GAP`] inline between frames.
//!
//! Batching is cheap here because a `SUBSCRIBE` names many streams at once - unlike Bitstamp,
//! whose channel frames each name exactly one channel and so cannot batch at all; see
//! `bitstamp::pacer` for that side of [`core_lib::venue::ControlPacer`].

use core_lib::net::WsConnector;
use core_lib::venue::{ControlPacer, Method, SessionError, ws_err};
use futures_util::SinkExt as _;
use std::collections::VecDeque;
use std::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::Message;

/// Streams named in a single control frame. Binance caps a connection at 1024 streams; this
/// only bounds how large one control frame gets.
const SUBSCRIBE_CHUNK: usize = 100;

/// Minimum spacing between control frames.
///
/// Binance allows 5 incoming messages per second, counting pings, pongs and control frames
/// together. 250ms keeps us at 4/s worst case, leaving a slot for a pong. The gap is short on
/// purpose: waiting here also pauses the read half.
const MIN_CONTROL_GAP: Duration = Duration::from_millis(250);

/// How many sent control frames are remembered for attributing a rejection back to the streams
/// it named.
///
/// Bounded because nothing removes an id whose reply never arrives: Binance answers every
/// request, but a socket that dies mid-flight leaves entries behind, and the next session
/// starts from a fresh pacer anyway. Well above the handful of frames a burst of subscribes
/// produces.
const INFLIGHT_IDS: usize = 32;

#[derive(Debug)]
pub struct BatchPacer {
    queue: Vec<(Method, Box<str>)>,
    next_control_id: u64,
    /// Ids sent on this socket and the streams each named, oldest first, for
    /// [`ControlPacer::names_for`].
    in_flight: VecDeque<(u64, Vec<Box<str>>)>,
    /// When the last control frame went out, for [`MIN_CONTROL_GAP`] pacing.
    last_control: Instant,
}

impl Default for BatchPacer {
    fn default() -> Self {
        Self {
            queue: Vec::new(),
            next_control_id: 1,
            in_flight: VecDeque::new(),
            // Backdated so the first subscribe of the process does not wait out the gap.
            // Falls back to `now` in the checked case: only possible moments after process
            // start on a fresh monotonic clock, and `now` just means one extra wait.
            last_control: Instant::now()
                .checked_sub(MIN_CONTROL_GAP)
                .unwrap_or_else(Instant::now),
        }
    }
}

impl ControlPacer for BatchPacer {
    fn enqueue(&mut self, method: Method, name: Box<str>) {
        self.queue.push((method, name));
    }

    /// Drains the whole queue in the order it was enqueued, batching only *consecutive* runs
    /// of the same method.
    ///
    /// Order has to be preserved rather than partitioned globally: an unsubscribe followed by
    /// a re-subscribe of the same stream, drained in one burst, would otherwise reach the wire
    /// as `SUBSCRIBE x` then `UNSUBSCRIBE x` - leaving the slot live locally while Binance had
    /// stopped sending, dark until the next reconnect. Alternating methods cost one frame
    /// each, [`MIN_CONTROL_GAP`] apart, which only happens for an interleaved burst; the
    /// common case is one run and so still one frame.
    async fn on_admitted<W: WsConnector>(
        &mut self,
        stream: &mut W::Stream,
    ) -> Result<(), SessionError<W>> {
        if self.queue.is_empty() {
            return Ok(());
        }
        let items = std::mem::take(&mut self.queue);
        let mut run: Vec<Box<str>> = Vec::new();
        let mut run_method: Option<Method> = None;

        for (method, name) in items {
            if run_method.is_some_and(|current| current != method) {
                let current = run_method.expect("guarded by is_some_and");
                self.send_control::<W>(stream, current, &run).await?;
                run.clear();
            }
            run_method = Some(method);
            run.push(name);
        }

        if let Some(method) = run_method {
            self.send_control::<W>(stream, method, &run).await?;
        }
        Ok(())
    }

    /// Binance never has a background deadline to wake for: every queued frame is flushed
    /// synchronously by [`Self::on_admitted`].
    fn next_deadline(&self) -> Option<Instant> {
        None
    }

    fn on_deadline<W: WsConnector>(
        &mut self,
        _stream: &mut W::Stream,
    ) -> impl Future<Output = Result<(), SessionError<W>>> + Send {
        std::future::ready(Ok(()))
    }

    /// Binance echoes the request id back on every reply, so a rejection can be attributed
    /// exactly - as long as the id is still one of the last [`INFLIGHT_IDS`] sent.
    fn names_for(&self, id: Option<u64>) -> &[Box<str>] {
        let Some(wanted) = id else { return &[] };
        self.in_flight
            .iter()
            .find(|(sent, _)| *sent == wanted)
            .map_or(&[], |(_, names)| names.as_slice())
    }
}

impl BatchPacer {
    /// Sends `names` as one or more control frames of `method`, at most [`SUBSCRIBE_CHUNK`] per
    /// frame, paced at least [`MIN_CONTROL_GAP`] apart.
    async fn send_control<W: WsConnector>(
        &mut self,
        stream: &mut W::Stream,
        method: Method,
        names: &[Box<str>],
    ) -> Result<(), SessionError<W>> {
        for chunk in names.chunks(SUBSCRIBE_CHUNK) {
            if let Some(wait) = MIN_CONTROL_GAP.checked_sub(self.last_control.elapsed()) {
                tokio::time::sleep(wait).await;
            }

            let id = self.next_control_id;
            self.next_control_id += 1;
            stream
                .send(Message::Text(control_payload(method, id, chunk).into()))
                .await
                .map_err(ws_err::<W>)?;
            self.last_control = Instant::now();

            self.in_flight.push_back((id, chunk.to_vec()));
            if self.in_flight.len() > INFLIGHT_IDS {
                self.in_flight.pop_front();
            }
        }
        Ok(())
    }
}

const fn method_str(method: Method) -> &'static str {
    match method {
        Method::Subscribe => "SUBSCRIBE",
        Method::Unsubscribe => "UNSUBSCRIBE",
    }
}

/// Builds `{"method":"<METHOD>","params":[...],"id":N}`.
///
/// Stream names are ASCII alphanumerics plus `@`, so none of them needs JSON escaping and this
/// can be assembled directly instead of going through a serializer.
fn control_payload(method: Method, id: u64, streams: &[Box<str>]) -> String {
    let mut out = String::from(r#"{"method":""#);
    out.push_str(method_str(method));
    out.push_str(r#"","params":["#);
    for (i, stream) in streams.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('"');
        out.push_str(stream);
        out.push('"');
    }
    out.push_str(r#"],"id":"#);
    out.push_str(&id.to_string());
    out.push('}');
    out
}

#[cfg(test)]
mod test {
    use super::{BatchPacer, ControlPacer as _, Method, control_payload};
    use core_lib::venue::test_util::{MockStream, ScriptedWs};
    use tokio_tungstenite::tungstenite::Message;

    /// Drains `pacer` onto a recording socket and returns the text of every frame it wrote.
    ///
    /// `start_paused` on the callers, so [`super::MIN_CONTROL_GAP`] between frames costs no
    /// real time.
    async fn drain(pacer: &mut BatchPacer) -> Vec<String> {
        let (mut stream, sent) = MockStream::recording();
        pacer.on_admitted::<ScriptedWs>(&mut stream).await.unwrap();

        let written = sent.lock().unwrap();
        written
            .iter()
            .map(|msg| match msg {
                Message::Text(text) => text.to_string(),
                other => panic!("expected a text control frame, got {other:?}"),
            })
            .collect()
    }

    /// Partitioning the batch globally put `SUBSCRIBE` ahead of `UNSUBSCRIBE` regardless of
    /// the order they were queued in, so an unsubscribe immediately followed by a
    /// re-subscribe reached Binance backwards: the slot stayed live locally while Binance
    /// stopped sending, dark until the next reconnect.
    #[tokio::test(start_paused = true)]
    async fn an_unsubscribe_followed_by_a_resubscribe_keeps_its_queue_order() {
        let mut pacer = BatchPacer::default();
        pacer.enqueue(Method::Unsubscribe, "btcusdt@depth@100ms".into());
        pacer.enqueue(Method::Subscribe, "btcusdt@depth@100ms".into());

        assert_eq!(
            drain(&mut pacer).await,
            vec![
                r#"{"method":"UNSUBSCRIBE","params":["btcusdt@depth@100ms"],"id":1}"#.to_owned(),
                r#"{"method":"SUBSCRIBE","params":["btcusdt@depth@100ms"],"id":2}"#.to_owned(),
            ]
        );
    }

    /// The common case is still one frame per method: only a *change* of method starts a new
    /// frame, so a burst of subscribes stays batched.
    #[tokio::test(start_paused = true)]
    async fn a_run_of_one_method_still_batches_into_a_single_frame() {
        let mut pacer = BatchPacer::default();
        pacer.enqueue(Method::Subscribe, "btcusdt@depth@100ms".into());
        pacer.enqueue(Method::Subscribe, "ethusdt@depth@100ms".into());
        pacer.enqueue(Method::Unsubscribe, "solusdt@depth@100ms".into());

        assert_eq!(
            drain(&mut pacer).await,
            vec![
                r#"{"method":"SUBSCRIBE","params":["btcusdt@depth@100ms","ethusdt@depth@100ms"],"id":1}"#.to_owned(),
                r#"{"method":"UNSUBSCRIBE","params":["solusdt@depth@100ms"],"id":2}"#.to_owned(),
            ]
        );
    }

    #[test]
    fn control_payload_names_the_method_and_every_stream() {
        let streams: Vec<Box<str>> = vec!["btcusdt@depth@100ms".into(), "ethusdt@depth".into()];
        assert_eq!(
            control_payload(Method::Subscribe, 7, &streams),
            r#"{"method":"SUBSCRIBE","params":["btcusdt@depth@100ms","ethusdt@depth"],"id":7}"#
        );
        assert_eq!(
            control_payload(Method::Unsubscribe, 8, &streams),
            r#"{"method":"UNSUBSCRIBE","params":["btcusdt@depth@100ms","ethusdt@depth"],"id":8}"#
        );
    }
}
