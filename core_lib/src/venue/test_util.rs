//! In-memory transport doubles, so the connection loop and a venue's pacer can be driven with
//! no socket and no HTTP client.
//!
//! Behind a feature rather than `#[cfg(test)]`: those items are not visible across crates, and
//! a venue crate's own tests need the same doubles `core_lib`'s do. That invisibility is why
//! several paths here - the idle sweep, panic recovery, draining commands while backing off -
//! had no test at all before.
//!
//! Every double is scripted up front and inspectable afterwards: what a connection sent, how
//! many times it reconnected, which URLs it fetched.

use crate::net::{RequestBuilder, Response, RestClient, WsConnector};
use bytes::Bytes;
use futures_util::{Sink, Stream};
use reqwest::IntoUrl;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio_tungstenite::tungstenite::Message;

/// The one failure a double can produce. A single type for both transports: nothing under test
/// distinguishes an HTTP failure from a socket failure by anything but where it surfaced.
#[derive(Debug, thiserror::Error)]
#[error("scripted transport failure: {0}")]
pub struct MockError(Box<str>);

impl MockError {
    pub fn new(what: &str) -> Self {
        Self(what.into())
    }
}

/// One scripted step of a socket's read half, consumed in order.
#[derive(Debug, Clone)]
pub enum Incoming {
    /// Handed to the session as a frame.
    Text(String),
    /// Handed to the session verbatim - for pings, pongs and close frames.
    Raw(Message),
    /// The read half yields an error, which ends the session and reconnects.
    Failed,
    /// The peer closed: the stream ends.
    Ended,
    /// Polling the read half panics. Nothing else can express "a session died in a way its
    /// own error type never named", which is the case [`crate::venue::connection::run`]'s
    /// `catch_unwind` exists for.
    Panics,
    /// Nothing more ever arrives on this socket: the read half parks forever, leaving the
    /// session's other `select!` arms - timers, the command queue - to drive it.
    Parks,
}

/// What one connected socket does, plus where its writes are recorded.
#[derive(Debug)]
pub struct MockStream {
    incoming: VecDeque<Incoming>,
    sent: Arc<Mutex<Vec<Message>>>,
}

impl MockStream {
    /// A standalone socket for a test that only cares about what gets written to it - a
    /// pacer's wire order, say - and never reads.
    pub fn recording() -> (Self, Arc<Mutex<Vec<Message>>>) {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let stream = Self {
            incoming: VecDeque::new(),
            sent: Arc::clone(&sent),
        };
        (stream, sent)
    }
}

impl Stream for MockStream {
    type Item = Result<Message, MockError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.incoming.pop_front() {
            Some(Incoming::Text(text)) => Poll::Ready(Some(Ok(Message::Text(text.into())))),
            Some(Incoming::Raw(msg)) => Poll::Ready(Some(Ok(msg))),
            Some(Incoming::Failed) => Poll::Ready(Some(Err(MockError::new("read half failed")))),
            Some(Incoming::Ended) => Poll::Ready(None),
            Some(Incoming::Panics) => panic!("scripted panic while polling the read half"),
            // No waker is registered, on purpose: this arm means "this socket will never
            // speak again", and the session has to keep making progress without it.
            Some(Incoming::Parks) | None => Poll::Pending,
        }
    }
}

impl Sink<Message> for MockStream {
    type Error = MockError;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), MockError>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: Message) -> Result<(), MockError> {
        self.sent.lock().expect("test mutex poisoned").push(item);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), MockError>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), MockError>> {
        Poll::Ready(Ok(()))
    }
}

#[derive(Debug)]
struct WsState {
    /// One entry per `connect`, consumed in order.
    sessions: VecDeque<Vec<Incoming>>,
    /// What every `connect` past the end of `sessions` gets, so a reconnect loop always has
    /// something to attach to rather than running out.
    fallback: Vec<Incoming>,
    connects: usize,
    sent: Arc<Mutex<Vec<Message>>>,
}

/// A [`WsConnector`] whose sockets are written out in advance, one script per connect.
#[derive(Debug, Clone)]
pub struct ScriptedWs {
    state: Arc<Mutex<WsState>>,
}

impl ScriptedWs {
    /// `sessions[n]` is what the `n`th connect does; every connect past the end parks.
    pub fn new(sessions: Vec<Vec<Incoming>>) -> Self {
        Self::with_fallback(sessions, vec![Incoming::Parks])
    }

    pub fn with_fallback(sessions: Vec<Vec<Incoming>>, fallback: Vec<Incoming>) -> Self {
        Self {
            state: Arc::new(Mutex::new(WsState {
                sessions: sessions.into(),
                fallback,
                connects: 0,
                sent: Arc::new(Mutex::new(Vec::new())),
            })),
        }
    }

    /// How many sockets have been opened - i.e. one plus the number of reconnects.
    ///
    /// # Panics
    /// If another thread panicked while holding the recording mutex, which in a test means
    /// the assertion that panicked has already failed.
    pub fn connects(&self) -> usize {
        self.state.lock().expect("test mutex poisoned").connects
    }

    /// Every message written to every socket so far, in order.
    ///
    /// # Panics
    /// As [`Self::connects`].
    pub fn sent(&self) -> Vec<Message> {
        let state = self.state.lock().expect("test mutex poisoned");
        let sent = state.sent.lock().expect("test mutex poisoned");
        sent.clone()
    }

    /// The text of every message written so far, skipping anything that is not text.
    ///
    /// # Panics
    /// As [`Self::connects`].
    pub fn sent_text(&self) -> Vec<String> {
        self.sent()
            .into_iter()
            .filter_map(|msg| match msg {
                Message::Text(text) => Some(text.to_string()),
                _ => None,
            })
            .collect()
    }
}

impl WsConnector for ScriptedWs {
    type Stream = MockStream;
    type Error = MockError;

    fn connect(&self, _url: &str) -> impl Future<Output = Result<MockStream, MockError>> + Send {
        let stream = {
            let mut state = self.state.lock().expect("test mutex poisoned");
            state.connects += 1;
            let script = state.sessions.pop_front().unwrap_or_else(|| state.fallback.clone());
            MockStream {
                incoming: script.into(),
                sent: Arc::clone(&state.sent),
            }
        };
        std::future::ready(Ok(stream))
    }
}

#[derive(Debug)]
pub struct StubResponse {
    body: Bytes,
}

impl Response for StubResponse {
    type Error = MockError;

    fn error_for_status(self) -> Result<Self, MockError> {
        Ok(self)
    }

    fn bytes(self) -> impl Future<Output = Result<Bytes, MockError>> + Send + 'static {
        std::future::ready(Ok(self.body))
    }
}

#[derive(Debug)]
pub struct StubRequest {
    result: Result<StubResponse, MockError>,
}

impl RequestBuilder for StubRequest {
    type Response = StubResponse;
    type Error = MockError;

    fn send(self) -> impl Future<Output = Result<StubResponse, MockError>> + Send + 'static {
        std::future::ready(self.result)
    }
}

/// A URL-substring pattern and the body a matching request gets.
type Route = (Box<str>, Result<Bytes, Box<str>>);

#[derive(Debug)]
struct RestState {
    /// URL-substring routes, checked in order. A route is consumed when it matches, unless it
    /// is the last one left for its pattern - so a repeated fetch keeps getting the final body.
    routes: Vec<Route>,
    /// One body per request that no route matched, consumed in order; past the end every
    /// request gets `fallback`.
    bodies: VecDeque<Result<Bytes, Box<str>>>,
    fallback: Result<Bytes, Box<str>>,
    urls: Vec<String>,
}

impl RestState {
    /// The body for `url`: the first matching route, or the next scripted body.
    fn answer(&mut self, url: &str) -> Result<Bytes, Box<str>> {
        if let Some(hit) = self.routes.iter().position(|(pattern, _)| url.contains(&**pattern)) {
            let still_needed = self
                .routes
                .iter()
                .filter(|(pattern, _)| **pattern == *self.routes[hit].0)
                .count()
                == 1;
            return if still_needed {
                self.routes[hit].1.clone()
            } else {
                self.routes.remove(hit).1
            };
        }
        self.bodies.pop_front().unwrap_or_else(|| self.fallback.clone())
    }
}

/// A [`RestClient`] that answers from a script instead of the network, recording every URL it
/// was asked for.
#[derive(Debug, Clone)]
pub struct StubRest {
    state: Arc<Mutex<RestState>>,
}

impl StubRest {
    /// Answers every request with `body`.
    pub fn always(body: &str) -> Self {
        Self::scripted(Vec::new(), Ok(Bytes::copy_from_slice(body.as_bytes())))
    }

    /// Fails every request.
    pub fn always_failing() -> Self {
        Self::scripted(Vec::new(), Err("scripted REST failure".into()))
    }

    fn scripted(bodies: Vec<Result<Bytes, Box<str>>>, fallback: Result<Bytes, Box<str>>) -> Self {
        Self {
            state: Arc::new(Mutex::new(RestState {
                routes: Vec::new(),
                bodies: bodies.into(),
                fallback,
                urls: Vec::new(),
            })),
        }
    }

    /// Every URL fetched so far, in order.
    ///
    /// # Panics
    /// If another thread panicked while holding the recording mutex, which in a test means
    /// the assertion that panicked has already failed.
    pub fn urls(&self) -> Vec<String> {
        self.state.lock().expect("test mutex poisoned").urls.clone()
    }

    /// Answers requests whose URL contains `pattern` with `body`, ahead of whatever
    /// [`Self::always`]/[`Self::always_failing`] set up. Routes are checked in the order they
    /// were added, so the most specific pattern goes first.
    ///
    /// # Panics
    /// As [`Self::urls`].
    #[must_use]
    pub fn with_route(self, pattern: &str, body: &str) -> Self {
        self.with_changing_route(pattern, &[body])
    }

    /// As [`Self::with_route`], but each matching request in turn gets the next `bodies` entry,
    /// the last one repeating - for a listing that changes between refreshes.
    ///
    /// # Panics
    /// As [`Self::urls`].
    #[must_use]
    pub fn with_changing_route(self, pattern: &str, bodies: &[&str]) -> Self {
        let mut state = self.state.lock().expect("test mutex poisoned");
        for body in bodies {
            state
                .routes
                .push((pattern.into(), Ok(Bytes::copy_from_slice(body.as_bytes()))));
        }
        drop(state);
        self
    }
}

impl RestClient for StubRest {
    type Builder = StubRequest;

    fn get(&self, url: impl IntoUrl) -> StubRequest {
        let scripted = {
            let mut state = self.state.lock().expect("test mutex poisoned");
            let requested = url.into_url().map_or_else(|err| err.to_string(), String::from);
            let answer = state.answer(&requested);
            state.urls.push(requested);
            answer
        };

        StubRequest {
            result: match scripted {
                Ok(body) => Ok(StubResponse { body }),
                Err(why) => Err(MockError(why)),
            },
        }
    }
}

// ---------------------------------------------------------------------------------------
// A venue with no wire format worth the name
// ---------------------------------------------------------------------------------------

use crate::venue::pending::PendingDiffs;
use crate::venue::spec::{
    BootstrapRetry, ControlPacer, Decoder, FrameAction, FrameCtx, Method, Retry,
    SnapshotFetchError, Venue,
};
use crate::venue::symbol::Symbol;
use crate::venue::table::{Slot, SlotState};
use std::collections::HashSet;
use std::time::Instant;

/// A [`Venue`] whose only job is to exercise the generic connection and supervisor machinery.
///
/// Its "wire format" is deliberately not JSON: a data frame is `<symbol>:<cursor>`, a snapshot
/// body is a bare cursor, and a listing is a comma-separated list of symbols. Everything the
/// real venues express through `serde` seeds is beside the point here - what these tests need
/// is a venue that can be driven frame by frame from a script.
#[derive(Debug)]
pub struct TestVenue;

#[derive(Debug, Clone, Default)]
pub struct TestConfig;

/// Counts staged diffs and nothing else: no venue logic here is under test.
#[derive(Debug, Default)]
pub struct TestPending {
    staged: Vec<u64>,
}

impl TestPending {
    /// The cursors staged so far, so a test can check what a bootstrap would replay.
    #[must_use]
    pub fn cursors(&self) -> &[u64] {
        &self.staged
    }
}

impl PendingDiffs for TestPending {
    fn buffered(&self) -> usize {
        self.staged.len()
    }

    fn clear(&mut self) {
        self.staged.clear();
    }
}

/// One ready slot: the cursor of the last frame applied.
#[derive(Debug)]
pub struct TestReady {
    pub cursor: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum TestReplayError {
    #[error(transparent)]
    Fetch(#[from] SnapshotFetchError<StubRequest>),

    /// The snapshot did not reach the diffs already buffered - the shape both real venues hit,
    /// and the one whose recovery must not discard the buffer.
    #[error("snapshot cursor {snapshot} does not reach buffered diffs starting at {first}")]
    Stale { snapshot: u64, first: u64 },

    #[error("snapshot body was not a cursor")]
    Malformed,
}

impl BootstrapRetry for TestReplayError {
    fn retry(&self) -> Retry {
        match self {
            Self::Fetch(_) | Self::Stale { .. } => Retry::Refetch,
            Self::Malformed => Retry::Resync,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("listing body was not a symbol list")]
pub struct TestSymbolsError;

/// Sends every queued control frame immediately, as `SUBSCRIBE <name>` / `UNSUBSCRIBE <name>`,
/// and remembers the names for [`ControlPacer::names_for`].
#[derive(Debug, Default)]
pub struct TestPacer {
    queue: Vec<(Method, Box<str>)>,
    last_names: Vec<Box<str>>,
}

impl TestPacer {
    /// What is queued but not yet sent, so a test can prove no control frame was enqueued.
    #[must_use]
    pub fn queued(&self) -> &[(Method, Box<str>)] {
        &self.queue
    }
}

impl ControlPacer for TestPacer {
    fn enqueue(&mut self, method: Method, name: Box<str>) {
        self.queue.push((method, name));
    }

    async fn on_admitted<W: WsConnector>(
        &mut self,
        stream: &mut W::Stream,
    ) -> Result<(), crate::venue::session::SessionError<W>> {
        use futures_util::SinkExt as _;

        self.last_names.clear();
        for (method, name) in std::mem::take(&mut self.queue) {
            let verb = match method {
                Method::Subscribe => "SUBSCRIBE",
                Method::Unsubscribe => "UNSUBSCRIBE",
            };
            stream
                .send(Message::Text(format!("{verb} {name}").into()))
                .await
                .map_err(crate::venue::session::ws_err::<W>)?;
            self.last_names.push(name);
        }
        Ok(())
    }

    fn next_deadline(&self) -> Option<Instant> {
        None
    }

    fn on_deadline<W: WsConnector>(
        &mut self,
        _stream: &mut W::Stream,
    ) -> impl Future<Output = Result<(), crate::venue::session::SessionError<W>>> + Send {
        std::future::ready(Ok(()))
    }

    fn names_for(&self, _id: Option<u64>) -> &[Box<str>] {
        &self.last_names
    }
}

impl Venue for TestVenue {
    type Config = TestConfig;
    type Ready = TestReady;
    type Stage = ();
    type Pending = TestPending;
    type ReplayError = TestReplayError;
    type SymbolsError = TestSymbolsError;
    type Pacer = TestPacer;

    fn stream_url(_cfg: &Self::Config) -> String {
        "test://stream".to_owned()
    }

    fn symbols_url(_cfg: &Self::Config) -> String {
        "test://listing".to_owned()
    }

    fn parse_symbols(body: Bytes) -> Result<HashSet<Symbol>, TestSymbolsError> {
        let listed = std::str::from_utf8(&body).map_err(|_| TestSymbolsError)?;
        listed
            .split(',')
            .filter(|name| !name.is_empty())
            .map(|name| Symbol::new(name.into()).map_err(|_| TestSymbolsError))
            .collect()
    }

    fn snapshot_url(_cfg: &Self::Config, symbol: &mut Symbol) -> String {
        format!("test://snapshot/{symbol}")
    }

    fn wire_name(_cfg: &Self::Config, symbol: &Symbol) -> Box<str> {
        symbol.as_str().into()
    }

    fn on_frame<'t>(
        ctx: FrameCtx<'t, '_, '_, TestReady, (), TestPending>,
        bytes: Bytes,
    ) -> FrameAction<'t, TestReady, TestPending> {
        let Some((name, cursor)) = std::str::from_utf8(&bytes)
            .ok()
            .and_then(|text| text.split_once(':'))
            .and_then(|(name, raw)| raw.parse::<u64>().ok().map(|cursor| (name, cursor)))
        else {
            return FrameAction::Ignored {
                name: "unparseable".into(),
            };
        };

        let Some(slot) = ctx.table.get_mut(name) else {
            return FrameAction::Ignored { name: name.into() };
        };

        match &mut slot.state {
            SlotState::Bootstrapping(boot) => {
                boot.pending.staged.push(cursor);
                FrameAction::Buffer { slot, cursor }
            }
            SlotState::Ready(ready) => {
                ready.cursor = cursor;
                slot.last_frame = Instant::now();
                FrameAction::Handled
            }
        }
    }

    /// Mirrors both real venues' rule: the snapshot must reach at least as far as the earliest
    /// buffered diff, or there is a window whose changes were never seen.
    fn seed_and_replay(
        _slot: &mut Slot<TestReady, TestPending>,
        pending: &TestPending,
        first_buffered: Option<u64>,
        body: Bytes,
        _dec: &mut Decoder<()>,
    ) -> Result<TestReady, TestReplayError> {
        let seeded: u64 = std::str::from_utf8(&body)
            .ok()
            .and_then(|text| text.trim().parse().ok())
            .ok_or(TestReplayError::Malformed)?;

        if let Some(first) = first_buffered
            && seeded < first
        {
            return Err(TestReplayError::Stale {
                snapshot: seeded,
                first,
            });
        }

        Ok(TestReady {
            cursor: pending.staged.last().copied().unwrap_or(seeded),
        })
    }
}
