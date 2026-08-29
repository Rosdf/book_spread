use crate::connector::book_publisher::BookReader;
use crate::instrument::InstrumentId;

#[derive(Debug)]
pub struct Subscribe {
    instrument_id: InstrumentId,
    tx: oneshot::Sender<anyhow::Result<BookReader>>,
}

impl Subscribe {
    pub fn new(
        instrument_id: InstrumentId,
    ) -> (Self, oneshot::Receiver<anyhow::Result<BookReader>>) {
        let (tx, rx) = oneshot::channel();
        (Self { instrument_id, tx }, rx)
    }

    pub fn into_parts(self) -> (InstrumentId, oneshot::Sender<anyhow::Result<BookReader>>) {
        (self.instrument_id, self.tx)
    }
}

/// A request to tear down one instrument's stream.
///
/// Fire-and-forget: no reply channel, because the teardown is already observable through the
/// book channel - the publisher's drop hands the reader an empty book and then `None`. The
/// field is private so a reply channel could be added later without changing call sites.
#[derive(Debug)]
pub struct Unsubscribe {
    instrument_id: InstrumentId,
}

impl Unsubscribe {
    pub fn new(instrument_id: InstrumentId) -> Self {
        Self { instrument_id }
    }

    pub fn into_instrument(self) -> InstrumentId {
        self.instrument_id
    }
}

#[derive(Debug)]
pub enum ConnectorEvent {
    Subscribe(Subscribe),
    Unsubscribe(Unsubscribe),
    ShutDown(oneshot::Sender<()>),
}

#[derive(Debug)]
pub struct ConnectorRx {
    rx: tokio::sync::mpsc::UnboundedReceiver<ConnectorEvent>,
}

#[derive(Debug)]
pub(crate) struct ConnectorTx {
    tx: tokio::sync::mpsc::UnboundedSender<ConnectorEvent>,
}

impl ConnectorRx {
    pub async fn recv(&mut self) -> Option<ConnectorEvent> {
        self.rx.recv().await
    }
}

impl ConnectorTx {
    pub(crate) fn send(&self, event: ConnectorEvent) {
        let _ = self.tx.send(event);
    }
}

/// `pub(crate)` rather than `pub(super)`: `venue::supervisor`'s own tests stand a
/// connector up from one of these, and they live outside this module.
pub(crate) fn create_event_channel() -> (ConnectorRx, ConnectorTx) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    (ConnectorRx { rx }, ConnectorTx { tx })
}
