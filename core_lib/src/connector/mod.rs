use serde::Deserialize;
use crate::connector::book_publisher::BookReader;
use crate::connector::events::{
    create_event_channel, ConnectorEvent, ConnectorRx, ConnectorTx, Subscribe, Unsubscribe,
};
use crate::venue::ConnectorConfig;

pub mod events;
pub mod book_publisher;

#[derive(Debug)]
pub struct ConnectorHandle {
    tx: ConnectorTx,
    task: tokio::task::JoinHandle<()>,
}

pub trait Connector {
    /// The venue's own extras - its endpoints and wire-format knobs. Not the whole connector
    /// config: `run` receives those wrapped in a [`ConnectorConfig`], which carries the shared
    /// `CoreConfig` alongside them.
    type Config: for<'de> Deserialize<'de> + Send + 'static;

    fn run(rx: ConnectorRx, config: ConnectorConfig<Self::Config>) -> impl Future<Output = ()> + Send + 'static;
}

impl ConnectorHandle {
    /// Spawns `C`'s connector and returns the handle to drive it.
    ///
    /// `config` pairs the shared tuning with `C`'s own extras. `ConnectorConfig::default()`
    /// is the usual call: its `Default` runs through the venue's `Defaults` impl, so both
    /// halves come out with that venue's chosen values rather than a generic baseline.
    pub fn new<C: Connector>(config: ConnectorConfig<C::Config>) -> Self {
        let (rx, tx) = create_event_channel();
        let task = tokio::task::spawn(async move {
            C::run(rx, config).await;
        });

        Self {tx, task}
    }

    pub fn subscribe(&self, symbol: Box<str>) -> oneshot::Receiver<anyhow::Result<BookReader>> {
        let (event, rx) = Subscribe::new(symbol);
        self.tx.send(ConnectorEvent::Subscribe(event));
        rx
    }

    /// Tears down one symbol's stream. An unknown symbol or an invalid name is logged by the
    /// connector and otherwise ignored - the teardown is already observable in-band through
    /// the book channel, so there is nothing for this call to report.
    pub fn unsubscribe(&self, symbol: Box<str>) {
        self.tx
            .send(ConnectorEvent::Unsubscribe(Unsubscribe::new(symbol)));
    }

    pub async fn shutdown(self) {
        let (tx, rx) = oneshot::channel();
        self.tx.send(ConnectorEvent::ShutDown(tx));
        if rx.await.is_err() {
            tracing::error!("Connector failed to shut down, aborting task");
            self.task.abort();
        }

        let _ = self.task.await;
    }
}