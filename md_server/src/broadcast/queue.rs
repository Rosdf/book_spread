use crate::broadcast::broadcaster::Join;
use crate::client::ClientHandshake;
use md_wire::grpc::Rejected;
use tokio::sync::mpsc;

#[derive(Debug)]
pub(crate) struct BroadcasterTx<C>(mpsc::UnboundedSender<Join<C>>);
#[derive(Debug)]
pub(crate) struct BroadcasterRx<C>(mpsc::UnboundedReceiver<Join<C>>);

impl<C> BroadcasterTx<C> {
    pub(crate) fn send(&self, join: Join<C>) -> Result<(), Join<C>> {
        self.0.send(join).map_err(|err| err.0)
    }
}

impl<C: ClientHandshake> BroadcasterRx<C> {
    /// Refuses every join still queued, and every one that arrives before the last sender
    /// goes. `rejected` is cloned per join rather than borrowed, because each refusal consumes
    /// the client it is written on.
    pub(crate) async fn drain(mut self, rejected: Rejected) {
        while let Some(join) = self.0.recv().await {
            join.reject(rejected.clone()).await;
        }
    }

    pub(super) async fn recv(&mut self) -> Option<Join<C>> {
        self.0.recv().await
    }
}

pub(crate) fn make_broadcaster_channel<C>() -> (BroadcasterTx<C>, BroadcasterRx<C>) {
    let (tx, rx) = mpsc::unbounded_channel();
    (BroadcasterTx(tx), BroadcasterRx(rx))
}
