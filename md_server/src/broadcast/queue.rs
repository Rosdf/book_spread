use crate::broadcast::broadcaster::Join;
use tokio::io::AsyncWrite;
use tokio::sync::mpsc;

#[derive(Debug)]
pub(crate) struct BroadcasterTx<S>(mpsc::UnboundedSender<Join<S>>);
#[derive(Debug)]
pub(crate) struct BroadcasterRx<S>(mpsc::UnboundedReceiver<Join<S>>);

impl<S> BroadcasterTx<S> {
    pub(crate) fn send(&self, join: Join<S>) -> Result<(), Join<S>> {
        self.0.send(join).map_err(|err| err.0)
    }
}

impl<S> BroadcasterRx<S> {
    pub(crate) async fn drain(mut self, why: &str)
    where
        S: AsyncWrite + Unpin,
    {
        while let Some(join) = self.0.recv().await {
            join.reject(why).await;
        }
    }
    
    pub(super) async fn recv(&mut self) -> Option<Join<S>> {
        self.0.recv().await
    }
}

pub(crate) fn make_broadcaster_channel<S>() -> (BroadcasterTx<S>, BroadcasterRx<S>) {
    let (tx, rx) = mpsc::unbounded_channel();
    (BroadcasterTx(tx), BroadcasterRx(rx))
}
