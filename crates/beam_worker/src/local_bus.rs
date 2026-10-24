//! An in-process riverbase bus (tests and single-process runs).
//!
//! Riverbase ships NATS, Postgres and Redis buses. [`LocalBus`] implements
//! the same [`MessageBus`] and [`StreamBus`] traits over a tokio broadcast
//! channel, so a domain's command outbox and a [`crate::QueueWorker`] in the
//! same process can talk without a broker.

use std::pin::Pin;

use async_trait::async_trait;
use futures_util::Stream;
use riverbase_core::command::MessageBus;
use riverbase_core::transport::StreamBus;
use riverbase_core::RiverbaseResult;
use serde_json::Value;
use tokio::sync::broadcast;

/// Broadcast-backed bus: every subscriber of a topic gets each later message.
#[derive(Clone)]
pub struct LocalBus {
    tx: broadcast::Sender<(String, Value)>,
}

impl Default for LocalBus {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalBus {
    #[must_use]
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(1024);
        Self { tx }
    }
}

#[async_trait]
impl MessageBus for LocalBus {
    async fn publish(&self, topic: &str, payload: Value) -> RiverbaseResult<()> {
        // No subscriber is not an error: the queue's tick picks the work up.
        let _ = self.tx.send((topic.to_owned(), payload));
        Ok(())
    }
}

#[async_trait]
impl StreamBus for LocalBus {
    async fn subscribe_values(
        &self,
        topic: &str,
    ) -> RiverbaseResult<Pin<Box<dyn Stream<Item = Value> + Send>>> {
        let rx = self.tx.subscribe();
        let topic = topic.to_owned();
        Ok(Box::pin(futures_util::stream::unfold(
            (rx, topic),
            |(mut rx, topic)| async move {
                loop {
                    match rx.recv().await {
                        Ok((t, value)) if t == topic => return Some((value, (rx, topic))),
                        Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                        Err(broadcast::error::RecvError::Closed) => return None,
                    }
                }
            },
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;
    use serde_json::json;

    #[tokio::test]
    async fn subscribers_get_only_their_topic() {
        let bus = LocalBus::new();
        let mut tasks = bus.subscribe_values("a").await.unwrap();
        bus.publish("b", json!(1)).await.unwrap();
        bus.publish("a", json!(2)).await.unwrap();
        assert_eq!(tasks.next().await, Some(json!(2)));
    }
}
