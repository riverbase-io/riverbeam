use async_trait::async_trait;
use beam_types::StreamChunk;
use tokio::sync::Mutex;

use riverbase_core::RiverbaseResult;

/// Pluggable sink for serialized stream frames.
///
/// `channel` is `ws.channel.{session}:{request}`, the routing key SSE subscribers use.
#[async_trait]
pub trait StreamTransport: Send + Sync {
    async fn publish(&self, channel: &str, frame: &StreamChunk) -> RiverbaseResult<String>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct PublishedFrame {
    pub channel: String,
    pub frame: StreamChunk,
}

/// Test/dev transport that records every published frame in order.
#[derive(Default)]
pub struct InMemoryTransport {
    frames: Mutex<Vec<PublishedFrame>>,
}

impl InMemoryTransport {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn frames(&self) -> Vec<PublishedFrame> {
        self.frames.lock().await.clone()
    }
}

#[async_trait]
impl StreamTransport for InMemoryTransport {
    async fn publish(&self, channel: &str, frame: &StreamChunk) -> RiverbaseResult<String> {
        let mut guard = self.frames.lock().await;
        let txid = format!("mem-{}", guard.len());
        guard.push(PublishedFrame {
            channel: channel.to_string(),
            frame: frame.clone(),
        });
        Ok(txid)
    }
}
