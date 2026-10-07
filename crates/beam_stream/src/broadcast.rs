use async_trait::async_trait;
use beam_types::StreamChunk;
use tokio::sync::broadcast;

use riverbase_core::RiverbaseResult;
use crate::transport::{PublishedFrame, StreamTransport};

/// Fan-out transport backed by a Tokio broadcast channel.
///
/// SSE subscribers call [`BroadcastTransport::subscribe`] and filter received
/// frames by `channel` (`ws.channel.{session}:{request}`). Stands in for the
/// `RtcBridge` SSE bridge in the reference server.
pub struct BroadcastTransport {
    sender: broadcast::Sender<PublishedFrame>,
}

impl BroadcastTransport {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity.max(1));
        Self { sender }
    }

    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<PublishedFrame> {
        self.sender.subscribe()
    }
}

impl Default for BroadcastTransport {
    fn default() -> Self {
        Self::new(1024)
    }
}

#[async_trait]
impl StreamTransport for BroadcastTransport {
    async fn publish(&self, channel: &str, frame: &StreamChunk) -> RiverbaseResult<String> {
        let published = PublishedFrame {
            channel: channel.to_string(),
            frame: frame.clone(),
        };
        // Ok if there are currently no subscribers; frames are best-effort.
        let _ = self.sender.send(published);
        Ok(channel.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BeamStream;
    use beam_types::StreamChunkType;
    use std::sync::Arc;

    fn done_frame() -> StreamChunk {
        StreamChunk {
            chunk_type: StreamChunkType::Done,
            data: None,
            result: None,
            error: None,
            id: None,
            agent_id: None,
            agent_name: Some("demo".into()),
            session_id: Some("s".into()),
            interaction_id: None,
            run_id: Some("r".into()),
            step_id: None,
            message_id: None,
            timestamp: None,
        }
    }

    #[tokio::test]
    async fn subscriber_receives_published_frame() {
        let transport = Arc::new(BroadcastTransport::new(8));
        let mut rx = transport.subscribe();
        let stream = BeamStream::new(transport.clone());
        stream.publish_event("s", "r", done_frame()).await.unwrap();
        let received = rx.recv().await.unwrap();
        assert_eq!(received.channel, "ws.channel.s:r");
        assert_eq!(received.frame.chunk_type, StreamChunkType::Done);
    }
}
