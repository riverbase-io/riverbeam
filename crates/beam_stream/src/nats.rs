//! NATS-backed [`StreamTransport`] and the SSE bridge subscription helper.
//!
//! The worker process publishes stream frames to a per-channel NATS subject;
//! the API process subscribes to that subject and bridges frames to SSE. This
//! replaces the in-process [`crate::BroadcastTransport`] for distributed
//! deployments. Frames are serialized as the same [`StreamChunk`] JSON used on
//! the wire, so subscribers on either stack observe identical payloads.

use async_nats::Subscriber;
use async_trait::async_trait;
use beam_types::StreamChunk;
use riverbase_core::transport::NatsMessageBus;

use riverbase_core::RiverbaseResult;
use crate::transport::StreamTransport;

/// Default NATS subject prefix for beam stream frames.
pub const DEFAULT_STREAM_PREFIX: &str = "beam.stream";

/// Map a logical channel (`ws.channel.{session}:{request}`) to a NATS subject.
///
/// `:` is not a clean NATS token separator, so it is normalized to `.` and the
/// configured prefix is prepended (`beam.stream.ws.channel.{session}.{request}`).
#[must_use]
pub fn subject_for_channel(prefix: &str, channel: &str) -> String {
    let normalized = channel.replace([':'], ".");
    format!("{prefix}.{normalized}")
}

/// [`StreamTransport`] that publishes frames to NATS via [`NatsMessageBus`].
#[derive(Clone)]
pub struct NatsTransport {
    bus: NatsMessageBus,
    prefix: String,
}

impl NatsTransport {
    /// Build a transport over an existing [`NatsMessageBus`] with the default prefix.
    #[must_use]
    pub fn new(bus: NatsMessageBus) -> Self {
        Self::with_prefix(bus, DEFAULT_STREAM_PREFIX)
    }

    /// Build a transport with a custom subject prefix.
    #[must_use]
    pub fn with_prefix(bus: NatsMessageBus, prefix: impl Into<String>) -> Self {
        Self {
            bus,
            prefix: prefix.into(),
        }
    }

    /// Connect to a NATS server and build a transport with the default prefix.
    ///
    /// # Errors
    /// Returns a transport error if the connection fails.
    pub async fn connect(server_addr: &str) -> RiverbaseResult<Self> {
        let bus = NatsMessageBus::connect(server_addr)
            .await
            .map_err(|e| crate::BEM_101.with_data(e.to_string()))?;
        Ok(Self::new(bus))
    }

    /// The NATS subject this transport uses for a given logical channel.
    #[must_use]
    pub fn subject(&self, channel: &str) -> String {
        subject_for_channel(&self.prefix, channel)
    }

    /// Subscribe to a channel's frames (SSE bridge side).
    ///
    /// # Errors
    /// Returns a transport error if the subscription fails.
    pub async fn subscribe(&self, channel: &str) -> RiverbaseResult<Subscriber> {
        self.bus
            .subscribe(self.subject(channel))
            .await
            .map_err(|e| crate::BEM_102.with_data(e.to_string()))
    }
}

#[async_trait]
impl StreamTransport for NatsTransport {
    async fn publish(&self, channel: &str, frame: &StreamChunk) -> RiverbaseResult<String> {
        use riverbase_core::command::MessageBus;
        let subject = self.subject(channel);
        let payload =
            serde_json::to_value(frame).map_err(|e| crate::BEM_103.with_data(e.to_string()))?;
        self.bus
            .publish(&subject, payload)
            .await
            .map_err(|e| crate::BEM_104.with_data(e.to_string()))?;
        Ok(subject)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_maps_to_clean_subject() {
        let subject = subject_for_channel(DEFAULT_STREAM_PREFIX, "ws.channel.sess-1:req-2");
        assert_eq!(subject, "beam.stream.ws.channel.sess-1.req-2");
    }

    #[tokio::test]
    async fn roundtrip_over_local_nats() {
        use beam_types::StreamChunkType;
        use futures_util::StreamExt;

        let Ok(url) = std::env::var("NATS_URL") else {
            eprintln!("skipping: NATS_URL not set");
            return;
        };
        let transport = NatsTransport::connect(&url).await.expect("connect nats");
        let channel = format!("ws.channel.{}:{}", uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
        let mut sub = transport.subscribe(&channel).await.expect("subscribe");

        let frame = StreamChunk {
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
        };
        transport.publish(&channel, &frame).await.expect("publish");

        let msg = tokio::time::timeout(std::time::Duration::from_secs(2), sub.next())
            .await
            .expect("frame within timeout")
            .expect("subscription open");
        let received: StreamChunk = serde_json::from_slice(&msg.payload).expect("decode frame");
        assert_eq!(received.chunk_type, StreamChunkType::Done);
        assert_eq!(received.agent_name.as_deref(), Some("demo"));
    }
}
