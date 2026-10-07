use std::collections::HashMap;
use std::sync::Arc;

use beam_types::{AgentMessage, ContentPart, ContentPartType, StreamChunk, StreamChunkType};
use tokio::sync::Mutex;

use riverbase_core::RiverbaseResult;
use crate::transport::StreamTransport;

/// Stream publish tuning (ports the `BEAM_STREAM_PUBLISH_CHUNK_*` config knobs).
#[derive(Debug, Clone)]
pub struct StreamConfig {
    /// Buffer consecutive text deltas until this many estimated tokens accrue.
    /// `0` disables buffering (every frame publishes immediately).
    pub publish_chunk_token_threshold: u32,
    /// Hard cap on buffered characters before a forced flush. `0` disables.
    pub publish_chunk_max_chars: usize,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            publish_chunk_token_threshold: 0,
            publish_chunk_max_chars: 0,
        }
    }
}

struct ChunkBuffer {
    frame: StreamChunk,
    text: String,
    token_count: u32,
}

/// Publishes agent stream frames, merging consecutive text deltas to reduce
/// transport pressure (faithful to the Python `RiverbeamStream` buffer logic,
/// minus the time-based sweeper which is unnecessary for the synchronous path).
pub struct BeamStream {
    transport: Arc<dyn StreamTransport>,
    config: StreamConfig,
    buffers: Mutex<HashMap<String, ChunkBuffer>>,
}

impl BeamStream {
    #[must_use]
    pub fn new(transport: Arc<dyn StreamTransport>) -> Self {
        Self::with_config(transport, StreamConfig::default())
    }

    #[must_use]
    pub fn with_config(transport: Arc<dyn StreamTransport>, config: StreamConfig) -> Self {
        Self {
            transport,
            config,
            buffers: Mutex::new(HashMap::new()),
        }
    }

    #[must_use]
    pub fn stream_key(session_id: &str, request_id: &str) -> String {
        format!("{session_id}:{request_id}")
    }

    fn channel(stream_key: &str) -> String {
        format!("ws.channel.{stream_key}")
    }

    /// Append one frame to the stream for `session_id:request_id`.
    pub async fn publish_event(
        &self,
        session_id: &str,
        request_id: &str,
        frame: StreamChunk,
    ) -> RiverbaseResult<String> {
        let stream_key = Self::stream_key(session_id, request_id);

        if self.config.publish_chunk_token_threshold == 0 {
            return self.publish_frame(&stream_key, &frame).await;
        }

        let mut buffers = self.buffers.lock().await;

        // Flush before any terminal/non-text frame to preserve ordering.
        let merge_text = chunk_text(&frame);
        if frame.chunk_type != StreamChunkType::Chunk || merge_text.is_none() {
            if let Some(flushed) = take_flush(&mut buffers, &stream_key) {
                self.publish_frame(&stream_key, &flushed).await?;
            }
            return self.publish_frame(&stream_key, &frame).await;
        }

        let content = merge_text.unwrap_or_default();
        let est_tokens = estimate_tokens(&content);

        let can_merge = buffers
            .get(&stream_key)
            .is_some_and(|b| can_merge(&b.frame, &frame));
        if !can_merge {
            if let Some(flushed) = take_flush(&mut buffers, &stream_key) {
                self.publish_frame(&stream_key, &flushed).await?;
            }
        }

        let buffer = buffers.entry(stream_key.clone()).or_insert_with(|| ChunkBuffer {
            frame: frame.clone(),
            text: String::new(),
            token_count: 0,
        });
        buffer.text.push_str(&content);
        buffer.token_count += est_tokens;

        let hit_token = buffer.token_count >= self.config.publish_chunk_token_threshold;
        let hit_chars =
            self.config.publish_chunk_max_chars > 0 && buffer.text.len() >= self.config.publish_chunk_max_chars;

        if hit_token || hit_chars {
            if let Some(flushed) = take_flush(&mut buffers, &stream_key) {
                return self.publish_frame(&stream_key, &flushed).await;
            }
        }
        Ok("buffered".to_string())
    }

    /// Flush any buffered text for this stream (call before closing a request).
    pub async fn flush(&self, session_id: &str, request_id: &str) -> RiverbaseResult<()> {
        let stream_key = Self::stream_key(session_id, request_id);
        let flushed = {
            let mut buffers = self.buffers.lock().await;
            take_flush(&mut buffers, &stream_key)
        };
        if let Some(frame) = flushed {
            self.publish_frame(&stream_key, &frame).await?;
        }
        Ok(())
    }

    async fn publish_frame(
        &self,
        stream_key: &str,
        frame: &StreamChunk,
    ) -> RiverbaseResult<String> {
        let channel = Self::channel(stream_key);
        self.transport.publish(&channel, frame).await
    }
}

fn take_flush(buffers: &mut HashMap<String, ChunkBuffer>, stream_key: &str) -> Option<StreamChunk> {
    let buffer = buffers.remove(stream_key)?;
    let mut frame = buffer.frame;
    if let Some(data) = frame.data.as_mut() {
        set_message_text(data, &buffer.text);
    }
    Some(frame)
}

fn estimate_tokens(text: &str) -> u32 {
    if text.is_empty() {
        return 0;
    }
    ((text.len() + 3) / 4).max(1) as u32
}

fn chunk_text(frame: &StreamChunk) -> Option<String> {
    let data = frame.data.as_ref()?;
    let text = message_text(data);
    if text.is_empty() {
        None
    } else {
        Some(text)
    }
}

fn message_text(message: &AgentMessage) -> String {
    message
        .content
        .iter()
        .filter_map(|p| p.text.as_deref())
        .collect::<Vec<_>>()
        .join("")
}

fn set_message_text(message: &mut AgentMessage, text: &str) {
    message.content = vec![ContentPart {
        part_type: ContentPartType::Text,
        text: Some(text.to_string()),
        url: None,
        data: None,
        mime_type: None,
        metadata: serde_json::Value::Object(serde_json::Map::default()),
        media_id: None,
    }];
}

fn can_merge(buffered: &StreamChunk, incoming: &StreamChunk) -> bool {
    if buffered.chunk_type != StreamChunkType::Chunk || incoming.chunk_type != StreamChunkType::Chunk
    {
        return false;
    }
    if buffered.message_id != incoming.message_id {
        return false;
    }
    if buffered.agent_name != incoming.agent_name {
        return false;
    }
    if buffered.run_id != incoming.run_id {
        return false;
    }
    match (buffered.data.as_ref(), incoming.data.as_ref()) {
        (Some(a), Some(b)) => a.id == b.id && a.name == b.name && a.tool_call_id == b.tool_call_id,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::InMemoryTransport;
    use beam_types::{AgentMessageRole, AgentMessageType};
    use uuid::Uuid;

    fn text_chunk(message_id: &str, text: &str) -> StreamChunk {
        // Deltas of one assistant message share a stable AgentMessage id.
        let data_id = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        StreamChunk {
            chunk_type: StreamChunkType::Chunk,
            data: Some(AgentMessage {
                role: AgentMessageRole::Ai,
                msg_type: AgentMessageType::Message,
                content: vec![ContentPart::text(text)],
                id: data_id,
                ..Default::default()
            }),
            result: None,
            error: None,
            id: None,
            agent_id: None,
            agent_name: Some("demo".into()),
            session_id: Some("s".into()),
            interaction_id: None,
            run_id: Some("r".into()),
            step_id: None,
            message_id: Some(message_id.into()),
            timestamp: None,
        }
    }

    fn done_chunk() -> StreamChunk {
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
    async fn publishes_immediately_when_buffering_disabled() {
        let transport = Arc::new(InMemoryTransport::new());
        let stream = BeamStream::new(transport.clone());
        stream
            .publish_event("s", "r", text_chunk("m1", "hello"))
            .await
            .unwrap();
        let frames = transport.frames().await;
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].channel, "ws.channel.s:r");
    }

    #[tokio::test]
    async fn merges_text_deltas_until_threshold() {
        let transport = Arc::new(InMemoryTransport::new());
        let stream = BeamStream::with_config(
            transport.clone(),
            StreamConfig {
                publish_chunk_token_threshold: 100,
                publish_chunk_max_chars: 0,
            },
        );
        stream.publish_event("s", "r", text_chunk("m1", "foo")).await.unwrap();
        stream.publish_event("s", "r", text_chunk("m1", "bar")).await.unwrap();
        assert!(transport.frames().await.is_empty());

        // Terminal frame flushes the merged buffer first, then publishes itself.
        stream.publish_event("s", "r", done_chunk()).await.unwrap();
        let frames = transport.frames().await;
        assert_eq!(frames.len(), 2);
        let merged = frames[0].frame.data.as_ref().unwrap();
        assert_eq!(message_text(merged), "foobar");
        assert_eq!(frames[1].frame.chunk_type, StreamChunkType::Done);
    }

    #[tokio::test]
    async fn flush_emits_buffered_text() {
        let transport = Arc::new(InMemoryTransport::new());
        let stream = BeamStream::with_config(
            transport.clone(),
            StreamConfig {
                publish_chunk_token_threshold: 100,
                publish_chunk_max_chars: 0,
            },
        );
        stream.publish_event("s", "r", text_chunk("m1", "partial")).await.unwrap();
        stream.flush("s", "r").await.unwrap();
        let frames = transport.frames().await;
        assert_eq!(frames.len(), 1);
        assert_eq!(message_text(frames[0].frame.data.as_ref().unwrap()), "partial");
    }
}
