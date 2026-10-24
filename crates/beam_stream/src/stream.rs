use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use beam_types::{AgentMessage, ContentPart, ContentPartType, StreamChunk, StreamChunkType};
use serde_json::Value;
use tokio::sync::Mutex;

use crate::transport::StreamTransport;
use riverbase_core::RiverbaseResult;

/// Stream publish tuning (Python's `BEAM_STREAM_PUBLISH_CHUNK_*` keys).
#[derive(Debug, Clone)]
pub struct StreamConfig {
    /// Buffer consecutive text deltas until this many estimated tokens accrue.
    /// `0` disables buffering (every frame publishes immediately).
    pub publish_chunk_token_threshold: u32,
    /// Flush a buffer this long after its first delta. `0` disables.
    pub publish_chunk_max_wait_ms: u64,
    /// How often the sweeper looks for buffers past `max_wait`.
    pub publish_chunk_sweep_interval_ms: u64,
    /// Hard cap on buffered characters before a forced flush. `0` disables.
    pub publish_chunk_max_chars: usize,
    /// `BEAM_STREAM_BUFFER_METRICS_ENABLED`: count flushes by reason.
    pub buffer_metrics_enabled: bool,
    /// `BEAM_STREAM_BUFFER_METRICS_LOG_INTERVAL_SECONDS`: log the counts this often.
    pub buffer_metrics_log_interval_secs: u64,
}

impl Default for StreamConfig {
    /// Python's defaults: buffering off; when on, 120 ms max wait, a 50 ms
    /// sweep and 12000 characters.
    fn default() -> Self {
        Self {
            publish_chunk_token_threshold: 0,
            publish_chunk_max_wait_ms: 120,
            publish_chunk_sweep_interval_ms: 50,
            publish_chunk_max_chars: 12_000,
            buffer_metrics_enabled: true,
            buffer_metrics_log_interval_secs: 60,
        }
    }
}

impl StreamConfig {
    /// The defaults with Python's environment keys applied.
    #[must_use]
    pub fn from_env() -> Self {
        let d = Self::default();
        Self {
            publish_chunk_token_threshold: beam_env::number(
                "BEAM_STREAM_PUBLISH_CHUNK_TOKEN_THRESHOLD",
            )
            .unwrap_or(d.publish_chunk_token_threshold),
            publish_chunk_max_wait_ms: beam_env::number("BEAM_STREAM_PUBLISH_CHUNK_MAX_WAIT_MS")
                .unwrap_or(d.publish_chunk_max_wait_ms),
            publish_chunk_sweep_interval_ms: beam_env::number(
                "BEAM_STREAM_PUBLISH_CHUNK_SWEEP_INTERVAL_MS",
            )
            .unwrap_or(d.publish_chunk_sweep_interval_ms),
            publish_chunk_max_chars: beam_env::number("BEAM_STREAM_PUBLISH_CHUNK_MAX_CHARS")
                .unwrap_or(d.publish_chunk_max_chars),
            buffer_metrics_enabled: beam_env::flag(
                "BEAM_STREAM_BUFFER_METRICS_ENABLED",
                d.buffer_metrics_enabled,
            ),
            buffer_metrics_log_interval_secs: beam_env::number(
                "BEAM_STREAM_BUFFER_METRICS_LOG_INTERVAL_SECONDS",
            )
            .unwrap_or(d.buffer_metrics_log_interval_secs),
        }
    }

    fn max_wait(&self) -> Option<Duration> {
        (self.publish_chunk_max_wait_ms > 0)
            .then(|| Duration::from_millis(self.publish_chunk_max_wait_ms))
    }
}

struct ChunkBuffer {
    frame: StreamChunk,
    text: String,
    token_count: u32,
    first_buffered_at: Instant,
}

/// Why a buffer was flushed (Python's metrics reasons).
#[derive(Debug, Clone, Copy)]
enum FlushReason {
    Token,
    Time,
    Size,
}

#[derive(Default)]
struct FlushCounts {
    token: u64,
    time: u64,
    size: u64,
    last_log: Option<Instant>,
}

/// Publishes agent stream frames, merging consecutive text deltas to reduce
/// transport pressure. A buffer flushes on the
/// token threshold, the character cap, `max_wait` (checked on publish and by
/// the sweeper, [`BeamStream::start_sweeper`]), or any other frame.
pub struct BeamStream {
    transport: Arc<dyn StreamTransport>,
    config: StreamConfig,
    buffers: Mutex<HashMap<String, ChunkBuffer>>,
    counts: std::sync::Mutex<FlushCounts>,
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
            counts: std::sync::Mutex::new(FlushCounts::default()),
        }
    }

    /// A shared stream with its sweeper running when buffering is on and a
    /// tokio runtime is present.
    #[must_use]
    pub fn shared(transport: Arc<dyn StreamTransport>, config: StreamConfig) -> Arc<Self> {
        let stream = Arc::new(Self::with_config(transport, config));
        if tokio::runtime::Handle::try_current().is_ok() {
            stream.start_sweeper();
        }
        stream
    }

    #[must_use]
    pub fn config(&self) -> &StreamConfig {
        &self.config
    }

    /// Flush buffers past `max_wait` every sweep interval (Python's
    /// `_buffer_sweeper_loop`). The task ends when the stream is dropped.
    /// No-op when buffering or `max_wait` is off.
    pub fn start_sweeper(self: &Arc<Self>) -> Option<tokio::task::JoinHandle<()>> {
        if self.config.publish_chunk_token_threshold == 0 || self.config.max_wait().is_none() {
            return None;
        }
        let interval = Duration::from_millis(match self.config.publish_chunk_sweep_interval_ms {
            0 => 50,
            ms => ms.max(10),
        });
        let weak: Weak<Self> = Arc::downgrade(self);
        Some(tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let Some(stream) = weak.upgrade() else { return };
                if let Err(e) = stream.flush_due().await {
                    tracing::warn!(error = %e, "stream buffer sweeper error");
                }
            }
        }))
    }

    #[must_use]
    pub fn stream_key(session_id: &str, request_id: &str) -> String {
        format!("{session_id}:{request_id}")
    }

    fn channel(stream_key: &str) -> String {
        format!("ws.channel.{stream_key}")
    }

    /// Publish a JSON frame (a durable payload). Lifecycle frames whose type
    /// is not a `StreamChunk` type (Python's `started`, `cancelled`, ...) are
    /// not published: Python's `publish_event` rejects them too. Returns
    /// whether it was published.
    ///
    /// # Errors
    /// `BEM-103` for a `chunk`, `done` or `error` frame that does not parse
    /// (so the outbox retries and counts it); transport errors.
    pub async fn publish_value(
        &self,
        session_id: &str,
        request_id: &str,
        frame: &Value,
    ) -> RiverbaseResult<bool> {
        let kind = frame
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if !matches!(kind, "chunk" | "done" | "error") {
            return Ok(false);
        }
        let chunk = serde_json::from_value::<StreamChunk>(frame.clone())
            .map_err(|e| crate::BEM_103.with_data(format!("{kind} frame: {e}")))?;
        self.publish_event(session_id, request_id, chunk).await?;
        Ok(true)
    }

    /// Append one frame to the stream for `session_id:request_id`.
    ///
    /// # Errors
    /// Transport errors.
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

        let now = Instant::now();
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

        let buffer = buffers
            .entry(stream_key.clone())
            .or_insert_with(|| ChunkBuffer {
                frame: frame.clone(),
                text: String::new(),
                token_count: 0,
                first_buffered_at: now,
            });
        buffer.text.push_str(&content);
        buffer.token_count += est_tokens;

        let hit_token = buffer.token_count >= self.config.publish_chunk_token_threshold;
        let hit_chars = self.config.publish_chunk_max_chars > 0
            && buffer.text.chars().count() >= self.config.publish_chunk_max_chars;
        let hit_time = self
            .config
            .max_wait()
            .is_some_and(|wait| now.duration_since(buffer.first_buffered_at) >= wait);

        if hit_token || hit_chars || hit_time {
            let reason = if hit_token {
                FlushReason::Token
            } else if hit_chars {
                FlushReason::Size
            } else {
                FlushReason::Time
            };
            if let Some(flushed) = take_flush(&mut buffers, &stream_key) {
                drop(buffers);
                let id = self.publish_frame(&stream_key, &flushed).await?;
                self.record_flush(reason);
                return Ok(id);
            }
        }
        Ok("buffered".to_string())
    }

    /// Flush every buffer older than `max_wait` (the sweeper's step).
    ///
    /// # Errors
    /// Transport errors.
    pub async fn flush_due(&self) -> RiverbaseResult<usize> {
        let Some(wait) = self.config.max_wait() else {
            return Ok(0);
        };
        let now = Instant::now();
        // Publish under the buffer lock, as Python holds the stream lock: a
        // frame published meanwhile cannot overtake the older buffered text.
        let mut buffers = self.buffers.lock().await;
        let keys: Vec<String> = buffers
            .iter()
            .filter(|(_, b)| now.duration_since(b.first_buffered_at) >= wait)
            .map(|(k, _)| k.clone())
            .collect();
        let mut flushed = 0;
        for key in keys {
            if let Some(frame) = take_flush(&mut buffers, &key) {
                self.publish_frame(&key, &frame).await?;
                self.record_flush(FlushReason::Time);
                flushed += 1;
            }
        }
        Ok(flushed)
    }

    /// Flush any buffered text for this stream (call before closing a request).
    ///
    /// # Errors
    /// Transport errors.
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

    /// Count, and log the counts every
    /// interval.
    fn record_flush(&self, reason: FlushReason) {
        if !self.config.buffer_metrics_enabled {
            return;
        }
        let Ok(mut counts) = self.counts.lock() else {
            return;
        };
        match reason {
            FlushReason::Token => counts.token += 1,
            FlushReason::Time => counts.time += 1,
            FlushReason::Size => counts.size += 1,
        }
        let interval = Duration::from_secs(self.config.buffer_metrics_log_interval_secs);
        let now = Instant::now();
        let last = *counts.last_log.get_or_insert(now);
        if interval.is_zero() || now.duration_since(last) < interval {
            return;
        }
        tracing::info!(
            token = counts.token,
            time = counts.time,
            size = counts.size,
            "stream publish buffer flush metrics"
        );
        *counts = FlushCounts {
            last_log: Some(now),
            ..FlushCounts::default()
        };
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

/// About 4 characters per token.
fn estimate_tokens(text: &str) -> u32 {
    if text.is_empty() {
        return 0;
    }
    u32::try_from(text.chars().count().div_ceil(4))
        .unwrap_or(u32::MAX)
        .max(1)
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
    if buffered.chunk_type != StreamChunkType::Chunk
        || incoming.chunk_type != StreamChunkType::Chunk
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

    /// Plan 021 D2: a blank flag is unset, so metrics stay on (before, a
    /// blank value turned them off). The only test here that reads the env.
    #[test]
    fn a_blank_metrics_flag_keeps_the_default() {
        std::env::set_var("BEAM_STREAM_BUFFER_METRICS_ENABLED", " ");
        let enabled = StreamConfig::from_env().buffer_metrics_enabled;
        std::env::remove_var("BEAM_STREAM_BUFFER_METRICS_ENABLED");
        assert!(enabled);
    }

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
            execution_id: None,
            sequence: None,
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
            execution_id: None,
            sequence: None,
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
                publish_chunk_max_wait_ms: 0,
                ..StreamConfig::default()
            },
        );
        stream
            .publish_event("s", "r", text_chunk("m1", "foo"))
            .await
            .unwrap();
        stream
            .publish_event("s", "r", text_chunk("m1", "bar"))
            .await
            .unwrap();
        assert!(transport.frames().await.is_empty());

        // Terminal frame flushes the merged buffer first, then publishes itself.
        stream.publish_event("s", "r", done_chunk()).await.unwrap();
        let frames = transport.frames().await;
        assert_eq!(frames.len(), 2);
        let merged = frames[0].frame.data.as_ref().unwrap();
        assert_eq!(message_text(merged), "foobar");
        assert_eq!(frames[1].frame.chunk_type, StreamChunkType::Done);
    }

    fn buffering(max_wait_ms: u64, max_chars: usize) -> StreamConfig {
        StreamConfig {
            publish_chunk_token_threshold: 100,
            publish_chunk_max_wait_ms: max_wait_ms,
            publish_chunk_max_chars: max_chars,
            ..StreamConfig::default()
        }
    }

    /// Python's time rule: a buffer older than `max_wait` flushes on the next
    /// publish, and the sweeper flushes it without one.
    #[tokio::test]
    async fn buffers_flush_after_max_wait() {
        let transport = Arc::new(InMemoryTransport::new());
        let stream = BeamStream::shared(transport.clone(), buffering(20, 0));
        stream
            .publish_event("s", "r", text_chunk("m1", "foo"))
            .await
            .unwrap();
        assert!(transport.frames().await.is_empty());
        tokio::time::sleep(Duration::from_millis(150)).await;
        let frames = transport.frames().await;
        assert_eq!(frames.len(), 1, "the sweeper flushed the buffer");
        assert_eq!(message_text(frames[0].frame.data.as_ref().unwrap()), "foo");

        let quiet = Arc::new(InMemoryTransport::new());
        let stream = BeamStream::with_config(quiet.clone(), buffering(20, 0));
        stream
            .publish_event("s", "r", text_chunk("m1", "a"))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        stream
            .publish_event("s", "r", text_chunk("m1", "b"))
            .await
            .unwrap();
        let frames = quiet.frames().await;
        assert_eq!(frames.len(), 1, "an old buffer flushes on publish");
        assert_eq!(message_text(frames[0].frame.data.as_ref().unwrap()), "ab");
    }

    #[tokio::test]
    async fn the_character_cap_counts_characters() {
        let transport = Arc::new(InMemoryTransport::new());
        let stream = BeamStream::with_config(transport.clone(), buffering(0, 4));
        stream
            .publish_event("s", "r", text_chunk("m1", "éé"))
            .await
            .unwrap();
        assert!(
            transport.frames().await.is_empty(),
            "2 characters (4 bytes) stay buffered"
        );
        stream
            .publish_event("s", "r", text_chunk("m1", "éé"))
            .await
            .unwrap();
        assert_eq!(transport.frames().await.len(), 1);
    }

    #[tokio::test]
    async fn only_stream_chunks_are_published_as_json() {
        let transport = Arc::new(InMemoryTransport::new());
        let stream = BeamStream::new(transport.clone());
        let started = serde_json::json!({"type": "started", "execution_id": "e"});
        assert!(!stream.publish_value("s", "r", &started).await.unwrap());
        // A redacted tool result is not a message: an error, not a silent skip.
        let redacted = serde_json::json!({"type": "chunk", "data": "[redacted]"});
        assert_eq!(
            stream
                .publish_value("s", "r", &redacted)
                .await
                .unwrap_err()
                .errcode
                .as_str(),
            "BEM-103"
        );
        let done = serde_json::json!({"type": "done", "sequence": 2, "id": "x"});
        assert!(stream.publish_value("s", "r", &done).await.unwrap());
        let frames = transport.frames().await;
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].frame.sequence, Some(2));
    }

    #[tokio::test]
    async fn flush_emits_buffered_text() {
        let transport = Arc::new(InMemoryTransport::new());
        let stream = BeamStream::with_config(
            transport.clone(),
            StreamConfig {
                publish_chunk_token_threshold: 100,
                publish_chunk_max_chars: 0,
                publish_chunk_max_wait_ms: 0,
                ..StreamConfig::default()
            },
        );
        stream
            .publish_event("s", "r", text_chunk("m1", "partial"))
            .await
            .unwrap();
        stream.flush("s", "r").await.unwrap();
        let frames = transport.frames().await;
        assert_eq!(frames.len(), 1);
        assert_eq!(
            message_text(frames[0].frame.data.as_ref().unwrap()),
            "partial"
        );
    }
}
