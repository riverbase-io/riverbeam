//! Runtime-neutral events and their wire mapping (Python `kernel/contracts/events.py`,
//! `runtime/events.py`).

use std::future::Future;

use beam_types::{AgentMessage, AgentResult, AgentStep, StreamChunk, StreamChunkType, TokenUsage};
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use riverbase_core::RiverbaseResult;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::contract::EventStream;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeEventType {
    ExecutionStarted,
    StepStarted,
    StepCompleted,
    StepFailed,
    MessageDelta,
    MessageCompleted,
    ToolRequested,
    ToolStarted,
    ToolCompleted,
    ToolFailed,
    ActionRequired,
    ActionResolved,
    Usage,
    /// What one model call sends, by category (plan 023 A2): the payload
    /// holds `step` and `usage`.
    ContextUsage,
    ExecutionInterrupted,
    ExecutionCompleted,
    ExecutionFailed,
    ExecutionCancelled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeEvent {
    pub event_id: String,
    #[serde(default)]
    pub sequence: Option<i64>,
    #[serde(rename = "type")]
    pub event_type: RuntimeEventType,
    pub timestamp: DateTime<Utc>,
    pub execution_id: Uuid,
    pub run_id: Uuid,
    #[serde(default)]
    pub step: Option<AgentStep>,
    #[serde(default)]
    pub message: Option<AgentMessage>,
    #[serde(default)]
    pub action: Option<Value>,
    #[serde(default)]
    pub usage: Option<TokenUsage>,
    #[serde(default)]
    pub payload: Map<String, Value>,
}

impl RuntimeEvent {
    #[must_use]
    pub fn new(event_type: RuntimeEventType, execution_id: Uuid, run_id: Uuid) -> Self {
        Self {
            event_id: Uuid::new_v4().to_string(),
            sequence: None,
            event_type,
            timestamp: Utc::now(),
            execution_id,
            run_id,
            step: None,
            message: None,
            action: None,
            usage: None,
            payload: Map::new(),
        }
    }

    /// The terminal result carried by a completed or interrupted event.
    #[must_use]
    pub fn result(&self) -> Option<AgentResult> {
        self.payload
            .get("result")
            .and_then(|raw| serde_json::from_value(raw.clone()).ok())
    }

    /// Whether this event ends the run's stream.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.event_type,
            RuntimeEventType::ExecutionCompleted
                | RuntimeEventType::ExecutionInterrupted
                | RuntimeEventType::ExecutionFailed
                | RuntimeEventType::ExecutionCancelled
        )
    }
}

/// The wire chunk for an event.
/// Step, tool and usage events have none.
#[must_use]
pub fn runtime_event_to_stream_chunk(event: &RuntimeEvent) -> Option<StreamChunk> {
    let mut chunk = StreamChunk::error_frame(String::new());
    chunk.error = None;
    chunk.id = Some(event.event_id.clone());
    chunk.execution_id = Some(event.execution_id.to_string());
    chunk.run_id = Some(event.run_id.to_string());
    chunk.timestamp = Some(event.timestamp.to_rfc3339());
    match event.event_type {
        RuntimeEventType::ExecutionFailed => {
            chunk.chunk_type = StreamChunkType::Error;
            chunk.error = Some(
                event
                    .payload
                    .get("error")
                    .and_then(Value::as_str)
                    .unwrap_or("runtime failed")
                    .to_owned(),
            );
        }
        RuntimeEventType::ExecutionCompleted | RuntimeEventType::ExecutionInterrupted => {
            chunk.chunk_type = StreamChunkType::Done;
            chunk.result = event.result();
        }
        RuntimeEventType::MessageDelta
        | RuntimeEventType::MessageCompleted
        | RuntimeEventType::ActionRequired => {
            chunk.chunk_type = StreamChunkType::Chunk;
            chunk.data.clone_from(&event.message);
        }
        _ => return None,
    }
    Some(chunk)
}

/// Read a runtime stream to its end: the events in order and the terminal
/// result.
///
/// # Errors
/// See [`terminal_result`]; also the stream's own error.
pub async fn collect_events(
    mut stream: EventStream,
) -> RiverbaseResult<(Vec<RuntimeEvent>, AgentResult)> {
    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        let event = event?;
        let terminal = event.is_terminal();
        events.push(event);
        if terminal {
            break;
        }
    }
    let result = terminal_result(&events)?;
    Ok((events, result))
}

/// The result of a finished event sequence.
///
/// # Errors
/// `BEM-394` for an `execution_failed` or `execution_cancelled` event; `BEM-390`
/// (public `BEM-423`, Python `StreamProtocolError`) when the sequence has no
/// terminal event or the terminal event carries no result.
pub fn terminal_result(events: &[RuntimeEvent]) -> RiverbaseResult<AgentResult> {
    let Some(last) = events.iter().rev().find(|e| e.is_terminal()) else {
        return Err(crate::BEM_390.raise());
    };
    match last.event_type {
        RuntimeEventType::ExecutionFailed | RuntimeEventType::ExecutionCancelled => {
            let error = last
                .payload
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("runtime failed")
                .to_owned();
            Err(crate::BEM_394.with_data(error))
        }
        _ => last.result().ok_or_else(|| crate::BEM_390.raise()),
    }
}

/// Numbers and sends one run's events.
pub(crate) struct Emitter {
    tx: mpsc::UnboundedSender<RuntimeEvent>,
    execution_id: Uuid,
    run_id: Uuid,
    sequence: i64,
}

impl Emitter {
    pub(crate) fn event(&self, event_type: RuntimeEventType) -> RuntimeEvent {
        RuntimeEvent::new(event_type, self.execution_id, self.run_id)
    }

    pub(crate) fn emit(&mut self, mut event: RuntimeEvent) {
        self.sequence += 1;
        event.sequence = Some(self.sequence);
        // The receiver is gone only when the stream was dropped; the driver
        // future is dropped with it, so there is nothing to report.
        let _ = self.tx.send(event);
    }
}

/// Turn a driver future into an [`EventStream`]: events it emits are yielded as
/// they come, and its error, if any, is the stream's last item. Dropping the
/// stream drops the driver, which stops the run.
pub(crate) fn drive<F, Fut>(execution_id: Uuid, run_id: Uuid, driver: F) -> EventStream
where
    F: FnOnce(Emitter) -> Fut,
    Fut: Future<Output = RiverbaseResult<()>> + Send + 'static,
{
    let (tx, mut rx) = mpsc::unbounded_channel();
    let future = driver(Emitter {
        tx,
        execution_id,
        run_id,
        sequence: 0,
    });
    async_stream::stream! {
        let mut future = std::pin::pin!(future);
        let outcome = loop {
            tokio::select! {
                biased;
                Some(event) = rx.recv() => yield Ok(event),
                outcome = &mut future => break outcome,
            }
        };
        while let Ok(event) = rx.try_recv() {
            yield Ok(event);
        }
        if let Err(e) = outcome {
            yield Err(e);
        }
    }
    .boxed()
}

/// The result skeleton every runtime fills in: the invoke's identity, plus text.
pub(crate) fn agent_result(
    input: &beam_types::AgentInput,
    execution_id: Uuid,
    text: &str,
    usage: TokenUsage,
) -> AgentResult {
    AgentResult {
        content: vec![beam_types::ContentPart::text(text)],
        agent_name: input.agent_name.clone().unwrap_or_default(),
        model_used: None,
        session_id: Some(input.session_id),
        request_id: input.request_id,
        interaction_id: input.interaction_id,
        execution_id: Some(execution_id),
        token_usage: Some(usage),
        tool_calls: Vec::new(),
        metadata: Value::Object(Map::new()),
        latency_ms: None,
        interrupted: false,
        action_required: None,
        citations: Vec::new(),
    }
}

/// An AI text message for `input`'s run.
pub(crate) fn ai_message(
    input: &beam_types::AgentInput,
    text: &str,
    is_delta: bool,
) -> AgentMessage {
    AgentMessage {
        role: beam_types::AgentMessageRole::Ai,
        content: vec![beam_types::ContentPart::text(text)],
        run_id: Some(input.run_id),
        request_id: input.request_id,
        interaction_id: input.interaction_id,
        execution_id: input.execution_id,
        is_delta,
        ..AgentMessage::default()
    }
}

/// rig's usage as Beam's (saturating: Beam counts in `u32`).
pub(crate) fn token_usage(usage: &rig_core::completion::Usage) -> TokenUsage {
    let n = |v: Option<u64>| u32::try_from(v.unwrap_or(0)).unwrap_or(u32::MAX);
    let (prompt, completion) = (n(usage.input_tokens), n(usage.output_tokens));
    TokenUsage {
        prompt_tokens: prompt,
        completion_tokens: completion,
        total_tokens: usage
            .total_tokens
            .map_or(prompt.saturating_add(completion), |t| n(Some(t))),
        cache_read_tokens: n(usage.cached_input_tokens),
        cache_write_tokens: n(usage.cache_creation_input_tokens),
    }
}

/// A terminal event carrying `result`.
pub(crate) fn done_event(emitter: &Emitter, result: &AgentResult) -> RuntimeEvent {
    let kind = if result.interrupted {
        RuntimeEventType::ExecutionInterrupted
    } else {
        RuntimeEventType::ExecutionCompleted
    };
    let mut event = emitter.event(kind);
    event.usage.clone_from(&result.token_usage);
    event.payload.insert(
        "result".into(),
        serde_json::to_value(result).unwrap_or(Value::Null),
    );
    event
}

#[cfg(test)]
mod tests {
    use super::token_usage;

    /// Plan 023 A1: rig's cache counters reach Beam's usage.
    #[test]
    fn cache_tokens_are_kept() {
        let mut usage = rig_core::completion::Usage::default();
        usage.input_tokens = Some(120);
        usage.output_tokens = Some(30);
        usage.cached_input_tokens = Some(100);
        usage.cache_creation_input_tokens = Some(20);
        let used = token_usage(&usage);
        assert_eq!((used.prompt_tokens, used.total_tokens), (120, 150));
        assert_eq!((used.cache_read_tokens, used.cache_write_tokens), (100, 20));
    }
}
