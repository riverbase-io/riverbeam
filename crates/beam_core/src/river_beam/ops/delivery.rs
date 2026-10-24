//! Stream delivery: durable frames, lifecycle events, the held `done`, and
//! failures (Python `orchestration/invocation/dispatch.py`).

use crate::river_beam::{agent_id_for, RiverBeam};
use beam_store::ops::OutboxEvent;
use beam_stream::durable::{durable_event_type, durable_payload};
use beam_types::{AgentInput, StreamChunk, StreamChunkType};
use riverbase_core::{RiverbaseError, RiverbaseResult};
use serde_json::Value;
use std::sync::Mutex;
use uuid::Uuid;

/// The `done` frame, held until the
/// result is persisted.
#[derive(Default)]
pub(in crate::river_beam) struct PendingDone(Mutex<Option<Value>>);

impl PendingDone {
    pub(super) fn hold(&self, frame: Value) {
        if let Ok(mut slot) = self.0.lock() {
            *slot = Some(frame);
        }
    }

    pub(in crate::river_beam) fn take(&self) -> Option<Value> {
        self.0.lock().ok().and_then(|mut slot| slot.take())
    }
}

pub(super) fn frame_error(e: impl std::fmt::Display) -> RiverbaseError {
    crate::BEM_138.with_data(e.to_string())
}

impl RiverBeam {
    /// The stream route of an invoke.
    pub(super) fn route(input: &AgentInput) -> (String, String, Option<String>) {
        let user = input
            .user_id()
            .filter(|u| !u.is_empty())
            .unwrap_or_else(|| "anonymous".into());
        (
            user,
            input.session_id.to_string(),
            input.request_id.map(|r| r.to_string()),
        )
    }

    /// The route keys, unless the frame has them.
    pub(super) fn stamp_route(input: &AgentInput, payload: &mut Value) {
        let Value::Object(map) = payload else { return };
        let (user, session, request) = Self::route(input);
        map.entry("user_id").or_insert(Value::String(user));
        map.entry("session_id").or_insert(Value::String(session));
        map.entry("request_id")
            .or_insert(Value::String(request.unwrap_or_default()));
        if let Some(execution_id) = input.execution_id {
            map.entry("execution_id")
                .or_insert(Value::String(execution_id.to_string()));
        }
    }

    /// Record the frame (its sequence, the event
    /// row and an unsent outbox row), then publish it and mark it sent.
    pub(in crate::river_beam) async fn deliver_durable(
        &self,
        input: &AgentInput,
        mut payload: Value,
        event_type: &str,
    ) -> RiverbaseResult<()> {
        Self::stamp_route(input, &mut payload);
        let recorded = match input.execution_id {
            Some(execution_id) => {
                let org = input
                    .auth_context
                    .as_ref()
                    .and_then(|a| a.org_id.as_deref())
                    .and_then(|o| Uuid::parse_str(o.trim()).ok());
                let stored = durable_payload(&payload, beam_telemetry::privacy_mode());
                Some(
                    self.ops
                        .append_event(execution_id, org, &stored, event_type)
                        .await?,
                )
            }
            None => None,
        };
        self.deliver_lifecycle(input, payload, recorded.as_ref())
            .await
    }

    /// Publish now and mark the row sent; a failed
    /// publish stays pending for the relay. Not streaming, the row is sent.
    pub(super) async fn deliver_lifecycle(
        &self,
        input: &AgentInput,
        mut payload: Value,
        recorded: Option<&OutboxEvent>,
    ) -> RiverbaseResult<()> {
        if let (Some(event), Value::Object(map)) = (recorded, &mut payload) {
            map.insert("sequence".into(), Value::from(event.sequence));
            map.insert("id".into(), Value::String(event.id.to_string()));
        }
        if !input.stream {
            if let Some(event) = recorded {
                self.ops.mark_sent(event.outbox_id).await?;
            }
            return Ok(());
        }
        let Some(stream) = &self.beam_stream else {
            return Ok(());
        };
        // Python routes a request without an id to `{session}:`.
        let request = input.request_id.map(|r| r.to_string()).unwrap_or_default();
        // Publish and mark sent in one `try`; a
        // failure only logs, and the relay re-sends (subscribers dedupe).
        let delivered = async {
            stream
                .publish_value(&input.session_id.to_string(), &request, &payload)
                .await?;
            if let Some(event) = recorded {
                self.ops.mark_sent(event.outbox_id).await?;
            }
            RiverbaseResult::Ok(())
        };
        if let Err(e) = delivered.await {
            let kind = payload
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or_default();
            tracing::warn!(kind, error = %e, "lifecycle publish failed; the outbox will retry");
        }
        Ok(())
    }

    /// One frame of a run: `done` is held; lifecycle
    /// frames are durable; the rest is published live when streaming.
    pub(in crate::river_beam) async fn emit_frame(
        &self,
        input: &AgentInput,
        chunk: StreamChunk,
        pending_done: &PendingDone,
    ) -> RiverbaseResult<()> {
        let serialized = serde_json::to_value(&chunk).map_err(frame_error)?;
        if chunk.chunk_type == StreamChunkType::Done {
            pending_done.hold(serialized);
            return Ok(());
        }
        if let Some(kind) = durable_event_type(&serialized) {
            return self.deliver_durable(input, serialized, kind).await;
        }
        if input.stream {
            if let (Some(stream), Some(request_id)) = (&self.beam_stream, input.request_id) {
                stream
                    .publish_event(
                        &input.session_id.to_string(),
                        &request_id.to_string(),
                        chunk,
                    )
                    .await?;
            }
        }
        Ok(())
    }

    /// Flush buffered text deltas of this invoke's stream.
    pub(in crate::river_beam) async fn flush_stream(
        &self,
        input: &AgentInput,
    ) -> RiverbaseResult<()> {
        if let (true, Some(stream), Some(request_id)) =
            (input.stream, &self.beam_stream, input.request_id)
        {
            stream
                .flush(&input.session_id.to_string(), &request_id.to_string())
                .await?;
        }
        Ok(())
    }

    /// The held `done` frame, after persist.
    pub(in crate::river_beam) async fn publish_done(
        &self,
        input: &AgentInput,
        frame: Option<Value>,
    ) -> RiverbaseResult<()> {
        let Some(frame) = frame else { return Ok(()) };
        let kind = durable_event_type(&frame).unwrap_or("done");
        self.deliver_durable(input, frame, kind).await
    }

    /// One terminal `error` frame (best effort).
    pub(in crate::river_beam) async fn publish_failure(
        &self,
        input: &AgentInput,
        err: &RiverbaseError,
    ) {
        let mut frame =
            StreamChunk::error_frame(crate::public_errors::stream_error_text(err.clone()));
        frame.id = Some(Uuid::new_v4().to_string());
        frame.session_id = Some(input.session_id.to_string());
        frame.agent_name.clone_from(&input.agent_name);
        frame.agent_id = input
            .agent_name
            .as_deref()
            .map(|n| agent_id_for(n).to_string());
        frame.interaction_id = input.interaction_id.map(|i| i.to_string());
        frame.run_id = Some(input.run_id.to_string());
        frame.timestamp = Some(chrono::Utc::now().to_rfc3339());
        let result = match serde_json::to_value(&frame) {
            Ok(payload) => self.deliver_durable(input, payload, "error").await,
            Err(e) => Err(frame_error(e)),
        };
        if let Err(e) = result {
            tracing::warn!(error = %e, "stream error publish failed");
        }
    }
}
