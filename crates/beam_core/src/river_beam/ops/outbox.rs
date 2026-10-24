//! The outbox relay and dead letters (Python `store/ops/relay.py`).

use super::delivery::frame_error;
use crate::river_beam::RiverBeam;
use beam_audit::{AuditAction, AuditEvent};
use beam_store::ops::{NewDeadLetter, OutboxEvent, OUTBOX_CLAIM_BATCH, OUTBOX_MAX_ATTEMPTS};
use beam_types::AgentInput;
use riverbase_core::{RiverbaseError, RiverbaseResult};
use serde_json::{json, Value};
use std::time::Duration;
use uuid::Uuid;

/// What one relay pass did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RelayStats {
    pub claimed: usize,
    pub sent: usize,
    pub failed: usize,
}

/// Set the outbox gauges from a snapshot.
#[allow(clippy::cast_precision_loss)] // Gauges are f64; counts stay far below 2^52.
pub(super) fn observe_outbox(snapshot: &beam_store::ops::OutboxSnapshot) {
    let metrics = beam_telemetry::metrics();
    metrics.set_gauge("outbox_pending_count", snapshot.pending as f64);
    metrics.set_gauge(
        "outbox_oldest_pending_age_seconds",
        snapshot.oldest_age_seconds,
    );
    metrics.set_gauge("outbox_dead_letter_count", snapshot.dead as f64);
}

impl RiverBeam {
    /// The failed invoke as a dead
    /// letter, its request snapshot redacted (best effort).
    pub(in crate::river_beam) async fn dead_letter_failure(
        &self,
        input: &AgentInput,
        err: &RiverbaseError,
    ) {
        let snapshot = match input.execution_id {
            Some(id) => self
                .executions
                .records()
                .get(id)
                .await
                .ok()
                .flatten()
                .and_then(|row| row.metadata.get("invoke_request").cloned()),
            None => None,
        };
        let payload = beam_telemetry::redact_secrets(&json!({
            "execution_id": input.execution_id.map(|e| e.to_string()),
            "session_id": input.session_id.to_string(),
            "invoke_request": snapshot,
        }));
        let letter = NewDeadLetter {
            task_name: "invoke_agent".into(),
            payload,
            error: Some(crate::public_errors::stream_error_text(err.clone())),
            execution_id: input.execution_id,
            org_id: input
                .auth_context
                .as_ref()
                .and_then(|a| a.org_id.as_deref())
                .and_then(|o| Uuid::parse_str(o.trim()).ok()),
            error_code: None,
        };
        if let Err(e) = self.ops.enqueue_dead_letter(letter).await {
            tracing::error!(error = %e, "DLQ enqueue failed during invoke cleanup");
            beam_telemetry::metrics().increment("invoke_cleanup_failed", 1);
        }
    }

    // --- outbox relay -----------------------------------------------------------

    /// Claim due outbox rows, publish each outside the
    /// claim, then mark it sent or failed (at least once; subscribers dedupe
    /// by `sequence`).
    ///
    /// # Errors
    /// Store errors while claiming.
    pub async fn relay_once(&self, worker_id: &str) -> RiverbaseResult<RelayStats> {
        let claimed = self.ops.claim_batch(worker_id, OUTBOX_CLAIM_BATCH).await?;
        let metrics = beam_telemetry::metrics();
        let mut stats = RelayStats {
            claimed: claimed.len(),
            ..RelayStats::default()
        };
        for event in claimed {
            match self.publish_stored_event(&event).await {
                Ok(()) => {
                    self.ops.mark_sent(event.outbox_id).await?;
                    stats.sent += 1;
                    metrics.increment("outbox_publish_rate", 1);
                }
                Err(e) => {
                    stats.failed += 1;
                    metrics.increment("outbox_publish_failures", 1);
                    if event.attempts < OUTBOX_MAX_ATTEMPTS {
                        metrics.increment("outbox_retry_count", 1);
                    }
                    self.ops.mark_failed(event.outbox_id, &e.errmesg).await?;
                }
            }
        }
        observe_outbox(&self.ops.snapshot().await?);
        Ok(stats)
    }

    /// Route from the frame, else from the
    /// execution row.
    pub(super) async fn publish_stored_event(&self, event: &OutboxEvent) -> RiverbaseResult<()> {
        let text = |key: &str| {
            event
                .payload
                .get(key)
                .and_then(Value::as_str)
                .filter(|v| !v.is_empty())
                .map(str::to_owned)
        };
        let (mut session_id, mut request_id) = (text("session_id"), text("request_id"));
        if session_id.is_none() || request_id.is_none() {
            if let Some(row) = self.executions.records().get(event.execution_id).await? {
                session_id = session_id.or_else(|| Some(row.session_id.to_string()));
                request_id = request_id.or_else(|| {
                    row.metadata
                        .get("request_id")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                });
            }
        }
        let (Some(session_id), Some(request_id)) = (session_id, request_id) else {
            return Err(crate::BEM_138.with_data("outbox event has no stream route"));
        };
        let Some(stream) = &self.beam_stream else {
            return Err(crate::BEM_138.with_data("no stream transport is configured"));
        };
        let mut payload = event.payload.as_object().cloned().unwrap_or_default();
        payload.insert("id".into(), Value::String(event.id.to_string()));
        payload.insert("sequence".into(), Value::from(event.sequence));
        payload
            .entry("type")
            .or_insert_with(|| Value::String(event.event_type.clone()));
        payload
            .entry("execution_id")
            .or_insert_with(|| Value::String(event.execution_id.to_string()));
        stream
            .publish_value(&session_id, &request_id, &Value::Object(payload))
            .await?;
        Ok(())
    }

    /// Relay until `stop` turns true; a full batch
    /// is followed at once (0.2 s), an idle pass sleeps 1 s; sent and dead
    /// rows are purged every minute.
    pub async fn run_outbox_relay(
        &self,
        worker_id: &str,
        mut stop: tokio::sync::watch::Receiver<bool>,
    ) {
        const PURGE_EVERY: Duration = Duration::from_mins(1);
        let mut last_purge: Option<std::time::Instant> = None;
        while !*stop.borrow() {
            let claimed = match self.relay_once(worker_id).await {
                Ok(stats) => stats.claimed,
                Err(e) => {
                    tracing::error!(error = %e, "outbox relay failed");
                    0
                }
            };
            if last_purge.is_none_or(|t| t.elapsed() >= PURGE_EVERY) {
                if let Err(e) = self.ops.purge().await {
                    tracing::warn!(error = %e, "outbox purge failed");
                }
                last_purge = Some(std::time::Instant::now());
            }
            let delay = if claimed > 0 {
                Duration::from_millis(200)
            } else {
                Duration::from_secs(1)
            };
            tokio::select! {
                _ = stop.changed() => {}
                () = tokio::time::sleep(delay) => {}
            }
        }
    }

    // --- reconcile ----------------------------------------------------------------

    /// `list_dead_letters`: the pending ones.
    ///
    /// # Errors
    /// Store errors.
    pub async fn list_dead_letters(&self) -> RiverbaseResult<Value> {
        let letters = self.ops.list_dead_letters("pending").await?;
        Ok(json!({ "letters": letters }))
    }

    /// `retry_dead_letter`: count the attempt, audit `OPERATOR_RECOVERY`, and
    /// retry the letter's execution.
    ///
    /// # Errors
    /// `BEM-488` for an unknown letter; the retried execution's error.
    pub async fn retry_dead_letter(&self, letter_id: Uuid) -> RiverbaseResult<Value> {
        let letter = self.ops.retry_dead_letter(letter_id).await?;
        let mut event = AuditEvent::new(AuditAction::OperatorRecovery);
        event.metadata = json!({"task": "retry_dead_letter"});
        self.audit.log(event).await;
        let execution_id = letter
            .payload
            .get("execution_id")
            .and_then(Value::as_str)
            .and_then(|e| Uuid::parse_str(e).ok())
            .or(letter.execution_id);
        if let Some(execution_id) = execution_id {
            self.retry_execution(execution_id).await?;
        }
        serde_json::to_value(letter).map_err(frame_error)
    }

    /// `discard_dead_letter`.
    ///
    /// # Errors
    /// `BEM-488` for an unknown letter.
    pub async fn discard_dead_letter(&self, letter_id: Uuid) -> RiverbaseResult<Value> {
        let letter = self.ops.discard_dead_letter(letter_id).await?;
        serde_json::to_value(letter).map_err(frame_error)
    }

    // --- retention -------------------------------------------------------------
}
