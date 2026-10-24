//! Reconcile and client-tool expiries (Python
//! `orchestration/tasks/{reconcile,retention}.py`).

use super::outbox::observe_outbox;
use crate::river_beam::RiverBeam;
use riverbase_core::RiverbaseResult;
use serde_json::{json, Value};
use std::time::Duration;
use uuid::Uuid;

/// How long an expired client-tool action waits before it is offered again
/// when its resume did not finish it (Python rc8 `EXPIRY_RETRY`).
pub const CLIENT_TOOL_EXPIRY_RETRY: Duration = Duration::from_mins(5);

/// Metadata flag on a resume the expiry sweep made: when its actions are no
/// longer pending (a client answered first, or another sweep ran it), the
/// resume does nothing instead of failing the execution.
pub(in crate::river_beam) const CLIENT_TOOL_EXPIRY: &str = "beam_client_tool_expiry";

impl RiverBeam {
    /// `reconcile_now` under the reconcile lock:
    /// lease-expired runs retry, documents stuck `INDEXING` fail, abandoned
    /// reservations expire, and expired publisher leases are released (the
    /// relay republishes). Another reconciler holding the lock skips the pass.
    ///
    /// # Errors
    /// Store errors on the lock, the lease release or the snapshot.
    pub async fn reconcile_now(&self) -> RiverbaseResult<Value> {
        let Some(lock) = self.ops.try_reconcile_lock().await? else {
            return Ok(json!({"skipped": true, "reason": "another reconcile holds the lock"}));
        };
        let result = self.reconcile_locked().await;
        lock.release().await;
        result
    }

    pub(super) async fn reconcile_locked(&self) -> RiverbaseResult<Value> {
        let mut failed = Vec::new();
        let leases_expired = match self.executions.retry_expired_leases().await {
            Ok(moved) => moved.len(),
            Err(e) => {
                failed.push(json!({"kind": "lease", "error": e.errmesg}));
                0
            }
        };
        let mut indexing_failed = 0;
        if let Some(records) = self.knowledge.as_ref().and_then(|k| k.document_records()) {
            match records.fail_stuck_indexing(15).await {
                Ok(n) => indexing_failed = n,
                Err(e) => failed.push(json!({"kind": "indexing", "error": e.errmesg})),
            }
        }
        let mut reservations_expired = 0;
        if let Some(guard) = self.usage.as_ref().map(|u| &u.guard) {
            match guard.expire_abandoned().await {
                Ok(n) => reservations_expired = n,
                Err(e) => failed.push(json!({"kind": "reservation", "error": e.errmesg})),
            }
        }
        let released = self.ops.release_expired_leases().await?;
        let snapshot = self.ops.snapshot().await?;
        observe_outbox(&snapshot);
        Ok(json!({
            "unpublished": snapshot.pending + snapshot.dead,
            "published": 0,
            "publisher_leases_released": released,
            "retrying": leases_expired,
            "leases_expired": leases_expired,
            "indexing_failed": indexing_failed,
            "reservations_expired": reservations_expired,
            "failed": failed,
        }))
    }

    // --- client tools -----------------------------------------------------------

    /// Client-tool actions nobody answered by their `expires_at`, as resumes
    /// that expire them (Python rc8 `find_due_client_tools` and
    /// `expire_client_tools`): the run continues and the model hears that no
    /// client ran the tool.
    ///
    /// A rig run waits for an answer to every call it holds, so there is one
    /// resume per interaction, answering all of its pending client-tool
    /// actions. An interaction is left alone while one of its actions still
    /// waits for a human, or one of its client tools has not expired yet.
    /// Each action due, with the rest of its interaction's when a resume is
    /// built, is claimed for [`CLIENT_TOOL_EXPIRY_RETRY`]: no other
    /// sweep offers it meanwhile, and a resume that did not finish it is
    /// offered again after it.
    ///
    /// # Errors
    /// Store errors.
    pub async fn due_client_tool_expiries(
        &self,
        now: chrono::DateTime<chrono::Utc>,
        limit: i64,
    ) -> RiverbaseResult<Vec<beam_types::InvokeRequest>> {
        let retry = chrono::TimeDelta::from_std(CLIENT_TOOL_EXPIRY_RETRY)
            .unwrap_or_else(|_| chrono::TimeDelta::minutes(5));
        let claimed = self
            .store
            .claim_due_client_tool_actions(now, now + retry, limit)
            .await?;
        let mut seen = std::collections::HashSet::new();
        let mut requests = Vec::new();
        for interaction_id in claimed.iter().map(|a| a.interaction_id) {
            if !seen.insert(interaction_id) {
                continue;
            }
            let pending = self.store.list_pending_actions(interaction_id).await?;
            let waiting = pending.iter().any(|a| {
                a.action_type != beam_store::ActionType::ClientTool
                    || a.expires_at.is_none_or(|at| at > now)
            });
            if waiting || pending.is_empty() {
                continue;
            }
            // The resume answers every pending action, claimed by this sweep or
            // not (a claim's `limit` can split an interaction): hold them all
            // back, so no other sweep builds a second resume of it.
            let ids: Vec<Uuid> = pending.iter().map(|a| a.id).collect();
            self.store
                .defer_client_tool_actions(&ids, now + retry)
                .await?;
            let Some(interaction) = self.store.get_interaction(interaction_id).await? else {
                tracing::warn!(%interaction_id, "client tool actions have no interaction; not expired");
                continue;
            };
            let first = &pending[0];
            let session = self.store.get_session(first.session_id).await?;
            let mut metadata = serde_json::Map::new();
            metadata.insert(CLIENT_TOOL_EXPIRY.into(), Value::Bool(true));
            requests.push(beam_types::InvokeRequest {
                agent_name: interaction.agent_name,
                content: Vec::new(),
                session_id: first.session_id,
                model_override: None,
                metadata: Value::Object(metadata),
                interaction_id: Some(interaction_id),
                execution_id: first.execution_id,
                request_id: Some(Uuid::new_v4()),
                idempotency_key: None,
                stream: true,
                actions: Some(
                    pending
                        .iter()
                        .map(|action| beam_types::ActionResponseEnvelope {
                            action_id: action.id.to_string(),
                            lc_action_id: action.lc_action_id.clone(),
                            kind: beam_runtime::actions::CLIENT_TOOL_KIND.into(),
                            request_id: action
                                .payload
                                .pointer("/request/request_id")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_owned(),
                            status: beam_types::ActionResponseStatus::Expired,
                            payload: None,
                            errors: Vec::new(),
                            responder_id: None,
                            responded_at: now,
                        })
                        .collect(),
                ),
                auth_context: Some(beam_types::InvokeAuthContext {
                    profile_id: session.as_ref().and_then(|s| s.profile_id.clone()),
                    user_id: session.as_ref().and_then(|s| s.user_id.clone()),
                    org_id: session
                        .as_ref()
                        .and_then(|s| s.org_id.clone())
                        .or_else(|| first.org_id.clone()),
                    mcp_token: None,
                }),
                context: None,
                client: None,
            });
        }
        Ok(requests)
    }

    /// For a resume the expiry sweep made whose actions are no longer pending
    /// (a client answered first, or another sweep's resume ran): the empty
    /// result it ends with, without touching the execution.
    ///
    /// # Errors
    /// Store errors other than a missing action.
    pub(in crate::river_beam) async fn stale_client_tool_expiry(
        &self,
        req: &beam_types::InvokeRequest,
    ) -> RiverbaseResult<Option<beam_types::AgentResult>> {
        if req
            .metadata
            .get(CLIENT_TOOL_EXPIRY)
            .and_then(Value::as_bool)
            != Some(true)
        {
            return Ok(None);
        }
        for action in req.actions.iter().flatten() {
            let Ok(id) = Uuid::parse_str(&action.action_id) else {
                continue;
            };
            let pending = match self.store.get_action(id).await {
                Ok(row) => row.status == beam_store::ActionStatus::Pending,
                Err(e) if e.errcode.as_str() == crate::BEM_027.errcode => false,
                Err(e) => return Err(e),
            };
            if !pending {
                tracing::info!(action_id = %id, "client tool expiry skipped: the action was answered");
                return Ok(Some(beam_types::AgentResult {
                    content: Vec::new(),
                    agent_name: req.agent_name.clone(),
                    model_used: None,
                    session_id: Some(req.session_id),
                    request_id: req.request_id,
                    interaction_id: req.interaction_id,
                    execution_id: req.execution_id,
                    token_usage: None,
                    tool_calls: Vec::new(),
                    metadata: json!({"skipped": "client tool actions already answered"}),
                    latency_ms: None,
                    interrupted: false,
                    action_required: None,
                    citations: Vec::new(),
                }));
            }
        }
        Ok(None)
    }

    /// [`Self::due_client_tool_expiries`] as of now.
    ///
    /// # Errors
    /// Store errors.
    pub async fn client_tool_expiries_due_now(
        &self,
        limit: i64,
    ) -> RiverbaseResult<Vec<beam_types::InvokeRequest>> {
        self.due_client_tool_expiries(chrono::Utc::now(), limit)
            .await
    }

    // --- dead letters ----------------------------------------------------------
}
