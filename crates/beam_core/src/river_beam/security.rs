//! Security checks around an invoke (input, runtime, payloads) and their audit.

use beam_audit::{AuditAction, AuditEvent};
use beam_security::{SecurityContext, SecurityDecision, SecurityRequest, SecurityStage};
use beam_types::AgentInput;
use serde_json::Value;

use riverbase_core::RiverbaseResult;

use super::{narrowed_max_steps, python_action_response, RiverBeam};

impl RiverBeam {
    /// Python `enforce_input_policy`.
    pub(super) async fn enforce_input_policy(&self, input: &AgentInput) -> RiverbaseResult<()> {
        let ids = |payload: &mut serde_json::Map<String, Value>| {
            payload.insert(
                "interaction_id".into(),
                input
                    .interaction_id
                    .map_or(Value::Null, |i| Value::String(i.to_string())),
            );
            payload.insert("run_id".into(), Value::String(input.run_id.to_string()));
        };
        if let Some(actions) = input.actions.as_ref().filter(|a| !a.is_empty()) {
            let mut payload = serde_json::Map::new();
            ids(&mut payload);
            payload.insert(
                "actions".into(),
                Value::Array(actions.iter().map(python_action_response).collect()),
            );
            self.check_security_payload(SecurityStage::BeforeHitlResume, input, payload)
                .await?;
        }
        for text in input
            .content
            .iter()
            .filter_map(|p| p.text.as_deref())
            .filter(|t| !t.is_empty())
        {
            let mut payload = serde_json::Map::new();
            payload.insert("content".into(), Value::String(text.to_owned()));
            ids(&mut payload);
            self.check_security_payload(SecurityStage::BeforeInput, input, payload)
                .await?;
        }
        Ok(())
    }

    /// More steps than the policy allows is
    /// refused (`BEM-070`), then the `RUNTIME` stage runs.
    pub(super) async fn enforce_runtime_policy(
        &self,
        input: &AgentInput,
        step_count: usize,
        token_count: u64,
    ) -> RiverbaseResult<()> {
        // The pinned revision's `execution.max_steps` narrows the policy's.
        let limit = narrowed_max_steps(self.pinned_spec(input).as_ref(), self.security.max_steps());
        if u64::try_from(step_count).unwrap_or(u64::MAX) > limit {
            return Err(
                crate::BEM_070.with_data(format!("max steps exceeded: {step_count} > {limit}"))
            );
        }
        let mut payload = serde_json::Map::new();
        payload.insert("run_id".into(), Value::String(input.run_id.to_string()));
        payload.insert("step_count".into(), Value::from(step_count));
        payload.insert("elapsed_seconds".into(), Value::from(0.0));
        payload.insert("token_count".into(), Value::from(token_count));
        self.check_security_payload(SecurityStage::Runtime, input, payload)
            .await
    }

    /// Run the security engine for `stage` over `content`. Returns an error when
    /// a deny/terminate/quarantine verdict is enforced.
    pub(super) async fn check_security(
        &self,
        stage: SecurityStage,
        input: &AgentInput,
        content: &str,
    ) -> RiverbaseResult<()> {
        let mut payload = serde_json::Map::new();
        payload.insert("content".into(), Value::String(content.to_string()));
        if let Some(interaction_id) = input.interaction_id {
            payload.insert(
                "interaction_id".into(),
                Value::String(interaction_id.to_string()),
            );
        }
        payload.insert("run_id".into(), Value::String(input.run_id.to_string()));
        self.check_security_payload(stage, input, payload).await
    }

    /// [`Self::check_security`] with a prepared payload.
    pub(super) async fn check_security_payload(
        &self,
        stage: SecurityStage,
        input: &AgentInput,
        payload: serde_json::Map<String, Value>,
    ) -> RiverbaseResult<()> {
        let request = SecurityRequest::new(stage)
            .with_payload(payload)
            .with_context(SecurityContext {
                agent_id: input.agent_name.clone(),
                tenant_id: beam_execution::identity::normalize_identity(
                    input
                        .auth_context
                        .as_ref()
                        .and_then(|a| a.org_id.as_deref()),
                ),
                user_id: input.user_id(),
                interaction_id: input.interaction_id.map(|id| id.to_string()),
                session_id: Some(input.session_id.to_string()),
            });

        let decision = self.security.check(&request).await;
        self.audit_security(stage, input, &decision).await;

        // Python's handlers block only deny, terminate and quarantine;
        // `require_approval` is recorded for the HITL layer, not raised.
        if decision.denies() && self.security.enforce() {
            let message = decision
                .user_message
                .clone()
                .unwrap_or_else(|| "request blocked by security policy".to_string());
            return Err(crate::BEM_061.with_data(message));
        }
        Ok(())
    }

    pub(super) async fn audit_security(
        &self,
        stage: SecurityStage,
        input: &AgentInput,
        decision: &SecurityDecision,
    ) {
        // Python `_audit_status`.
        use beam_security::SecurityAction as A;
        let status = if decision.denies() {
            "blocked"
        } else if matches!(
            decision.action,
            A::Warn | A::Sanitize | A::Mask | A::RequireApproval
        ) {
            "flagged"
        } else {
            "ok"
        };
        let mut event = AuditEvent::new(AuditAction::SecurityEvent)
            .agent_name(input.agent_name.clone())
            .user_id(input.user_id())
            .session_id(Some(input.session_id))
            .run_id(Some(input.run_id))
            .status(status);
        if decision.denies() {
            event = event.error(decision.user_message.clone());
        }
        // Python `_build_audit_event` metadata (enum values, not debug names).
        let value = |v: Value| {
            v.as_str()
                .map_or(Value::Null, |s| Value::String(s.to_owned()))
        };
        event.metadata = serde_json::json!({
            "stage": value(serde_json::to_value(stage).unwrap_or(Value::Null)),
            "outcome_action": value(serde_json::to_value(decision.action).unwrap_or(Value::Null)),
            "score": decision.score,
            "severity": value(serde_json::to_value(decision.severity).unwrap_or(Value::Null)),
            "violation_type": decision.violation_type.map_or(Value::Null, |v| value(serde_json::to_value(v).unwrap_or(Value::Null))),
            "matched_rules": decision.matched_rules,
            "narration": decision.narration,
            "matched_patterns": decision.metadata.get("matched_patterns").cloned().unwrap_or_else(|| Value::Array(Vec::new())),
        });
        event.trace_id = beam_telemetry::current_span().map(|s| s.trace_id);
        self.audit.log(event).await;
    }

    /// `BEFORE_CONTEXT_INJECTION` once per
    /// selected untrusted item, with its source and provenance.
    pub(super) async fn check_selected_untrusted(
        &self,
        items: &[beam_context::ContextItem],
        input: &AgentInput,
    ) -> RiverbaseResult<()> {
        let mut seen = std::collections::HashSet::new();
        for item in items {
            if !item.id.is_empty() && !seen.insert(item.id.as_str()) {
                continue;
            }
            if item.content.is_empty() {
                continue;
            }
            let mut payload = serde_json::Map::new();
            payload.insert("content".into(), Value::String(item.content.clone()));
            payload.insert("source".into(), Value::String(item.source.clone()));
            payload.insert(
                "provenance".into(),
                item.provenance.clone().map_or(Value::Null, Value::String),
            );
            payload.insert(
                "item_id".into(),
                if item.id.is_empty() {
                    Value::Null
                } else {
                    Value::String(item.id.clone())
                },
            );
            payload.insert("run_id".into(), Value::String(input.run_id.to_string()));
            payload.insert(
                "interaction_id".into(),
                input
                    .interaction_id
                    .map_or(Value::Null, |i| Value::String(i.to_string())),
            );
            self.check_security_payload(SecurityStage::BeforeContextInjection, input, payload)
                .await?;
        }
        Ok(())
    }
}
