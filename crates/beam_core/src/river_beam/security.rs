//! Security checks around an invoke (input, runtime, payloads) and their audit.

use beam_audit::{AuditAction, AuditEvent};
use beam_security::{SecurityContext, SecurityDecision, SecurityRequest, SecurityStage};
use beam_types::{AgentInput, AgentResult};
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
        payload.insert("token_count".into(), Value::from(token_count));
        self.check_security_payload(SecurityStage::Runtime, input, payload)
            .await
    }

    /// Run the security checks for `stage` over `content`. Returns an error when
    /// a deny/terminate/quarantine verdict is enforced.
    pub(super) async fn check_security(
        &self,
        stage: SecurityStage,
        input: &AgentInput,
        content: &str,
    ) -> RiverbaseResult<()> {
        self.check_security_payload(stage, input, content_payload(input, content))
            .await
    }

    /// [`Self::check_security`] with a prepared payload.
    pub(super) async fn check_security_payload(
        &self,
        stage: SecurityStage,
        input: &AgentInput,
        payload: serde_json::Map<String, Value>,
    ) -> RiverbaseResult<()> {
        self.decide_security(stage, input, payload)
            .await
            .map(|_| ())
    }

    /// `BEFORE_OUTPUT` on the answer, with the agent's instructions for the
    /// prompt-echo check (OUT-003). A `Mask` verdict, when enforced, masks
    /// what the output detectors flag in `result` and in its held `done`
    /// frame, so neither the client nor the transcript gets it.
    pub(super) async fn guard_output(
        &self,
        input: &AgentInput,
        instructions: String,
        result: &mut AgentResult,
        done: Option<&mut Value>,
    ) -> RiverbaseResult<()> {
        let output = super::result_text(result);
        if output.is_empty() {
            return Ok(());
        }
        let mut payload = content_payload(input, &output);
        payload.insert("instructions".into(), Value::String(instructions));
        let decision = self
            .decide_security(SecurityStage::BeforeOutput, input, payload)
            .await?;
        if decision.action != beam_security::SecurityAction::Mask || !self.security.enforce() {
            return Ok(());
        }
        let detectors = self.security.output_detectors();
        // A manager without output detectors masks through its decision: the
        // answer becomes its `sanitized_content`.
        let sanitized = decision.sanitized_content.filter(|_| detectors.is_empty());
        let masked = |text: &str| match &sanitized {
            Some(sanitized) => sanitized.clone(),
            None => beam_security::mask_flagged(&detectors, text),
        };
        if sanitized.is_some() {
            result.content.retain(|p| p.text.is_none());
            result.content.insert(0, beam_types::ContentPart::text(""));
        }
        for part in &mut result.content {
            if let Some(text) = part.text.as_mut() {
                *text = masked(text);
            }
        }
        if let Some(frame) = done {
            match (&sanitized, frame.pointer_mut("/result/content")) {
                (Some(_), Some(content)) => {
                    *content = serde_json::to_value(&result.content).unwrap_or(Value::Null);
                }
                (None, Some(Value::Array(parts))) => {
                    for part in parts {
                        if let Some(Value::String(text)) = part.get_mut("text") {
                            *text = masked(text);
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// The decision for `stage`; an error when a deny verdict is enforced.
    pub(super) async fn decide_security(
        &self,
        stage: SecurityStage,
        input: &AgentInput,
        payload: serde_json::Map<String, Value>,
    ) -> RiverbaseResult<SecurityDecision> {
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
        Ok(decision)
    }

    /// The audit event of a stream the guard stopped (`AFTER_LLM`, as the
    /// text came from the model): the finding's rule and pattern.
    pub(super) async fn audit_stream_block(
        &self,
        input: &AgentInput,
        finding: Option<&beam_security::Finding>,
    ) {
        let Some(finding) = finding else { return };
        let echo = finding.detector == "prompt_echo";
        let mut metadata = serde_json::Map::new();
        metadata.insert(
            "matched_patterns".into(),
            serde_json::json!([finding.pattern_id]),
        );
        let decision = SecurityDecision {
            action: beam_security::SecurityAction::Deny,
            severity: beam_security::SecuritySeverity::Critical,
            matched_rules: vec![if echo { "OUT-003" } else { "OUT-001" }.to_owned()],
            narration: "streamed output blocked".into(),
            user_message: Some("This request could not be completed for security reasons.".into()),
            metadata,
            violation_type: Some(beam_security::ViolationType::OutputSecretDetected),
            score: finding.severity.score(),
            sanitized_content: None,
        };
        self.audit_security(SecurityStage::AfterLlm, input, &decision)
            .await;
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

/// A check's payload: the content and the invoke's ids.
fn content_payload(input: &AgentInput, content: &str) -> serde_json::Map<String, Value> {
    let mut payload = serde_json::Map::new();
    payload.insert("content".into(), Value::String(content.to_string()));
    if let Some(interaction_id) = input.interaction_id {
        payload.insert(
            "interaction_id".into(),
            Value::String(interaction_id.to_string()),
        );
    }
    payload.insert("run_id".into(), Value::String(input.run_id.to_string()));
    payload
}
