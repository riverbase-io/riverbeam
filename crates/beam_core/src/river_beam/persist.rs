//! Saving an invoke's turns, steps and result, and the catalog check-in.

use beam_agents::human_message_from_input;
use beam_audit::{AuditAction, AuditEvent};
use beam_security::SecurityStage;
use beam_store::{
    agent_id_for, ActionRow, ActionStatus, ActionType, AgentRow, InteractionStatus, ModelRow,
    RunStatus, StepRow, StepStatus,
};
use beam_types::{AgentInput, AgentMessage, AgentMessageRole, AgentMessageType, AgentResult};
use beam_usage::UsageEvent;
use chrono::Utc;
use serde_json::Value;
use uuid::Uuid;

use riverbase_core::RiverbaseResult;

use super::{
    check_client_answer, python_action_response, turn_message, user_query_text, RiverBeam,
};

impl RiverBeam {
    pub(super) async fn save_input_turn(&self, input: &AgentInput) -> RiverbaseResult<()> {
        // Resume turn: validate + resolve the pending HITL actions instead of a
        // fresh human turn (ports `_save_action_result_message`).
        if let Some(actions) = &input.actions {
            return self.save_resume_turn(input, actions).await;
        }
        // `BEFORE_MEMORY_WRITE` on the text, then
        // the user message, saved before dispatch.
        let text = user_query_text(input);
        if !text.is_empty() {
            self.check_security(SecurityStage::BeforeMemoryWrite, input, &text)
                .await?;
        }
        let human = turn_message(input, human_message_from_input(input));
        self.save_turn(input, &[human]).await
    }

    /// The transcript is the only message writer.
    pub(super) async fn save_turn(
        &self,
        input: &AgentInput,
        messages: &[AgentMessage],
    ) -> RiverbaseResult<()> {
        let agent_id = agent_id_for(input.agent_name.as_deref().unwrap_or("unknown"));
        self.transcript
            .save_turn(
                agent_id,
                input.session_id,
                messages,
                input.user_id().as_deref(),
            )
            .await
    }

    pub(super) async fn save_resume_turn(
        &self,
        input: &AgentInput,
        actions: &[beam_types::ActionResponseEnvelope],
    ) -> RiverbaseResult<()> {
        if input.interaction_id.is_none() {
            return Err(crate::BEM_062.raise());
        }
        // Validate every action before writing anything.
        let mut rows = Vec::with_capacity(actions.len());
        for action in actions {
            let action_id = Uuid::parse_str(&action.action_id)
                .map_err(|_| crate::BEM_063.with_data(action.action_id.clone()))?;
            let row = self.store.get_action(action_id).await?;
            // Only this interaction's own actions (Python rc8 `_check_pending`):
            // the command checked the caller's session through one action only.
            if row.session_id != input.session_id
                || Some(row.interaction_id) != input.interaction_id
                || row
                    .execution_id
                    .is_some_and(|e| Some(e) != input.execution_id)
            {
                return Err(crate::BEM_399.with_data(format!(
                    "action {action_id} is not one of this interaction's"
                )));
            }
            if row.status != ActionStatus::Pending {
                return Err(crate::BEM_066.with_data(action_id.to_string()));
            }
            // Only a result needs a capable client; any client may decline,
            // and the sweep expires (Python rc8 `_check_client_answer`).
            if row.action_type == ActionType::ClientTool
                && action.status == beam_types::ActionResponseStatus::Submitted
            {
                check_client_answer(&row, input.client.as_ref())?;
            }
            rows.push(row);
        }

        // Port of Python `_save_action_result_message`: one human `action_result`
        // message per action, content = `json.dumps(action_response)` as text.
        let mut messages = Vec::with_capacity(actions.len());
        for (action, row) in actions.iter().zip(&rows) {
            let response = python_action_response(action);
            self.store.resolve_action(row.id, response.clone()).await?;
            let step = match row.step_id {
                Some(step_id) => self
                    .store
                    .list_steps(row.run_id)
                    .await?
                    .into_iter()
                    .find(|s| s.id == step_id),
                None => None,
            };
            let mut message = turn_message(
                input,
                AgentMessage::text(AgentMessageRole::Human, crate::pyjson::dumps(&response)),
            );
            message.msg_type = AgentMessageType::ActionResult;
            message.step_id = step.as_ref().map(|s| s.id);
            message.metadata = serde_json::json!({
                "action_id": row.id.to_string(),
                "action_response": response,
                "step_id": step.as_ref().map(|s| s.id.to_string()),
                "step_name": step.as_ref().map(|s| s.name.clone()),
                "step_status": step.as_ref().map(|_| "completed"),
            });
            messages.push(message);
        }
        self.save_turn(input, &messages).await?;

        self.audit
            .log(
                AuditEvent::new(AuditAction::AgentResumed)
                    .agent_name(input.agent_name.clone())
                    .user_id(input.user_id())
                    .session_id(Some(input.session_id))
                    .run_id(Some(input.run_id)),
            )
            .await;
        Ok(())
    }

    /// Catalog check-in for registered agents/models (worker startup hook).
    ///
    /// # Errors
    /// The store's errors.
    pub async fn checkin_catalog(&self) -> RiverbaseResult<()> {
        let agents: Vec<AgentRow> = self
            .registry
            .list_agents()
            .into_iter()
            .map(|m| AgentRow::active(&m))
            .collect();
        self.store.checkin_agents(&agents).await?;
        // Python checks models in too (`check_in_models`), keyed by `UUID_GENF(name)`.
        let models: Vec<ModelRow> = self
            .router
            .list_models()
            .iter()
            .map(ModelRow::active)
            .collect();
        self.store.checkin_models(&models).await?;
        // Profiles reference plans by `UUID_GENF(name)`.
        let plans: Vec<_> = self
            .usage
            .iter()
            .flat_map(|u| u.plans.values())
            .map(|p| p.to_row("active"))
            .collect();
        if !plans.is_empty() {
            self.store.checkin_plans(&plans).await?;
        }
        Ok(())
    }

    /// Record a step a runtime reported (Python writes one `agent_step` row per step).
    pub(super) async fn save_runtime_step(
        &self,
        input: &AgentInput,
        step: &beam_types::AgentStep,
    ) -> RiverbaseResult<()> {
        let status = match step.status.as_str() {
            "completed" => StepStatus::Completed,
            "failed" => StepStatus::Failed,
            "interrupted" => StepStatus::Interrupted,
            "resumed" => StepStatus::Resumed,
            "pending" => StepStatus::Pending,
            _ => StepStatus::Running,
        };
        self.store
            .save_step(&StepRow {
                // Python: `UUID_GENF(lc_step_id)`.
                id: agent_id_for(&step.lc_step_id),
                session_id: input.session_id,
                interaction_id: input.interaction_id.unwrap_or(input.run_id),
                run_id: input.run_id,
                agent_id: agent_id_for(input.agent_name.as_deref().unwrap_or("unknown")),
                status,
                lc_step_id: Some(step.lc_step_id.clone()),
                name: step.step_name.clone(),
                input: step.input.clone(),
                result: step.result.clone(),
            })
            .await
    }

    pub(super) async fn persist_result(
        &self,
        input: &AgentInput,
        result: &AgentResult,
    ) -> RiverbaseResult<()> {
        // Python `coerce_persisted_action_type` for every pending action, before any write,
        // so an unsupported type leaves nothing half-persisted.
        let action_types = match (&result.action_required, result.interrupted) {
            (Some(actions), true) => actions
                .iter()
                .map(|action| {
                    // Legacy entries carry the type at the top; Python-shaped ones
                    // (`{action_id, lc_action_id, action_request}`) inside the request.
                    let raw = ["type", "action_type", "kind"]
                        .iter()
                        .find_map(|key| action.get(*key).and_then(Value::as_str))
                        .or_else(|| {
                            action
                                .pointer("/action_request/action_type")
                                .and_then(Value::as_str)
                        });
                    ActionType::coerce(raw)
                        .ok_or_else(|| crate::BEM_068.with_data(raw.unwrap_or_default().to_owned()))
                })
                .collect::<RiverbaseResult<Vec<_>>>()?,
            _ => Vec::new(),
        };
        let ai_msg = turn_message(
            input,
            AgentMessage {
                role: AgentMessageRole::Ai,
                msg_type: AgentMessageType::Message,
                content: result.content.clone(),
                ..Default::default()
            },
        );
        self.save_turn(input, &[ai_msg]).await?;
        // The turn is saved: bring the session's summary up to date (Python
        // `compact` after the persisted turn, plan 023 B3). Only logged: the
        // next read summarizes what this one could not.
        if let Err(e) = self.transcript.compact(input.session_id).await {
            tracing::warn!(session_id = %input.session_id, error = %e, "session compaction failed");
        }

        // HITL: persist a pending agent_action row per requested action.
        if result.interrupted {
            if let Some(actions) = &result.action_required {
                let agent_id = agent_id_for(input.agent_name.as_deref().unwrap_or("unknown"));
                for (action, action_type) in actions.iter().zip(action_types.iter().copied()) {
                    let action_id = action
                        .get("action_id")
                        .and_then(Value::as_str)
                        .and_then(|s| Uuid::parse_str(s).ok())
                        .unwrap_or_else(Uuid::new_v4);
                    // Python rc8 `SqlActionStore`: tool actions name their tool,
                    // and a client tool's action expires.
                    let request_payload = action.pointer("/action_request/payload");
                    let payload_field = |key: &str| {
                        request_payload
                            .and_then(|p| p.get(key))
                            .and_then(Value::as_str)
                    };
                    let tool_name = matches!(
                        action_type,
                        ActionType::ToolApproval | ActionType::ClientTool
                    )
                    .then(|| payload_field("tool_name").map(str::to_owned))
                    .flatten();
                    let expires_at = (action_type == ActionType::ClientTool)
                        .then(|| payload_field("expires_at"))
                        .flatten()
                        .and_then(|raw| chrono::DateTime::parse_from_rfc3339(raw).ok())
                        .map(|t| t.with_timezone(&Utc));
                    self.store
                        .create_action(ActionRow {
                            id: action_id,
                            session_id: input.session_id,
                            execution_id: input.execution_id,
                            interaction_id: input.interaction_id.unwrap_or(input.run_id),
                            run_id: input.run_id,
                            step_id: None,
                            message_id: None,
                            agent_id,
                            action_type,
                            status: ActionStatus::Pending,
                            lc_action_id: action
                                .get("lc_action_id")
                                .and_then(Value::as_str)
                                .map(str::to_owned),
                            // Python stores `{"request": action_request}`.
                            payload: action.get("action_request").map_or_else(
                                || action.clone(),
                                |request| serde_json::json!({"request": request}),
                            ),
                            result: None,
                            resolved_at: None,
                            org_id: input.auth_context.as_ref().and_then(|a| a.org_id.clone()),
                            tool_name,
                            expires_at,
                            sweep_after: None,
                        })
                        .await?;
                    if let Some(execution_id) = input.execution_id {
                        self.executions
                            .records()
                            .link_action(action_id, execution_id);
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) async fn finalize_invocation(
        &self,
        input: &AgentInput,
        result: &AgentResult,
    ) -> RiverbaseResult<()> {
        let run_status = if result.interrupted {
            RunStatus::Interrupted
        } else {
            RunStatus::Completed
        };
        self.store.complete_run(input.run_id, run_status).await?;
        if !result.interrupted {
            if let Some(interaction_id) = input.interaction_id {
                self.store
                    .complete_interaction(interaction_id, InteractionStatus::Completed)
                    .await?;
            }
            self.maybe_auto_remember(input, result).await;
        }
        self.store.update_session_activity(input.session_id).await?;

        // Usage accounting.
        if let (Some(tracker), Some(user_id)) =
            (self.usage.as_ref().map(|u| &u.tracker), input.user_id())
        {
            let tokens = result
                .token_usage
                .as_ref()
                .map_or(0, |u| u64::from(u.total_tokens));
            tracker
                .record(
                    &user_id,
                    &UsageEvent {
                        tokens,
                        requests: 1,
                        model_used: result.model_used.clone(),
                        modalities: input.modalities().into_iter().collect(),
                    },
                )
                .await?;
        }

        let action = if result.interrupted {
            AuditAction::AgentInterrupted
        } else {
            AuditAction::AgentCompleted
        };
        self.audit
            .log(
                AuditEvent::new(action)
                    .agent_name(input.agent_name.clone())
                    .user_id(input.user_id())
                    .session_id(Some(input.session_id))
                    .run_id(Some(input.run_id))
                    .execution_id(input.execution_id)
                    .trace_id(beam_telemetry::current_span().map(|s| s.trace_id)),
            )
            .await;
        Ok(())
    }

    // --- usage accounting ----------------------------
}
