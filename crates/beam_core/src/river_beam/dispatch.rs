//! Dispatching a turn to its agent's runtime.

use std::sync::Arc;

use beam_context::ContextManifest;
use beam_execution::{ExecutionRow, Lease};
use beam_gateway::fit::{
    Compaction, ContextBudget, HeuristicSummarizer, InvocationFitter, ModelSummarizer, Summarizer,
};
use beam_runtime::{
    runtime_event_to_stream_chunk, AgentRuntime, ContextFitter, ModelCaller, RuntimeContext,
    RuntimeStartRequest,
};
use beam_types::{AgentContext, AgentInput, AgentResult};
use futures_util::StreamExt;
use serde_json::Value;

use riverbase_core::RiverbaseResult;

use super::{
    execution_ref, ops, resume_request, settled_usage, ModelSource, RiverBeam, TracedModel,
};

/// A dispatched turn: its result and the steps the run took, counted from
/// the runtime's step events (fluvius-beam counted `agent_steps`).
pub(super) struct Dispatched {
    pub(super) result: AgentResult,
    pub(super) steps: usize,
}

impl RiverBeam {
    pub(super) async fn dispatch_agent(
        &self,
        input: &AgentInput,
        ctx: &AgentContext,
        execution: &ExecutionRow,
        lease: Lease,
        pending_done: &ops::PendingDone,
    ) -> RiverbaseResult<Dispatched> {
        let agent_name = input.agent_name.as_deref().unwrap_or_default();
        // A new run records `started` first.
        if input.actions.as_ref().is_none_or(std::vec::Vec::is_empty) {
            let started =
                serde_json::json!({"type": "started", "execution_id": execution.id.to_string()});
            self.deliver_durable(input, started, "started").await?;
        }
        // Every agent runs on a runtime (plan 021 D3); a missing name or an
        // unbound agent is unknown.
        let Some(runtime) = self.runtimes.find(agent_name) else {
            return Err(crate::BEM_021.with_data(agent_name.to_owned()));
        };
        self.registry.validate_input(agent_name, input)?;
        Box::pin(self.dispatch_runtime(
            runtime.as_ref(),
            input,
            ctx,
            execution,
            lease,
            pending_done,
        ))
        .await
    }

    /// The context a runtime gets for one start or resume.
    pub(super) fn runtime_context(
        &self,
        input: &AgentInput,
        ctx: Option<&AgentContext>,
        lease: Option<Lease>,
    ) -> RuntimeContext {
        RuntimeContext {
            auth: input.auth_context.clone(),
            history: ctx
                .map(|c| c.conversation_history.clone())
                .unwrap_or_default(),
            metadata: input.metadata.as_object().cloned().unwrap_or_default(),
            model_gateway: self.models.fallback(),
            tool_runtime: self.tool_source.executor(),
            available_tools: ctx.map(|c| c.available_tools.clone()).unwrap_or_default(),
            restrict_tools: self.tool_source.restricts(),
            lease,
            spotlight: beam_types::Spotlight::from_flag(self.config.security_spotlight),
            ..RuntimeContext::default()
        }
    }

    /// Start or resume a runtime-bound agent and read its events to the end,
    /// publishing wire chunks as they come when the invoke streams.
    pub(super) async fn dispatch_runtime(
        &self,
        runtime: &dyn AgentRuntime,
        input: &AgentInput,
        ctx: &AgentContext,
        execution: &ExecutionRow,
        lease: Lease,
        pending_done: &ops::PendingDone,
    ) -> RiverbaseResult<Dispatched> {
        // Python's runtimes open `beam.runtime` around a run.
        let mut attributes = serde_json::Map::new();
        attributes.insert(
            "execution_id".into(),
            Value::String(execution.id.to_string()),
        );
        beam_telemetry::in_span(
            beam_telemetry::SPAN_RUNTIME,
            None,
            attributes,
            Box::pin(self.dispatch_runtime_in_span(
                runtime,
                input,
                ctx,
                execution,
                lease,
                pending_done,
            )),
        )
        .await
    }

    pub(super) async fn dispatch_runtime_in_span(
        &self,
        runtime: &dyn AgentRuntime,
        input: &AgentInput,
        ctx: &AgentContext,
        execution: &ExecutionRow,
        lease: Lease,
        pending_done: &ops::PendingDone,
    ) -> RiverbaseResult<Dispatched> {
        let mut context = self.runtime_context(input, Some(ctx), Some(lease));
        if let ModelSource::Gateway(factory) = &self.models {
            let name = input.agent_name.as_deref().unwrap_or_default();
            let metadata = self.registry.get_metadata(name)?;
            let chain = self.router.resolve_chain(input, Some(&metadata))?;
            context.model_gateway = Arc::new(factory.gateway(&chain)?);
        }
        let meter = Arc::new(std::sync::Mutex::new(beam_types::TokenUsage::default()));
        context.model_gateway = Arc::new(TracedModel {
            inner: context.model_gateway.clone(),
            meter: meter.clone(),
        });
        // Every model call of the attempt is fitted and metered.
        let base = self
            .manifests
            .get(&input.run_id.to_string())
            .await
            .ok()
            .flatten();
        context.fitter = base.as_ref().map(|base| {
            self.context_fitter(input, ctx, base, &context.model_gateway, execution.id)
        });
        let actions_given = input.actions.as_ref().is_some_and(|a| !a.is_empty());
        let mut events = if actions_given {
            runtime.resume(resume_request(runtime, input, execution)?, context)
        } else {
            runtime.start(
                RuntimeStartRequest {
                    execution_id: execution.id,
                    run_id: input.run_id,
                    input: input.clone(),
                    runtime_ref: Some(execution_ref(runtime, execution)?),
                },
                context,
            )
        };
        let mut guard = self.output_guard(input, ctx);
        let mut seen = Vec::new();
        // A step is reported when it starts and when it ends: count its id.
        let mut steps = std::collections::HashSet::new();
        // The model call whose provider usage comes next.
        let mut pending_call = None;
        while let Some(event) = events.next().await {
            let event = event?;
            match event.event_type {
                beam_runtime::RuntimeEventType::ContextUsage => {
                    if let Some(base) = &base {
                        pending_call = self.record_call_context(input, base, &event).await;
                    }
                }
                beam_runtime::RuntimeEventType::Usage => {
                    if let (Some(step), Some(usage), Some(base)) =
                        (pending_call.take(), &event.usage, &base)
                    {
                        self.reconcile_call(base, step, u64::from(usage.prompt_tokens))
                            .await;
                    }
                }
                _ => {}
            }
            if let Some(chunk) = runtime_event_to_stream_chunk(&event) {
                for frame in self
                    .guard_frames(input, guard.as_mut(), Some(chunk))
                    .await?
                {
                    self.emit_frame(input, frame, pending_done).await?;
                }
            }
            if let Some(step) = &event.step {
                steps.insert(step.lc_step_id.clone());
                self.save_runtime_step(input, step).await?;
            }
            let terminal = event.is_terminal();
            seen.push(event);
            if terminal {
                break;
            }
        }
        for frame in self.guard_frames(input, guard.as_mut(), None).await? {
            self.emit_frame(input, frame, pending_done).await?;
        }
        self.flush_stream(input).await?;
        let mut result = beam_runtime::terminal_result(&seen)?;
        let metered = meter.lock().map(|m| m.clone()).unwrap_or_default();
        result.token_usage = settled_usage(result.token_usage.take(), &metered);
        Ok(Dispatched {
            result,
            steps: steps.len(),
        })
    }

    /// The run's stream guard (plan 030 D2): on when the invoke streams,
    /// security enforces and `BEAM_SECURITY_STREAM_HOLDBACK` is not 0. It runs
    /// the output detectors and the prompt-echo check on the agent's
    /// instructions.
    fn output_guard(&self, input: &AgentInput, ctx: &AgentContext) -> Option<ops::OutputGuard> {
        let holdback = self.config.security_stream_holdback;
        if !input.stream || holdback == 0 || !self.security.enforce() {
            return None;
        }
        let mut detectors = self.security.output_detectors();
        let echo =
            beam_security::PromptEcho::new(&super::instructions_of(&ctx.conversation_history));
        if !echo.is_empty() {
            detectors.push(Arc::new(echo));
        }
        (!detectors.is_empty()).then(|| ops::OutputGuard::new(detectors, holdback))
    }

    /// The frames to send for `chunk` through the stream guard, or the held
    /// text when `chunk` is `None` (the run's events ended). A blocked stream
    /// is audited before its error ends the run.
    async fn guard_frames(
        &self,
        input: &AgentInput,
        guard: Option<&mut ops::OutputGuard>,
        chunk: Option<beam_types::StreamChunk>,
    ) -> RiverbaseResult<Vec<beam_types::StreamChunk>> {
        let Some(guard) = guard else {
            return Ok(chunk.into_iter().collect());
        };
        let frames = match chunk {
            Some(chunk) => guard.admit(chunk),
            None => guard.finish(),
        };
        if frames.is_err() {
            self.audit_stream_block(input, guard.blocked()).await;
        }
        frames
    }

    /// The attempt's fitter (plan 023 A2 and B): the invoke's window, output
    /// and reserve from its manifest, the request's `token_budget` when it is
    /// smaller, and the pinned spec's compaction (on by default, D4).
    fn context_fitter(
        &self,
        input: &AgentInput,
        ctx: &AgentContext,
        base: &ContextManifest,
        caller: &Arc<dyn ModelCaller>,
        execution_id: uuid::Uuid,
    ) -> Arc<dyn ContextFitter> {
        let model = ctx.model_config_resolved.as_ref();
        let window_assumed = model
            .and_then(|m| m.context_window_tokens)
            .is_none_or(|w| w == 0);
        let mut budget =
            ContextBudget::new(base.window_tokens, base.output_tokens, base.reserve_tokens);
        if let Some(limit) = input.context.as_ref().and_then(|c| c.token_budget) {
            budget.window = budget.window.min(
                limit
                    .saturating_add(budget.output)
                    .saturating_add(budget.reserve),
            );
        }
        let policy = self
            .pinned_spec(input)
            .and_then(|spec| spec.context.compaction)
            .unwrap_or_default();
        let model_name = model.map(|m| m.name.clone());
        let mut fitter = InvocationFitter::new(budget.clone(), model_name.clone(), window_assumed);
        if policy.enabled {
            // rc8 `budget_for`: the compaction reserve, at most a tenth of the window.
            budget.compact_reserve = policy.reserve_tokens.min(budget.window / 10);
            budget.soft_ratio = policy.soft_ratio;
            budget.hard_ratio = policy.hard_ratio;
            budget.target_ratio = policy.target_ratio;
            let summarizer: Arc<dyn Summarizer> = if policy.summarizer == "model" {
                Arc::new(ModelSummarizer {
                    caller: caller.clone(),
                    model: input.model_override.clone().or(model_name.clone()),
                    execution_id,
                    run_id: input.run_id,
                })
            } else {
                Arc::new(HeuristicSummarizer)
            };
            fitter = InvocationFitter::new(budget, model_name, window_assumed).with_compaction(
                Compaction {
                    keep_tool_results: policy.keep_tool_results,
                    keep_recent_units: policy.keep_recent_units,
                    summarizer,
                },
            );
        }
        Arc::new(fitter)
    }

    /// Record one model call's context (plan 023 A2): its manifest
    /// `{operation}:{step}`, and the durable `context_usage` frame when the
    /// caller asked for it. Returns the call's step. A failure is logged: the
    /// run goes on.
    async fn record_call_context(
        &self,
        input: &AgentInput,
        base: &ContextManifest,
        event: &beam_runtime::RuntimeEvent,
    ) -> Option<u32> {
        let step = event
            .payload
            .get("step")
            .and_then(Value::as_u64)
            .and_then(|s| u32::try_from(s).ok())?;
        let usage: beam_types::ContextUsage =
            serde_json::from_value(event.payload.get("usage").cloned().unwrap_or(Value::Null))
                .ok()?;
        let transformed: Vec<(String, String)> = event
            .payload
            .get("transformed")
            .and_then(|t| serde_json::from_value(t.clone()).ok())
            .unwrap_or_default();
        let operation = base
            .operation_id
            .clone()
            .unwrap_or_else(|| input.run_id.to_string());
        let call_id = format!("{operation}:{step}");
        let manifest = ContextManifest {
            operation_id: Some(call_id.clone()),
            parent_operation_id: Some(operation),
            operation_kind: "model_call".into(),
            step,
            decisions: transformed
                .into_iter()
                .map(|(item_id, reason)| beam_context::ManifestDecision {
                    item_id,
                    source: String::new(),
                    action: beam_context::DecisionAction::Transformed,
                    reason,
                    tokens: 0,
                    content_hash: "0".repeat(16),
                    trust: beam_context::Trust::Untrusted,
                })
                .collect(),
            input_tokens: usage.used_tokens,
            output_tokens: usage.output_tokens,
            reserve_tokens: usage.reserve_tokens,
            window_tokens: usage.window_tokens,
            counter_method: usage.counter_method.clone(),
            actual_prompt_tokens: None,
            estimate_drift: None,
            usage: Some(usage.clone()),
            ..base.clone()
        };
        if let Err(e) = self.manifests.put(&call_id, &manifest).await {
            tracing::warn!(call = %call_id, error = %e, "context_manifest_write_failed");
        }
        if input.context.as_ref().is_some_and(|c| c.emit_context_usage) {
            let frame = serde_json::json!({"type": "context_usage", "step": step, "usage": usage});
            if let Err(e) = self.deliver_durable(input, frame, "context_usage").await {
                tracing::warn!(call = %call_id, error = %e, "context_usage_emit_failed");
            }
        }
        Some(step)
    }

    /// The provider's prompt tokens for call `step`: the
    /// call's manifest, and the invoke's for the first call.
    async fn reconcile_call(&self, base: &ContextManifest, step: u32, prompt_tokens: u64) {
        let Some(operation) = base.operation_id.as_deref() else {
            return;
        };
        if prompt_tokens == 0 {
            return;
        }
        let call_id = format!("{operation}:{step}");
        if let Err(e) = self.manifests.reconcile(&call_id, prompt_tokens).await {
            tracing::warn!(call = %call_id, error = %e, "context_manifest_reconcile_failed");
        }
        if step == 1 {
            if let Err(e) = self.manifests.reconcile(operation, prompt_tokens).await {
                tracing::warn!(error = %e, "context_manifest_reconcile_failed");
            }
        }
    }
}
