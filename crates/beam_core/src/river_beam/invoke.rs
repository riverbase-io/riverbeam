//! The invoke entry points, the attempt loop, failures, and execution cancel and retry.

use std::sync::Arc;

use beam_audit::{AuditAction, AuditEvent};
use beam_execution::idempotency::normalize_idempotency_scope;
use beam_execution::{ExecutionRow, ExecutionStatus, Lease, StopReason};
use beam_runtime::RuntimeContext;
use beam_security::SecurityStage;
use beam_store::{InteractionStatus, RunStatus};
use beam_types::{AgentContext, AgentInput, AgentResult, InvokeRequest};
use beam_usage::UsagePlan;
use riverbase_core::RiverbaseError;
use serde_json::Value;
use uuid::Uuid;

use riverbase_core::RiverbaseResult;

use super::{
    error_category, execution_ref, ops, replay_result, result_text, resume_request, to_agent_input,
    wait_for_stop, AttemptGuard, AttemptOutcome, HeldLease, InvokeIdentity, RiverBeam,
};

impl RiverBeam {
    /// `cancel-execution`: cancel, expire pending HITL actions, stop the run if it is here.
    ///
    /// # Errors
    /// `BEM-377` when unknown; `BEM-370` when already finished.
    pub async fn cancel_execution(&self, execution_id: Uuid) -> RiverbaseResult<Value> {
        let row = self.executions.cancel(execution_id).await?;
        // Durable records expire the actions in the same database already.
        if !self.executions.records().durable() {
            self.store.expire_pending_actions(execution_id).await?;
        }
        // The runtime records the cancel on its saved run (Python
        // `ExecutionControlService`). The execution is cancelled either way.
        if let Some(runtime) = self.runtime_for_agent(row.agent_id) {
            if runtime.capabilities().cancellation {
                let cancelled = match execution_ref(runtime.as_ref(), &row) {
                    Ok(runtime_ref) => {
                        runtime
                            .cancel(&runtime_ref, &RuntimeContext::default())
                            .await
                    }
                    Err(e) => Err(e),
                };
                if let Err(e) = cancelled {
                    tracing::warn!(%execution_id, error = %e, "runtime cancel failed");
                }
            }
        }
        Ok(serde_json::json!({"execution_id": row.id.to_string(), "status": row.status.as_str()}))
    }

    /// `retry-execution`: move a failed execution to `RETRYING` and run its
    /// stored request again on the same execution (Python `retry` + `redispatch`).
    ///
    /// # Errors
    /// `BEM-377`, `BEM-378`, `BEM-384`, or the redispatched invoke's error.
    pub async fn retry_execution(&self, execution_id: Uuid) -> RiverbaseResult<AgentResult> {
        // Refuse before touching the row, so a draining instance leaves it `FAILED`.
        let _admission = self.executions.admit()?;
        let snapshot = self.executions.retry(execution_id).await?;
        let mut request: InvokeRequest = serde_json::from_value(snapshot)
            .map_err(|e| crate::BEM_384.with_data(e.to_string()))?;
        request.execution_id = Some(execution_id);
        self.invoke_result(request).await
    }

    /// Invoke pipeline entry — returns JSON matching Python `AgentResult.model_dump`.
    ///
    /// # Errors
    /// [`Self::invoke_result`]'s errors, and `BEM-046` when the result cannot be
    /// serialized.
    pub async fn invoke(&self, req: InvokeRequest) -> RiverbaseResult<Value> {
        let result = self.invoke_result(req).await?;
        serde_json::to_value(result).map_err(|e| crate::BEM_046.with_data(e.to_string()))
    }

    /// Invoke pipeline entry, returning the typed [`AgentResult`].
    ///
    /// # Errors
    /// Any pipeline phase's error: admission, validation, security, usage, the
    /// agent run or the store.
    pub async fn invoke_result(&self, req: InvokeRequest) -> RiverbaseResult<AgentResult> {
        self.invoke_on(req, None).await
    }

    /// [`Self::invoke_result`]; a new execution runs on `revision` when set,
    /// else on the traffic's pick.
    pub(crate) async fn invoke_on(
        &self,
        req: InvokeRequest,
        revision: Option<Uuid>,
    ) -> RiverbaseResult<AgentResult> {
        // Optional RLS: every store connection of this invoke, from resolving
        // the execution on, is scoped to the caller's org.
        let org = req.auth_context.as_ref().and_then(|a| a.org_id.clone());
        // Boxed: the pipeline future is large (debug stacks overflow).
        beam_store::org_scope::with_org(org.as_deref(), Box::pin(self.invoke_in_org(req, revision)))
            .await
    }

    pub(super) async fn invoke_in_org(
        &self,
        req: InvokeRequest,
        revision: Option<Uuid>,
    ) -> RiverbaseResult<AgentResult> {
        let _admission = self.executions.admit()?;
        if let Some(skipped) = self.stale_client_tool_expiry(&req).await? {
            return Ok(skipped);
        }
        // The stored request never holds a credential: a host's
        // MCP token stays on this call only.
        let mut request_snapshot = serde_json::to_value(&req).ok();
        if let Some(auth) = request_snapshot
            .as_mut()
            .and_then(|s| s.get_mut("auth_context"))
            .and_then(Value::as_object_mut)
        {
            auth.remove("mcp_token");
        }
        let auth = req.auth_context.as_ref();
        let idempotency = normalize_idempotency_scope(
            req.idempotency_key.as_deref(),
            auth.and_then(|a| a.user_id.as_deref()),
            &req.agent_name,
            auth.and_then(|a| a.org_id.as_deref()),
        );
        let resumed: Vec<Uuid> = req
            .actions
            .iter()
            .flatten()
            .filter_map(|a| Uuid::parse_str(&a.action_id).ok())
            .collect();
        let mut agent_input = to_agent_input(req);
        agent_input.execution_id = self
            .executions
            .resolve(agent_input.execution_id, idempotency.as_ref(), &resumed)
            .await?;
        // One `beam.execution` span per invoke, a child
        // of the caller's (`traceparent` in the metadata) when there is one.
        let parent = beam_telemetry::current_span()
            .or_else(|| beam_telemetry::extract(&agent_input.metadata));
        let ids = beam_telemetry::logging::Correlation {
            execution_id: agent_input.execution_id.map(|e| e.to_string()),
            run_id: Some(agent_input.run_id.to_string()),
            trace_id: None,
            request_id: agent_input.request_id.map(|r| r.to_string()),
        };
        // The execution id `ensure` assigns, for the failure line.
        let assigned = Arc::new(std::sync::OnceLock::new());
        let identity = InvokeIdentity {
            idempotency,
            request_snapshot,
            assigned: Arc::clone(&assigned),
            revision,
        };
        beam_telemetry::in_span(
            beam_telemetry::SPAN_EXECUTION,
            parent.as_ref(),
            serde_json::Map::new(),
            async {
                let metrics = beam_telemetry::metrics();
                metrics.increment("request_count", 1);
                let result = Box::pin(self.run_invoke_pipeline(agent_input, identity)).await;
                match &result {
                    Ok(_) => metrics.increment("execution_completed", 1),
                    Err(err) => {
                        metrics.increment("execution_failed", 1);
                        let public = crate::public_errors::public_code(err.errcode.as_str());
                        let ids = beam_telemetry::logging::Correlation {
                            execution_id: ids
                                .execution_id
                                .clone()
                                .or_else(|| assigned.get().map(Uuid::to_string)),
                            trace_id: beam_telemetry::current_span().map(|s| s.trace_id),
                            ..ids
                        };
                        beam_telemetry::logging::log_invoke_failure(
                            &ids,
                            public,
                            error_category(public),
                            &err.errmesg,
                        );
                    }
                }
                result
            },
        )
        .await
    }

    pub(super) async fn run_invoke_pipeline(
        &self,
        agent_input: AgentInput,
        identity: InvokeIdentity,
    ) -> RiverbaseResult<AgentResult> {
        let eval_revision = identity.revision;
        let (agent_input, ensured, usage_plan) =
            self.bootstrap_invocation(agent_input, identity).await?;
        if ensured.replay_terminal {
            // Python returns the execution's identity with empty content.
            return Ok(replay_result(&agent_input));
        }
        let mut agent_input = agent_input;
        let mut execution = ensured.execution;
        let guard = AttemptGuard {
            executions: self.executions.clone(),
            execution_id: execution.id,
            run_id: agent_input.run_id,
        };
        let mut stop = self.executions.watch(execution.id, agent_input.run_id);
        // Pin a new execution, or resume under
        // its revision's compatibility policy.
        let revision = self
            .resolve_revision(&mut agent_input, &mut execution, eval_revision)
            .await;
        let attempt_rows = match revision {
            Ok(()) => {
                self.create_attempt_rows(
                    &agent_input,
                    ensured.attempt_no,
                    execution.runtime_ref.as_ref(),
                )
                .await
            }
            Err(e) => Err(e),
        };
        let AttemptOutcome { result, held } = match attempt_rows {
            Ok(()) => {
                // The run row keeps the trace.
                if let Some(span) = beam_telemetry::current_span() {
                    if let Err(e) = self
                        .store
                        .set_run_trace_id(agent_input.run_id, &span.trace_id)
                        .await
                    {
                        tracing::warn!(error = %e, "recording the run's trace id failed");
                    }
                }
                Box::pin(self.run_attempt(&agent_input, &execution, usage_plan.as_ref(), &mut stop))
                    .await
            }
            Err(e) => AttemptOutcome {
                result: Err(e),
                held: None,
            },
        };
        // The order is fixed: stop the heartbeat, handle the failure, release
        // the lease, then forget the stop signal (the guard).
        if let Some(held) = &held {
            held.stop_heartbeat();
        }
        if let Err(err) = &result {
            self.handle_failure(&agent_input, ensured.attempt_no, err)
                .await;
        }
        if let Some(held) = held {
            if let Err(e) = self.executions.release(&held.lease).await {
                tracing::warn!(error = %e, "lease release failed during invoke cleanup");
            }
        }
        drop(guard);
        result
    }

    /// One attempt of an execution, with the lease it claimed.
    pub(super) async fn run_attempt(
        &self,
        agent_input: &AgentInput,
        execution: &ExecutionRow,
        usage_plan: Option<&UsagePlan>,
        stop: &mut tokio::sync::watch::Receiver<Option<StopReason>>,
    ) -> AttemptOutcome {
        let mut held = None;
        let result =
            Box::pin(self.attempt(agent_input, execution, usage_plan, stop, &mut held)).await;
        AttemptOutcome { result, held }
    }

    /// One attempt of an execution, in Python's `InvokeFlow` order. `held`
    /// is set as soon as the run lease is claimed, so the caller releases it
    /// whatever happens after.
    #[allow(clippy::too_many_lines)]
    async fn attempt(
        &self,
        agent_input: &AgentInput,
        execution: &ExecutionRow,
        usage_plan: Option<&UsagePlan>,
        stop: &mut tokio::sync::watch::Receiver<Option<StopReason>>,
        held: &mut Option<HeldLease>,
    ) -> RiverbaseResult<AgentResult> {
        // `BEFORE_HITL_RESUME` on a resume (an
        // identity is required), then `BEFORE_INPUT` once per text part.
        self.enforce_input_policy(agent_input).await?;
        // A runtime refuses a bad resume before its actions are resolved.
        if agent_input.actions.as_ref().is_some_and(|a| !a.is_empty()) {
            if let Some(runtime) = agent_input
                .agent_name
                .as_deref()
                .and_then(|n| self.runtimes.find(n))
            {
                let request = resume_request(runtime.as_ref(), agent_input, execution)?;
                runtime
                    .check_resume(&request, &self.runtime_context(agent_input, None, None))
                    .await?;
            }
        }
        self.save_input_turn(agent_input).await?;
        let prepared = self.build_agent_context(agent_input).await?;
        let agent_ctx = prepared.agent;
        let model = agent_ctx.model_config_resolved.clone();
        // Hold the composed context's tokens for the attempt (Python `reserve`
        // with `state.reserved_tokens`). A failed attempt releases them; a
        // dropped one releases them from a task.
        let reserved = prepared.envelope.input_tokens
            + prepared.envelope.output_tokens
            + prepared.envelope.reserve_tokens;
        tracing::debug!(
            operation_id = prepared
                .manifest
                .operation_id
                .as_deref()
                .unwrap_or_default(),
            input_tokens = prepared.envelope.input_tokens,
            "context composed"
        );
        let mut reservation = self
            .reserve_usage(agent_input, usage_plan, model.as_ref(), reserved)
            .await?;
        let outcome = Box::pin(async {
            // Claim the run lease, then renew it while the agent runs.
            let claimed = self.executions.claim(agent_input.run_id).await?;
            *held = Some(HeldLease {
                lease: claimed,
                heartbeat: self.executions.spawn_heartbeat(execution.id, claimed),
            });
            if let Some(reason) = self.executions.stop_reason(execution.id).await? {
                return Err(reason.error(execution.id));
            }
            // The `RUNTIME` stage before dispatch.
            self.enforce_runtime_policy(agent_input, 0, 0).await?;
            // Python holds the `done` frame until the result is persisted.
            let pending_done = ops::PendingDone::default();
            let dispatched = Box::pin(self.dispatch_guarded(
                agent_input,
                &agent_ctx,
                execution,
                stop,
                claimed,
                &pending_done,
            ))
            .await?;
            let mut agent_res = dispatched.result;
            // A result without citations gets the
            // context's.
            if agent_res.citations.is_empty() {
                agent_res.citations.clone_from(&prepared.citations);
            }

            // `BEFORE_OUTPUT` on the answer text, then the
            // `RUNTIME` stage with the steps and tokens the run used.
            let output = result_text(&agent_res);
            if !output.is_empty() {
                self.check_security(SecurityStage::BeforeOutput, agent_input, &output)
                    .await?;
            }
            // The steps the runtime reported.
            let steps = dispatched.steps;
            let tokens = agent_res
                .token_usage
                .as_ref()
                .map_or(0, |u| u64::from(u.total_tokens));
            self.enforce_runtime_policy(agent_input, steps, tokens)
                .await?;
            // A worker whose lease was taken over must not write results.
            self.executions.assert_fence(&claimed).await?;
            // Python's finish order: settle and record cost, then persist and
            // complete. A cost-ledger failure leaves the execution open.
            if let Some(held) = reservation.as_mut() {
                held.settle(&agent_res).await;
            }
            self.record_cost(agent_input, &agent_res, model.as_ref())
                .await?;
            self.persist_result(agent_input, &agent_res).await?;
            // An interrupted run's `done` waits for `WAITING_FOR_INPUT`: a client
            // that answers it at once (an automatic client tool) must find the
            // execution waiting, or `resume` refuses the answer.
            let mut done = pending_done.take();
            if !agent_res.interrupted {
                self.publish_done(agent_input, done.take()).await?;
            }
            self.finalize_invocation(agent_input, &agent_res).await?;
            let status = if agent_res.interrupted {
                ExecutionStatus::WaitingForInput
            } else {
                ExecutionStatus::Completed
            };
            self.executions.finish(execution.id, status).await?;
            self.publish_done(agent_input, done).await?;
            let mut agent_res = agent_res;
            agent_res.execution_id = Some(execution.id);
            Ok(agent_res)
        })
        .await;
        if outcome.is_err() {
            if let Some(held) = reservation.as_mut() {
                held.release().await;
            }
        }
        outcome
    }

    /// Dispatch, stopping early on cancel, lease loss, or the execution timeout.
    pub(super) async fn dispatch_guarded(
        &self,
        input: &AgentInput,
        ctx: &AgentContext,
        execution: &ExecutionRow,
        stop: &mut tokio::sync::watch::Receiver<Option<StopReason>>,
        lease: Lease,
        pending_done: &ops::PendingDone,
    ) -> RiverbaseResult<super::dispatch::Dispatched> {
        // A deadline already passed times out at once.
        let remaining =
            execution
                .timeout_at
                .map_or(std::time::Duration::from_secs(u64::MAX / 4), |t| {
                    (t - chrono::Utc::now())
                        .to_std()
                        .unwrap_or(std::time::Duration::ZERO)
                });
        tokio::select! {
            result = self.dispatch_agent(input, ctx, execution, lease, pending_done) => result,
            reason = wait_for_stop(stop) => Err(reason.error(execution.id)),
            () = tokio::time::sleep(remaining) => {
                if let Err(e) = self.executions.timeout(execution.id).await {
                    tracing::warn!(error = %e, "marking execution timed out failed");
                }
                Err(StopReason::TimedOut.error(execution.id))
            }
        }
    }

    pub(super) async fn handle_failure(
        &self,
        input: &AgentInput,
        attempt_no: i32,
        err: &RiverbaseError,
    ) {
        let site = err.errcode.as_str();
        let resuming = input.actions.as_ref().is_some_and(|a| !a.is_empty());
        // Another worker owns this execution (or its saved run) now: leave its rows alone.
        if site == crate::BEM_376.errcode || site == crate::BEM_397.errcode {
            // A resume that lost to another resume of the execution (the rig
            // claim) still ends its own run.
            if resuming && site == crate::BEM_397.errcode {
                let _ = self
                    .store
                    .complete_run(input.run_id, RunStatus::Failed)
                    .await;
            }
            return;
        }
        // A resume refused before its actions were resolved: the execution keeps
        // waiting for a valid answer, and the actions stay pending. A revision
        // that cannot serve the resume refuses it the same way. An answer to
        // actions that are no longer pending (`BEM-066`: another resume took
        // them, or a stale duplicate) changes nothing either.
        let refused = [
            crate::BEM_066.errcode,
            crate::BEM_393.errcode,
            crate::BEM_399.errcode,
            crate::BEM_491.errcode,
            crate::BEM_493.errcode,
        ];
        if resuming && refused.contains(&site) {
            // The run ends first, so a refused resume deciding after this one
            // does not take it for a competitor still running.
            let _ = self
                .store
                .complete_run(input.run_id, RunStatus::Failed)
                .await;
            if let Some(execution_id) = input.execution_id {
                self.return_refused_resume(execution_id, input.run_id, attempt_no)
                    .await;
            }
            return;
        }
        if let Some(execution_id) = input.execution_id {
            // `fail` keeps a status cancel or timeout already set; a run stopped by
            // drain (cancelled here, not on the row) is recorded `FAILED`.
            let public = crate::public_errors::public_code(site).unwrap_or(site);
            if let Err(e) = self
                .executions
                .fail(execution_id, Some(public), Some(&err.errmesg))
                .await
            {
                tracing::warn!(error = %e, "recording the failed execution failed");
            }
        }
        let _ = self
            .store
            .complete_run(input.run_id, RunStatus::Failed)
            .await;
        if let Some(interaction_id) = input.interaction_id {
            let _ = self
                .store
                .complete_interaction(interaction_id, InteractionStatus::Failed)
                .await;
        }
        self.audit
            .log(
                AuditEvent::new(AuditAction::AgentFailed)
                    .agent_name(input.agent_name.clone())
                    .user_id(input.user_id())
                    .session_id(Some(input.session_id))
                    .run_id(Some(input.run_id))
                    .execution_id(input.execution_id)
                    .trace_id(beam_telemetry::current_span().map(|s| s.trace_id))
                    .status("error")
                    .error(Some(err.errmesg.clone())),
            )
            .await;
        // The dead letter, then the terminal `error` frame.
        self.dead_letter_failure(input, err).await;
        self.publish_failure(input, err).await;
    }

    /// A refused resume hands its execution back to waiting for input, unless
    /// another attempt has it: one still running, one that completed it (its
    /// `finish` follows), or a later one that has not created its run yet.
    /// Several refused resumes at once each end their run first, so the last
    /// to decide sees none of the others running.
    pub(super) async fn return_refused_resume(
        &self,
        execution_id: Uuid,
        run_id: Uuid,
        attempt_no: i32,
    ) {
        let execution = match self.executions.records().get(execution_id).await {
            Ok(Some(row)) if row.status == ExecutionStatus::Running => row,
            Ok(_) => return,
            Err(e) => {
                tracing::warn!(error = %e, "reading the refused resume's execution failed");
                return;
            }
        };
        let runs = match self.store.list_execution_runs(execution_id).await {
            Ok(runs) => runs,
            Err(e) => {
                tracing::warn!(error = %e, "listing the execution's runs failed");
                return;
            }
        };
        let current = execution.current_attempt;
        let taken = runs.iter().any(|r| {
            (r.status == RunStatus::Running && r.id != run_id) || r.status == RunStatus::Completed
        }) || (current != attempt_no && !runs.iter().any(|r| r.attempt_no == current));
        if taken {
            tracing::info!(%execution_id, "refused resume: another attempt has the execution");
            return;
        }
        // Only while no attempt started since this read.
        if let Err(e) = self
            .executions
            .return_to_waiting(execution_id, current)
            .await
        {
            tracing::warn!(error = %e, "returning the execution to waiting_for_input failed");
        }
    }
}
