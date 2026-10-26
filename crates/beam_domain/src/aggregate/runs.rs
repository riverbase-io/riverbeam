//! Agent runs: `invoke`, `stream`, `resume`, `cancel-execution`,
//! `retry-execution`.
//!
//! A run is never started here. `invoke`, `stream` and `resume` record the
//! session and queue an `invoke_agent` task carrying the full request and
//! the run's `execution_id`: a new id for a new run, the interrupted
//! execution's for a resume. A worker runs it and streams its frames.

use beam_store::tasks::{NewTask, WorkerTask};
use beam_types::{
    ActionResponseEnvelope, ClientCapabilities, ContentPart, InvokeAuthContext, InvokeRequest,
    RequestContext,
};
use riverbase_core::base::AggregateContext;
use riverbase_core::RiverbaseResult;
use serde_json::{json, Value};
use uuid::Uuid;

use super::{queued, BeamAggregate};
use crate::commands::{
    CancelExecutionPayload, InvokePayload, ResumePayload, RetryExecutionPayload, StreamPayload,
};
use beam_store::repo::domain::{DomainExec, DomainSql};

use crate::local::{sql, BeamSql, SessionRecord};

/// What `invoke`, `stream` and `resume` ask for, typed as the payloads
/// decode it: a malformed part is refused with `WEB-001` before
/// anything is read.
struct RunRequest {
    content: Vec<ContentPart>,
    session_id: Uuid,
    model_override: Option<String>,
    metadata: Option<Value>,
    request_id: Option<Uuid>,
    idempotency_key: Option<String>,
    context: Option<RequestContext>,
    /// The calling client's capabilities (Python rc8 `ClientCapabilities`).
    client: Option<ClientCapabilities>,
    /// What a resume continues; `None` for a new run.
    resume: Option<Resume>,
}

/// A resume: the action results, the interrupted interaction (found from the
/// actions when absent), and the execution the caller expects, if it names one.
struct Resume {
    interaction_id: Option<Uuid>,
    actions: Vec<ActionResponseEnvelope>,
    execution_id: Option<Uuid>,
}

/// `invoke` and `stream` resume a run when they carry actions (Python rc8's
/// shared `InvokeRequest`). The ids are accepted only with actions: a new run
/// under a caller's id would continue a row the caller may not own.
///
/// # Errors
/// `BEM-502` for an interaction or execution id without actions.
fn resume_of(
    actions: Option<Vec<ActionResponseEnvelope>>,
    interaction_id: Option<Uuid>,
    execution_id: Option<Uuid>,
) -> RiverbaseResult<Option<Resume>> {
    match actions.filter(|a| !a.is_empty()) {
        Some(actions) => Ok(Some(Resume {
            interaction_id,
            actions,
            execution_id,
        })),
        None if interaction_id.is_some() || execution_id.is_some() => Err(beam_core::BEM_502
            .with_data("interaction_id and execution_id are accepted only with actions")),
        None => Ok(None),
    }
}

/// The caller's ids (normalized), or none.
#[must_use]
pub fn invoke_auth_context(ctx: &AggregateContext) -> Option<InvokeAuthContext> {
    let normalize = |v: Option<Uuid>| {
        beam_execution::identity::normalize_identity(v.map(|u| u.to_string()).as_deref())
    };
    let auth = InvokeAuthContext {
        profile_id: normalize(ctx.actor.profile_id),
        user_id: normalize(ctx.actor.user_id),
        org_id: normalize(ctx.organization_id),
        mcp_token: None,
    };
    (auth.profile_id.is_some() || auth.user_id.is_some() || auth.org_id.is_some()).then_some(auth)
}

/// The task payload, inside a `beam.command` span whose `traceparent` rides
/// in the request metadata, so the worker's `beam.execution` continues it.
///
/// # Errors
/// `BEM-001` when the request does not serialize.
pub fn serialize_request(mut request: InvokeRequest) -> RiverbaseResult<Value> {
    // A queued request never carries a credential: the worker
    // mints MCP tokens itself.
    if let Some(auth) = request.auth_context.as_mut() {
        auth.mcp_token = None;
    }
    let span =
        beam_telemetry::start_span(beam_telemetry::SPAN_COMMAND, None, &serde_json::Map::new());
    let mut metadata = request.metadata.as_object().cloned().unwrap_or_default();
    beam_telemetry::inject(&mut metadata, &span);
    request.metadata = Value::Object(metadata);
    serde_json::to_value(&request).map_err(|e| beam_core::BEM_001.with_data(e.to_string()))
}

/// The first five words of the first text part.
fn session_name(request: &InvokeRequest) -> String {
    request
        .content
        .iter()
        .find_map(|p| p.text.as_deref().filter(|t| !t.is_empty()))
        .map(|t| t.split_whitespace().take(5).collect::<Vec<_>>().join(" "))
        .unwrap_or_default()
}

/// The run an idempotency key already started (or queued) for this caller
/// and agent: `{execution_id, request_id}`. Takes the key's advisory lock
/// first (the worker takes the same one), so two commands with one key
/// cannot both queue a run.
async fn existing_run<R: DomainExec + ?Sized>(
    db: &R,
    key: &str,
    auth: Option<&InvokeAuthContext>,
    agent_name: &str,
) -> RiverbaseResult<Option<Value>> {
    let Some(scope) = beam_execution::idempotency::normalize_idempotency_scope(
        Some(key),
        auth.and_then(|a| a.user_id.as_deref()),
        agent_name,
        auth.and_then(|a| a.org_id.as_deref()),
    ) else {
        return Ok(None);
    };
    let Some(user) = scope.sql_user_id() else {
        return Ok(None);
    };
    db.lock_idempotency(scope.lock_parts()).await.map_err(sql)?;
    let started = db
        .started_run(&scope.key, &user.to_string(), &scope.agent_name)
        .await
        .map_err(sql)?;
    if started.is_some() {
        return Ok(started);
    }
    // Queued but not started yet: the same task answers.
    db.queued_run(&scope.key, agent_name, scope.user_id.clone())
        .await
        .map_err(sql)
}

/// The interaction the first answered action belongs to, in `session`.
///
/// # Errors
/// `BEM-062` for actions without an action id; `BEM-501` for an action not
/// in the session.
async fn action_interaction<R: DomainExec + ?Sized>(
    db: &R,
    actions: &[ActionResponseEnvelope],
    session: Uuid,
) -> RiverbaseResult<Uuid> {
    let action = actions
        .first()
        .and_then(|a| Uuid::parse_str(&a.action_id).ok())
        .ok_or_else(|| beam_core::BEM_062.raise())?;
    DomainSql::action_interaction(db, action, session)
        .await
        .map_err(sql)?
        .and_then(|row| {
            row.get("interaction_id")
                .and_then(Value::as_str)
                .and_then(|id| Uuid::parse_str(id).ok())
        })
        .ok_or_else(|| beam_core::BEM_501.with_data(json!({"action_id": action})))
}

/// The interrupted execution and interaction `resume` continues, in `session`.
///
/// # Errors
/// As [`action_interaction`] and [`interrupted_execution`]; `BEM-502` when the
/// caller names another execution than the interaction's.
async fn resume_target<R: DomainExec + ?Sized>(
    db: &R,
    resume: &Resume,
    session: Uuid,
) -> RiverbaseResult<(Uuid, Uuid)> {
    let interaction = match resume.interaction_id {
        Some(id) => id,
        None => action_interaction(db, &resume.actions, session).await?,
    };
    let execution = interrupted_execution(db, interaction, session).await?;
    if resume.execution_id.is_some_and(|id| id != execution) {
        return Err(beam_core::BEM_502.with_data("the actions belong to another execution"));
    }
    Ok((execution, interaction))
}

/// The execution a resume continues: the one behind `interaction` in
/// `session`, waiting for input.
///
/// # Errors
/// `BEM-501` for an interaction not in the session; `BEM-502` for an
/// execution that is not waiting for input, or that another resume is
/// already continuing.
async fn interrupted_execution<R: DomainExec + ?Sized>(
    db: &R,
    interaction: Uuid,
    session: Uuid,
) -> RiverbaseResult<Uuid> {
    let found = db
        .interaction_execution(interaction, session)
        .await
        .map_err(sql)?
        .ok_or_else(|| beam_core::BEM_501.with_data(json!({"interaction_id": interaction})))?;
    if found.get("status").and_then(Value::as_str) != Some("WAITING_FOR_INPUT") {
        return Err(
            beam_core::BEM_502.with_data("the interaction's execution is not waiting for input")
        );
    }
    if found.get("busy").and_then(Value::as_bool) == Some(true) {
        return Err(
            beam_core::BEM_502.with_data("another resume of this execution is queued or running")
        );
    }
    found
        .get("execution_id")
        .and_then(Value::as_str)
        .and_then(|id| Uuid::parse_str(id).ok())
        .ok_or_else(|| beam_core::BEM_501.with_data(json!({"interaction_id": interaction})))
}

impl BeamAggregate {
    /// `invoke`: queue the run and return its ids. The `/invoke` alias waits
    /// for the result outside the command. With `actions` it resumes the
    /// interrupted run, as [`Self::resume`] does (Python rc8).
    ///
    /// # Errors
    /// Unknown agent `BEM-404`; another user's session `BEM-412`; a bad
    /// request `WEB-001`; an interaction or execution id without actions
    /// `BEM-400`; a resume's errors as [`Self::resume`]; store errors.
    pub async fn invoke(&mut self, p: InvokePayload) -> RiverbaseResult<Value> {
        self.queue_run(RunRequest {
            content: p.content,
            session_id: p.session_id,
            model_override: p.model_override,
            metadata: p.metadata,
            request_id: p.request_id,
            idempotency_key: p.idempotency_key,
            context: p.context,
            client: p.client,
            resume: resume_of(p.actions, p.interaction_id, p.execution_id)?,
        })
        .await
    }

    /// `stream`: queue the run and return its ids; frames arrive over SSE.
    /// With `actions` it resumes the interrupted run (agent-app's resume).
    ///
    /// # Errors
    /// As [`Self::invoke`].
    pub async fn stream(&mut self, p: StreamPayload) -> RiverbaseResult<Value> {
        self.queue_run(RunRequest {
            content: p.content,
            session_id: p.session_id,
            model_override: p.model_override,
            metadata: p.metadata,
            request_id: p.request_id,
            idempotency_key: p.idempotency_key,
            context: p.context,
            client: p.client,
            resume: resume_of(p.actions, p.interaction_id, p.execution_id)?,
        })
        .await
    }

    /// `resume`: queue the continuation of an interrupted run in the
    /// caller's session.
    ///
    /// # Errors
    /// As [`Self::invoke`]; `BEM-404` for an interaction not in the
    /// session; `BEM-400` when its execution is not waiting for input or
    /// another resume of it is queued or running.
    pub async fn resume(&mut self, p: ResumePayload) -> RiverbaseResult<Value> {
        self.queue_run(RunRequest {
            content: p.content,
            session_id: p.session_id,
            model_override: p.model_override,
            metadata: p.metadata,
            request_id: p.request_id,
            idempotency_key: None,
            context: p.context,
            client: p.client,
            resume: Some(Resume {
                interaction_id: Some(p.interaction_id),
                actions: p.actions,
                execution_id: None,
            }),
        })
        .await
    }

    async fn queue_run(&mut self, run: RunRequest) -> RiverbaseResult<Value> {
        let caller = self.caller();
        let identifier = self.identifier();
        let agent = self.db().agent(&identifier).await?;
        let agent_name = agent
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or(identifier.as_str())
            .to_owned();
        let agent_id = agent
            .get("_id")
            .and_then(Value::as_str)
            .and_then(|s| Uuid::parse_str(s).ok())
            .unwrap_or_else(|| beam_store::agent_id_for(&agent_name));
        let auth = invoke_auth_context(self.context());

        if let Some(key) = run.idempotency_key.as_deref() {
            if let Some(mut first) =
                existing_run(&self.db(), key, auth.as_ref(), &agent_name).await?
            {
                first["session_id"] = json!(run.session_id);
                first["agent_name"] = json!(agent_name);
                first["status"] = json!("existing");
                return Ok(first);
            }
        }

        let (execution_id, interaction_id, actions) = match run.resume {
            Some(resume) => {
                self.db()
                    .owned_session(&run.session_id.to_string(), &caller)
                    .await?;
                let (execution, interaction) =
                    resume_target(&self.db(), &resume, run.session_id).await?;
                (execution, Some(interaction), Some(resume.actions))
            }
            None => (Uuid::new_v4(), None, None),
        };
        let request_id = run.request_id.unwrap_or_else(Uuid::new_v4);
        // Only the worker's client-tool expiry sweep may mark its resumes
        // (beam_core `CLIENT_TOOL_EXPIRY`); a caller's flag is dropped.
        let mut metadata = run.metadata;
        if let Some(fields) = metadata.as_mut().and_then(Value::as_object_mut) {
            fields.remove("beam_client_tool_expiry");
        }
        // Every queued run publishes its frames (the aliases wait on them).
        let request = InvokeRequest {
            agent_name: agent_name.clone(),
            content: run.content,
            session_id: run.session_id,
            model_override: run.model_override,
            metadata: metadata.unwrap_or(Value::Null),
            interaction_id,
            execution_id: Some(execution_id),
            request_id: Some(request_id),
            idempotency_key: run.idempotency_key,
            stream: true,
            actions,
            auth_context: auth,
            context: run.context,
            client: run.client,
        };

        if interaction_id.is_none() {
            let text = |k: &str| {
                request
                    .metadata
                    .get(k)
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|v| !v.is_empty())
                    .map(str::to_owned)
            };
            let auth = request.auth_context.as_ref();
            let record = SessionRecord {
                session_id: request.session_id,
                agent_id,
                user_id: auth.and_then(|a| a.user_id.clone()),
                org_id: auth.and_then(|a| a.org_id.clone()),
                profile_id: auth.and_then(|a| a.profile_id.clone()),
                name: session_name(&request),
                resource: text("resource"),
                resource_id: text("resource_id"),
            };
            self.db().record_session(&record, &caller).await?;
        }

        let task = NewTask {
            execution_id: Some(execution_id),
            ..NewTask::of(WorkerTask::InvokeAgent, serialize_request(request)?)
        };
        let task_id = self.enqueue(task).await?;
        Ok(json!({
            "task_id": task_id,
            "execution_id": execution_id,
            "request_id": request_id,
            "session_id": run.session_id,
            "agent_name": agent_name,
            "status": "queued",
        }))
    }

    /// `cancel-execution`: a run still queued is cancelled in one statement,
    /// so a worker claiming it at the same moment either gets it first (and
    /// the cancel is queued) or never sees it. No pipeline ends a run
    /// cancelled while queued, so an `end_run` task publishes its terminal
    /// frame. A started run is cancelled by a worker task.
    ///
    /// # Errors
    /// `BEM-412` for another user's run; `BEM-404` for an unknown one.
    pub async fn cancel_execution(&mut self, p: CancelExecutionPayload) -> RiverbaseResult<Value> {
        let caller = self.caller();
        let execution = p.execution_id.to_string();
        let cancelled = self
            .db()
            .cancel_queued_invoke(&execution, caller.admin, caller.user_id.clone())
            .await
            .map_err(sql)?;
        if let Some(task) = cancelled {
            // A task requeued after its worker died has an execution row: cancel that too.
            if task
                .get("attempt_count")
                .and_then(Value::as_i64)
                .unwrap_or(0)
                > 0
            {
                self.enqueue(NewTask {
                    execution_id: Some(p.execution_id),
                    ..NewTask::of(
                        WorkerTask::CancelExecution,
                        json!({"execution_id": execution, "org_id": caller.org_id}),
                    )
                })
                .await?;
            }
            let run = &task["payload"];
            self.enqueue(NewTask {
                execution_id: Some(p.execution_id),
                ..NewTask::of(
                    WorkerTask::EndRun,
                    json!({
                        "session_id": run.get("session_id"),
                        "request_id": run.get("request_id"),
                        "execution_id": execution,
                        "agent_name": run.get("agent_name"),
                        "org_id": caller.org_id,
                        "reason": "cancelled",
                    }),
                )
            })
            .await?;
            return Ok(json!({"execution_id": execution, "status": "cancelled"}));
        }
        // Still queued, but another user's.
        let foreign = self.db().invoke_queued(&execution).await.map_err(sql)?;
        if foreign {
            return Err(beam_core::BEM_071.with_data("agent_execution belongs to another user"));
        }
        self.db().writable_execution(&execution, &caller).await?;
        let task_id = self
            .enqueue(NewTask {
                execution_id: Some(p.execution_id),
                ..NewTask::of(
                    WorkerTask::CancelExecution,
                    json!({"execution_id": execution, "org_id": caller.org_id}),
                )
            })
            .await?;
        let mut out = queued(task_id, "cancel_execution");
        out["execution_id"] = json!(execution);
        Ok(out)
    }

    /// `retry-execution`: a worker runs the failed execution's stored
    /// request again.
    ///
    /// # Errors
    /// `BEM-412` for another user's execution; `BEM-404` for an unknown one.
    pub async fn retry_execution(&mut self, p: RetryExecutionPayload) -> RiverbaseResult<Value> {
        let caller = self.caller();
        let execution = p.execution_id.to_string();
        self.db().writable_execution(&execution, &caller).await?;
        let task = NewTask {
            execution_id: Some(p.execution_id),
            retryable: false,
            max_attempts: 1,
            ..NewTask::of(
                WorkerTask::RetryExecution,
                json!({"execution_id": execution, "reason": p.reason, "org_id": caller.org_id}),
            )
        };
        let task_id = self.enqueue(task).await?;
        let mut out = queued(task_id, "retry_execution");
        out["execution_id"] = json!(execution);
        Ok(out)
    }
}
