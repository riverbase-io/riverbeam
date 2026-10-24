use std::sync::Arc;

use beam_agents::AgentRegistry;
use beam_audit::AuditLogger;
use beam_context::{
    ContextBuilder, ContextEnvelope, ContextManifest, ManifestStore, ProfileResolver,
    ResourceResolver,
};
use beam_execution::idempotency::IdempotencyScope;
use beam_execution::{ExecutionController, ExecutionRow, StopReason};
use beam_gateway::GatewayFactory;
use beam_knowledge::KnowledgeManager;
use beam_memory::TranscriptManager;
use beam_routing::ModelRouter;
use beam_runtime::{
    ensure_capability, AgentRuntime, ModelCaller, RuntimeExecutionRef, RuntimeRegistry,
    RuntimeResumeRequest, RuntimeStateStore, ToolExecutor,
};
use beam_security::{DisabledSecurityManager, SecurityManagement};
use beam_store::{agent_id_for, ActionRow, BeamStore};
use beam_stream::BeamStream;
use beam_tools::ToolService;
use beam_types::{
    AgentContext, AgentInput, AgentMessage, AgentResult, InvokeRequest, Modality, ModelConfig,
    ModelProvider,
};
use beam_usage::{CostStore, UsageGuard, UsagePlan, UsageTracker};
use chrono::Utc;
use serde_json::Value;
use uuid::Uuid;

use crate::config::BeamConfig;
use riverbase_core::RiverbaseResult;

mod access;
mod builder;
mod context;
mod dispatch;
mod invoke;
mod memory;
mod ops;
mod persist;
mod revisions;
mod security;
#[cfg(test)]
mod tests;
mod tool_policy;
mod usage;
mod wiring;

pub use builder::RiverBeamBuilder;
pub use revisions::{
    assert_public_schemas_compatible, narrowed_max_steps, spec_from_registration, validate_spec,
};

/// Central orchestrator — 7-phase invoke pipeline (in-memory; no SQL/security/usage).
pub struct RiverBeam {
    config: BeamConfig,
    registry: Arc<AgentRegistry>,
    router: ModelRouter,
    store: Arc<dyn BeamStore>,
    knowledge: Option<Arc<KnowledgeManager>>,
    /// Session transcript: every turn is saved
    /// here, and history and summaries are read from it. The memory settings
    /// are read from `config`.
    transcript: Arc<TranscriptManager>,
    context_builder: ContextBuilder,
    /// Context manifests; follows the store.
    manifests: Arc<dyn ManifestStore>,
    profile_resolver: Option<Arc<dyn ProfileResolver>>,
    resource_resolver: Option<Arc<dyn ResourceResolver>>,
    /// Quotas and plans, when configured.
    usage: Option<UsageAccounting>,
    cost_store: Arc<dyn CostStore>,
    /// Where model calls go: the host's caller, or a gateway per run.
    models: ModelSource,
    audit: Arc<AuditLogger>,
    security: Arc<dyn SecurityManagement>,
    beam_stream: Option<Arc<BeamStream>>,
    executions: Arc<ExecutionController>,
    /// Agents bound to a runtime; other agents dispatch through `registry`.
    runtimes: Arc<RuntimeRegistry>,
    runtime_states: Arc<dyn RuntimeStateStore>,
    /// The tool runtime and catalog, or a host's executor.
    tool_source: ToolSource,
    /// Stream outbox and dead letters; follows the store unless set.
    ops: Arc<dyn beam_store::OpsStore>,
    /// Legal holds; Postgres on a Postgres beam.
    holds: Arc<dyn beam_store::LegalHoldStore>,
    /// Agent revisions and execution pins; Postgres on a Postgres beam.
    revisions: Arc<beam_execution::revisions::RevisionBook>,
    revision_store: Option<beam_execution::PgRevisionStore>,
    eval_store: Arc<dyn beam_evals::EvaluationStore>,
}

/// Quota enforcement and tracking with their plans (Python
/// `UsageAccounting`): always all set, or none.
struct UsageAccounting {
    guard: Arc<UsageGuard>,
    tracker: Arc<UsageTracker>,
    /// Plans by name.
    plans: std::collections::HashMap<String, UsagePlan>,
    /// The plan a user without a profile gets.
    default_plan: Option<String>,
}

/// Where a run's model calls go (plan 021 D4: one or the other).
enum ModelSource {
    /// The host's caller ([`RiverBeamBuilder::model_caller`]), else `NoModel`.
    Caller(Arc<dyn ModelCaller>),
    /// A gateway built per run from the routed model chain.
    Gateway(Arc<GatewayFactory>),
}

impl ModelSource {
    /// The caller a runtime context starts with; with a gateway, `NoModel`
    /// until dispatch builds the run's gateway.
    fn fallback(&self) -> Arc<dyn ModelCaller> {
        match self {
            Self::Caller(caller) => caller.clone(),
            Self::Gateway(_) => Arc::new(beam_runtime::NoModel),
        }
    }
}

/// Where tool calls go: the catalog, a host's executor, or nowhere.
enum ToolSource {
    /// The tool runtime and its registry.
    Catalog(Arc<ToolService>),
    /// A host's executor without a catalog.
    Executor(Arc<dyn ToolExecutor>),
    None,
}

impl ToolSource {
    fn executor(&self) -> Arc<dyn ToolExecutor> {
        match self {
            Self::Catalog(service) => service.clone(),
            Self::Executor(executor) => executor.clone(),
            Self::None => Arc::new(beam_runtime::NoTools),
        }
    }

    fn catalog(&self) -> Option<&Arc<ToolService>> {
        match self {
            Self::Catalog(service) => Some(service),
            Self::Executor(_) | Self::None => None,
        }
    }

    /// Whether a run may call only the tools its context lists.
    fn restricts(&self) -> bool {
        matches!(self, Self::Catalog(_))
    }
}

/// An enforcing (or advisory) manager unless
/// security is off; decisions persist in `store` when one is given.
///
/// # Errors
/// `BEM-148` when the security rules fail to load (Python fails startup).
#[cfg(feature = "security-rules")]
fn build_security(
    config: &BeamConfig,
    policy: beam_security::SecurityPolicy,
    store: Option<Arc<dyn beam_security::SecurityStore>>,
) -> RiverbaseResult<Arc<dyn SecurityManagement>> {
    if !config.security_enabled {
        return Ok(Arc::new(DisabledSecurityManager));
    }
    let manager = beam_security::SecurityManager::new(policy).with_enforce(config.security_enforce);
    if !manager.is_ready() {
        return Err(crate::BEM_148.with_data("SECURITY_ENABLED is on"));
    }
    Ok(Arc::new(match store {
        Some(store) => manager.with_store(store),
        None => manager,
    }))
}

/// Built without the security rules (feature `security-rules` off): only
/// the no-op manager exists, so a beam with security enabled refuses to
/// start rather than run unchecked.
///
/// # Errors
/// `BEM-148` while `SECURITY_ENABLED` is on.
#[cfg(not(feature = "security-rules"))]
fn build_security(
    config: &BeamConfig,
    _policy: beam_security::SecurityPolicy,
    _store: Option<Arc<dyn beam_security::SecurityStore>>,
) -> RiverbaseResult<Arc<dyn SecurityManagement>> {
    if config.security_enabled {
        return Err(crate::BEM_148.with_data(
            "this build has no security rules (feature `security-rules` off); set SECURITY_ENABLED=0",
        ));
    }
    Ok(Arc::new(DisabledSecurityManager))
}

/// `AUDIT_SINKS` resolved key by key.
///
/// # Errors
/// `BEM-147` on an unknown sink key (Python fails startup the same way).
fn build_audit(
    config: &BeamConfig,
    sql: Option<&Arc<dyn beam_audit::AuditSink>>,
) -> RiverbaseResult<AuditLogger> {
    // Python builds no sinks when audit is off.
    if !config.audit_enabled {
        let logger = AuditLogger::new(Vec::new());
        logger.set_enabled(false);
        return Ok(logger);
    }
    let sinks = beam_audit::build_audit_sinks(&config.audit_sinks, &config.audit_file_path, sql)
        .map_err(|e| crate::BEM_147.with_data(e))?;
    let logger = AuditLogger::new(sinks);
    logger.set_enabled(config.audit_enabled);
    Ok(logger)
}

/// Opens `beam.model` around each model call of a run (Python's
/// `ModelGateway.complete` does), whatever caller the run uses. The messages
/// are kept only in `full_debug`. Every completed call's usage is added to
/// the attempt's meter, nested calls included (plan 023 D10).
struct TracedModel {
    inner: Arc<dyn ModelCaller>,
    meter: Arc<std::sync::Mutex<beam_types::TokenUsage>>,
}

impl ModelCaller for TracedModel {
    fn stream(&self, call: beam_runtime::ModelCall) -> beam_runtime::ModelStream {
        use futures_util::StreamExt;
        let mut attributes = serde_json::Map::new();
        attributes.insert(
            "model".into(),
            call.model.clone().map_or(Value::Null, Value::String),
        );
        let messages = if beam_telemetry::privacy_mode() == beam_telemetry::PrivacyMode::FullDebug {
            serde_json::to_value(&call.prepared.chat_history).unwrap_or(Value::Null)
        } else {
            Value::Null
        };
        attributes.insert("messages".into(), messages);
        let _span = beam_telemetry::start_span(beam_telemetry::SPAN_MODEL, None, &attributes);
        let meter = self.meter.clone();
        self.inner
            .stream(call)
            .inspect(move |item| {
                if let Ok(beam_runtime::ModelStreamItem::Completed(reply)) = item {
                    if let Ok(mut used) = meter.lock() {
                        used.add(&reply.token_usage());
                    }
                }
            })
            .boxed()
    }
}

/// The usage a dispatch settles (plan 023 D10): the larger of each field of
/// what the runtime reported and what went through the gateway, so a rig
/// run (which reports its gateway calls) is not counted twice, and a custom
/// runtime's nested calls are not lost.
fn settled_usage(
    reported: Option<beam_types::TokenUsage>,
    metered: &beam_types::TokenUsage,
) -> Option<beam_types::TokenUsage> {
    if *metered == beam_types::TokenUsage::default() {
        return reported;
    }
    Some(reported.map_or_else(|| metered.clone(), |r| r.max_fields(metered)))
}

/// The context one attempt runs with (Python `state.agent_context`,
/// `state.envelope` and `state.manifest`).
struct PreparedContext {
    agent: AgentContext,
    envelope: ContextEnvelope,
    manifest: ContextManifest,
    /// Citations of the selected knowledge.
    citations: Vec<Value>,
}

/// Identity resolved before the pipeline runs.
struct InvokeIdentity {
    idempotency: Option<IdempotencyScope>,
    request_snapshot: Option<Value>,
    /// Set once `ensure` assigns the execution id.
    assigned: Arc<std::sync::OnceLock<Uuid>>,
    /// The revision a new execution runs on instead of the traffic's pick
    /// (an eval case); never set from a request.
    revision: Option<Uuid>,
}

/// Forgets the run's stop signal when the attempt ends, including when the
/// invoke future is dropped mid-run. The lease is not released on drop: it
/// expires, and reconcile retries the execution.
struct AttemptGuard {
    executions: Arc<ExecutionController>,
    execution_id: Uuid,
    run_id: Uuid,
}

impl Drop for AttemptGuard {
    fn drop(&mut self) {
        self.executions.unwatch(self.execution_id, self.run_id);
    }
}

/// The run lease an attempt claimed, and the heartbeat renewing it. Dropping
/// it (the invoke future was dropped mid-run) drops the sender, which also
/// ends the heartbeat.
struct HeldLease {
    lease: beam_execution::Lease,
    heartbeat: tokio::sync::watch::Sender<bool>,
}

impl HeldLease {
    fn stop_heartbeat(&self) {
        let _ = self.heartbeat.send(true);
    }
}

/// What one attempt ends with: its result, and the lease it
/// claimed, if it got that far.
struct AttemptOutcome {
    result: RiverbaseResult<AgentResult>,
    held: Option<HeldLease>,
}

/// A usage reservation held for one attempt. It is settled once with the
/// tokens used, or released; dropped unsettled (the invoke was cancelled) it
/// is released from a task, so its tokens are not held until expiry.
struct HeldReservation {
    usage: Arc<UsageGuard>,
    id: Option<Uuid>,
    reserved: u64,
}

impl HeldReservation {
    /// Settle with the result's tokens, or the reserved amount when it reports
    /// none.
    async fn settle(&mut self, result: &AgentResult) {
        let Some(id) = self.id.take() else { return };
        let actual = result
            .token_usage
            .as_ref()
            .map(|u| u64::from(u.total_tokens))
            .filter(|t| *t > 0)
            .unwrap_or(self.reserved);
        if let Err(e) = self.usage.settle(id, actual).await {
            tracing::warn!(reservation_id = %id, error = %e, "usage settle failed");
        }
    }

    async fn release(&mut self) {
        let Some(id) = self.id.take() else { return };
        if let Err(e) = self.usage.release(id).await {
            tracing::warn!(reservation_id = %id, error = %e, "usage release failed");
        }
    }
}

impl Drop for HeldReservation {
    fn drop(&mut self) {
        let Some(id) = self.id.take() else { return };
        let usage = self.usage.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(beam_store::org_scope::propagate(async move {
                if let Err(e) = usage.release(id).await {
                    tracing::warn!(reservation_id = %id, error = %e, "usage release failed");
                }
            }));
        }
    }
}

/// What a replayed (already terminal) execution returns.
/// The execution's runtime ref, or the one `runtime` allocates for it when the
/// execution was created before its agent was bound.
fn execution_ref(
    runtime: &dyn AgentRuntime,
    execution: &ExecutionRow,
) -> RiverbaseResult<RuntimeExecutionRef> {
    execution
        .runtime_ref
        .as_ref()
        .filter(|v| !v.is_null())
        .map_or_else(
            || Ok(runtime.allocate_ref(execution.id)),
            RuntimeExecutionRef::from_value,
        )
}

/// The resume request for `input`'s actions. A checkpoint is only readable by
/// the runtime that wrote it, so a `LangGraph` checkpoint cannot resume on rig.
fn resume_request(
    runtime: &dyn AgentRuntime,
    input: &AgentInput,
    execution: &ExecutionRow,
) -> RiverbaseResult<RuntimeResumeRequest> {
    let runtime_ref = execution_ref(runtime, execution)?;
    if runtime_ref.runtime != runtime.runtime_name() {
        return Err(crate::BEM_392.with_data(format!(
            "execution was checkpointed by runtime '{}'; '{}' cannot resume it",
            runtime_ref.runtime,
            runtime.runtime_name()
        )));
    }
    ensure_capability(runtime, "resume")?;
    Ok(RuntimeResumeRequest {
        execution_id: execution.id,
        run_id: input.run_id,
        runtime_ref,
        actions: input.actions.clone().unwrap_or_default(),
        input: input.clone(),
    })
}

fn replay_result(input: &AgentInput) -> AgentResult {
    AgentResult {
        content: Vec::new(),
        agent_name: input.agent_name.clone().unwrap_or_default(),
        model_used: None,
        session_id: Some(input.session_id),
        request_id: input.request_id,
        interaction_id: input.interaction_id,
        execution_id: input.execution_id,
        token_usage: None,
        tool_calls: Vec::new(),
        metadata: Value::Object(serde_json::Map::new()),
        latency_ms: None,
        interrupted: false,
        action_required: None,
        citations: Vec::new(),
    }
}

/// Resolves when a stop reason is published; never resolves if the sender is gone.
async fn wait_for_stop(stop: &mut tokio::sync::watch::Receiver<Option<StopReason>>) -> StopReason {
    loop {
        if let Some(reason) = *stop.borrow_and_update() {
            return reason;
        }
        if stop.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

/// A submitted client-tool answer must come from a client that declared the
/// tool, before the action expires.
fn check_client_answer(
    row: &ActionRow,
    client: Option<&beam_types::ClientCapabilities>,
) -> RiverbaseResult<()> {
    let tool = row
        .tool_name
        .clone()
        .or_else(|| {
            row.payload
                .pointer("/request/payload/tool_name")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_default();
    if !client.is_some_and(|c| c.runs(&tool)) {
        return Err(crate::BEM_399.with_data(format!("resuming client did not declare {tool}")));
    }
    if row.expires_at.is_some_and(|at| at <= Utc::now()) {
        return Err(crate::BEM_399.with_data(format!("the {tool} action expired")));
    }
    Ok(())
}

/// `ActionResponseEnvelope.model_dump(mode="json")`: every field, in Python's order.
fn python_action_response(action: &beam_types::ActionResponseEnvelope) -> Value {
    let status = serde_json::to_value(action.status).unwrap_or(Value::Null);
    serde_json::json!({
        "action_id": action.action_id,
        "lc_action_id": action.lc_action_id,
        "kind": action.kind,
        "request_id": action.request_id,
        "status": status,
        "payload": action.payload,
        "errors": action.errors,
        "responder_id": action.responder_id,
        "responded_at": crate::pyjson::pydantic_datetime(action.responded_at),
    })
}

/// Python `categorize_error`, from the public code.
fn error_category(public: Option<&str>) -> &'static str {
    match public.unwrap_or_default() {
        "BEM-428" => "TIMEOUT",
        "BEM-427" => "CANCELLED",
        "BEM-425" | "BEM-426" | "BEM-429" => "CONFLICT",
        "BEM-430" | "BEM-423" | "BEM-424" => "RUNTIME",
        "BEM-422" => "MODEL",
        code if code.starts_with("BEM-4") => "BEAM",
        _ => "INTERNAL",
    }
}

/// `payload["memory_id"]`.
fn memory_id(payload: &Value) -> RiverbaseResult<String> {
    payload
        .get("memory_id")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| crate::BEM_004.with_data("memory_id is required"))
}

/// Python truthiness of a JSON value.
fn json_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().is_some_and(|f| f != 0.0),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

/// `message` stamped with the run's ids (Python sets them on every turn message).
fn turn_message(input: &AgentInput, mut message: AgentMessage) -> AgentMessage {
    message.run_id = Some(input.run_id);
    message.request_id = input.request_id;
    message.interaction_id = input.interaction_id;
    message.execution_id = input.execution_id;
    message
}

/// The envelope without the current turn, for agents that append the input
/// themselves (the legacy `Agent` trait and rig's prompt). The composer
/// orders the current turn last.
fn history_before_current(
    result: &beam_context::ContextBuildResult,
    input: &AgentInput,
) -> Vec<AgentMessage> {
    let current = format!("current:{}", input.run_id);
    let mut messages = result.envelope.messages.clone();
    let selected = result
        .manifest
        .decisions
        .iter()
        .any(|d| d.item_id == current && d.action == beam_context::DecisionAction::Selected);
    if selected {
        messages.pop();
    }
    messages
}

/// The transcript summary's token counter.
fn count_tokens(text: &str) -> usize {
    usize::try_from(beam_gateway::TokenCounter.count_text(text, None).tokens).unwrap_or(usize::MAX)
}

fn to_agent_input(req: InvokeRequest) -> AgentInput {
    AgentInput {
        content: req.content,
        session_id: req.session_id,
        interaction_id: req.interaction_id.or_else(|| Some(Uuid::new_v4())),
        request_id: req.request_id,
        run_id: Uuid::new_v4(),
        agent_name: Some(req.agent_name),
        model_override: req.model_override,
        metadata: req.metadata,
        auth_context: req.auth_context,
        stream: req.stream,
        actions: req.actions,
        context: req.context,
        execution_id: req.execution_id,
        runtime_ref: None,
        client: req.client,
    }
}

fn default_dev_models() -> Vec<ModelConfig> {
    vec![ModelConfig {
        name: "gpt-4o-mini".into(),
        provider: ModelProvider::Openai,
        provider_config: serde_json::json!({}),
        modalities: vec![Modality::Text],
        capabilities: vec![],
        cost_per_1k_tokens: 0.0,
        max_tokens: 4096,
        api_key: None,
        extra: serde_json::json!({}),
        context_window_tokens: None,
        credential_ref: None,
        max_output_tokens: None,
    }]
}

fn user_query_text(input: &AgentInput) -> String {
    input
        .content
        .iter()
        .filter_map(|p| p.text.as_deref())
        .collect::<Vec<_>>()
        .join(" ")
}

fn result_text(result: &AgentResult) -> String {
    result
        .content
        .iter()
        .filter_map(|p| p.text.as_deref())
        .collect::<Vec<_>>()
        .join(" ")
}
