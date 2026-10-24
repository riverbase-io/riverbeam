//! `RigRuntime`: Beam drives rig's sans-I/O `AgentRun` (replaces `LangGraph`, D1).
//!
//! Beam owns every effect: model turns go through the [`ModelCaller`] seam,
//! tool calls through the [`ToolExecutor`] seam, and the serialized run is
//! saved after every step. A tool that needs approval, or that the calling
//! client runs, holds the run: it is saved with its pending calls, and a resume
//! on any worker supplies the human's decision or the client's output as the
//! tool result (rig's `ExecutingTools` state carries the calls).
//!
//! [`ModelCaller`]: crate::seams::ModelCaller
//! [`ToolExecutor`]: crate::seams::ToolExecutor

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use beam_types::tool_policy::{
    ask_rules, RememberOption, ToolDecision, ToolLayer, ToolMode, ToolScope,
};
use beam_types::{
    ActionResponseStatus, AgentInput, AgentMessage, AgentMessageRole, AgentMessageType,
    ContentPart, TokenUsage, ToolCallRecord,
};
use futures_util::StreamExt;
use rig_agent::run::{
    prepare_request, AgentRun, AgentRunStep, ModelTurn, ModelTurnOutcome, PendingToolCall,
    PromptError, RunEntry, RunSpec, RUN_FORMAT,
};
use rig_core::completion::{Message, ProviderCapabilities};
use rig_core::message::{AssistantContent, AssistantMessage, ToolResultContent, UserContent};
use riverbase_core::RiverbaseResult;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use uuid::Uuid;

use crate::actions::{
    build_action_request, validate_action_request, validate_action_response, ActionRequestArgs,
    CLIENT_TOOL_KIND, TOOL_APPROVAL_KIND,
};
use crate::compat::{assert_checkpoint_readable, CheckpointVersions, CompatibilityPolicy};
use crate::content::{user_content, user_message};
use crate::contract::{
    AgentRuntime, EventStream, RuntimeCapabilities, RuntimeContext, RuntimeExecutionRef,
    RuntimeResumeRequest, RuntimeStartRequest, RuntimeState,
};
use crate::events::{
    agent_result, ai_message, done_event, drive, token_usage, Emitter, RuntimeEventType,
};
use crate::seams::{
    ClientAnswer, ModelCall, ModelReply, ModelStreamItem, ToolInfo, ToolInvocation,
};
use crate::state::{status, RuntimeStateRecord, RuntimeStateStore};

pub const RIG_RUNTIME_NAME: &str = "rig";
/// Version of Beam's use of the run envelope (entries, ref shape).
pub const RIG_ADAPTER_VERSION: &str = "1.0.0";
/// The pinned rig release; must match `rig-agent` in this crate's manifest.
pub const RIG_LIBRARY_VERSION: &str = "rig-agent-0.44.0";
/// Run entry kind recording the calls a run is held on.
const HELD_KIND: &str = "beam.held_calls";

fn default_max_turns() -> usize {
    8
}

/// What an app team writes when it rewrites a `LangGraph` graph as one agent loop.
/// An `AgentSpec` compiles from it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RigAgentDef {
    pub name: String,
    #[serde(default)]
    pub preamble: Option<String>,
    /// Model alias for the gateway.
    #[serde(default)]
    pub model: Option<String>,
    /// Tools the agent may call (the allowlist), in advertised order.
    #[serde(default)]
    pub tools: Vec<String>,
    /// Model-call budget for one execution.
    #[serde(default = "default_max_turns")]
    pub max_turns: usize,
    #[serde(default)]
    pub temperature: Option<f64>,
    #[serde(default)]
    pub max_tokens: Option<u64>,
    /// Tools whose every call waits for a human, on top of the tool runtime's own rule.
    #[serde(default)]
    pub approval_required: Vec<String>,
    /// JSON schema of the final answer, when the agent returns structured output.
    #[serde(default)]
    pub output_schema: Option<Value>,
}

impl RigAgentDef {
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            preamble: None,
            model: None,
            tools: Vec::new(),
            max_turns: default_max_turns(),
            temperature: None,
            max_tokens: None,
            approval_required: Vec::new(),
            output_schema: None,
        }
    }

    fn spec(&self) -> RunSpec {
        RunSpec {
            preamble: self.preamble.clone(),
            temperature: self.temperature,
            max_tokens: self.max_tokens,
            max_turns: Some(self.max_turns),
            output_schema: self.output_schema.clone(),
            ..RunSpec::new()
        }
    }
}

/// Why a call holds the run.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum HoldKind {
    /// A human approves it (`beam.tool_approval`). Runs held before client
    /// tools existed have no kind and are approvals.
    #[default]
    Approval,
    /// The calling client runs it (`beam.client_tool`).
    Client,
}

/// A call the run is held on, recorded in the run's entries.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct HeldCall {
    action_id: Uuid,
    request_id: String,
    call_id: String,
    tool: String,
    #[serde(default)]
    kind: HoldKind,
    /// What the approval may remember, from the decision that held the call
    /// (plan 023 C4). Runs held before then have none: once only.
    #[serde(default)]
    remember_options: Vec<RememberOption>,
    #[serde(default)]
    layer: Option<ToolLayer>,
    #[serde(default)]
    locked: bool,
}

enum Decision {
    /// Run the call, with the human's edited arguments when given, and store
    /// what the approval remembers.
    Approve {
        remember: RememberOption,
        edited_args: Option<Value>,
    },
    Deny(String),
    /// The call is not run; the text is the model's tool result.
    Refuse(String),
    /// The client ran the call and answered.
    Client(ClientAnswer),
}

/// rc8's tool-result errors for client tools the client did not run.
const CLIENT_DECLINED: &str = "declined by user";
const CLIENT_EXPIRED: &str = "client tool expired: no capable client responded";

/// A refusal as the model sees a failed tool (`{"error": ...}`, as `beam_tools` reports one).
fn refusal(text: &str) -> Decision {
    Decision::Refuse(beam_types::pyjson::dumps_unicode(&json!({"error": text})))
}

/// The rig agent-loop runtime for one [`RigAgentDef`].
#[derive(Clone)]
pub struct RigRuntime {
    def: Arc<RigAgentDef>,
    states: Arc<dyn RuntimeStateStore>,
}

impl RigRuntime {
    #[must_use]
    pub fn new(def: RigAgentDef, states: Arc<dyn RuntimeStateStore>) -> Self {
        Self {
            def: Arc::new(def),
            states,
        }
    }

    #[must_use]
    pub fn def(&self) -> &RigAgentDef {
        &self.def
    }
}

fn state_id(runtime_ref: &RuntimeExecutionRef) -> RiverbaseResult<Uuid> {
    runtime_ref
        .reference
        .get("state_id")
        .and_then(Value::as_str)
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(|| crate::BEM_392.with_data("runtime ref has no state_id"))
}

fn prompt_error(e: &PromptError) -> riverbase_core::RiverbaseError {
    match e {
        PromptError::MaxTurns { max_turns, .. } => {
            crate::BEM_395.with_data(format!("max_turns = {max_turns}"))
        }
        other => crate::BEM_394.with_data(other.to_string()),
    }
}

fn text_of(content: &[ContentPart]) -> String {
    content
        .iter()
        .filter_map(|p| p.text.as_deref())
        .collect::<Vec<_>>()
        .join("\n")
}

/// Beam's conversation history as rig messages. Only plain human and AI turns
/// carry over (human turns with their images); tool traffic and actions stay
/// in Beam's transcript.
fn rig_history(history: &[AgentMessage]) -> Vec<Message> {
    history
        .iter()
        .filter(|m| m.msg_type == AgentMessageType::Message)
        .filter_map(|m| match m.role {
            AgentMessageRole::Human => {
                let content = user_content(&m.content, "\n");
                (!content.is_empty()).then_some(Message::User { content })
            }
            AgentMessageRole::Ai => {
                let text = text_of(&m.content);
                (!text.is_empty()).then(|| Message::assistant(text))
            }
            AgentMessageRole::System | AgentMessageRole::Tool => None,
        })
        .collect()
}

/// The definition's run spec with the context envelope's system text
/// (instructions, the workflow marker) appended to the preamble. The envelope
/// is the only history the run gets; there is no rig-side memory.
fn spec_with_context(def: &RigAgentDef, history: &[AgentMessage]) -> RunSpec {
    let mut spec = def.spec();
    let system: Vec<String> = history
        .iter()
        .filter(|m| m.role == AgentMessageRole::System)
        .map(|m| text_of(&m.content))
        .filter(|t| !t.is_empty())
        .collect();
    if !system.is_empty() {
        let parts: Vec<String> = spec.preamble.take().into_iter().chain(system).collect();
        spec.preamble = Some(parts.join("\n\n"));
    }
    spec
}

/// One start or resume of a run.
struct Driver {
    def: Arc<RigAgentDef>,
    /// The saved run this driver reads and writes (the ref's `state_id`).
    state_id: Uuid,
    states: Arc<dyn RuntimeStateStore>,
    ctx: RuntimeContext,
    input: AgentInput,
    execution_id: Uuid,
    run_id: Uuid,
    tool_records: Vec<ToolCallRecord>,
    usage: TokenUsage,
    model_used: Option<String>,
}

impl Driver {
    async fn hold_of(&self, call: &PendingToolCall) -> Option<(HoldKind, ToolDecision)> {
        hold_of(&self.def, &self.ctx, &self.input, call).await
    }

    fn allowed_tools(&self) -> Vec<String> {
        if self.ctx.available_tools.is_empty() && !self.ctx.restrict_tools {
            return self.def.tools.clone();
        }
        self.def
            .tools
            .iter()
            .filter(|t| self.ctx.available_tools.contains(t))
            .cloned()
            .collect()
    }

    async fn save(&self, run: &AgentRun, run_status: &str) -> RiverbaseResult<()> {
        let state =
            serde_json::to_value(run).map_err(|e| crate::BEM_396.with_data(e.to_string()))?;
        let record = RuntimeStateRecord {
            state_id: self.state_id,
            execution_id: self.execution_id,
            run_id: self.run_id,
            runtime_name: RIG_RUNTIME_NAME.into(),
            run_format: i32::try_from(RUN_FORMAT).unwrap_or(i32::MAX),
            adapter_version: RIG_ADAPTER_VERSION.into(),
            library_version: RIG_LIBRARY_VERSION.into(),
            status: run_status.into(),
            turn: i32::try_from(run.turn()).unwrap_or(i32::MAX),
            state,
            updated_at: chrono::Utc::now(),
        };
        self.states.save(&record, self.ctx.lease.as_ref()).await
    }

    async fn call_model(
        &mut self,
        emitter: &mut Emitter,
        call: ModelCall,
    ) -> RiverbaseResult<ModelReply> {
        let mut stream = self.ctx.model_gateway.stream(call);
        let mut reply = None;
        while let Some(item) = stream.next().await {
            match item? {
                ModelStreamItem::TextDelta(delta) => {
                    let mut event = emitter.event(RuntimeEventType::MessageDelta);
                    event.message = Some(ai_message(&self.input, &delta, true));
                    emitter.emit(event);
                }
                ModelStreamItem::Completed(done) => reply = Some(done),
            }
        }
        let reply =
            reply.ok_or_else(|| crate::BEM_390.with_data("model stream ended without a reply"))?;
        let used = token_usage(&reply.usage);
        self.usage.add(&used);
        if reply.model_used.is_some() {
            self.model_used.clone_from(&reply.model_used);
        }
        let mut usage = emitter.event(RuntimeEventType::Usage);
        usage.usage = Some(used);
        emitter.emit(usage);
        Ok(reply)
    }

    /// Answer every pending call: preresolved results as given, held calls by
    /// the human's decision, the rest by running the tool.
    async fn run_tools(
        &mut self,
        emitter: &mut Emitter,
        calls: &[PendingToolCall],
        decisions: &HashMap<String, Decision>,
    ) -> Vec<UserContent> {
        let mut results = Vec::with_capacity(calls.len());
        for call in calls {
            if let Some(preresolved) = &call.preresolved_result {
                results.push(preresolved.clone());
                continue;
            }
            let call_id = call.tool_call.id.to_string();
            let name = call.tool_call.function.name.to_string();
            // The human's edited arguments replace the model's; the tool
            // runtime checks them (`BEFORE_TOOL`, the schema) as any others.
            let args = match decisions.get(&call_id) {
                Some(Decision::Approve {
                    edited_args: Some(edited),
                    ..
                }) => edited.clone(),
                _ => call.tool_call.function.arguments_value(),
            };
            let refused = match decisions.get(&call_id) {
                Some(Decision::Deny(reason)) => {
                    Some(format!("The user denied this tool call: {reason}"))
                }
                Some(Decision::Refuse(text)) => Some(text.clone()),
                _ => None,
            };
            if let Some(text) = refused {
                self.record_tool(&name, &args, &call_id, &text, None);
                results.push(UserContent::ToolResult(
                    call.tool_call.result(vec![ToolResultContent::text(text)]),
                ));
                continue;
            }
            let mut started = emitter.event(RuntimeEventType::ToolStarted);
            started.payload = tool_payload(&name, &call_id, &args);
            emitter.emit(started);
            let clock = Instant::now();
            let invocation = ToolInvocation {
                execution_id: self.execution_id,
                run_id: self.run_id,
                call_id: call_id.clone(),
                name: name.clone(),
                args: args.clone(),
                input: Some(Arc::new(self.input.clone())),
                allowed: Some(self.allowed_tools()),
                dry_run: self
                    .input
                    .metadata
                    .get("dry_run")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                approved: matches!(decisions.get(&call_id), Some(Decision::Approve { .. })),
            };
            let outcome = match decisions.get(&call_id) {
                Some(Decision::Client(answer)) => {
                    self.ctx
                        .tool_runtime
                        .client_result(invocation, answer.clone())
                        .await
                }
                _ => self.ctx.tool_runtime.execute(invocation).await,
            };
            let latency = clock.elapsed().as_secs_f64() * 1000.0;
            let (kind, text, result) = match outcome {
                Ok(out) => (
                    RuntimeEventType::ToolCompleted,
                    out.clone(),
                    call.tool_call.result(vec![ToolResultContent::text(out)]),
                ),
                Err(e) => (
                    RuntimeEventType::ToolFailed,
                    e.errmesg.clone(),
                    call.tool_call
                        .error_result(vec![ToolResultContent::text(e.errmesg)]),
                ),
            };
            let mut finished = emitter.event(kind);
            finished.payload = tool_payload(&name, &call_id, &args);
            finished
                .payload
                .insert("result".into(), Value::String(text.clone()));
            emitter.emit(finished);
            let mut message = AgentMessage {
                role: AgentMessageRole::Tool,
                msg_type: AgentMessageType::ToolResult,
                content: vec![ContentPart::text(text.clone())],
                tool_call_id: Some(call_id.clone()),
                tool_name: Some(name.clone()),
                tool_args: Some(args.clone()),
                tool_result: Some(Value::String(text.clone())),
                ..ai_message(&self.input, "", false)
            };
            message.content = vec![ContentPart::text(text.clone())];
            let mut completed = emitter.event(RuntimeEventType::MessageCompleted);
            completed.message = Some(message);
            emitter.emit(completed);
            self.record_tool(&name, &args, &call_id, &text, Some(latency));
            results.push(UserContent::ToolResult(result));
        }
        results
    }

    fn record_tool(
        &mut self,
        name: &str,
        args: &Value,
        call_id: &str,
        result: &str,
        latency: Option<f64>,
    ) {
        self.tool_records.push(ToolCallRecord {
            tool_name: name.to_owned(),
            args: args.clone(),
            tool_call_id: Some(call_id.to_owned()),
            result: Some(result.to_owned()),
            latency_ms: latency,
            usage_metadata: None,
            step_id: None,
            result_step_id: None,
        });
    }

    /// Hold the run on its approval-gated and client calls: save it and end with
    /// an interrupted result carrying one Python-shaped action entry per call.
    async fn hold(
        &mut self,
        mut run: AgentRun,
        emitter: &mut Emitter,
        calls: &[PendingToolCall],
        holds: Vec<Option<(HoldKind, ToolDecision)>>,
    ) -> RiverbaseResult<()> {
        let mut held = Vec::new();
        let mut entries = Vec::new();
        let now = chrono::Utc::now();
        for (call, (kind, decision)) in calls
            .iter()
            .zip(holds)
            .filter_map(|(c, hold)| hold.map(|hold| (c, hold)))
        {
            let call_id = call.tool_call.id.to_string();
            let name = call.tool_call.function.name.to_string();
            let action_id = Uuid::new_v4();
            let request = hold_request(&self.ctx, call, kind, &decision, now)?;
            let entry = json!({
                "lc_action_id": call_id,
                "action_id": action_id.to_string(),
                "action_request": request,
            });
            let mut message = ai_message(&self.input, &request.title, false);
            message.msg_type = AgentMessageType::Action;
            message.metadata =
                json!({"step_status": "action_required", "actions": [entry.clone()]});
            let mut event = emitter.event(RuntimeEventType::ActionRequired);
            event.message = Some(message);
            event.action = Some(entry.clone());
            emitter.emit(event);
            held.push(HeldCall {
                action_id,
                request_id: request.request_id.clone(),
                call_id,
                tool: name,
                kind,
                remember_options: decision.remember_options(),
                layer: Some(decision.layer),
                locked: decision.locked,
            });
            entries.push(entry);
        }
        run.append_entry(RunEntry {
            kind: HELD_KIND.into(),
            turn: run.turn(),
            value: serde_json::to_value(&held).unwrap_or(Value::Null),
        });
        self.save(&run, status::INTERRUPTED).await?;
        let mut result = agent_result(&self.input, self.execution_id, "", self.usage.clone());
        result.content = Vec::new();
        result.interrupted = true;
        result.action_required = Some(entries);
        result.tool_calls = std::mem::take(&mut self.tool_records);
        result.model_used.clone_from(&self.model_used);
        let done = done_event(emitter, &result);
        emitter.emit(done);
        Ok(())
    }

    /// Report one step (Python's `agent_step`): a model turn or a batch of tool calls.
    fn emit_step(
        &self,
        emitter: &mut Emitter,
        name: &str,
        turn: usize,
        status: &str,
        input: Value,
        result: Value,
    ) {
        let kind = if status == step_status::COMPLETED {
            RuntimeEventType::StepCompleted
        } else {
            RuntimeEventType::StepStarted
        };
        let mut event = emitter.event(kind);
        event.step = Some(beam_types::AgentStep {
            step_id: None,
            lc_step_id: format!("{}:{name}:{turn}", self.run_id),
            step_name: name.to_owned(),
            status: status.to_owned(),
            input,
            result: Some(result),
        });
        emitter.emit(event);
    }

    fn emit_tools_step(
        &self,
        emitter: &mut Emitter,
        run: &AgentRun,
        calls: &[PendingToolCall],
        status: &str,
    ) {
        let input = calls
            .iter()
            .map(|c| {
                json!({
                    "tool_name": c.tool_call.function.name.as_str(),
                    "tool_call_id": c.tool_call.id.to_string(),
                    "args": c.tool_call.function.arguments_value(),
                })
            })
            .collect::<Vec<_>>();
        let results = self
            .tool_records
            .iter()
            .filter(|r| {
                calls
                    .iter()
                    .any(|c| Some(c.tool_call.id.to_string()) == r.tool_call_id)
            })
            .map(|r| json!({"tool_call_id": r.tool_call_id, "result": r.result}))
            .collect::<Vec<_>>();
        self.emit_step(
            emitter,
            "tools",
            run.turn(),
            status,
            json!({"calls": input}),
            json!({"results": results}),
        );
    }

    /// Advance the run until it finishes or holds for approval.
    // One arm per rig step kind; splitting it would scatter the protocol.
    #[allow(clippy::too_many_lines)]
    async fn run_loop(&mut self, mut run: AgentRun, emitter: &mut Emitter) -> RiverbaseResult<()> {
        let spec = spec_with_context(&self.def, &self.ctx.history);
        loop {
            match run.next_step().map_err(|e| prompt_error(&e))? {
                AgentRunStep::CallModel {
                    prompt,
                    history,
                    turn,
                } => {
                    let tools = self.ctx.tool_runtime.definitions(&self.allowed_tools());
                    // Fit the outgoing call to its budget: only
                    // the request changes; the saved run keeps every message.
                    let (history, step) = match &self.ctx.fitter {
                        Some(fitter) => {
                            let sent = fitter
                                .fit(spec.preamble.as_deref(), &prompt, history, &tools)
                                .await;
                            let mut event = emitter.event(RuntimeEventType::ContextUsage);
                            event.payload.insert("step".into(), json!(sent.step));
                            event.payload.insert(
                                "usage".into(),
                                serde_json::to_value(&sent.usage).unwrap_or(Value::Null),
                            );
                            event
                                .payload
                                .insert("transformed".into(), json!(sent.transformed));
                            emitter.emit(event);
                            (sent.history, Some(sent.step))
                        }
                        None => (history, None),
                    };
                    let prepared = prepare_request(
                        &spec,
                        &ProviderCapabilities::default(),
                        &history,
                        tools,
                        run.output_tool_name(),
                        None,
                    )
                    .map_err(|e| crate::BEM_394.with_data(e.to_string()))?;
                    run.advertise_tools(turn, prepared.tools.clone());
                    let reply = self
                        .call_model(
                            emitter,
                            ModelCall {
                                execution_id: self.execution_id,
                                run_id: self.run_id,
                                turn,
                                model: self
                                    .input
                                    .model_override
                                    .clone()
                                    .or_else(|| self.def.model.clone()),
                                prompt,
                                prepared: prepared.clone(),
                            },
                        )
                        .await?;
                    if let (Some(fitter), Some(step)) = (&self.ctx.fitter, step) {
                        fitter.observe(step, &token_usage(&reply.usage)).await;
                    }
                    let requested: Vec<Value> = reply
                        .choice
                        .iter()
                        .filter_map(|item| match item {
                            AssistantContent::ToolCall(call) => Some(json!({
                                "tool_name": call.function.name.as_str(),
                                "tool_call_id": call.id.to_string(),
                            })),
                            _ => None,
                        })
                        .collect();
                    for item in &reply.choice {
                        if let AssistantContent::ToolCall(call) = item {
                            let mut event = emitter.event(RuntimeEventType::ToolRequested);
                            event.payload = tool_payload(
                                call.function.name.as_str(),
                                &call.id.to_string(),
                                &call.function.arguments_value(),
                            );
                            emitter.emit(event);
                        }
                    }
                    let model_turn = ModelTurn::new(
                        AssistantMessage::default(),
                        reply.choice,
                        reply.usage,
                        prepared.executable_tool_names,
                        prepared.allowed_tool_names,
                        reply.raw,
                    )
                    .with_finish_reason(reply.finish_reason);
                    match run
                        .model_response(model_turn)
                        .map_err(|e| prompt_error(&e))?
                    {
                        ModelTurnOutcome::Continue { .. } | ModelTurnOutcome::TurnRetried => {}
                        ModelTurnOutcome::NeedsResolution(_) => {
                            run.resolve_unhandled_invalid_tool_call()
                                .map_err(|e| prompt_error(&e))?;
                        }
                    }
                    self.emit_step(
                        emitter,
                        "model",
                        turn,
                        step_status::COMPLETED,
                        json!({"turn": turn}),
                        json!({"tool_calls": requested}),
                    );
                    self.save(&run, status::RUNNING).await?;
                }
                AgentRunStep::CallTools { calls } => {
                    let mut holds = Vec::with_capacity(calls.len());
                    for call in &calls {
                        holds.push(self.hold_of(call).await);
                    }
                    if holds.iter().any(Option::is_some) {
                        self.emit_tools_step(emitter, &run, &calls, step_status::INTERRUPTED);
                        return self.hold(run, emitter, &calls, holds).await;
                    }
                    let results = self.run_tools(emitter, &calls, &HashMap::new()).await;
                    run.tool_results(results).map_err(|e| prompt_error(&e))?;
                    self.emit_tools_step(emitter, &run, &calls, step_status::COMPLETED);
                    self.save(&run, status::RUNNING).await?;
                }
                AgentRunStep::Done(response) => {
                    let text = response.output();
                    self.save(&run, status::COMPLETED).await?;
                    let mut completed = emitter.event(RuntimeEventType::MessageCompleted);
                    completed.message = Some(ai_message(&self.input, &text, false));
                    emitter.emit(completed);
                    let mut result =
                        agent_result(&self.input, self.execution_id, &text, self.usage.clone());
                    result.tool_calls = std::mem::take(&mut self.tool_records);
                    result.model_used.clone_from(&self.model_used);
                    let done = done_event(emitter, &result);
                    emitter.emit(done);
                    return Ok(());
                }
            }
        }
    }

    /// Continue a held run with the human's answers. The run is claimed first, so
    /// of two resumes racing for one held run only one runs the tools.
    async fn resume(
        &mut self,
        request: &RuntimeResumeRequest,
        emitter: &mut Emitter,
    ) -> RiverbaseResult<()> {
        let Prepared {
            mut run,
            held,
            decisions,
            calls,
        } = prepare_resume(&self.def, &self.ctx, self.states.as_ref(), request).await?;
        if !self
            .states
            .claim(
                self.state_id,
                status::INTERRUPTED,
                status::RUNNING,
                self.run_id,
            )
            .await?
        {
            return Err(crate::BEM_397.with_data("another run is already resuming this execution"));
        }
        for call in &held {
            let mut event = emitter.event(RuntimeEventType::ActionResolved);
            event.payload.insert(
                "action_id".into(),
                Value::String(call.action_id.to_string()),
            );
            event.payload.insert(
                "approved".into(),
                Value::Bool(matches!(
                    decisions.get(&call.call_id),
                    Some(Decision::Approve { .. } | Decision::Client(_))
                )),
            );
            emitter.emit(event);
        }
        // "Allow for this conversation" and "always allow" (plan 023 C4):
        // only a choice the held call offered; a locked one offers `once`.
        let scope = tool_scope(&self.def, &self.input);
        for call in &held {
            let Some(Decision::Approve { remember, .. }) = decisions.get(&call.call_id) else {
                continue;
            };
            if *remember == RememberOption::Once {
                continue;
            }
            let decision = ToolDecision {
                mode: ToolMode::Ask,
                layer: call.layer.unwrap_or_default(),
                locked: call.locked,
                ..ToolDecision::default()
            };
            if let Err(e) = self
                .ctx
                .tool_runtime
                .remember(&call.tool, &scope, &decision, *remember)
                .await
            {
                tracing::warn!(tool = %call.tool, error = %e, "remembering the approval failed");
            }
        }
        run.append_entry(RunEntry {
            kind: HELD_KIND.into(),
            turn: run.turn(),
            value: json!([]),
        });
        let results = self.run_tools(emitter, &calls, &decisions).await;
        run.tool_results(results).map_err(|e| prompt_error(&e))?;
        self.emit_tools_step(emitter, &run, &calls, step_status::COMPLETED);
        self.save(&run, status::RUNNING).await?;
        self.run_loop(run, emitter).await
    }

    /// Record a failed run, unless another worker owns it now.
    async fn mark_failed(&self, err: &riverbase_core::RiverbaseError) {
        if err.errcode.as_str() == crate::BEM_397.errcode {
            return;
        }
        if let Err(e) = self.states.set_status(self.state_id, status::FAILED).await {
            tracing::warn!(execution_id = %self.execution_id, error = %e, "marking the rig run failed");
        }
    }
}

/// Errors that refuse a resume before anything changed.
const RESUME_REFUSALS: [&str; 4] = ["BEM-392", "BEM-393", "BEM-397", "BEM-399"];

/// Step statuses (Python's `agent_step.status` labels, lower case on the wire).
mod step_status {
    pub const COMPLETED: &str = "completed";
    pub const INTERRUPTED: &str = "interrupted";
}

/// Who `input`'s calls run for, with the agent's `approval_required` tools
/// as its `ask` rules (plan 023 part C).
fn tool_scope(def: &RigAgentDef, input: &AgentInput) -> ToolScope {
    ToolScope::of(input, ask_rules(&def.approval_required))
}

/// Why `call` holds the run, if it does, and the decision that holds it. A
/// client tool holds even when it runs without asking; when its decision
/// asks, its single action asks the client to confirm. A denied call, or one
/// whose policy cannot be read, does not hold: the tool runtime refuses it
/// (plan 023 D8).
async fn hold_of(
    def: &RigAgentDef,
    ctx: &RuntimeContext,
    input: &AgentInput,
    call: &PendingToolCall,
) -> Option<(HoldKind, ToolDecision)> {
    if call.preresolved_result.is_some() {
        return None;
    }
    let name = call.tool_call.function.name.as_str();
    let decision = match ctx.tool_runtime.decide(name, &tool_scope(def, input)).await {
        Ok(decision) => decision,
        Err(e) => {
            tracing::warn!(tool = name, error = %e, "tool policy unavailable");
            return None;
        }
    };
    if decision.mode == ToolMode::Deny {
        return None;
    }
    if ctx.tool_runtime.tool_info(name).is_some_and(|t| t.client) {
        Some((HoldKind::Client, decision))
    } else if decision.mode == ToolMode::Ask {
        Some((HoldKind::Approval, decision))
    } else {
        None
    }
}

/// The validated action a held call raises, in Python rc8's shape: the
/// request id is the call id, the payload names the tool, its arguments and risk.
fn hold_request(
    ctx: &RuntimeContext,
    call: &PendingToolCall,
    kind: HoldKind,
    decision: &ToolDecision,
    now: chrono::DateTime<chrono::Utc>,
) -> RiverbaseResult<beam_types::ActionRequestEnvelope> {
    let call_id = call.tool_call.id.to_string();
    let name = call.tool_call.function.name.to_string();
    let args = call.tool_call.function.arguments_value();
    let info = ctx
        .tool_runtime
        .tool_info(&name)
        .unwrap_or_else(|| ToolInfo {
            description: String::new(),
            risk: "medium".into(),
            timeout_seconds: 0.0,
            client: false,
        });
    let args_object = if args.is_object() {
        args.clone()
    } else {
        json!({})
    };
    let remember_options = json!(decision.remember_options());
    let (kind_name, title, payload) = match kind {
        HoldKind::Approval => {
            // rc8's layers: which one decided, and why.
            let layer = decision.layer.as_str();
            (
                TOOL_APPROVAL_KIND,
                format!("Approve {name}"),
                json!({
                    "tool_name": name,
                    "call_id": call_id,
                    "args": args_object,
                    "risk": info.risk,
                    "remember_options": remember_options,
                    "decided_by": {"layer": layer, "reason": decision.reason},
                }),
            )
        }
        HoldKind::Client => {
            let wait = std::time::Duration::try_from_secs_f64(info.timeout_seconds.max(0.0))
                .ok()
                .and_then(|d| chrono::TimeDelta::from_std(d).ok())
                .unwrap_or_else(|| chrono::TimeDelta::minutes(10));
            (
                CLIENT_TOOL_KIND,
                format!("Run {name}"),
                json!({
                    "tool_name": name,
                    "call_id": call_id,
                    "args": args_object,
                    "risk": info.risk,
                    "requires_confirmation": decision.mode == ToolMode::Ask,
                    "remember_options": remember_options,
                    "expires_at": now
                        .checked_add_signed(wait)
                        .unwrap_or(chrono::DateTime::<chrono::Utc>::MAX_UTC)
                        .to_rfc3339(),
                }),
            )
        }
    };
    validate_action_request(build_action_request(ActionRequestArgs {
        kind: kind_name.into(),
        request_id: call_id.clone(),
        title,
        description: info.description,
        payload,
        metadata: Some(json!({"tool_name": name, "tool_args": args, "tool_call_id": call_id})),
        ..ActionRequestArgs::default()
    }))
}

/// Fail with `BEM-392` unless this build can read `record`.
fn readable(record: &RuntimeStateRecord) -> RiverbaseResult<()> {
    if record.runtime_name != RIG_RUNTIME_NAME {
        return Err(crate::BEM_392.with_data(format!(
            "state was written by runtime '{}'",
            record.runtime_name
        )));
    }
    if i64::from(record.run_format) != i64::from(RUN_FORMAT) {
        return Err(crate::BEM_392.with_data(format!(
            "run is format {}, this rig reads format {RUN_FORMAT}",
            record.run_format
        )));
    }
    assert_checkpoint_readable(
        CompatibilityPolicy::Strict,
        CheckpointVersions {
            pinned_adapter: Some(&record.adapter_version),
            deployed_adapter: Some(RIG_ADAPTER_VERSION),
            pinned_library: Some(&record.library_version),
            deployed_library: Some(RIG_LIBRARY_VERSION),
        },
        None,
        None,
    )
}

/// A held run, checked and ready to continue.
struct Prepared {
    run: AgentRun,
    held: Vec<HeldCall>,
    decisions: HashMap<String, Decision>,
    calls: Vec<PendingToolCall>,
}

/// Load a held run and match the resume's answers to its held calls. Changes nothing.
///
/// Fails closed: a call that needs approval now and has no answer is never
/// run. If the run held other calls, its rule changed while the run waited
/// (a grant expired, an `ask` rule was added), so the model gets a refusal as
/// that call's result; refusing the resume would refuse every later one too.
/// Without a held-call record the resume is refused.
async fn prepare_resume(
    def: &RigAgentDef,
    ctx: &RuntimeContext,
    states: &dyn RuntimeStateStore,
    request: &RuntimeResumeRequest,
) -> RiverbaseResult<Prepared> {
    let id = state_id(&request.runtime_ref)?;
    let record = states
        .load(id)
        .await?
        .ok_or_else(|| crate::BEM_393.with_data(id.to_string()))?;
    readable(&record)?;
    if record.status != status::INTERRUPTED {
        return Err(crate::BEM_399.with_data(format!(
            "the run is {}, not waiting for input",
            record.status
        )));
    }
    let mut run: AgentRun = serde_json::from_value(record.state)
        .map_err(|e| crate::BEM_392.with_data(e.to_string()))?;
    let held: Vec<HeldCall> = run
        .last_entry_of(HELD_KIND)
        .map(|e| serde_json::from_value(e.value.clone()))
        .transpose()
        .map_err(|e| crate::BEM_392.with_data(format!("held calls: {e}")))?
        .unwrap_or_default();
    let mut decisions = decisions(&held, request)?;
    let AgentRunStep::CallTools { calls } = run.next_step().map_err(|e| prompt_error(&e))? else {
        return Err(crate::BEM_399.with_data("the saved run is not waiting on tool calls"));
    };
    for call in &calls {
        let call_id = call.tool_call.id.to_string();
        if decisions.contains_key(&call_id)
            || hold_of(def, ctx, &request.input, call).await.is_none()
        {
            continue;
        }
        let tool = call.tool_call.function.name.as_str();
        if held.is_empty() {
            return Err(crate::BEM_399.with_data(format!("no answer for the call to {tool}")));
        }
        decisions.insert(
            call_id,
            refusal(&format!(
                "approval required: the policy for {tool} changed while the run waited"
            )),
        );
    }
    Ok(Prepared {
        run,
        held,
        decisions,
        calls,
    })
}

/// The human's decision for each held call, from the resume's action responses.
fn decisions(
    held: &[HeldCall],
    request: &RuntimeResumeRequest,
) -> RiverbaseResult<HashMap<String, Decision>> {
    let mut decisions = HashMap::new();
    for call in held {
        let response = request
            .actions
            .iter()
            .find(|a| a.action_id == call.action_id.to_string() || a.request_id == call.request_id)
            .ok_or_else(|| {
                crate::BEM_399.with_data(format!("no response for held call to {}", call.tool))
            })?;
        // Python rc8: the response's kind is the held action's. `beam.approval`
        // answers runs held before rc8's tool approval.
        let kinds: &[&str] = match call.kind {
            HoldKind::Approval => &[TOOL_APPROVAL_KIND, "beam.approval"],
            HoldKind::Client => &[CLIENT_TOOL_KIND],
        };
        if !kinds.contains(&response.kind.as_str()) {
            return Err(crate::BEM_399.with_data(format!(
                "a {} response does not answer the held call to {}",
                response.kind, call.tool
            )));
        }
        let decision = if call.kind == HoldKind::Client {
            client_decision(response)?
        } else if response.status == ActionResponseStatus::Submitted {
            let payload = validate_action_response(response)?;
            let field = |name: &str| payload.as_ref().and_then(|p| p.get(name).cloned());
            if field("approved").and_then(|v| v.as_bool()).unwrap_or(false) {
                // A choice the held call did not offer (a locked decision
                // offers `once` only) is ignored, as is a forged one.
                let remember = field("remember")
                    .and_then(|v| v.as_str().and_then(RememberOption::parse))
                    .filter(|r| call.remember_options.contains(r))
                    .unwrap_or(RememberOption::Once);
                Decision::Approve {
                    remember,
                    edited_args: field("edited_args").filter(Value::is_object),
                }
            } else {
                Decision::Deny(
                    field("comment")
                        .and_then(|v| v.as_str().map(str::to_owned))
                        .unwrap_or_else(|| "not approved".into()),
                )
            }
        } else {
            let status = serde_json::to_value(response.status)
                .ok()
                .and_then(|v| v.as_str().map(str::to_owned))
                .unwrap_or_default();
            Decision::Deny(format!("the action was {status}"))
        };
        decisions.insert(call.call_id.clone(), decision);
    }
    Ok(decisions)
}

/// The client's answer to a held client call (Python rc8 `ToolGate.refusal`
/// and `_client_result`). Declining needs no payload; expiry is the sweep's.
fn client_decision(response: &beam_types::ActionResponseEnvelope) -> RiverbaseResult<Decision> {
    Ok(match response.status {
        ActionResponseStatus::Expired => refusal(CLIENT_EXPIRED),
        ActionResponseStatus::Cancelled | ActionResponseStatus::Invalid => refusal(CLIENT_DECLINED),
        ActionResponseStatus::Submitted => {
            let payload = validate_action_response(response)?.unwrap_or_else(|| json!({}));
            if payload.get("approved").and_then(Value::as_bool) == Some(false) {
                refusal(CLIENT_DECLINED)
            } else {
                Decision::Client(ClientAnswer {
                    output: payload.get("output").cloned().unwrap_or(Value::Null),
                    error: payload
                        .get("error")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                })
            }
        }
    })
}

fn tool_payload(name: &str, call_id: &str, args: &Value) -> Map<String, Value> {
    let mut payload = Map::new();
    payload.insert("tool_name".into(), Value::String(name.to_owned()));
    payload.insert("tool_call_id".into(), Value::String(call_id.to_owned()));
    payload.insert("args".into(), args.clone());
    payload
}

#[async_trait]
impl AgentRuntime for RigRuntime {
    fn runtime_name(&self) -> &str {
        RIG_RUNTIME_NAME
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            streaming: true,
            durable_execution: true,
            resume: true,
            cancellation: true,
            state_inspection: true,
            checkpointing: true,
            structured_output: self.def.output_schema.is_some(),
            native_tools: true,
            ..RuntimeCapabilities::default()
        }
    }

    fn adapter_version(&self) -> &str {
        RIG_ADAPTER_VERSION
    }

    fn tool_rules(&self) -> Vec<beam_types::tool_policy::ToolRule> {
        ask_rules(&self.def.approval_required)
    }

    fn library_version(&self) -> &str {
        RIG_LIBRARY_VERSION
    }

    fn allocate_ref(&self, execution_id: Uuid) -> RuntimeExecutionRef {
        let mut reference = Map::new();
        reference.insert("state_id".into(), Value::String(execution_id.to_string()));
        RuntimeExecutionRef {
            runtime: RIG_RUNTIME_NAME.into(),
            runtime_version: Some(RIG_ADAPTER_VERSION.into()),
            reference,
        }
    }

    fn start(&self, request: RuntimeStartRequest, context: RuntimeContext) -> EventStream {
        let (def, states) = (self.def.clone(), self.states.clone());
        drive(
            request.execution_id,
            request.run_id,
            move |mut emitter| async move {
                let state_id = request
                    .runtime_ref
                    .as_ref()
                    .and_then(|r| state_id(r).ok())
                    .unwrap_or(request.execution_id);
                let mut driver = Driver {
                    def,
                    state_id,
                    states,
                    ctx: context,
                    input: request.input,
                    execution_id: request.execution_id,
                    run_id: request.run_id,
                    tool_records: Vec::new(),
                    usage: TokenUsage::default(),
                    model_used: None,
                };
                // A run whose worker died mid-loop continues from its last saved
                // step: completed model turns are not sent again.
                let saved = driver.states.load(state_id).await?;
                let continued = saved
                    .filter(|r| r.status == status::RUNNING && readable(r).is_ok())
                    .and_then(|r| serde_json::from_value::<AgentRun>(r.state).ok());
                let mut started = emitter.event(RuntimeEventType::ExecutionStarted);
                started
                    .payload
                    .insert("continued".into(), Value::Bool(continued.is_some()));
                emitter.emit(started);
                let run = continued.unwrap_or_else(|| {
                    AgentRun::from_spec(
                        &spec_with_context(&driver.def, &driver.ctx.history),
                        user_message(&driver.input.content, "\n"),
                        Some(rig_history(&driver.ctx.history)),
                    )
                });
                let outcome = driver.run_loop(run, &mut emitter).await;
                if let Err(e) = &outcome {
                    driver.mark_failed(e).await;
                }
                outcome
            },
        )
    }

    fn resume(&self, request: RuntimeResumeRequest, context: RuntimeContext) -> EventStream {
        let (def, states) = (self.def.clone(), self.states.clone());
        drive(
            request.execution_id,
            request.run_id,
            move |mut emitter| async move {
                let mut driver = Driver {
                    def,
                    state_id: state_id(&request.runtime_ref)?,
                    states,
                    ctx: context,
                    input: request.input.clone(),
                    execution_id: request.execution_id,
                    run_id: request.run_id,
                    tool_records: Vec::new(),
                    usage: TokenUsage::default(),
                    model_used: None,
                };
                let outcome = driver.resume(&request, &mut emitter).await;
                // A resume refused before the run moved leaves the saved run as it was.
                if let Err(e) = &outcome {
                    if !RESUME_REFUSALS.contains(&e.errcode.as_str()) {
                        driver.mark_failed(e).await;
                    }
                }
                outcome
            },
        )
    }

    async fn check_resume(
        &self,
        request: &RuntimeResumeRequest,
        context: &RuntimeContext,
    ) -> RiverbaseResult<()> {
        prepare_resume(&self.def, context, self.states.as_ref(), request)
            .await
            .map(|_| ())
    }

    async fn cancel(
        &self,
        runtime_ref: &RuntimeExecutionRef,
        _context: &RuntimeContext,
    ) -> RiverbaseResult<()> {
        let id = state_id(runtime_ref)?;
        self.states.set_status(id, status::CANCELLED).await?;
        Ok(())
    }

    async fn inspect(
        &self,
        runtime_ref: &RuntimeExecutionRef,
        _context: &RuntimeContext,
    ) -> RiverbaseResult<Option<RuntimeState>> {
        let id = state_id(runtime_ref)?;
        Ok(self.states.load(id).await?.map(|record| {
            let mut metadata = Map::new();
            metadata.insert("turn".into(), json!(record.turn));
            metadata.insert("run_format".into(), json!(record.run_format));
            metadata.insert("adapter_version".into(), json!(record.adapter_version));
            metadata.insert("library_version".into(), json!(record.library_version));
            metadata.insert("updated_at".into(), json!(record.updated_at.to_rfc3339()));
            RuntimeState {
                runtime_ref: runtime_ref.clone(),
                status: Some(record.status),
                metadata,
            }
        }))
    }
}
