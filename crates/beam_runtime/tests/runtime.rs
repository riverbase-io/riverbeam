//! Runtime contract and `RigRuntime` driver tests, on the
//! in-memory state store and scripted model and tools.

use std::sync::Arc;
use std::time::Duration;

use beam_execution::{ExecutionRecords, InMemoryExecutionRecords, Lease};
use beam_runtime::state::RuntimeStateStore;
use beam_runtime::{
    collect_events, runtime_event_to_stream_chunk, AgentRuntime, DirectModelRuntime,
    InMemoryRuntimeStateStore, RigAgentDef, RigRuntime, RuntimeCapabilities, RuntimeContext,
    RuntimeEventType, RuntimeResumeRequest, RuntimeStartRequest, ScriptedModel, ScriptedTools,
    ScriptedTurn,
};
use beam_runtime::{ModelCall, ModelCaller, ModelReply, ModelStream, ModelStreamItem};
use beam_types::{
    ActionResponseEnvelope, ActionResponseStatus, AgentInput, AgentResult, ContentPart,
    ContentPartType, StreamChunkType,
};
use futures_util::stream::{self, StreamExt};
use rig_core::completion::Message;
use rig_core::message::{DocumentSourceKind, ImageMediaType, UserContent};
use serde_json::{json, Value};
use uuid::Uuid;

fn input(text: &str) -> AgentInput {
    AgentInput {
        content: vec![ContentPart::text(text)],
        session_id: Uuid::new_v4(),
        interaction_id: Some(Uuid::new_v4()),
        execution_id: None,
        request_id: Some(Uuid::new_v4()),
        run_id: Uuid::new_v4(),
        runtime_ref: None,
        agent_name: Some("payer".into()),
        model_override: None,
        metadata: Value::Null,
        auth_context: None,
        stream: true,
        actions: None,
        context: None,
        client: None,
    }
}

fn tools() -> ScriptedTools {
    ScriptedTools::new()
        .with("add", false, |args| {
            Ok(format!("{}", args["a"].as_i64().unwrap_or(0) + 1))
        })
        .with("lookup", false, |_| Ok("found".into()))
        .with("pay", true, |_| Ok("paid".into()))
}

fn def() -> RigAgentDef {
    RigAgentDef {
        tools: vec!["add".into(), "lookup".into(), "pay".into()],
        preamble: Some("You pay invoices.".into()),
        ..RigAgentDef::new("payer")
    }
}

fn context(model: &ScriptedModel, tools: &ScriptedTools) -> RuntimeContext {
    RuntimeContext {
        model_gateway: Arc::new(model.clone()),
        tool_runtime: Arc::new(tools.clone()),
        ..RuntimeContext::default()
    }
}

fn start_request(execution_id: Uuid, input: AgentInput) -> RuntimeStartRequest {
    RuntimeStartRequest {
        execution_id,
        run_id: input.run_id,
        input,
        runtime_ref: None,
    }
}

fn approval(action: &Value, approved: bool) -> ActionResponseEnvelope {
    ActionResponseEnvelope {
        action_id: action["action_id"].as_str().unwrap().to_owned(),
        lc_action_id: action["lc_action_id"].as_str().map(str::to_owned),
        kind: action["action_request"]["kind"]
            .as_str()
            .unwrap()
            .to_owned(),
        request_id: action["action_request"]["request_id"]
            .as_str()
            .unwrap()
            .to_owned(),
        status: ActionResponseStatus::Submitted,
        payload: Some(json!({"approved": approved, "comment": "not this one"})),
        errors: vec![],
        responder_id: None,
        responded_at: chrono::Utc::now(),
    }
}

#[tokio::test]
async fn rig_takes_envelope_system_text_as_preamble_and_turns_as_history() {
    let model = ScriptedModel::new([ScriptedTurn::text("ok")]);
    let tools = tools();
    let runtime = RigRuntime::new(def(), Arc::new(InMemoryRuntimeStateStore::new()));
    let mut ctx = context(&model, &tools);
    ctx.history = vec![
        beam_types::AgentMessage::text(beam_types::AgentMessageRole::System, "Answer in French."),
        beam_types::AgentMessage::text(beam_types::AgentMessageRole::Human, "earlier question"),
        beam_types::AgentMessage::text(beam_types::AgentMessageRole::Ai, "earlier answer"),
    ];
    let stream = runtime.start(start_request(Uuid::new_v4(), input("now")), ctx);
    collect_events(stream).await.unwrap();
    let seen = format!("{:?}", model.seen()[0].prepared.chat_history);
    assert!(
        seen.contains(r"You pay invoices.\n\nAnswer in French."),
        "{seen}"
    );
    assert!(
        seen.contains("earlier question") && seen.contains("earlier answer"),
        "{seen}"
    );
}

#[tokio::test]
async fn rig_tool_loop_runs_two_tools_then_answers() {
    let model = ScriptedModel::new([
        ScriptedTurn::tool_call("c1", "add", json!({"a": 1})),
        ScriptedTurn::tool_call("c2", "lookup", json!({})),
        ScriptedTurn::text("the answer is 2"),
    ]);
    let tools = tools();
    let runtime = RigRuntime::new(def(), Arc::new(InMemoryRuntimeStateStore::new()));
    let execution_id = Uuid::new_v4();
    let stream = runtime.start(
        start_request(execution_id, input("add one")),
        context(&model, &tools),
    );
    let (events, result) = collect_events(stream).await.unwrap();

    assert_eq!(result.content[0].text.as_deref(), Some("the answer is 2"));
    assert_eq!(result.execution_id, Some(execution_id));
    assert!(!result.interrupted);
    let names: Vec<_> = result
        .tool_calls
        .iter()
        .map(|t| t.tool_name.as_str())
        .collect();
    assert_eq!(names, ["add", "lookup"]);
    assert_eq!(result.tool_calls[0].result.as_deref(), Some("2"));
    assert_eq!(
        result.token_usage.as_ref().unwrap().total_tokens,
        45,
        "three scripted turns"
    );
    assert_eq!(
        (model.calls(), tools.executed()),
        (3, vec!["add".to_owned(), "lookup".to_owned()])
    );

    // The model saw the preamble and the advertised tools on every turn.
    let first = &model.seen()[0];
    assert_eq!(first.prepared.tools.len(), 3);
    assert!(format!("{:?}", first.prepared.chat_history).contains("You pay invoices."));

    // Sequence numbers are dense, and the wire chunks end with one done frame.
    let sequences: Vec<i64> = events.iter().map(|e| e.sequence.unwrap()).collect();
    assert_eq!(
        sequences,
        (1..=i64::try_from(events.len()).unwrap()).collect::<Vec<_>>()
    );
    let chunks: Vec<_> = events
        .iter()
        .filter_map(runtime_event_to_stream_chunk)
        .collect();
    assert_eq!(chunks.last().unwrap().chunk_type, StreamChunkType::Done);
    assert_eq!(
        chunks
            .iter()
            .filter(|c| c.chunk_type == StreamChunkType::Done)
            .count(),
        1
    );
    let tool_messages = chunks
        .iter()
        .filter(|c| c.data.as_ref().is_some_and(|m| m.tool_name.is_some()))
        .count();
    assert_eq!(tool_messages, 2);
}

#[tokio::test]
async fn approval_holds_the_run_and_another_worker_resumes_it() {
    // One script shared by both "workers": completed turns are never re-sent.
    let model = ScriptedModel::new([
        ScriptedTurn::tool_call("c1", "pay", json!({"invoice": 7})),
        ScriptedTurn::text("paid invoice 7"),
    ]);
    let tools = tools();
    let states: Arc<InMemoryRuntimeStateStore> = Arc::new(InMemoryRuntimeStateStore::new());
    let execution_id = Uuid::new_v4();

    let worker_a = RigRuntime::new(def(), states.clone());
    let first = input("pay invoice 7");
    let (events, held) = collect_events(worker_a.start(
        start_request(execution_id, first.clone()),
        context(&model, &tools),
    ))
    .await
    .unwrap();
    assert!(held.interrupted);
    assert!(
        tools.executed().is_empty(),
        "the gated tool waits for the human"
    );
    let actions = held.action_required.clone().unwrap();
    assert_eq!(actions.len(), 1);
    let action = &actions[0];
    assert_eq!(action["action_request"]["kind"], "beam.tool_approval");
    assert_eq!(action["action_request"]["action_type"], "tool_approval");
    assert_eq!(action["action_request"]["request_id"], "c1");
    let payload = &action["action_request"]["payload"];
    assert_eq!(
        (&payload["tool_name"], &payload["call_id"], &payload["args"]),
        (&json!("pay"), &json!("c1"), &json!({"invoice": 7}))
    );
    // An unlocked decision offers every remember choice (plan 023 C4).
    assert_eq!(
        payload["remember_options"],
        json!(["once", "session", "always"])
    );
    assert_eq!(action["lc_action_id"], "c1");
    assert!(events
        .iter()
        .any(|e| e.event_type == RuntimeEventType::ActionRequired));
    let runtime_ref = worker_a.allocate_ref(execution_id);
    let inspected = worker_a
        .inspect(&runtime_ref, &RuntimeContext::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(inspected.status.as_deref(), Some("interrupted"));

    // Worker A is gone. Worker B has only the saved state.
    drop(worker_a);
    let worker_b = RigRuntime::new(def(), states.clone());
    let mut second = input("approve");
    second.session_id = first.session_id;
    let resumed = worker_b.resume(
        RuntimeResumeRequest {
            execution_id,
            run_id: second.run_id,
            runtime_ref: runtime_ref.clone(),
            actions: vec![approval(action, true)],
            input: second,
        },
        context(&model, &tools),
    );
    let (_, done) = collect_events(resumed).await.unwrap();
    assert_eq!(done.content[0].text.as_deref(), Some("paid invoice 7"));
    assert_eq!(tools.executed(), ["pay"]);
    assert_eq!(model.calls(), 2, "turn 1 was not sent to the model again");
    let state = states.load(execution_id).await.unwrap().unwrap();
    assert_eq!((state.status.as_str(), state.turn), ("completed", 2));

    // A finished run cannot be resumed again.
    let again = worker_b.resume(
        RuntimeResumeRequest {
            execution_id,
            run_id: Uuid::new_v4(),
            runtime_ref,
            actions: vec![approval(action, true)],
            input: input("again"),
        },
        context(&model, &tools),
    );
    assert_eq!(
        collect_events(again).await.unwrap_err().errcode.as_str(),
        "BEM-399"
    );
}

#[tokio::test]
async fn a_denied_call_is_not_run_and_the_model_hears_why() {
    let model = ScriptedModel::new([
        ScriptedTurn::tool_call("c1", "pay", json!({})),
        ScriptedTurn::text("ok, not paying"),
    ]);
    let tools = tools();
    let runtime = RigRuntime::new(def(), Arc::new(InMemoryRuntimeStateStore::new()));
    let execution_id = Uuid::new_v4();
    let (_, held) = collect_events(runtime.start(
        start_request(execution_id, input("pay")),
        context(&model, &tools),
    ))
    .await
    .unwrap();
    let action = held.action_required.unwrap()[0].clone();
    let next = input("no");
    let (_, done) = collect_events(runtime.resume(
        RuntimeResumeRequest {
            execution_id,
            run_id: next.run_id,
            runtime_ref: runtime.allocate_ref(execution_id),
            actions: vec![approval(&action, false)],
            input: next,
        },
        context(&model, &tools),
    ))
    .await
    .unwrap();
    assert_eq!(done.content[0].text.as_deref(), Some("ok, not paying"));
    assert!(tools.executed().is_empty());
    assert!(format!("{:?}", model.seen()[1].prompt)
        .contains("The user denied this tool call: not this one"));
}

#[tokio::test]
async fn a_resume_without_a_matching_response_is_refused_and_keeps_the_run() {
    let model = ScriptedModel::new([ScriptedTurn::tool_call("c1", "pay", json!({}))]);
    let tools = tools();
    let states = Arc::new(InMemoryRuntimeStateStore::new());
    let runtime = RigRuntime::new(def(), states.clone());
    let execution_id = Uuid::new_v4();
    collect_events(runtime.start(
        start_request(execution_id, input("pay")),
        context(&model, &tools),
    ))
    .await
    .unwrap();
    let next = input("?");
    let err = collect_events(runtime.resume(
        RuntimeResumeRequest {
            execution_id,
            run_id: next.run_id,
            runtime_ref: runtime.allocate_ref(execution_id),
            actions: vec![],
            input: next,
        },
        context(&model, &tools),
    ))
    .await
    .unwrap_err();
    assert_eq!(err.errcode.as_str(), "BEM-399");
    assert_eq!(
        states.load(execution_id).await.unwrap().unwrap().status,
        "interrupted"
    );
}

#[tokio::test]
async fn a_run_of_another_format_fails_closed() {
    let model = ScriptedModel::new([ScriptedTurn::tool_call("c1", "pay", json!({}))]);
    let tools = tools();
    let states = Arc::new(InMemoryRuntimeStateStore::new());
    let runtime = RigRuntime::new(def(), states.clone());
    let execution_id = Uuid::new_v4();
    let (_, held) = collect_events(runtime.start(
        start_request(execution_id, input("pay")),
        context(&model, &tools),
    ))
    .await
    .unwrap();
    let action = held.action_required.unwrap()[0].clone();
    let resume = |runtime: &RigRuntime| {
        let next = input("yes");
        runtime.resume(
            RuntimeResumeRequest {
                execution_id,
                run_id: next.run_id,
                runtime_ref: runtime.allocate_ref(execution_id),
                actions: vec![approval(&action, true)],
                input: next,
            },
            context(&model, &tools),
        )
    };

    // The envelope says another format: rig refuses it by name.
    let mut record = states.load(execution_id).await.unwrap().unwrap();
    let saved = record.clone();
    record.state["format"] = json!(beam_runtime::RUN_FORMAT + 1);
    states.save(&record, None).await.unwrap();
    assert_eq!(
        collect_events(resume(&runtime))
            .await
            .unwrap_err()
            .errcode
            .as_str(),
        "BEM-392"
    );

    // The row says another format.
    let mut record = saved.clone();
    record.run_format += 1;
    states.save(&record, None).await.unwrap();
    assert_eq!(
        collect_events(resume(&runtime))
            .await
            .unwrap_err()
            .errcode
            .as_str(),
        "BEM-392"
    );

    // Another rig version wrote it.
    let mut record = saved.clone();
    record.library_version = "rig-agent-0.43.0".into();
    states.save(&record, None).await.unwrap();
    assert_eq!(
        collect_events(resume(&runtime))
            .await
            .unwrap_err()
            .errcode
            .as_str(),
        "BEM-392"
    );
    assert!(tools.executed().is_empty());
}

#[tokio::test]
async fn a_stream_without_done_is_a_protocol_error() {
    let stream = stream::iter(Vec::new()).boxed();
    let err = collect_events(stream).await.unwrap_err();
    assert_eq!(err.errcode.as_str(), "BEM-390");
}

#[tokio::test]
async fn max_turns_ends_the_run_as_failed() {
    let model = ScriptedModel::new([
        ScriptedTurn::tool_call("c1", "add", json!({"a": 1})),
        ScriptedTurn::tool_call("c2", "add", json!({"a": 2})),
    ]);
    let tools = tools();
    let states = Arc::new(InMemoryRuntimeStateStore::new());
    let runtime = RigRuntime::new(
        RigAgentDef {
            max_turns: 1,
            ..def()
        },
        states.clone(),
    );
    let execution_id = Uuid::new_v4();
    let err = collect_events(runtime.start(
        start_request(execution_id, input("loop")),
        context(&model, &tools),
    ))
    .await
    .unwrap_err();
    assert_eq!(err.errcode.as_str(), "BEM-395");
    assert_eq!(
        states.load(execution_id).await.unwrap().unwrap().status,
        "failed"
    );
}

#[tokio::test]
async fn a_stale_worker_cannot_save_state() {
    let records = Arc::new(InMemoryExecutionRecords::new());
    let run_id = Uuid::new_v4();
    let ttl = Duration::from_mins(1);
    let old_lease = Uuid::new_v4();
    let old_fence = records
        .claim_lease(run_id, "worker-a", old_lease, ttl)
        .await
        .unwrap();
    // Worker B takes the run over (worker A's lease expired, say).
    records
        .claim_lease(run_id, "worker-a", Uuid::new_v4(), ttl)
        .await
        .unwrap();
    // The exact lease worker A held: only the fence comparison rejects it.
    let stale = Lease {
        run_id,
        lease_id: old_lease,
        fence: old_fence,
    };
    let states = Arc::new(InMemoryRuntimeStateStore::new().fenced_by(records));
    let model = ScriptedModel::new([ScriptedTurn::text("hi")]);
    let runtime = RigRuntime::new(def(), states.clone());
    let execution_id = Uuid::new_v4();
    let mut req = start_request(execution_id, input("hi"));
    req.run_id = run_id;
    let ctx = RuntimeContext {
        lease: Some(stale),
        ..context(&model, &tools())
    };
    let err = collect_events(runtime.start(req, ctx)).await.unwrap_err();
    assert_eq!(err.errcode.as_str(), "BEM-397");
    assert!(states.load(execution_id).await.unwrap().is_none());
}

#[tokio::test]
async fn direct_runtime_echoes_like_python() {
    let runtime = DirectModelRuntime::echo();
    let execution_id = Uuid::new_v4();
    let (events, result) = collect_events(runtime.start(
        start_request(execution_id, input("hello")),
        RuntimeContext::default(),
    ))
    .await
    .unwrap();
    assert_eq!(result.content[0].text.as_deref(), Some("echo: hello"));
    let kinds: Vec<_> = events.iter().map(|e| e.event_type).collect();
    assert_eq!(
        kinds,
        [
            RuntimeEventType::MessageDelta,
            RuntimeEventType::MessageCompleted,
            RuntimeEventType::ExecutionCompleted
        ]
    );
    let next = input("x");
    let err = collect_events(runtime.resume(
        RuntimeResumeRequest {
            execution_id,
            run_id: next.run_id,
            runtime_ref: runtime.allocate_ref(execution_id),
            actions: vec![],
            input: next,
        },
        RuntimeContext::default(),
    ))
    .await
    .unwrap_err();
    assert_eq!(err.errcode.as_str(), "BEM-391");
}

#[test]
fn capabilities_use_python_field_names() {
    let caps = serde_json::to_value(RuntimeCapabilities::default()).unwrap();
    let keys: Vec<_> = caps.as_object().unwrap().keys().cloned().collect();
    assert_eq!(
        keys,
        [
            "streaming",
            "durable_execution",
            "resume",
            "cancellation",
            "state_inspection",
            "checkpointing",
            "time_travel",
            "subgraphs",
            "structured_output",
            "native_tools"
        ]
    );
    let rig = RigRuntime::new(def(), Arc::new(InMemoryRuntimeStateStore::new())).capabilities();
    assert!(rig.durable_execution && rig.resume && rig.cancellation && !rig.time_travel);
}

/// Hold a run on `pay` and return the runtime, its state store and the action entry.
async fn held_run(
    model: &ScriptedModel,
    tools: &ScriptedTools,
) -> (RigRuntime, Arc<InMemoryRuntimeStateStore>, Uuid, Value) {
    let states = Arc::new(InMemoryRuntimeStateStore::new());
    let runtime = RigRuntime::new(def(), states.clone());
    let execution_id = Uuid::new_v4();
    let (_, held) = collect_events(runtime.start(
        start_request(execution_id, input("pay")),
        context(model, tools),
    ))
    .await
    .unwrap();
    let action = held.action_required.unwrap()[0].clone();
    (runtime, states, execution_id, action)
}

fn resume_with(runtime: &RigRuntime, execution_id: Uuid, action: &Value) -> RuntimeResumeRequest {
    let next = input("yes");
    RuntimeResumeRequest {
        execution_id,
        run_id: next.run_id,
        runtime_ref: runtime.allocate_ref(execution_id),
        actions: vec![approval(action, true)],
        input: next,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_racing_resumes_run_the_approved_tool_once() {
    let model = ScriptedModel::new([
        ScriptedTurn::tool_call("c1", "pay", json!({})),
        ScriptedTurn::text("paid"),
        ScriptedTurn::text("paid again"),
    ]);
    let tools = tools();
    let (runtime, _, execution_id, action) = held_run(&model, &tools).await;
    let (a, b) = tokio::join!(
        collect_events(runtime.resume(
            resume_with(&runtime, execution_id, &action),
            context(&model, &tools)
        )),
        collect_events(runtime.resume(
            resume_with(&runtime, execution_id, &action),
            context(&model, &tools)
        )),
    );
    let outcomes = [a.map(|_| ()), b.map(|_| ())];
    assert_eq!(outcomes.iter().filter(|o| o.is_ok()).count(), 1);
    let lost = outcomes.iter().find_map(|o| o.as_ref().err()).unwrap();
    assert!(
        ["BEM-397", "BEM-399"].contains(&lost.errcode.as_str()),
        "{lost:?}"
    );
    assert_eq!(tools.executed(), ["pay"]);
}

/// A call that ran without asking when the run held, but needs approval by
/// the resume (a grant expired, an `ask` rule was added), is refused to the
/// model and never run; the resume goes on, so the run is not stuck.
#[tokio::test]
async fn a_call_whose_policy_tightened_while_held_is_refused() {
    let model = ScriptedModel::new([
        ScriptedTurn::ToolCalls(vec![
            ("c1".into(), "pay".into(), json!({})),
            ("c2".into(), "lookup".into(), json!({})),
        ]),
        ScriptedTurn::text("paid"),
    ]);
    let tools = tools();
    let (runtime, _, execution_id, action) = held_run(&model, &tools).await;
    let tightened = ScriptedTools::new()
        .with("lookup", true, |_| Ok("found".into()))
        .with("pay", true, |_| Ok("paid".into()));
    let (_, result) = collect_events(runtime.resume(
        resume_with(&runtime, execution_id, &action),
        context(&model, &tightened),
    ))
    .await
    .expect("the resume goes on");
    assert_eq!(result.content[0].text.as_deref(), Some("paid"));
    assert_eq!(tightened.executed(), ["pay"]);
    assert!(tools.executed().is_empty());
}

#[tokio::test]
async fn a_missing_held_record_fails_closed() {
    let model = ScriptedModel::new([ScriptedTurn::tool_call("c1", "pay", json!({}))]);
    let tools = tools();
    let (runtime, states, execution_id, action) = held_run(&model, &tools).await;
    let mut record = states.load(execution_id).await.unwrap().unwrap();
    record.state["entries"] = json!([]);
    states.save(&record, None).await.unwrap();
    let err = collect_events(runtime.resume(
        resume_with(&runtime, execution_id, &action),
        context(&model, &tools),
    ))
    .await
    .unwrap_err();
    assert_eq!(err.errcode.as_str(), "BEM-399");
    assert!(
        tools.executed().is_empty(),
        "the gated tool never runs without an answer"
    );
    assert_eq!(
        states.load(execution_id).await.unwrap().unwrap().status,
        "interrupted"
    );
}

/// A gateway that never answers turn 2 the first time it is asked: the worker
/// is killed while waiting on it.
struct HangsOnce {
    inner: ScriptedModel,
    hung: std::sync::atomic::AtomicBool,
}

impl ModelCaller for HangsOnce {
    fn stream(&self, call: ModelCall) -> ModelStream {
        if call.turn == 2 && !self.hung.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return stream::pending().boxed();
        }
        self.inner.stream(call)
    }
}

#[tokio::test]
async fn a_restarted_run_continues_from_its_last_saved_step() {
    let model = ScriptedModel::new([
        ScriptedTurn::tool_call("c1", "add", json!({"a": 1})),
        ScriptedTurn::text("the answer is 2"),
    ]);
    let tools = tools();
    let gateway = Arc::new(HangsOnce {
        inner: model.clone(),
        hung: std::sync::atomic::AtomicBool::new(false),
    });
    let ctx = || RuntimeContext {
        model_gateway: gateway.clone(),
        tool_runtime: Arc::new(tools.clone()),
        ..RuntimeContext::default()
    };
    let states = Arc::new(InMemoryRuntimeStateStore::new());
    let runtime = RigRuntime::new(def(), states.clone());
    let execution_id = Uuid::new_v4();

    // The worker dies waiting on turn 2, after turn 1 and its tool result were saved.
    let mut stream = runtime.start(start_request(execution_id, input("add one")), ctx());
    while let Ok(Some(_)) = tokio::time::timeout(Duration::from_millis(200), stream.next()).await {}
    drop(stream);
    assert_eq!(tools.executed(), ["add"]);
    assert_eq!(
        states.load(execution_id).await.unwrap().unwrap().status,
        "running"
    );

    // The retry starts again and picks up at turn 2.
    let (events, done) =
        collect_events(runtime.start(start_request(execution_id, input("add one")), ctx()))
            .await
            .unwrap();
    assert_eq!(events[0].payload["continued"], true);
    assert_eq!(done.content[0].text.as_deref(), Some("the answer is 2"));
    assert_eq!(
        tools.executed(),
        ["add"],
        "the finished tool step is not run again"
    );
    assert_eq!(model.calls(), 2, "turn 1 is not sent again");
    let steps: Vec<_> = events
        .iter()
        .filter_map(|e| e.step.as_ref())
        .map(|s| s.step_name.as_str())
        .collect();
    assert_eq!(steps, ["model"]);
}

/// A gateway that answers without streaming.
struct Whole;

impl ModelCaller for Whole {
    fn stream(&self, _call: ModelCall) -> ModelStream {
        let reply = ModelReply {
            choice: vec![rig_core::message::AssistantContent::text("whole answer")],
            usage: rig_core::completion::Usage::new(),
            finish_reason: None,
            raw: json!({}),
            model_used: Some("m".into()),
        };
        stream::iter([Ok(ModelStreamItem::Completed(reply))]).boxed()
    }
}

#[tokio::test]
async fn direct_model_runtime_reads_a_reply_without_deltas() {
    let runtime = DirectModelRuntime::model(None, None);
    let ctx = RuntimeContext {
        model_gateway: Arc::new(Whole),
        ..RuntimeContext::default()
    };
    let (events, result) =
        collect_events(runtime.start(start_request(Uuid::new_v4(), input("q")), ctx))
            .await
            .unwrap();
    assert_eq!(result.content[0].text.as_deref(), Some("whole answer"));
    assert_eq!(result.model_used.as_deref(), Some("m"));
    assert_eq!(
        events
            .iter()
            .filter(|e| e.event_type == RuntimeEventType::MessageDelta)
            .count(),
        1
    );
}

#[tokio::test]
async fn a_computed_empty_tool_list_offers_no_tools() {
    let model = ScriptedModel::new([ScriptedTurn::text("no tools")]);
    let runtime = RigRuntime::new(def(), Arc::new(InMemoryRuntimeStateStore::new()));
    let ctx = RuntimeContext {
        available_tools: vec![],
        restrict_tools: true,
        ..context(&model, &tools())
    };
    collect_events(runtime.start(start_request(Uuid::new_v4(), input("hi")), ctx))
        .await
        .unwrap();
    assert!(model.seen()[0].prepared.tools.is_empty());
}

fn image_part(part_type: ContentPartType) -> ContentPart {
    ContentPart {
        part_type,
        text: None,
        ..ContentPart::text("")
    }
}

/// The images of a user message, as `(source, media type)`.
fn images(message: &Message) -> Vec<(DocumentSourceKind, Option<ImageMediaType>)> {
    match message {
        Message::User { content } => content
            .iter()
            .filter_map(|c| match c {
                UserContent::Image(image) => Some((image.data.clone(), image.media_type.clone())),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

#[tokio::test]
async fn rig_sends_the_input_and_history_images_to_the_model() {
    let model = ScriptedModel::new([ScriptedTurn::text("a cat")]);
    let tools = tools();
    let runtime = RigRuntime::new(def(), Arc::new(InMemoryRuntimeStateStore::new()));
    let mut ctx = context(&model, &tools);
    let mut earlier =
        beam_types::AgentMessage::text(beam_types::AgentMessageRole::Human, "and this?");
    earlier.content.push(ContentPart {
        url: Some("https://example.com/dog.png".into()),
        ..image_part(ContentPartType::ImageUrl)
    });
    ctx.history = vec![
        earlier,
        beam_types::AgentMessage::text(beam_types::AgentMessageRole::Ai, "a dog"),
    ];
    let mut now = input("what is this?");
    now.content.push(ContentPart {
        data: Some("aGk=".into()),
        mime_type: Some("image/jpeg".into()),
        ..image_part(ContentPartType::ImageBase64)
    });
    collect_events(runtime.start(start_request(Uuid::new_v4(), now), ctx))
        .await
        .unwrap();

    let call = &model.seen()[0];
    let Message::User { content } = &call.prompt else {
        panic!("the prompt is a user message: {:?}", call.prompt);
    };
    assert_eq!(content[0], UserContent::text("what is this?"));
    assert_eq!(
        images(&call.prompt),
        [(
            DocumentSourceKind::base64("aGk="),
            Some(ImageMediaType::JPEG)
        )]
    );
    let history: Vec<_> = call.prepared.chat_history.iter().flat_map(images).collect();
    assert_eq!(
        history,
        [(DocumentSourceKind::url("https://example.com/dog.png"), None)]
    );
}

#[tokio::test]
async fn direct_model_runtime_sends_the_input_images() {
    let model = ScriptedModel::new([ScriptedTurn::text("a cat")]);
    let runtime = DirectModelRuntime::model(None, None);
    let mut now = input("what is this?");
    // No type named: sent as PNG, as Python does.
    now.content.push(ContentPart {
        data: Some("aGk=".into()),
        ..image_part(ContentPartType::ImageBase64)
    });
    let ctx = RuntimeContext {
        model_gateway: Arc::new(model.clone()),
        ..RuntimeContext::default()
    };
    collect_events(runtime.start(start_request(Uuid::new_v4(), now), ctx))
        .await
        .unwrap();
    assert_eq!(
        images(&model.seen()[0].prompt),
        [(
            DocumentSourceKind::base64("aGk="),
            Some(ImageMediaType::PNG)
        )]
    );
}

fn client_tools() -> ScriptedTools {
    tools()
        .with_client("request_input", false)
        .with_client("confirm_card", true)
}

fn client_def() -> RigAgentDef {
    let mut def = def();
    def.tools
        .extend(["request_input".into(), "confirm_card".into()]);
    def
}

fn answer(
    action: &Value,
    status: ActionResponseStatus,
    payload: Option<Value>,
) -> ActionResponseEnvelope {
    ActionResponseEnvelope {
        status,
        payload,
        ..approval(action, true)
    }
}

/// Start `client_def()` with `turns`, then resume its held actions with
/// `answers` (one per action) and return the done result and the model.
async fn client_round_trip(
    turns: Vec<ScriptedTurn>,
    answers: impl Fn(&[Value]) -> Vec<ActionResponseEnvelope>,
) -> (Vec<Value>, AgentResult, ScriptedModel, ScriptedTools) {
    let model = ScriptedModel::new(turns);
    let tools = client_tools();
    let runtime = RigRuntime::new(client_def(), Arc::new(InMemoryRuntimeStateStore::new()));
    let execution_id = Uuid::new_v4();
    let (_, held) = collect_events(runtime.start(
        start_request(execution_id, input("fill the form")),
        context(&model, &tools),
    ))
    .await
    .unwrap();
    assert!(held.interrupted);
    let actions = held.action_required.unwrap();
    let next = input("answered");
    let (_, done) = collect_events(runtime.resume(
        RuntimeResumeRequest {
            execution_id,
            run_id: next.run_id,
            runtime_ref: runtime.allocate_ref(execution_id),
            actions: answers(&actions),
            input: next,
        },
        context(&model, &tools),
    ))
    .await
    .unwrap();
    (actions, done, model, tools)
}

#[tokio::test]
async fn a_client_tool_holds_the_run_and_its_output_reaches_the_model() {
    let before = chrono::Utc::now();
    let (actions, done, model, tools) = client_round_trip(
        vec![
            ScriptedTurn::tool_call("c1", "request_input", json!({"title": "Income"})),
            ScriptedTurn::text("thanks"),
        ],
        |actions| {
            vec![answer(
                &actions[0],
                ActionResponseStatus::Submitted,
                Some(json!({"output": {"income": 5000}})),
            )]
        },
    )
    .await;
    assert_eq!(actions.len(), 1);
    let request = &actions[0]["action_request"];
    assert_eq!(
        (
            &request["kind"],
            &request["action_type"],
            &request["request_id"]
        ),
        (
            &json!("beam.client_tool"),
            &json!("client_tool"),
            &json!("c1")
        )
    );
    let payload = &request["payload"];
    assert_eq!(payload["tool_name"], "request_input");
    assert_eq!(payload["args"], json!({"title": "Income"}));
    assert_eq!(payload["requires_confirmation"], false);
    let expires: chrono::DateTime<chrono::Utc> =
        payload["expires_at"].as_str().unwrap().parse().unwrap();
    let wait = (expires - before).num_seconds();
    assert!((599..=601).contains(&wait), "{wait}");

    assert_eq!(done.content[0].text.as_deref(), Some("thanks"));
    assert!(
        tools.executed().is_empty(),
        "a client tool never runs on the server"
    );
    let prompt = format!("{:?}", model.seen()[1].prompt);
    assert!(
        prompt.contains("income") && prompt.contains("5000"),
        "{prompt}"
    );
}

#[tokio::test]
async fn a_client_tool_the_client_did_not_run_reaches_the_model_as_rc8_text() {
    let cases = [
        (
            ActionResponseStatus::Submitted,
            Some(json!({"error": "tab closed"})),
            "tab closed",
        ),
        (
            ActionResponseStatus::Submitted,
            Some(json!({"approved": false})),
            "declined by user",
        ),
        (ActionResponseStatus::Cancelled, None, "declined by user"),
        (
            ActionResponseStatus::Expired,
            None,
            "client tool expired: no capable client responded",
        ),
    ];
    for (status, payload, expected) in cases {
        let (_, done, model, _) = client_round_trip(
            vec![
                ScriptedTurn::tool_call("c1", "request_input", json!({})),
                ScriptedTurn::text("ok"),
            ],
            |actions| vec![answer(&actions[0], status, payload.clone())],
        )
        .await;
        assert_eq!(done.content[0].text.as_deref(), Some("ok"));
        let prompt = format!("{:?}", model.seen()[1].prompt);
        assert!(prompt.contains(expected), "{status:?}: {prompt}");
    }
}

#[tokio::test]
async fn a_batch_holds_client_and_approval_calls_and_runs_the_rest_on_resume() {
    let (actions, done, _, tools) = client_round_trip(
        vec![
            ScriptedTurn::ToolCalls(vec![
                ("c1".into(), "add".into(), json!({"a": 1})),
                ("c2".into(), "request_input".into(), json!({})),
                ("c3".into(), "pay".into(), json!({})),
                ("c4".into(), "confirm_card".into(), json!({})),
            ]),
            ScriptedTurn::text("done"),
        ],
        |actions| {
            actions
                .iter()
                .map(|a| match a["action_request"]["kind"].as_str() {
                    Some("beam.client_tool") => answer(
                        a,
                        ActionResponseStatus::Submitted,
                        Some(json!({"output": "filled", "approved": true})),
                    ),
                    _ => approval(a, true),
                })
                .collect()
        },
    )
    .await;
    let kinds: Vec<_> = actions
        .iter()
        .map(|a| {
            (
                a["action_request"]["request_id"]
                    .as_str()
                    .unwrap()
                    .to_owned(),
                a["action_request"]["kind"].as_str().unwrap().to_owned(),
                a["action_request"]["payload"]["requires_confirmation"].clone(),
            )
        })
        .collect();
    assert_eq!(
        kinds,
        [
            ("c2".to_owned(), "beam.client_tool".to_owned(), json!(false)),
            (
                "c3".to_owned(),
                "beam.tool_approval".to_owned(),
                Value::Null
            ),
            ("c4".to_owned(), "beam.client_tool".to_owned(), json!(true)),
        ]
    );
    assert_eq!(done.content[0].text.as_deref(), Some("done"));
    assert_eq!(tools.executed(), ["add", "pay"]);
}

#[tokio::test]
async fn a_run_held_before_client_tools_resumes_as_an_approval() {
    let model = ScriptedModel::new([
        ScriptedTurn::tool_call("c1", "pay", json!({})),
        ScriptedTurn::text("paid"),
    ]);
    let tools = tools();
    let (runtime, states, execution_id, action) = held_run(&model, &tools).await;
    // The previous build wrote held calls without `kind`.
    let mut record = states.load(execution_id).await.unwrap().unwrap();
    let entries = record.state["entries"].as_array_mut().unwrap();
    let held = entries
        .iter_mut()
        .rev()
        .find(|e| e["kind"] == "beam.held_calls")
        .unwrap();
    for call in held["value"].as_array_mut().unwrap() {
        assert!(call.as_object_mut().unwrap().remove("kind").is_some());
    }
    states.save(&record, None).await.unwrap();
    // Its console answers with the old kind and the old random request id.
    let mut request = resume_with(&runtime, execution_id, &action);
    request.actions[0].kind = "beam.approval".into();
    request.actions[0].request_id = Uuid::new_v4().to_string();
    let (_, done) = collect_events(runtime.resume(request, context(&model, &tools)))
        .await
        .unwrap();
    assert_eq!(done.content[0].text.as_deref(), Some("paid"));
    assert_eq!(tools.executed(), ["pay"]);
}

#[tokio::test]
async fn a_response_of_another_kind_does_not_answer_a_held_call() {
    let model = ScriptedModel::new([ScriptedTurn::tool_call("c1", "pay", json!({}))]);
    let tools = tools();
    let (runtime, states, execution_id, action) = held_run(&model, &tools).await;
    let mut request = resume_with(&runtime, execution_id, &action);
    // A client-tool answer with no payload would read as approved.
    request.actions[0].kind = "beam.client_tool".into();
    request.actions[0].payload = Some(json!({}));
    let err = collect_events(runtime.resume(request, context(&model, &tools)))
        .await
        .unwrap_err();
    assert_eq!(err.errcode.as_str(), "BEM-399");
    assert!(tools.executed().is_empty());
    assert_eq!(
        states.load(execution_id).await.unwrap().unwrap().status,
        "interrupted"
    );
}

/// Plan 030 (LLM01): a tool's result reaches the model inside `untrusted`
/// tags; the run's record keeps it as the tool gave it. Spotlighting off
/// sends it as it came.
#[tokio::test]
async fn tool_results_reach_the_model_tagged_as_untrusted() {
    for spotlight in [beam_types::Spotlight::Tagged, beam_types::Spotlight::Off] {
        let model = ScriptedModel::new([
            ScriptedTurn::tool_call("c1", "lookup", json!({})),
            ScriptedTurn::text("done"),
        ]);
        let tools = tools();
        let runtime = RigRuntime::new(def(), Arc::new(InMemoryRuntimeStateStore::new()));
        let ctx = RuntimeContext {
            spotlight,
            ..context(&model, &tools)
        };
        let (_, result) =
            collect_events(runtime.start(start_request(Uuid::new_v4(), input("hi")), ctx))
                .await
                .unwrap();
        assert_eq!(result.tool_calls[0].result.as_deref(), Some("found"));
        let second = &model.seen()[1];
        let prompt = format!("{:?} {:?}", second.prompt, second.prepared.chat_history);
        let tagged = prompt.contains("tool:lookup") && prompt.contains("found</untrusted>");
        assert_eq!(
            tagged,
            spotlight == beam_types::Spotlight::Tagged,
            "{prompt}"
        );
        assert!(prompt.contains("found"), "{prompt}");
    }
}
