//! `ToolRuntime` order, the tool service, `knowledge_search` and MCP over a stub server.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use beam_runtime::{ClientAnswer, ToolExecutor, ToolInvocation};
use beam_security::{
    SecurityAction, SecurityDecision, SecurityManagement, SecurityRequest, SecurityStage,
};
use beam_tools::{
    knowledge_search_tool, ExecuteContext, McpClient, ToolCallContext, ToolDefinition, ToolHandler,
    ToolRegistry, ToolRisk, ToolRuntime, ToolService, CLIENT_TOOL_TIMEOUT_SECONDS,
    KNOWLEDGE_SEARCH,
};
use beam_types::{AgentInput, InvokeAuthContext};
use riverbase_core::RiverbaseResult;
use serde_json::{json, Value};
use uuid::Uuid;

/// Counts calls; fails `fail_first` times, sleeps `delay` each call.
struct Counting {
    calls: AtomicUsize,
    fail_first: usize,
    delay: Duration,
}

const DOWN: riverbase_core::base::ErrorSpec = riverbase_core::base::ErrorSpec {
    error_type_base: "https://riverbase.io/~/rs/error/",
    http_status: 502,
    http_title: "Bad Gateway",
    errcode: "TST-001",
    errmesg: "flaky backend",
    errhint: None,
};

#[async_trait]
impl ToolHandler for Counting {
    async fn call(&self, args: Value, _ctx: &ToolCallContext) -> RiverbaseResult<Value> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(self.delay).await;
        if n < self.fail_first {
            return Err(DOWN.raise());
        }
        Ok(json!({"paid": args["amount"]}))
    }
}

fn counting(fail_first: usize, delay_ms: u64) -> Arc<Counting> {
    Arc::new(Counting {
        calls: AtomicUsize::new(0),
        fail_first,
        delay: Duration::from_millis(delay_ms),
    })
}

fn pay(handler: Arc<Counting>) -> ToolDefinition {
    ToolDefinition {
        input_schema: json!({"type": "object", "properties": {"amount": {"type": "integer"}}, "required": ["amount"]}),
        max_retries: 2,
        timeout_seconds: 0.2,
        ..ToolDefinition::new("pay")
    }
    .with_handler(handler)
}

fn ctx(execution_id: Uuid, call_id: &str) -> ExecuteContext {
    ExecuteContext {
        call: ToolCallContext {
            execution_id,
            call_id: call_id.into(),
            ..ToolCallContext::default()
        },
        ..ExecuteContext::default()
    }
}

#[tokio::test]
async fn a_side_effect_runs_once_per_execution_call_and_revision() {
    let runtime = ToolRuntime::default();
    let handler = counting(0, 0);
    let tool = pay(handler.clone());
    let execution = Uuid::new_v4();
    let first = runtime
        .execute(&tool, json!({"amount": 5}), &ctx(execution, "c1"))
        .await
        .unwrap();
    let again = runtime
        .execute(&tool, json!({"amount": 5}), &ctx(execution, "c1"))
        .await
        .unwrap();
    assert_eq!((first.cached, again.cached), (false, true));
    assert_eq!(again.output, json!({"paid": 5}));
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
    // Another call id runs again.
    runtime
        .execute(&tool, json!({"amount": 5}), &ctx(execution, "c2"))
        .await
        .unwrap();
    assert_eq!(handler.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn client_tools_run_on_the_client_and_their_answers_are_checked() {
    let wire: ToolDefinition =
        serde_json::from_value(json!({"name": "request_input", "source": "client"})).unwrap();
    assert!((wire.timeout_seconds - CLIENT_TOOL_TIMEOUT_SECONDS).abs() < f64::EPSILON);
    let explicit: ToolDefinition = serde_json::from_value(
        json!({"name": "highlight_field", "source": "client", "timeout_seconds": 30.0}),
    )
    .unwrap();
    assert!((explicit.timeout_seconds - 30.0).abs() < f64::EPSILON);
    let native: ToolDefinition = serde_json::from_value(json!({"name": "pay"})).unwrap();
    assert!((native.timeout_seconds - 30.0).abs() < f64::EPSILON);

    let runtime = ToolRuntime::default();
    let tool = ToolDefinition {
        input_schema: json!({"type": "object", "properties": {"title": {"type": "string"}}, "required": ["title"]}),
        output_schema: Some(
            json!({"type": "object", "properties": {"income": {"type": "integer"}}, "required": ["income"]}),
        ),
        ..ToolDefinition::client("request_input")
    };
    let args = json!({"title": "Income"});
    let r = runtime
        .execute(&tool, args.clone(), &ctx(Uuid::new_v4(), "c1"))
        .await
        .unwrap();
    assert_eq!(
        r.error.as_deref(),
        Some("client tool needs a resumable runtime")
    );

    let answer = |output: Value, error: Option<&str>| ClientAnswer {
        output,
        error: error.map(str::to_owned),
    };
    let ok = runtime
        .client_result(
            &tool,
            args.clone(),
            answer(json!({"income": 5000}), None),
            &ctx(Uuid::new_v4(), "c1"),
        )
        .await
        .unwrap();
    assert_eq!((ok.output, ok.error), (json!({"income": 5000}), None));
    let bad = runtime
        .client_result(
            &tool,
            args.clone(),
            answer(json!({"income": "lots"}), None),
            &ctx(Uuid::new_v4(), "c1"),
        )
        .await
        .unwrap();
    assert_eq!(
        bad.error.as_deref(),
        Some("invalid client output: invalid type for income: expected integer")
    );
    let failed = runtime
        .client_result(
            &tool,
            args,
            answer(Value::Null, Some("tab closed")),
            &ctx(Uuid::new_v4(), "c1"),
        )
        .await
        .unwrap();
    assert_eq!(failed.error.as_deref(), Some("tab closed"));
    let not_client = runtime
        .client_result(
            &pay(counting(0, 0)),
            json!({"amount": 1}),
            answer(json!(1), None),
            &ctx(Uuid::new_v4(), "c1"),
        )
        .await
        .unwrap();
    assert_eq!(
        not_client.error.as_deref(),
        Some("pay is not a client tool")
    );
}

#[tokio::test]
async fn refusals_follow_python_order_and_messages() {
    let runtime = ToolRuntime::default();
    let handler = counting(0, 0);
    let tool = pay(handler.clone());
    let mut c = ctx(Uuid::new_v4(), "c1");
    c.allowed = Some(vec!["other".into()]);
    let r = runtime
        .execute(&tool, json!({"amount": 1}), &c)
        .await
        .unwrap();
    assert_eq!(r.error.as_deref(), Some("undeclared tool: pay"));

    let mut c = ctx(Uuid::new_v4(), "c1");
    c.dry_run = true;
    let r = runtime
        .execute(&tool, json!({"amount": 1}), &c)
        .await
        .unwrap();
    assert_eq!(r.output, json!({"skipped": true, "reason": "dry_run"}));

    let gated = ToolDefinition {
        require_approval: true,
        ..pay(handler.clone())
    };
    let r = runtime
        .execute(&gated, json!({"amount": 1}), &ctx(Uuid::new_v4(), "c1"))
        .await
        .unwrap();
    assert_eq!(r.error.as_deref(), Some("approval required"));
    // The model cannot approve its own call with an `approved` argument.
    let r = runtime
        .execute(
            &gated,
            json!({"amount": 1, "approved": true}),
            &ctx(Uuid::new_v4(), "c1"),
        )
        .await
        .unwrap();
    assert_eq!(r.error.as_deref(), Some("approval required"));
    let mut approved = ctx(Uuid::new_v4(), "c1");
    approved.approved = true;
    assert!(runtime
        .execute(&gated, json!({"amount": 1}), &approved)
        .await
        .unwrap()
        .error
        .is_none());
    // A high-risk tool needs approval without `require_approval` (rc8 `default_mode`).
    let risky = ToolDefinition {
        risk: ToolRisk::High,
        ..pay(handler.clone())
    };
    let r = runtime
        .execute(&risky, json!({"amount": 1}), &ctx(Uuid::new_v4(), "c1"))
        .await
        .unwrap();
    assert_eq!(r.error.as_deref(), Some("approval required"));

    let r = runtime
        .execute(&tool, json!({}), &ctx(Uuid::new_v4(), "c1"))
        .await
        .unwrap();
    assert_eq!(
        r.error.as_deref(),
        Some("missing required args: ['amount']")
    );
    let r = runtime
        .execute(&tool, json!({"amount": "five"}), &ctx(Uuid::new_v4(), "c1"))
        .await
        .unwrap();
    assert_eq!(
        r.error.as_deref(),
        Some("invalid type for amount: expected integer")
    );
    assert_eq!(
        handler.calls.load(Ordering::SeqCst),
        1,
        "only the approved call ran"
    );
}

#[tokio::test]
async fn failures_retry_and_a_timeout_does_not() {
    let runtime = ToolRuntime::default();
    let flaky = counting(2, 0);
    let r = runtime
        .execute(
            &pay(flaky.clone()),
            json!({"amount": 1}),
            &ctx(Uuid::new_v4(), "c"),
        )
        .await
        .unwrap();
    assert!(r.error.is_none());
    assert_eq!(flaky.calls.load(Ordering::SeqCst), 3, "max_retries = 2");

    let down = counting(10, 0);
    let r = runtime
        .execute(
            &pay(down.clone()),
            json!({"amount": 1}),
            &ctx(Uuid::new_v4(), "c"),
        )
        .await
        .unwrap();
    assert_eq!(r.error.as_deref(), Some("flaky backend"));
    assert_eq!(down.calls.load(Ordering::SeqCst), 3);

    let slow = counting(0, 1000);
    let r = runtime
        .execute(
            &pay(slow.clone()),
            json!({"amount": 1}),
            &ctx(Uuid::new_v4(), "c"),
        )
        .await
        .unwrap();
    assert_eq!(r.error.as_deref(), Some("tool timeout"));
    assert_eq!(
        slow.calls.load(Ordering::SeqCst),
        1,
        "Python does not retry a timeout"
    );
}

struct BlockTools;

#[async_trait]
impl SecurityManagement for BlockTools {
    async fn check(&self, request: &SecurityRequest) -> SecurityDecision {
        if request.stage == SecurityStage::BeforeTool {
            SecurityDecision {
                action: SecurityAction::Deny,
                user_message: Some("no tools today".into()),
                ..SecurityDecision::default()
            }
        } else {
            SecurityDecision::allow()
        }
    }
}

#[tokio::test]
async fn a_security_block_stops_the_call() {
    let runtime = ToolRuntime::default().with_security(Arc::new(BlockTools));
    let handler = counting(0, 0);
    let err = runtime
        .execute(
            &pay(handler.clone()),
            json!({"amount": 1}),
            &ctx(Uuid::new_v4(), "c"),
        )
        .await
        .unwrap_err();
    assert_eq!(err.errcode.as_str(), "BEM-061");
    assert_eq!(handler.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn the_service_speaks_to_runtimes() {
    let registry = Arc::new(ToolRegistry::new());
    let handler = counting(0, 0);
    registry.register(pay(handler.clone()));
    registry.register(ToolDefinition {
        require_approval: true,
        ..ToolDefinition::new("approve_me")
    });
    registry.register(ToolDefinition {
        risk: ToolRisk::High,
        ..ToolDefinition::new("risky")
    });
    let service = ToolService::new(registry, Arc::new(ToolRuntime::default()));
    let defs = service.definitions(&["pay".into(), "missing".into()]);
    assert_eq!(defs.len(), 1);
    assert_eq!(defs[0].parameters["required"], json!(["amount"]));
    assert!(service.requires_approval("approve_me"));
    assert!(
        service.requires_approval("risky"),
        "rc8 holds high-risk tools"
    );
    assert!(!service.requires_approval("pay"));
    let invocation = |name: &str, args: Value| ToolInvocation {
        execution_id: Uuid::new_v4(),
        run_id: Uuid::new_v4(),
        call_id: "c1".into(),
        name: name.into(),
        args,
        input: None,
        allowed: None,
        dry_run: false,
        approved: false,
    };
    assert_eq!(
        service
            .execute(invocation("pay", json!({"amount": 3})))
            .await
            .unwrap(),
        "{\"paid\": 3}"
    );
    assert_eq!(
        service.execute(invocation("pay", json!({}))).await.unwrap(),
        "{\"error\": \"missing required args: ['amount']\"}"
    );
    assert_eq!(
        service
            .execute(invocation("missing", json!({})))
            .await
            .unwrap_err()
            .errcode
            .as_str(),
        "BEM-470"
    );
}

#[tokio::test]
async fn knowledge_search_ignores_the_callers_user() {
    use beam_knowledge::{HashEmbedder, InMemoryVectorStore, KnowledgeManager};
    let knowledge = Arc::new(KnowledgeManager::new(
        Arc::new(HashEmbedder::new(64)),
        Arc::new(InMemoryVectorStore::new()),
    ));
    knowledge
        .ingest(beam_knowledge::IngestRequest {
            scope: beam_knowledge::KnowledgeScope::User,
            user_id: Some("alice".into()),
            source_label: Some("a.md".into()),
            ..beam_knowledge::IngestRequest::text("alpha secret for alice", "alice")
        })
        .await
        .unwrap();
    knowledge
        .ingest(beam_knowledge::IngestRequest {
            scope: beam_knowledge::KnowledgeScope::User,
            user_id: Some("bob".into()),
            source_label: Some("b.md".into()),
            ..beam_knowledge::IngestRequest::text("alpha secret for bob", "bob")
        })
        .await
        .unwrap();
    let tool = knowledge_search_tool(knowledge);
    assert_eq!(tool.name, KNOWLEDGE_SEARCH);
    let call = ToolCallContext {
        execution_id: Uuid::new_v4(),
        call_id: "k".into(),
        auth: Some(InvokeAuthContext {
            profile_id: None,
            user_id: Some("alice".into()),
            org_id: None,
            mcp_token: None,
        }),
        input: None,
    };
    let hits = tool
        .callable
        .as_ref()
        .unwrap()
        .call(
            json!({"query": "alpha secret", "user_id": "bob", "k": 10}),
            &call,
        )
        .await
        .unwrap();
    let texts: Vec<&str> = hits
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|h| h["text"].as_str())
        .collect();
    assert!(!texts.is_empty());
    assert!(texts.iter().all(|t| t.contains("alice")), "{texts:?}");
}

// --- MCP over a JSON-RPC stub -------------------------------------------------------

#[derive(Clone, Default)]
struct Mcp {
    seen: Arc<Mutex<Vec<(String, HeaderMap)>>>,
    /// Refuse this many `tools/call` requests with "Session not found".
    terminate: Arc<AtomicUsize>,
}

async fn mcp_handler(
    State(mcp): State<Mcp>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let method = body["method"].as_str().unwrap_or_default().to_owned();
    mcp.seen
        .lock()
        .unwrap()
        .push((method.clone(), headers.clone()));
    let id = body["id"].clone();
    let result = match method.as_str() {
        "initialize" => {
            let reply = Json(
                json!({"jsonrpc": "2.0", "id": id, "result": {"protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}}, "serverInfo": {"name": "stub", "version": "1"}}}),
            );
            return ([("mcp-session-id", "s-1")], reply).into_response();
        }
        "tools/list" => json!({"tools": [
            {"name": "search", "description": "Search", "inputSchema": {"type": "object", "properties": {"q": {"type": "string"}}}},
            {"name": "delete_all", "description": "Danger", "inputSchema": {"type": "object"}}]}),
        "tools/call" => {
            if mcp.terminate.load(Ordering::SeqCst) > 0 {
                mcp.terminate.fetch_sub(1, Ordering::SeqCst);
                return (StatusCode::NOT_FOUND, "Session not found").into_response();
            }
            let q = body["params"]["arguments"]["q"]
                .as_str()
                .unwrap_or_default();
            json!({"content": [{"type": "text", "text": format!("found {q}")}], "isError": false})
        }
        _ => json!({}),
    };
    let sse = format!(
        "event: message\ndata: {}\n\n",
        json!({"jsonrpc": "2.0", "id": id, "result": result})
    );
    ([("content-type", "text/event-stream")], sse).into_response()
}

async fn mcp_server() -> (String, Mcp) {
    let mcp = Mcp::default();
    let app = Router::new()
        .route("/mcp", axum::routing::post(mcp_handler))
        .with_state(mcp.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}/mcp"), mcp)
}

fn input_with_token(token: &str) -> Arc<AgentInput> {
    Arc::new(AgentInput {
        content: vec![],
        session_id: Uuid::new_v4(),
        interaction_id: None,
        execution_id: None,
        request_id: None,
        run_id: Uuid::new_v4(),
        runtime_ref: None,
        agent_name: Some("a".into()),
        model_override: None,
        metadata: Value::Null,
        auth_context: Some(InvokeAuthContext {
            profile_id: None,
            user_id: Some("u".into()),
            org_id: None,
            mcp_token: Some(token.into()),
        }),
        stream: false,
        actions: None,
        context: None,
        client: None,
    })
}

#[tokio::test]
async fn an_mcp_allowlist_exposes_only_listed_tools_and_forwards_the_token() {
    let (url, mcp) = mcp_server().await;
    let client = Arc::new(
        McpClient::connect_http("crm", url, HashMap::new())
            .await
            .unwrap(),
    );
    assert_eq!(client.tools().len(), 2);
    let registry = Arc::new(ToolRegistry::new());
    let allowed: HashSet<String> = ["search".to_owned()].into();
    let registered = registry.register_mcp_server(&client, Some(&allowed));
    assert_eq!(
        registered
            .iter()
            .map(|d| d.name.as_str())
            .collect::<Vec<_>>(),
        ["crm__search"]
    );
    assert!(
        registry.get("crm__delete_all").is_none(),
        "unlisted tools are unreachable"
    );
    // Without an allowlist, nothing (BEAM_MCP_REQUIRE_ALLOWLIST defaults on).
    assert!(ToolRegistry::new()
        .register_mcp_server(&client, None)
        .is_empty());

    let service = ToolService::new(registry, Arc::new(ToolRuntime::default()));
    mcp.terminate.store(1, Ordering::SeqCst);
    let out = service
        .execute(ToolInvocation {
            execution_id: Uuid::new_v4(),
            run_id: Uuid::new_v4(),
            call_id: "m1".into(),
            name: "crm__search".into(),
            args: json!({"q": "acme", "limit": null}),
            input: Some(input_with_token("tok-123")),
            allowed: None,
            dry_run: false,
            approved: false,
        })
        .await
        .unwrap();
    assert_eq!(out, "found acme", "a terminated session was retried");
    let seen = mcp.seen.lock().unwrap().clone();
    let calls: Vec<&HeaderMap> = seen
        .iter()
        .filter(|(m, _)| m == "tools/call")
        .map(|(_, h)| h)
        .collect();
    assert_eq!(calls.len(), 2);
    assert!(calls.iter().all(|h| h["x-mcp-token"] == "tok-123"));
    assert!(calls.iter().all(|h| h["mcp-session-id"] == "s-1"));
}

#[tokio::test]
async fn a_stdio_server_gets_only_a_safe_environment() {
    // The "server" writes its environment to a file and exits; the handshake
    // then fails, which is fine: only the environment matters here.
    let dir = std::env::temp_dir().join(format!("beam-mcp-env-{}", Uuid::new_v4()));
    let script = format!("env > '{}'", dir.display());
    let extra = HashMap::from([("MCP_EXTRA".to_owned(), "yes".to_owned())]);
    let _ = McpClient::connect_stdio("envdump", "sh", &["-c".to_owned(), script], &extra).await;
    let mut env = String::new();
    for _ in 0..50 {
        if let Ok(text) = std::fs::read_to_string(&dir) {
            if !text.is_empty() {
                env = text;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let _ = std::fs::remove_file(&dir);
    assert!(env.contains("PATH="), "the child saw no PATH");
    assert!(env.contains("MCP_EXTRA=yes"));
    // cargo sets this for the test process; a child must not inherit it.
    assert!(std::env::var("CARGO_MANIFEST_DIR").is_ok());
    // Never print the environment: it holds secrets.
    assert!(
        !env.contains("CARGO_MANIFEST_DIR"),
        "the worker's environment leaked"
    );
}

struct Unicode;

#[async_trait]
impl ToolHandler for Unicode {
    async fn call(&self, _args: Value, _ctx: &ToolCallContext) -> RiverbaseResult<Value> {
        Ok(json!({"text": "tiếng Việt"}))
    }
}

#[tokio::test]
async fn tool_content_keeps_unicode_like_langchain() {
    let registry = Arc::new(ToolRegistry::new());
    registry.register(ToolDefinition::new("say").with_handler(Arc::new(Unicode)));
    let service = ToolService::new(registry, Arc::new(ToolRuntime::default()));
    let out = service
        .execute(ToolInvocation {
            execution_id: Uuid::new_v4(),
            run_id: Uuid::new_v4(),
            call_id: "u".into(),
            name: "say".into(),
            args: json!({}),
            input: None,
            allowed: None,
            dry_run: false,
            approved: false,
        })
        .await
        .unwrap();
    assert_eq!(out, "{\"text\": \"tiếng Việt\"}");
}

// --- Native sandbox tools -----------------------------------------------------------

async fn run_tool(service: &ToolService, name: &str, args: Value) -> Value {
    let out = service
        .execute(ToolInvocation {
            execution_id: Uuid::new_v4(),
            run_id: Uuid::new_v4(),
            call_id: name.into(),
            name: name.into(),
            args,
            input: None,
            allowed: None,
            dry_run: false,
            approved: false,
        })
        .await
        .unwrap();
    serde_json::from_str(&out).unwrap()
}

#[tokio::test]
async fn native_sandbox_tools_run_through_the_service() {
    let dir = tempfile::tempdir().unwrap();
    let root = beam_sandbox::SandboxRoot::new(dir.path()).unwrap();
    let tree = beam_sandbox::WorkingTree::new(root, vec!["main".into()]);
    tree.init().unwrap();
    let registry = Arc::new(ToolRegistry::new());
    for tool in beam_tools::native::sandbox_tools(Some(tree)) {
        registry.register_native(tool);
    }
    let service = ToolService::new(registry, Arc::new(ToolRuntime::default()));

    let written = run_tool(
        &service,
        "write_file",
        json!({"path": "notes/a.txt", "body": "hello"}),
    )
    .await;
    assert_eq!(written["written"], true, "{written}");
    let read = run_tool(&service, "read_file", json!({"path": "notes/a.txt"})).await;
    assert_eq!(read["body"], "hello");
    let listed = run_tool(&service, "list_tree", json!({})).await;
    assert_eq!(listed["paths"], json!(["notes/a.txt"]));
    let status = run_tool(&service, "git_status", json!({})).await;
    assert!(!status["paths"].as_array().unwrap().is_empty(), "{status}");
    // The jail holds: a path outside the root is an error for the model.
    let escaped = run_tool(&service, "read_file", json!({"path": "../outside.txt"})).await;
    assert!(escaped.get("error").is_some(), "{escaped}");
}
