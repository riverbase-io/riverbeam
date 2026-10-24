//! The D7 providers over their real wire formats, against a local stub server:
//! hermetic (no network, no keys). Each test checks what rig sends (path, auth,
//! model) and that the gateway reads the streamed reply, usage and tool calls.

use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::Router;
use beam_gateway::{GatewayFactory, RigProviderFactory};
use beam_runtime::{ModelCall, ModelCaller, ModelReply, ModelStreamItem};
use beam_types::ModelConfig;
use futures_util::StreamExt;
use rig_agent::run::{prepare_request, RunSpec};
use rig_core::completion::{Message, ProviderCapabilities, ToolDefinition};
use rig_core::message::{AssistantContent, ImageMediaType, ToolName, UserContent};
use serde_json::{json, Value};
use uuid::Uuid;

#[derive(Debug, Clone)]
struct Seen {
    path: String,
    headers: HeaderMap,
    body: Value,
}

#[derive(Clone)]
struct Stub {
    seen: Arc<Mutex<Vec<Seen>>>,
    status: StatusCode,
    content_type: &'static str,
    body: String,
}

async fn handle(State(stub): State<Stub>, uri: Uri, headers: HeaderMap, body: Bytes) -> Response {
    stub.seen.lock().unwrap().push(Seen {
        path: uri.to_string(),
        headers,
        body: serde_json::from_slice(&body).unwrap_or(Value::Null),
    });
    (
        stub.status,
        [("content-type", stub.content_type)],
        stub.body.clone(),
    )
        .into_response()
}

/// Serve `body` for every request; returns the base URL and the request log.
async fn serve(
    status: StatusCode,
    content_type: &'static str,
    body: String,
) -> (String, Arc<Mutex<Vec<Seen>>>) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let stub = Stub {
        seen: seen.clone(),
        status,
        content_type,
        body,
    };
    let app = Router::new().fallback(handle).with_state(stub);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}"), seen)
}

fn sse(events: &[(&str, Value)]) -> String {
    events
        .iter()
        .map(|(event, data)| {
            if event.is_empty() {
                format!("data: {data}\n\n")
            } else {
                format!("event: {event}\ndata: {data}\n\n")
            }
        })
        .collect()
}

fn call(with_tool: bool) -> ModelCall {
    let tools = if with_tool {
        vec![ToolDefinition::new(
            ToolName::new("add").unwrap(),
            "add one",
            json!({"type": "object", "properties": {"a": {"type": "integer"}}}),
        )]
    } else {
        vec![]
    };
    let spec = RunSpec {
        preamble: Some("Be brief.".into()),
        ..RunSpec::new()
    };
    ModelCall {
        execution_id: Uuid::new_v4(),
        run_id: Uuid::new_v4(),
        turn: 1,
        model: None,
        prompt: Message::user("hi"),
        prepared: prepare_request(
            &spec,
            &ProviderCapabilities::default(),
            &[],
            tools,
            None,
            None,
        )
        .unwrap(),
    }
}

async fn run(model: ModelConfig, call: ModelCall) -> Result<(String, ModelReply), String> {
    let factory = Arc::new(GatewayFactory::new(
        vec![model.clone()],
        Arc::new(RigProviderFactory),
    ));
    let gateway = factory.gateway(&[model]).map_err(|e| e.to_string())?;
    let mut stream = gateway.stream(call);
    let mut text = String::new();
    while let Some(item) = stream.next().await {
        match item {
            Ok(ModelStreamItem::TextDelta(t)) => text.push_str(&t),
            Ok(ModelStreamItem::Completed(reply)) => return Ok((text, reply)),
            Err(e) => return Err(format!("{}: {:?}", e.errcode.as_str(), e.errdata)),
        }
    }
    Err("no reply".into())
}

fn model(value: Value) -> ModelConfig {
    serde_json::from_value(value).unwrap()
}

fn texts(reply: &ModelReply) -> String {
    reply
        .choice
        .iter()
        .filter_map(|c| match c {
            AssistantContent::Text(t) => Some(t.text.as_str()),
            _ => None,
        })
        .collect()
}

fn openai_chunks(tool: bool) -> String {
    let chunk = |delta: Value, finish: Value| {
        json!({"id": "c1", "object": "chat.completion.chunk", "created": 1, "model": "m",
               "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]})
    };
    let mut events = vec![
        (
            "",
            chunk(json!({"role": "assistant", "content": "Hel"}), Value::Null),
        ),
        ("", chunk(json!({"content": "lo"}), Value::Null)),
    ];
    if tool {
        events.push((
            "",
            chunk(
                json!({"tool_calls": [{"index": 0, "id": "call_1", "type": "function",
                                        "function": {"name": "add", "arguments": "{\"a\":1}"}}]}),
                Value::Null,
            ),
        ));
    }
    let mut last = chunk(json!({}), json!(if tool { "tool_calls" } else { "stop" }));
    last["usage"] = json!({"prompt_tokens": 11, "completion_tokens": 3, "total_tokens": 14});
    events.push(("", last));
    let mut body = sse(&events);
    body.push_str("data: [DONE]\n\n");
    body
}

#[tokio::test]
async fn openai_compatible_streams_text_and_tool_calls() {
    let (base, seen) = serve(StatusCode::OK, "text/event-stream", openai_chunks(true)).await;
    let config = model(json!({
        "name": "fast", "provider": "nvidia", "api_key": "test-key",
        "provider_config": {"model_id": "vendor/model-x", "base_url": format!("{base}/v1")},
        "max_tokens": 8000,
    }));
    let (text, reply) = run(config, call(true)).await.unwrap();
    assert_eq!(text, "Hello");
    assert_eq!(texts(&reply), "Hello");
    assert!(reply
        .choice
        .iter()
        .any(|c| matches!(c, AssistantContent::ToolCall(t) if t.function.name.as_str() == "add")));
    assert_eq!(reply.usage.total_tokens, Some(14));
    assert_eq!(reply.model_used.as_deref(), Some("fast"));
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/v1/chat/completions");
    assert_eq!(request.headers["authorization"], "Bearer test-key");
    assert_eq!(
        request.body["model"], "vendor/model-x",
        "provider_config.model_id is sent"
    );
    assert_eq!(request.body["stream"], true);
    // NVIDIA output cap: window 8000 / 4, as Python's output_tokens.
    let cap = request
        .body
        .get("max_tokens")
        .or_else(|| request.body.get("max_completion_tokens"));
    assert_eq!(cap, Some(&json!(2000)));
    assert_eq!(request.body["tools"][0]["function"]["name"], "add");
}

#[tokio::test]
async fn anthropic_streams_messages() {
    let body = sse(&[
        (
            "message_start",
            json!({"type": "message_start", "message": {"id": "msg_1", "type": "message",
            "role": "assistant", "model": "claude-x", "content": [], "stop_reason": null,
            "stop_sequence": null, "usage": {"input_tokens": 9, "output_tokens": 1}}}),
        ),
        (
            "content_block_start",
            json!({"type": "content_block_start", "index": 0,
            "content_block": {"type": "text", "text": ""}}),
        ),
        (
            "content_block_delta",
            json!({"type": "content_block_delta", "index": 0,
            "delta": {"type": "text_delta", "text": "Hi there"}}),
        ),
        (
            "content_block_stop",
            json!({"type": "content_block_stop", "index": 0}),
        ),
        (
            "message_delta",
            json!({"type": "message_delta", "delta": {"stop_reason": "end_turn",
            "stop_sequence": null}, "usage": {"output_tokens": 4}}),
        ),
        ("message_stop", json!({"type": "message_stop"})),
    ]);
    let (base, seen) = serve(StatusCode::OK, "text/event-stream", body).await;
    let config = model(
        json!({"name": "claude-x", "provider": "anthropic", "api_key": "ak",
                              "provider_config": {"base_url": base}}),
    );
    let (text, reply) = run(config, call(false)).await.unwrap();
    assert_eq!(text, "Hi there");
    assert_eq!(texts(&reply), "Hi there");
    assert_eq!(reply.usage.input_tokens, Some(9));
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/v1/messages");
    assert_eq!(request.headers["x-api-key"], "ak");
    assert_eq!(request.body["model"], "claude-x");
}

#[tokio::test]
async fn gemini_streams_generate_content() {
    let body = sse(&[
        (
            "",
            json!({"candidates": [{"content": {"parts": [{"text": "Xin "}], "role": "model"}, "index": 0}]}),
        ),
        (
            "",
            json!({"candidates": [{"content": {"parts": [{"text": "chào"}], "role": "model"},
            "finishReason": "STOP", "index": 0}],
            "usageMetadata": {"promptTokenCount": 6, "candidatesTokenCount": 2, "totalTokenCount": 8}}),
        ),
    ]);
    let (base, seen) = serve(StatusCode::OK, "text/event-stream", body).await;
    let config = model(
        json!({"name": "gemini-x", "provider": "google", "api_key": "gk",
                              "provider_config": {"base_url": base}}),
    );
    let (text, reply) = run(config, call(false)).await.unwrap();
    assert_eq!(text, "Xin chào");
    assert_eq!(reply.usage.total_tokens, Some(8));
    let request = seen.lock().unwrap()[0].clone();
    assert!(
        request
            .path
            .contains("models/gemini-x:streamGenerateContent"),
        "{}",
        request.path
    );
    let keyed = request
        .headers
        .get("x-goog-api-key")
        .is_some_and(|v| v == "gk")
        || request.path.contains("key=gk");
    assert!(keyed, "the Gemini key is sent");
}

#[tokio::test]
async fn bedrock_sends_converse_with_the_bearer_key() {
    let (base, seen) = serve(
        StatusCode::BAD_REQUEST,
        "application/json",
        json!({"message": "stub refuses"}).to_string(),
    )
    .await;
    let config = model(
        json!({"name": "main", "provider": "bedrock", "api_key": "bedrock-key",
        "provider_config": {"model_id": "nvidia.nemotron-x", "region_name": "us-east-1", "endpoint_url": base}}),
    );
    let err = run(config, call(false)).await.unwrap_err();
    assert!(err.starts_with("BEM-464"), "{err}");
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/model/nvidia.nemotron-x/converse-stream");
    assert_eq!(request.headers["authorization"], "Bearer bedrock-key");
}

#[tokio::test]
async fn misconfigured_models_fail_at_build() {
    let bedrock = model(json!({"name": "b", "provider": "bedrock", "api_key": "k"}));
    let factory = Arc::new(GatewayFactory::new(
        vec![bedrock.clone()],
        Arc::new(RigProviderFactory),
    ));
    let err = factory.gateway(&[bedrock]).err().unwrap();
    assert_eq!(err.errcode.as_str(), "BEM-461");
    let keyless = model(json!({"name": "o", "provider": "openai"}));
    let factory = Arc::new(GatewayFactory::new(
        vec![keyless.clone()],
        Arc::new(RigProviderFactory),
    ));
    assert_eq!(
        factory.gateway(&[keyless]).err().unwrap().errcode.as_str(),
        "BEM-462"
    );
}

fn has_tool_call(reply: &ModelReply, name: &str) -> bool {
    reply
        .choice
        .iter()
        .any(|c| matches!(c, AssistantContent::ToolCall(t) if t.function.name.as_str() == name))
}

#[tokio::test]
async fn anthropic_streams_a_tool_call() {
    let body = sse(&[
        (
            "message_start",
            json!({"type": "message_start", "message": {"id": "msg_2", "type": "message",
            "role": "assistant", "model": "claude-x", "content": [], "stop_reason": null,
            "stop_sequence": null, "usage": {"input_tokens": 9, "output_tokens": 1}}}),
        ),
        (
            "content_block_start",
            json!({"type": "content_block_start", "index": 0,
            "content_block": {"type": "tool_use", "id": "toolu_1", "name": "add", "input": {}}}),
        ),
        (
            "content_block_delta",
            json!({"type": "content_block_delta", "index": 0,
            "delta": {"type": "input_json_delta", "partial_json": "{\"a\":1}"}}),
        ),
        (
            "content_block_stop",
            json!({"type": "content_block_stop", "index": 0}),
        ),
        (
            "message_delta",
            json!({"type": "message_delta", "delta": {"stop_reason": "tool_use",
            "stop_sequence": null}, "usage": {"output_tokens": 6}}),
        ),
        ("message_stop", json!({"type": "message_stop"})),
    ]);
    let (base, seen) = serve(StatusCode::OK, "text/event-stream", body).await;
    let config = model(
        json!({"name": "claude-x", "provider": "anthropic", "api_key": "ak",
                              "provider_config": {"base_url": base}}),
    );
    let (_, reply) = run(config, call(true)).await.unwrap();
    assert!(has_tool_call(&reply, "add"));
    assert_eq!(seen.lock().unwrap()[0].body["tools"][0]["name"], "add");
}

#[tokio::test]
async fn gemini_streams_a_function_call() {
    let body = sse(&[(
        "",
        json!({"candidates": [{"content": {"parts": [{"functionCall": {"name": "add", "args": {"a": 1}}}],
            "role": "model"}, "finishReason": "STOP", "index": 0}],
            "usageMetadata": {"promptTokenCount": 6, "candidatesTokenCount": 2, "totalTokenCount": 8}}),
    )]);
    let (base, _) = serve(StatusCode::OK, "text/event-stream", body).await;
    let config = model(
        json!({"name": "gemini-x", "provider": "google", "api_key": "gk",
                              "provider_config": {"base_url": base}}),
    );
    let (_, reply) = run(config, call(true)).await.unwrap();
    assert!(has_tool_call(&reply, "add"));
}

#[tokio::test]
async fn ollama_takes_the_server_root_like_python() {
    let (base, seen) = serve(StatusCode::OK, "text/event-stream", openai_chunks(false)).await;
    let config = model(
        json!({"name": "llama", "provider": "ollama", "provider_config": {"base_url": base}}),
    );
    let (text, _) = run(config, call(false)).await.unwrap();
    assert_eq!(text, "Hello");
    assert_eq!(seen.lock().unwrap()[0].path, "/v1/chat/completions");
}

#[tokio::test]
async fn a_transport_error_never_carries_the_key() {
    // A port nothing listens on: the client fails before any reply.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let secret = "gk-super-secret-value";
    let config = model(
        json!({"name": "gemini-x", "provider": "google", "api_key": secret,
                              "provider_config": {"base_url": base}}),
    );
    let err = run(config, call(false)).await.unwrap_err();
    assert!(err.starts_with("BEM-464"), "{err}");
    // rig's transport error prints the request URL, key included: it is scrubbed.
    assert!(err.contains("key=***"), "{err}");
    assert!(!err.contains(secret), "the key leaked: {err}");
    assert_eq!(
        beam_gateway::scrub("GET http://h/x?alt=sse&key=abc123) failed", None),
        "GET http://h/x?alt=sse&key=***) failed"
    );
}

fn image_call(prompt: Message, history: &[Message]) -> ModelCall {
    let prepared = prepare_request(
        &RunSpec::new(),
        &ProviderCapabilities::default(),
        history,
        vec![],
        None,
        None,
    )
    .unwrap();
    ModelCall {
        prompt,
        prepared,
        ..call(false)
    }
}

fn with_image(text: &str) -> Message {
    Message::User {
        content: vec![
            UserContent::text(text),
            UserContent::image_base64("aGk=", Some(ImageMediaType::PNG), None),
        ],
    }
}

fn nvidia(base: &str, modalities: &Value) -> ModelConfig {
    model(json!({
        "name": "vl", "provider": "nvidia", "api_key": "test-key",
        "provider_config": {"model_id": "vendor/vl", "base_url": format!("{base}/v1")},
        "modalities": modalities,
    }))
}

#[tokio::test]
async fn a_vision_model_gets_the_images() {
    let (base, seen) = serve(StatusCode::OK, "text/event-stream", openai_chunks(false)).await;
    let config = nvidia(&base, &json!(["text", "image"]));
    run(config, image_call(with_image("what is this?"), &[]))
        .await
        .unwrap();
    let body = seen.lock().unwrap()[0].body.clone();
    let user = body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["role"] == "user")
        .cloned()
        .unwrap();
    let parts = user["content"].as_array().unwrap();
    assert!(
        parts
            .iter()
            .any(|p| p["type"] == "text" && p["text"] == "what is this?"),
        "{user}"
    );
    assert!(
        parts
            .iter()
            .any(|p| p["type"] == "image_url"
                && p["image_url"]["url"] == "data:image/png;base64,aGk="),
        "{user}"
    );
}

#[tokio::test]
async fn a_text_model_gets_no_images() {
    let (base, seen) = serve(StatusCode::OK, "text/event-stream", openai_chunks(false)).await;
    let config = nvidia(&base, &json!(["text"]));
    // History images are left out; the turn's text is sent.
    let history = [with_image("earlier"), Message::assistant("a dog")];
    run(
        config.clone(),
        image_call(Message::user("and now?"), &history),
    )
    .await
    .unwrap();
    let body = seen.lock().unwrap()[0].body.to_string();
    assert!(
        body.contains("earlier") && !body.contains("image_url"),
        "{body}"
    );

    // An image in the turn's prompt is refused before anything is sent.
    let err = run(config, image_call(with_image("what is this?"), &[]))
        .await
        .unwrap_err();
    assert!(err.starts_with("BEM-481"), "{err}");
    assert_eq!(seen.lock().unwrap().len(), 1);
}

/// A model with a small window: 200 tokens, 50 kept for output.
fn small_window(base: &str, name: &str) -> ModelConfig {
    model(json!({
        "name": name, "provider": "nvidia", "api_key": "test-key",
        "provider_config": {"model_id": "vendor/small", "base_url": format!("{base}/v1")},
        "context_window_tokens": 200, "max_output_tokens": 50,
    }))
}

fn long_history() -> Vec<Message> {
    vec![
        Message::user("tell me everything ".repeat(200)),
        Message::assistant("here it is ".repeat(200)),
    ]
}

/// Plan 020: a prompt over the model's window fails with `BEM-484` before
/// the provider is called.
#[tokio::test]
async fn a_prompt_over_the_window_is_refused_before_the_call() {
    let (base, seen) = serve(StatusCode::OK, "text/event-stream", openai_chunks(false)).await;
    let err = run(
        small_window(&base, "small"),
        image_call(Message::user("and now?"), &long_history()),
    )
    .await
    .unwrap_err();
    assert!(err.starts_with("BEM-484"), "{err}");
    assert!(seen.lock().unwrap().is_empty(), "the provider was called");
}

/// Plan 020: the gateway moves on to a model with a larger window.
#[tokio::test]
async fn a_larger_window_answers_after_an_overflow() {
    let (base, seen) = serve(StatusCode::OK, "text/event-stream", openai_chunks(false)).await;
    let small = small_window(&base, "small");
    let big = model(json!({
        "name": "big", "provider": "nvidia", "api_key": "test-key",
        "provider_config": {"model_id": "vendor/big", "base_url": format!("{base}/v1")},
        "context_window_tokens": 128_000,
    }));
    let factory = Arc::new(GatewayFactory::new(
        vec![small.clone(), big.clone()],
        Arc::new(RigProviderFactory),
    ));
    let gateway = factory.gateway(&[small, big]).unwrap();
    let mut stream = gateway.stream(image_call(Message::user("and now?"), &long_history()));
    let mut answered = false;
    while let Some(item) = stream.next().await {
        if let ModelStreamItem::Completed(reply) = item.unwrap() {
            assert_eq!(reply.model_used.as_deref(), Some("big"));
            answered = true;
        }
    }
    assert!(answered);
    assert_eq!(
        seen.lock().unwrap().len(),
        1,
        "only the big model was called"
    );
}

/// Plan 020 review: a model that declares no window is not guarded, even
/// with a small `max_tokens` (an output limit, not the window).
#[tokio::test]
async fn a_model_without_a_window_is_not_guarded() {
    let (base, seen) = serve(StatusCode::OK, "text/event-stream", openai_chunks(false)).await;
    let config = model(json!({
        "name": "plain", "provider": "nvidia", "api_key": "test-key",
        "provider_config": {"model_id": "vendor/plain", "base_url": format!("{base}/v1")},
        "max_tokens": 200,
    }));
    run(
        config,
        image_call(Message::user("and now?"), &long_history()),
    )
    .await
    .expect("no window, no guard");
    assert_eq!(seen.lock().unwrap().len(), 1);
}
