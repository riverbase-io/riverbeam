//! Wire-compatibility roundtrip tests against golden fixtures.
//!
//! The fixtures are generated from the Python fluvius-beam stack by
//! `scripts/gen_wire_fixtures.py` (see `tests/wire_compat/README.md`). Each must
//! deserialize into the Rust type and re-serialize to the same JSON value after
//! normalization. `{}` and `[]` are dropped on both sides, because Python
//! publishes some paths with `exclude_none` and others without, and each such
//! field has the same default on both stacks. `null` is dropped from the Python
//! side, but from the Rust side only where Python's payload also had `null` at
//! that path (free-form `metadata` dicts carry nulls). Anywhere else Rust must
//! omit the field rather than emit `null`, because Pydantic rejects `null` for
//! its `dict` and `list` fields.

use beam_types::{
    ActionRequestEnvelope, ActionResponseEnvelope, AgentAction, AgentInput, AgentMessage,
    AgentMetadata, AgentResult, AgentStep, Artifact, ChatRequest, ContentPart, InvokeAuthContext,
    InvokeRequest, ModelConfig, RequestContext, StreamChunk, TokenUsage, ToolCallRecord,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{Map, Value};

macro_rules! fixture {
    ($name:literal) => {
        include_str!(concat!(
            "../../../tests/wire_compat/fixtures/",
            $name,
            ".json"
        ))
    };
}

/// Drop empty-object and empty-array values recursively. A `null` is dropped
/// when `reference` is `None` (the Python side) or when `reference` also has
/// `null` at the same path (Rust echoing Python).
fn normalize(value: Value, reference: Option<&Value>) -> Value {
    match value {
        Value::Object(map) => {
            let mut out = Map::new();
            for (key, item) in map {
                let child_ref = reference.map(|r| r.get(&key).unwrap_or(&Value::Bool(false)));
                let null_ok = child_ref.is_none_or(Value::is_null);
                let item = normalize(item, child_ref);
                let empty = match &item {
                    Value::Null => null_ok,
                    Value::Object(m) => m.is_empty(),
                    Value::Array(a) => a.is_empty(),
                    _ => false,
                };
                if !empty {
                    out.insert(key, item);
                }
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(
            items
                .into_iter()
                .enumerate()
                .map(|(i, v)| {
                    normalize(
                        v,
                        reference.map(|r| r.get(i).unwrap_or(&Value::Bool(false))),
                    )
                })
                .collect(),
        ),
        other => other,
    }
}

fn assert_roundtrip<T>(name: &str, fixture: &str)
where
    T: DeserializeOwned + Serialize,
{
    let raw: Value = serde_json::from_str(fixture).expect("fixture is valid JSON");
    let parsed: T = serde_json::from_value(raw.clone())
        .unwrap_or_else(|e| panic!("{name}: fixture does not deserialize: {e}"));
    let actual = normalize(
        serde_json::to_value(&parsed).expect("serializes back to JSON"),
        Some(&raw),
    );
    let expected = normalize(raw, None);
    assert_eq!(
        actual, expected,
        "{name}: roundtrip mismatch:\n  expected = {expected:#}\n  actual   = {actual:#}"
    );
}

macro_rules! roundtrip_tests {
    ($($test:ident: $ty:ty => $name:literal),* $(,)?) => {
        $(
            #[test]
            fn $test() {
                assert_roundtrip::<$ty>($name, fixture!($name));
            }
        )*
    };
}

roundtrip_tests! {
    content_parts: Vec<ContentPart> => "content_parts",
    invoke_auth_context: InvokeAuthContext => "invoke_auth_context",
    request_context: RequestContext => "request_context",
    invoke_request: InvokeRequest => "invoke_request",
    invoke_request_minimal: InvokeRequest => "invoke_request_minimal",
    chat_request: ChatRequest => "chat_request",
    chat_request_minimal: ChatRequest => "chat_request_minimal",
    agent_input: AgentInput => "agent_input",
    agent_input_minimal: AgentInput => "agent_input_minimal",
    agent_result: AgentResult => "agent_result",
    agent_result_minimal: AgentResult => "agent_result_minimal",
    agent_message: AgentMessage => "agent_message",
    agent_message_minimal: AgentMessage => "agent_message_minimal",
    stream_chunk_chunk: StreamChunk => "stream_chunk_chunk",
    stream_chunk_done: StreamChunk => "stream_chunk_done",
    stream_chunk_error: StreamChunk => "stream_chunk_error",
    action_response: ActionResponseEnvelope => "action_response",
    action_response_minimal: ActionResponseEnvelope => "action_response_minimal",
    action_request: ActionRequestEnvelope => "action_request",
    agent_action: AgentAction => "agent_action",
    agent_step: AgentStep => "agent_step",
    tool_call_record: ToolCallRecord => "tool_call_record",
    token_usage: TokenUsage => "token_usage",
    agent_metadata: AgentMetadata => "agent_metadata",
    agent_metadata_minimal: AgentMetadata => "agent_metadata_minimal",
    model_config: ModelConfig => "model_config",
    model_config_minimal: ModelConfig => "model_config_minimal",
    artifact: Artifact => "artifact",
}

/// Pydantic ignores unknown fields by default; Rust must too, so a newer Python
/// payload never breaks an older Rust consumer.
#[test]
fn unknown_fields_are_ignored() {
    let mut raw: Value = serde_json::from_str(fixture!("invoke_request")).unwrap();
    raw["field_added_later"] = Value::from("x");
    raw["content"][0]["also_new"] = Value::from(1);
    let parsed: InvokeRequest = serde_json::from_value(raw).expect("unknown fields ignored");
    assert_eq!(parsed.agent_name, "agent-calculator");
}

/// Pydantic accepts naive datetimes and dumps them without an offset; a request
/// carrying one must still deserialize in Rust (read as UTC).
#[test]
fn naive_datetimes_from_python_are_accepted() {
    let mut raw: Value = serde_json::from_str(fixture!("invoke_request")).unwrap();
    raw["context"]["facts"][0]["expiry"] = Value::from("2026-10-18T09:30:15");
    raw["actions"][0]["responded_at"] = Value::from(1_792_315_815);
    let parsed: InvokeRequest = serde_json::from_value(raw).expect("naive datetime accepted");
    let fact = &parsed.context.unwrap().facts[0];
    assert_eq!(
        fact.expiry.unwrap().to_rfc3339(),
        "2026-10-18T09:30:15+00:00"
    );
}

/// A `ModelConfig` read without its optional maps must not serialize them as
/// `null` (Pydantic rejects `null` for `dict` fields).
#[test]
fn model_config_without_maps_emits_no_null() {
    let parsed: ModelConfig =
        serde_json::from_value(serde_json::json!({"name": "m", "provider": "openai"})).unwrap();
    let out = serde_json::to_value(&parsed).unwrap();
    assert!(out.get("provider_config").is_none(), "{out}");
    assert!(out.get("extra").is_none(), "{out}");
}

/// Every fixture file in the directory is covered by a roundtrip test.
#[test]
fn every_fixture_is_tested() {
    let dir = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/wire_compat/fixtures"
    );
    let source = include_str!("wire_compat.rs");
    for entry in std::fs::read_dir(dir).expect("fixtures dir") {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let stem = path.file_stem().unwrap().to_str().unwrap();
        if stem.starts_with("error_") {
            continue; // error payloads are checked in beam_core (Phase 01 step 4)
        }
        assert!(
            source.contains(&format!("=> \"{stem}\"")),
            "fixture {stem}.json has no roundtrip test"
        );
    }
}
