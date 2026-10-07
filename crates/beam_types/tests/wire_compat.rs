//! Wire-compatibility roundtrip tests against golden fixtures.
//!
//! Each fixture is the canonical JSON shape shared with the Python
//! `riverbeam` stack. A fixture must deserialize into the Rust type and
//! re-serialize to a byte-equivalent JSON value, guaranteeing field-name and
//! shape fidelity so either stack can produce/consume the other's payloads.

use beam_types::{ActionResponseEnvelope, AgentResult, InvokeRequest, StreamChunk};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

fn assert_roundtrip<T>(fixture: &str)
where
    T: DeserializeOwned + Serialize,
{
    let expected: Value = serde_json::from_str(fixture).expect("fixture is valid JSON");
    let parsed: T = serde_json::from_value(expected.clone()).expect("fixture deserializes into T");
    let actual: Value = serde_json::to_value(&parsed).expect("T serializes back to JSON");
    assert_eq!(
        actual, expected,
        "roundtrip mismatch:\n  expected = {expected:#}\n  actual   = {actual:#}"
    );
}

#[test]
fn invoke_request_roundtrips() {
    assert_roundtrip::<InvokeRequest>(include_str!(
        "../../../tests/wire_compat/fixtures/invoke_request.json"
    ));
}

#[test]
fn agent_result_roundtrips() {
    assert_roundtrip::<AgentResult>(include_str!(
        "../../../tests/wire_compat/fixtures/agent_result.json"
    ));
}

#[test]
fn stream_chunk_done_roundtrips() {
    assert_roundtrip::<StreamChunk>(include_str!(
        "../../../tests/wire_compat/fixtures/stream_chunk_done.json"
    ));
}

#[test]
fn action_response_roundtrips() {
    assert_roundtrip::<ActionResponseEnvelope>(include_str!(
        "../../../tests/wire_compat/fixtures/action_response.json"
    ));
}
