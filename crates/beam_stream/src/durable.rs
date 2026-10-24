//! Which frames are durable, and what of them is stored (Python
//! `store/ops/outbox.py`: `durable_event_type`, `_durable_payload`).

use beam_telemetry::PrivacyMode;
use serde_json::Value;

/// The frame types stored for replay; rc8 adds `context_usage` (see plan 023 A2).
pub const DURABLE_TYPES: [&str; 7] = [
    "started",
    "interrupted",
    "done",
    "error",
    "cancelled",
    "timed_out",
    "context_usage",
];

/// The event type a frame is stored under, or
/// `None` for frames that stay live only (token deltas, plain text chunks).
#[must_use]
pub fn durable_event_type(payload: &Value) -> Option<&'static str> {
    let kind = payload
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if kind == "done" {
        let interrupted = payload
            .get("result")
            .and_then(|r| r.get("interrupted"))
            .is_some_and(truthy);
        return Some(if interrupted { "interrupted" } else { "done" });
    }
    if let Some(durable) = DURABLE_TYPES.iter().find(|t| **t == kind) {
        return Some(durable);
    }
    if kind != "chunk" {
        return None;
    }
    let data = payload.get("data").filter(|d| d.is_object())?;
    if data.get("is_delta").is_some_and(truthy) {
        return None;
    }
    match data.get("type").and_then(Value::as_str).unwrap_or_default() {
        "action" => Some("interrupted"),
        "action_result" => Some("tool_approval"),
        "tool_result" => Some("tool_result"),
        _ => None,
    }
}

/// Python truthiness for the JSON values a frame carries.
fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().is_some_and(|f| f != 0.0),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

const BODY_KEYS: [&str; 6] = [
    "content",
    "data",
    "text",
    "input_preview",
    "output_preview",
    "body",
];

/// Below `full_debug`, message bodies are dropped
/// (`metadata_only`) or replaced with `[redacted]`.
#[must_use]
pub fn durable_payload(payload: &Value, mode: PrivacyMode) -> Value {
    let Value::Object(map) = payload else {
        return payload.clone();
    };
    if mode == PrivacyMode::FullDebug {
        return payload.clone();
    }
    let mut out = map.clone();
    for key in BODY_KEYS {
        if let Some(value) = out.get_mut(key) {
            *value = if mode == PrivacyMode::Redacted {
                Value::String("[redacted]".into())
            } else {
                Value::Null
            };
        }
    }
    Value::Object(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn lifecycle_frames_are_durable_and_deltas_are_not() {
        assert_eq!(
            durable_event_type(&json!({"type": "done", "result": {}})),
            Some("done")
        );
        assert_eq!(
            durable_event_type(&json!({"type": "done", "result": {"interrupted": true}})),
            Some("interrupted")
        );
        assert_eq!(
            durable_event_type(&json!({"type": "started"})),
            Some("started")
        );
        assert_eq!(durable_event_type(&json!({"type": "error"})), Some("error"));
        let chunk = |data: Value| json!({"type": "chunk", "data": data});
        assert_eq!(
            durable_event_type(&chunk(json!({"type": "message", "is_delta": true}))),
            None
        );
        assert_eq!(durable_event_type(&chunk(json!({"type": "message"}))), None);
        assert_eq!(
            durable_event_type(&chunk(json!({"type": "action"}))),
            Some("interrupted")
        );
        assert_eq!(
            durable_event_type(&chunk(json!({"type": "action_result"}))),
            Some("tool_approval")
        );
        assert_eq!(
            durable_event_type(&chunk(json!({"type": "tool_result"}))),
            Some("tool_result")
        );
        assert_eq!(
            durable_event_type(&chunk(json!({"type": "tool_result", "is_delta": true}))),
            None
        );
        assert_eq!(durable_event_type(&json!({"type": "chunk"})), None);
    }

    #[test]
    fn stored_bodies_follow_the_privacy_mode() {
        let frame = json!({"type": "chunk", "data": {"content": "secret"}, "sequence": 3});
        assert_eq!(
            durable_payload(&frame, PrivacyMode::MetadataOnly),
            json!({"type": "chunk", "data": null, "sequence": 3})
        );
        assert_eq!(
            durable_payload(&frame, PrivacyMode::Redacted)["data"],
            "[redacted]"
        );
        assert_eq!(durable_payload(&frame, PrivacyMode::FullDebug), frame);
    }
}
