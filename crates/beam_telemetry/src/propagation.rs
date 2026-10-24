//! W3C trace context in an invoke's metadata (Python `telemetry/propagation.py`).

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// A trace and span id, carried as `traceparent`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceContext {
    pub trace_id: String,
    pub span_id: String,
    #[serde(default)]
    pub parent_span_id: Option<String>,
}

impl TraceContext {
    /// The `traceparent` header value.
    #[must_use]
    pub fn traceparent(&self) -> String {
        format!("00-{}-{}-01", self.trace_id, self.span_id)
    }
}

/// `metadata["traceparent"]` for `context`.
pub fn inject(metadata: &mut Map<String, Value>, context: &TraceContext) {
    metadata.insert("traceparent".into(), Value::String(context.traceparent()));
}

fn is_hex(value: &str, len: usize) -> bool {
    value.len() == len && value.bytes().all(|b| b.is_ascii_hexdigit())
}

/// The parent from `metadata["traceparent"]`. Python takes
/// any three-dash string; Rust also wants W3C's 32 and 16 hex digits, since
/// the trace id is stored on the run.
#[must_use]
pub fn extract(metadata: &Value) -> Option<TraceContext> {
    let raw = metadata.get("traceparent")?.as_str()?;
    if raw.matches('-').count() < 3 {
        return None;
    }
    let mut parts = raw.splitn(4, '-');
    let (_version, trace_id, span_id) = (parts.next()?, parts.next()?, parts.next()?);
    if !is_hex(trace_id, 32) || !is_hex(span_id, 16) {
        return None;
    }
    Some(TraceContext {
        trace_id: trace_id.to_owned(),
        span_id: span_id.to_owned(),
        parent_span_id: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_through_metadata() {
        let ctx = TraceContext {
            trace_id: "4bf92f3577b34da6a3ce929d0e0e4736".into(),
            span_id: "00f067aa0ba902b7".into(),
            parent_span_id: None,
        };
        let mut metadata = Map::new();
        inject(&mut metadata, &ctx);
        assert_eq!(
            metadata["traceparent"],
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        );
        assert_eq!(extract(&Value::Object(metadata)), Some(ctx));
        assert_eq!(
            extract(&serde_json::json!({"traceparent": "bad-value"})),
            None
        );
        assert_eq!(extract(&serde_json::json!({})), None);
        // Three dashes are not enough: the ids must be W3C hex.
        assert_eq!(
            extract(&serde_json::json!({"traceparent": "00-<script>-x-01"})),
            None
        );
    }
}
