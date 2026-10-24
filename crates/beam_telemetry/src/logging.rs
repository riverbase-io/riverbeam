//! Structured invoke failure logs (Python `telemetry/logging.py`). One JSON
//! line when `BEAM_LOG_JSON` is true, else a key=value line.

use serde_json::{Map, Value};

/// Correlation ids of a failed invoke.
#[derive(Debug, Clone, Default)]
pub struct Correlation {
    pub execution_id: Option<String>,
    pub run_id: Option<String>,
    pub trace_id: Option<String>,
    pub request_id: Option<String>,
}

/// The non-empty ids plus `error_code`.
#[must_use]
pub fn correlation_fields(ids: &Correlation, error_code: Option<&str>) -> Map<String, Value> {
    let mut fields = Map::new();
    for (key, value) in [
        ("execution_id", ids.execution_id.as_deref()),
        ("run_id", ids.run_id.as_deref()),
        ("trace_id", ids.trace_id.as_deref()),
        ("error_code", error_code),
        ("request_id", ids.request_id.as_deref()),
    ] {
        if let Some(v) = value.filter(|v| !v.is_empty()) {
            fields.insert(key.into(), Value::String(v.to_owned()));
        }
    }
    fields
}

fn log_json() -> bool {
    beam_env::flag("BEAM_LOG_JSON", false)
}

/// The failure line Python logs (`invoke_failed`); `error_code` is the public
/// code and `category` Python's error category.
#[must_use]
pub fn failure_line(
    ids: &Correlation,
    error_code: Option<&str>,
    category: &str,
    message: &str,
    json: bool,
) -> String {
    let mut fields = correlation_fields(ids, error_code);
    if json {
        let mut line = Map::new();
        line.insert("event".into(), Value::String("invoke_failed".into()));
        line.append(&mut fields);
        line.insert("error_category".into(), Value::String(category.to_owned()));
        line.insert("message".into(), Value::String(message.to_owned()));
        return Value::Object(line).to_string();
    }
    let get = |k: &str| {
        fields
            .get(k)
            .and_then(Value::as_str)
            .unwrap_or("None")
            .to_owned()
    };
    format!(
        "invoke_failed execution_id={} run_id={} trace_id={} error_code={}",
        get("execution_id"),
        get("run_id"),
        get("trace_id"),
        get("error_code")
    )
}

/// Log a failed invoke with its public error code.
pub fn log_invoke_failure(
    ids: &Correlation,
    error_code: Option<&str>,
    category: &str,
    message: &str,
) {
    tracing::error!(
        "{}",
        failure_line(ids, error_code, category, message, log_json())
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_line_carries_the_correlation_ids() {
        let ids = Correlation {
            execution_id: Some("e1".into()),
            run_id: Some("r1".into()),
            trace_id: Some("t1".into()),
            request_id: None,
        };
        let line: Value = serde_json::from_str(&failure_line(
            &ids,
            Some("BEM-430"),
            "runtime",
            "boom",
            true,
        ))
        .unwrap();
        assert_eq!(
            line,
            serde_json::json!({"event": "invoke_failed", "execution_id": "e1", "run_id": "r1", "trace_id": "t1",
                               "error_code": "BEM-430", "error_category": "runtime", "message": "boom"})
        );
        assert_eq!(
            failure_line(&ids, None, "runtime", "boom", false),
            "invoke_failed execution_id=e1 run_id=r1 trace_id=t1 error_code=None"
        );
    }
}
