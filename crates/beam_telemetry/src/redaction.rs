//! Strip credentials from dumps that leave the process: dead letters, exports
//! and audit metadata (Python `kernel/redaction.py`).

use serde_json::{Map, Value};

const SECRET_KEYS: [&str; 10] = [
    "api_key",
    "mcp_token",
    "password",
    "authorization",
    "secret",
    "access_token",
    "refresh_token",
    "nvidia_api_key",
    "credential",
    "client_secret",
];
const REDACTED: &str = "[redacted]";

fn looks_like_secret(value: &str) -> bool {
    let lowered = value.to_lowercase();
    lowered.starts_with("nvapi-") || lowered.starts_with("sk-") || lowered.starts_with("tok-")
}

/// Secret keys and secret-looking strings become
/// `[redacted]`, recursively.
#[must_use]
pub fn redact_secrets(value: &Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| {
                    let v = if SECRET_KEYS.contains(&k.to_lowercase().as_str()) {
                        Value::String(REDACTED.into())
                    } else {
                        redact_secrets(v)
                    };
                    (k.clone(), v)
                })
                .collect::<Map<String, Value>>(),
        ),
        Value::Array(items) => Value::Array(items.iter().map(redact_secrets).collect()),
        Value::String(s) if looks_like_secret(s) => Value::String(REDACTED.into()),
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn redacts_keys_and_token_shapes() {
        let dump = json!({
            "Authorization": "Bearer x",
            "nested": [{"mcp_token": "t", "note": "sk-live-123", "ok": "hello"}],
            "count": 3,
        });
        assert_eq!(
            redact_secrets(&dump),
            json!({
                "Authorization": "[redacted]",
                "nested": [{"mcp_token": "[redacted]", "note": "[redacted]", "ok": "hello"}],
                "count": 3,
            })
        );
    }
}
