//! Bounded tool output (Python `tools/idempotency.py::bound_tool_output`).

use beam_types::pyjson;
use serde_json::{json, Value};

/// Python's default `max_result_chars`.
pub const MAX_RESULT_CHARS: usize = 4000;

/// The output as is when its text fits in `max_chars`; otherwise a reference
/// with a summary (the first 400 characters) and the size.
#[must_use]
pub fn bound_tool_output(output: Value, max_chars: usize) -> Value {
    let text = match &output {
        Value::String(s) => s.clone(),
        other => pyjson::dumps(other),
    };
    // Python's `len` counts code points.
    let chars = text.chars().count();
    if chars <= max_chars {
        return output;
    }
    let digest = sha256_hex(text.as_bytes());
    let summary: String = text.chars().take(400).collect();
    json!({
        "artifact_ref": format!("tool-result:{}", &digest[..16]),
        "summary": format!("{summary}..."),
        "bytes": chars,
    })
}

fn sha256_hex(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    use std::fmt::Write as _;
    Sha256::digest(data)
        .iter()
        .fold(String::new(), |mut out, b| {
            let _ = write!(out, "{b:02x}");
            out
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn long_output_becomes_a_reference_like_python() {
        assert_eq!(bound_tool_output(json!("short"), 10), json!("short"));
        let long = "x".repeat(4001);
        let bounded = bound_tool_output(Value::String(long.clone()), MAX_RESULT_CHARS);
        assert_eq!(bounded["bytes"], 4001);
        // Python: hashlib.sha256(("x" * 4001).encode()).hexdigest()[:16]
        assert_eq!(bounded["artifact_ref"], "tool-result:ba07fa666a8a8206");
        assert_eq!(bounded["summary"].as_str().unwrap().len(), 403);
    }
}
