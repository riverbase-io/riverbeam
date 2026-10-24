//! Trace and audit privacy (Python `telemetry/privacy.py`), from
//! `BEAM_TRACE_PRIVACY`.

use std::sync::atomic::{AtomicU8, Ordering};

use serde_json::{Map, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrivacyMode {
    /// Only ids and a few status keys survive (default).
    MetadataOnly,
    /// Content keys become `[redacted]`.
    Redacted,
    /// Everything is kept (debugging only).
    FullDebug,
}

impl PrivacyMode {
    #[must_use]
    pub fn parse(raw: &str) -> Self {
        match raw.trim() {
            "redacted" => Self::Redacted,
            "full_debug" => Self::FullDebug,
            _ => Self::MetadataOnly,
        }
    }
}

const PREVIEW_KEYS: [&str; 7] = [
    "input_preview",
    "output_preview",
    "body",
    "text",
    "content",
    "args",
    "messages",
];
const METADATA_KEEP: [&str; 5] = ["status", "error_code", "action", "timestamp", "agent_name"];

/// 0: read `BEAM_TRACE_PRIVACY`; else the overriding mode + 1.
static OVERRIDE: AtomicU8 = AtomicU8::new(0);

/// `BEAM_TRACE_PRIVACY`, unknown values meaning
/// `metadata_only`.
#[must_use]
pub fn privacy_mode() -> PrivacyMode {
    match AtomicU8::load(&OVERRIDE, Ordering::Relaxed) {
        1 => PrivacyMode::MetadataOnly,
        2 => PrivacyMode::Redacted,
        3 => PrivacyMode::FullDebug,
        _ => beam_env::string("BEAM_TRACE_PRIVACY")
            .map_or(PrivacyMode::MetadataOnly, |v| PrivacyMode::parse(&v)),
    }
}

/// Override `BEAM_TRACE_PRIVACY` for this process (`None` reads it again).
pub fn set_privacy_mode(mode: Option<PrivacyMode>) {
    let code = match mode {
        None => 0,
        Some(PrivacyMode::MetadataOnly) => 1,
        Some(PrivacyMode::Redacted) => 2,
        Some(PrivacyMode::FullDebug) => 3,
    };
    OVERRIDE.store(code, Ordering::Relaxed);
}

/// A payload with what the privacy mode hides removed.
#[must_use]
pub fn apply_privacy(payload: &Map<String, Value>, mode: PrivacyMode) -> Map<String, Value> {
    if mode == PrivacyMode::FullDebug {
        return payload.clone();
    }
    let mut cleaned = payload.clone();
    for key in PREVIEW_KEYS {
        if let Some(value) = cleaned.get_mut(key) {
            *value = if mode == PrivacyMode::Redacted {
                Value::String("[redacted]".into())
            } else {
                Value::Null
            };
        }
    }
    if mode == PrivacyMode::MetadataOnly {
        cleaned.retain(|k, _| k.ends_with("_id") || METADATA_KEEP.contains(&k.as_str()));
    }
    cleaned
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn payload() -> Map<String, Value> {
        json!({"run_id": "r", "status": "ok", "content": "secret prompt", "model": "gpt"})
            .as_object()
            .cloned()
            .unwrap()
    }

    #[test]
    fn modes_match_python() {
        assert_eq!(
            Value::Object(apply_privacy(&payload(), PrivacyMode::MetadataOnly)),
            json!({"run_id": "r", "status": "ok"})
        );
        assert_eq!(
            Value::Object(apply_privacy(&payload(), PrivacyMode::Redacted)),
            json!({"run_id": "r", "status": "ok", "content": "[redacted]", "model": "gpt"})
        );
        assert_eq!(apply_privacy(&payload(), PrivacyMode::FullDebug), payload());
        assert_eq!(PrivacyMode::parse("nonsense"), PrivacyMode::MetadataOnly);
    }
}
