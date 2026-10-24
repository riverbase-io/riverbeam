use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use beam_telemetry::{privacy_mode, PrivacyMode};

use crate::events::AuditEvent;
use crate::sink::AuditSink;

/// Fans events out to the configured sinks; previews
/// follow `BEAM_TRACE_PRIVACY`.
pub struct AuditLogger {
    sinks: Vec<Arc<dyn AuditSink>>,
    enabled: AtomicBool,
}

impl Default for AuditLogger {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

impl AuditLogger {
    #[must_use]
    pub fn new(sinks: Vec<Arc<dyn AuditSink>>) -> Self {
        Self {
            sinks,
            enabled: AtomicBool::new(true),
        }
    }

    #[must_use]
    pub fn with_sink(mut self, sink: Arc<dyn AuditSink>) -> Self {
        self.sinks.push(sink);
        self
    }

    /// `AUDIT_ENABLED`.
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }

    #[must_use]
    pub fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    pub async fn log(&self, event: AuditEvent) {
        if !self.enabled() {
            return;
        }
        let mut event = apply_preview_privacy(event, privacy_mode());
        // Credentials never reach a sink.
        if !event.metadata.is_null() {
            event.metadata = beam_telemetry::redact_secrets(&event.metadata);
        }
        for sink in &self.sinks {
            sink.write(&event).await;
        }
    }

    pub async fn flush(&self) {
        for sink in &self.sinks {
            sink.flush().await;
        }
    }
}

/// Below `full_debug`, previews are dropped
/// (`metadata_only`) or replaced with `[redacted]`.
#[must_use]
pub fn apply_preview_privacy(mut event: AuditEvent, mode: PrivacyMode) -> AuditEvent {
    if mode == PrivacyMode::FullDebug {
        return event;
    }
    let replace = |preview: Option<String>| {
        preview.and_then(|_| (mode == PrivacyMode::Redacted).then(|| "[redacted]".to_owned()))
    };
    event.input_preview = replace(event.input_preview.take());
    event.output_preview = replace(event.output_preview.take());
    event
}

/// The sink keys `AUDIT_SINKS` accepts.
pub const AUDIT_SINK_KEYS: [&str; 3] = ["stdout", "file", "sql"];

/// Every configured key, in order. An unknown key
/// fails (with the available keys); `sql` without a database is skipped with
/// a warning, as in Python.
///
/// # Errors
/// The unknown key, as a message listing the available keys.
pub fn build_audit_sinks(
    names: &[String],
    file_path: &str,
    sql: Option<&Arc<dyn AuditSink>>,
) -> Result<Vec<Arc<dyn AuditSink>>, String> {
    let mut sinks: Vec<Arc<dyn AuditSink>> = Vec::new();
    for name in names {
        match name.trim().to_lowercase().as_str() {
            "stdout" => sinks.push(Arc::new(crate::sink::StdoutSink)),
            "file" => sinks.push(Arc::new(crate::sink::FileSink::new(file_path))),
            "sql" => {
                if let Some(sink) = sql {
                    sinks.push(Arc::clone(sink));
                } else {
                    tracing::warn!("AUDIT_SINKS includes 'sql' but no database is configured; skipping SqlSink");
                }
            }
            other => {
                return Err(format!(
                    "unknown audit sink '{other}'; available: {}",
                    AUDIT_SINK_KEYS.join(", ")
                ))
            }
        }
    }
    Ok(sinks)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::AuditAction;
    use crate::sink::InMemorySink;

    #[tokio::test]
    async fn logger_fans_out_to_sinks() {
        let sink = Arc::new(InMemorySink::new());
        let logger = AuditLogger::new(vec![sink.clone()]);
        logger
            .log(AuditEvent::new(AuditAction::RequestReceived))
            .await;
        logger
            .log(AuditEvent::new(AuditAction::AgentCompleted))
            .await;
        assert_eq!(sink.events().len(), 2);
        assert_eq!(sink.events()[0].action, AuditAction::RequestReceived);
        logger.set_enabled(false);
        logger.log(AuditEvent::new(AuditAction::AgentFailed)).await;
        assert_eq!(sink.events().len(), 2, "AUDIT_ENABLED=false drops events");
    }

    #[test]
    fn previews_follow_the_privacy_mode() {
        let mut event = AuditEvent::new(AuditAction::AgentCompleted);
        event.input_preview = Some("the user's question".into());
        let redacted = apply_preview_privacy(event.clone(), PrivacyMode::Redacted);
        assert_eq!(redacted.input_preview.as_deref(), Some("[redacted]"));
        assert_eq!(redacted.output_preview, None);
        assert_eq!(
            apply_preview_privacy(event.clone(), PrivacyMode::MetadataOnly).input_preview,
            None
        );
        assert_eq!(
            apply_preview_privacy(event, PrivacyMode::FullDebug)
                .input_preview
                .as_deref(),
            Some("the user's question")
        );
    }

    #[tokio::test]
    async fn sinks_resolve_key_by_key() {
        let dir = std::env::temp_dir().join(format!("beam-audit-{}", uuid::Uuid::new_v4()));
        let path = dir.join("audit.jsonl");
        let sinks = build_audit_sinks(
            &["stdout".into(), "file".into(), "sql".into()],
            path.to_str().unwrap(),
            None,
        )
        .unwrap();
        assert_eq!(sinks.len(), 2, "sql without a database is skipped");
        let logger = AuditLogger::new(sinks);
        logger
            .log(AuditEvent::new(AuditAction::RequestReceived))
            .await;
        logger.flush().await;
        let line = std::fs::read_to_string(&path).unwrap();
        let row: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(row["action"], "request_received");
        let _ = std::fs::remove_dir_all(dir);
        let Err(err) = build_audit_sinks(&["kafka".into()], "x", None) else {
            panic!("an unknown sink must fail");
        };
        assert_eq!(
            err,
            "unknown audit sink 'kafka'; available: stdout, file, sql"
        );
    }
}
