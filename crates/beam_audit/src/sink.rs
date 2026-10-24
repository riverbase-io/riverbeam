//! Audit sinks (Python `audit/stores/`): stdout (a log line), a JSONL file,
//! and SQL (`agent_event`, behind the `postgres` feature).

use std::io::Write as _;
use std::path::PathBuf;
use std::sync::{Mutex, RwLock};

use async_trait::async_trait;

use crate::events::AuditEvent;

/// Destination for audit events.
#[async_trait]
pub trait AuditSink: Send + Sync {
    async fn write(&self, event: &AuditEvent);

    async fn flush(&self) {}
}

/// One structured log line per event (not JSON on stdout).
#[derive(Default)]
pub struct StdoutSink;

#[async_trait]
impl AuditSink for StdoutSink {
    async fn write(&self, event: &AuditEvent) {
        let none = || "None".to_owned();
        tracing::info!(
            "audit_event action={} agent={} user={} model={} status={} latency_ms={} tokens={}",
            event.action.as_str(),
            event.agent_name.clone().unwrap_or_else(none),
            event.user_id.clone().unwrap_or_else(none),
            event.model_used.clone().unwrap_or_else(none),
            event.status,
            event.latency_ms.map_or_else(none, |l| l.to_string()),
            event
                .token_usage
                .as_ref()
                .map_or_else(none, |t| t.total_tokens.to_string()),
        );
    }
}

/// JSON lines appended to `AUDIT_FILE_PATH`, opened on the
/// first write.
pub struct FileSink {
    path: PathBuf,
    file: Mutex<Option<std::fs::File>>,
}

impl FileSink {
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            file: Mutex::new(None),
        }
    }

    fn append(&self, line: &str) -> std::io::Result<()> {
        let mut guard = self
            .file
            .lock()
            .map_err(|_| std::io::Error::other("audit file lock poisoned"))?;
        if guard.is_none() {
            if let Some(parent) = self.path.parent().filter(|p| !p.as_os_str().is_empty()) {
                std::fs::create_dir_all(parent)?;
            }
            *guard = Some(
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&self.path)?,
            );
        }
        if let Some(file) = guard.as_mut() {
            writeln!(file, "{line}")?;
        }
        Ok(())
    }
}

#[async_trait]
impl AuditSink for FileSink {
    async fn write(&self, event: &AuditEvent) {
        let line = match serde_json::to_string(event) {
            Ok(line) => line,
            Err(e) => {
                tracing::error!(error = %e, "audit event not serializable");
                return;
            }
        };
        if let Err(e) = self.append(&line) {
            tracing::error!(path = %self.path.display(), error = %e, "FileSink failed to write");
        }
    }

    async fn flush(&self) {
        if let Ok(mut guard) = self.file.lock() {
            if let Some(file) = guard.as_mut() {
                let _ = file.flush();
            }
        }
    }
}

/// Collects events in memory for assertions in tests.
#[derive(Default)]
pub struct InMemorySink {
    events: RwLock<Vec<AuditEvent>>,
}

impl InMemorySink {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn events(&self) -> Vec<AuditEvent> {
        self.events.read().map(|e| e.clone()).unwrap_or_default()
    }
}

#[async_trait]
impl AuditSink for InMemorySink {
    async fn write(&self, event: &AuditEvent) {
        if let Ok(mut guard) = self.events.write() {
            guard.push(event.clone());
        }
    }
}

#[cfg(feature = "postgres")]
pub use sql::SqlSink;

#[cfg(feature = "postgres")]
mod sql {
    use async_trait::async_trait;
    use beam_store::{Db, PgBeamStore};
    use serde_json::{json, Value};
    use uuid::Uuid;

    use super::AuditSink;
    use crate::events::AuditEvent;

    /// One `agent_event` row per event, written through its
    /// entity; a failed write is logged, never raised.
    pub struct SqlSink {
        db: Db,
    }

    impl SqlSink {
        /// The sink on `store`'s database.
        #[must_use]
        pub fn for_store(store: &PgBeamStore) -> Self {
            Self {
                db: store.db().clone(),
            }
        }

        async fn insert(&self, event: &AuditEvent) -> Result<(), String> {
            let uuid = |v: Option<&str>| v.and_then(|s| Uuid::parse_str(s.trim()).ok());
            let token_usage = event
                .token_usage
                .as_ref()
                .and_then(|t| serde_json::to_value(t).ok());
            let metadata = if event.metadata.is_object() {
                event.metadata.clone()
            } else {
                Value::Object(serde_json::Map::new())
            };
            let data = json!({
                "action": event.action.db_label(),
                "recorded_at": event.timestamp,
                "agent_name": event.agent_name,
                "user_id": uuid(event.user_id.as_deref()),
                "session_id": event.session_id,
                "run_id": event.run_id,
                "model_used": event.model_used,
                "modalities_used": Value::from(event.modalities_used.clone()),
                "token_usage": token_usage,
                "latency_ms": event.latency_ms,
                "status": event.status,
                "error": event.error,
                "input_preview": event.input_preview,
                "output_preview": event.output_preview,
                "metadata_json": metadata,
                // The invoke's org (Python leaves it null; optional RLS needs it).
                "org_id": beam_store::org_scope::current_org(),
                "_insert": true,
            });
            self.db
                .audit()
                .append_event(data)
                .await
                .map_err(|e| e.to_string())
        }
    }

    #[async_trait]
    impl AuditSink for SqlSink {
        async fn write(&self, event: &AuditEvent) {
            if let Err(e) = self.insert(event).await {
                tracing::error!(action = %event.action.as_str(), error = %e, "SqlSink failed to write audit event");
            }
        }
    }
}
