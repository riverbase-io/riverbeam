use std::sync::RwLock;

use async_trait::async_trait;

use crate::events::AuditEvent;

/// Destination for audit events.
#[async_trait]
pub trait AuditSink: Send + Sync {
    async fn write(&self, event: &AuditEvent);
}

/// Prints JSON audit lines to stdout (stand-in for a tracing/log sink).
#[derive(Default)]
pub struct StdoutSink;

#[async_trait]
impl AuditSink for StdoutSink {
    async fn write(&self, event: &AuditEvent) {
        if let Ok(line) = serde_json::to_string(event) {
            println!("{line}");
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
