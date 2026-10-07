use std::sync::Arc;

use crate::events::AuditEvent;
use crate::sink::AuditSink;

/// Fans an audit event out to all configured sinks.
#[derive(Default)]
pub struct AuditLogger {
    sinks: Vec<Arc<dyn AuditSink>>,
}

impl AuditLogger {
    #[must_use]
    pub fn new(sinks: Vec<Arc<dyn AuditSink>>) -> Self {
        Self { sinks }
    }

    #[must_use]
    pub fn with_sink(mut self, sink: Arc<dyn AuditSink>) -> Self {
        self.sinks.push(sink);
        self
    }

    pub async fn log(&self, event: AuditEvent) {
        for sink in &self.sinks {
            sink.write(&event).await;
        }
    }
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
        logger.log(AuditEvent::new(AuditAction::RequestReceived)).await;
        logger.log(AuditEvent::new(AuditAction::AgentCompleted)).await;
        assert_eq!(sink.events().len(), 2);
        assert_eq!(sink.events()[0].action, AuditAction::RequestReceived);
    }
}
