use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use beam_types::TokenUsage;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditAction {
    RequestReceived,
    AgentInvoked,
    AgentCompleted,
    AgentFailed,
    AgentInterrupted,
    AgentResumed,
    ToolCalled,
    ModelRouted,
    QuotaChecked,
    QuotaExceeded,
    SecurityEvent,
    AgentPublished,
    PolicyChanged,
    ContextResourceAccessed,
    MemoryWritten,
    MemoryDeleted,
    ToolApproval,
    CredentialUsed,
    OperatorRecovery,
}

impl AuditAction {
    /// The Python value (`request_received`, ...).
    #[must_use]
    pub fn as_str(self) -> String {
        serde_json::to_value(self)
            .ok()
            .and_then(|v| v.as_str().map(str::to_owned))
            .unwrap_or_default()
    }

    /// The `auditaction` enum label (the Python member name).
    #[must_use]
    pub fn db_label(self) -> String {
        self.as_str().to_uppercase()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AuditEvent {
    #[serde(default = "Utc::now")]
    pub timestamp: DateTime<Utc>,
    pub action: AuditAction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_used: Option<String>,
    #[serde(default)]
    pub modalities_used: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_usage: Option<TokenUsage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<f64>,
    #[serde(default = "ok_status")]
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_preview: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_preview: Option<String>,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub metadata: Value,
}

fn ok_status() -> String {
    "ok".into()
}

impl AuditEvent {
    #[must_use]
    pub fn new(action: AuditAction) -> Self {
        Self {
            timestamp: Utc::now(),
            action,
            agent_name: None,
            run_id: None,
            execution_id: None,
            trace_id: None,
            user_id: None,
            session_id: None,
            model_used: None,
            modalities_used: Vec::new(),
            token_usage: None,
            latency_ms: None,
            status: ok_status(),
            error: None,
            input_preview: None,
            output_preview: None,
            metadata: Value::Null,
        }
    }

    #[must_use]
    pub fn agent_name(mut self, name: Option<String>) -> Self {
        self.agent_name = name;
        self
    }

    #[must_use]
    pub fn user_id(mut self, user_id: Option<String>) -> Self {
        self.user_id = user_id;
        self
    }

    #[must_use]
    pub fn session_id(mut self, session_id: Option<Uuid>) -> Self {
        self.session_id = session_id;
        self
    }

    #[must_use]
    pub fn run_id(mut self, run_id: Option<Uuid>) -> Self {
        self.run_id = run_id;
        self
    }

    #[must_use]
    pub fn execution_id(mut self, execution_id: Option<Uuid>) -> Self {
        self.execution_id = execution_id;
        self
    }

    #[must_use]
    pub fn trace_id(mut self, trace_id: Option<String>) -> Self {
        self.trace_id = trace_id;
        self
    }

    #[must_use]
    pub fn status(mut self, status: impl Into<String>) -> Self {
        self.status = status.into();
        self
    }

    #[must_use]
    pub fn error(mut self, error: Option<String>) -> Self {
        self.error = error;
        self
    }
}
