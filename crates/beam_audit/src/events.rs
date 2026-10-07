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
    pub user_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_used: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_usage: Option<TokenUsage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<f64>,
    #[serde(default = "ok_status")]
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
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
            user_id: None,
            session_id: None,
            model_used: None,
            token_usage: None,
            latency_ms: None,
            status: ok_status(),
            error: None,
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
