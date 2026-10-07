use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InteractionStatus {
    InProgress,
    Completed,
    Failed,
}

impl InteractionStatus {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InProgress => "in_progress",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Running,
    Completed,
    Failed,
    Interrupted,
}

impl RunStatus {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Interrupted => "interrupted",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionStatus {
    Pending,
    Resolved,
    Cancelled,
}

impl ActionStatus {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Resolved => "resolved",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AgentRow {
    pub id: Uuid,
    pub name: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ModelRow {
    pub id: Uuid,
    pub name: String,
    pub provider: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SessionRow {
    pub id: Uuid,
    pub agent_id: Uuid,
    pub user_id: Option<String>,
    pub profile_id: Option<String>,
    pub name: Option<String>,
    pub last_active_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InteractionRow {
    pub id: Uuid,
    pub session_id: Uuid,
    pub agent_id: Uuid,
    pub agent_name: String,
    pub status: InteractionStatus,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RunRow {
    pub id: Uuid,
    pub session_id: Uuid,
    pub interaction_id: Uuid,
    pub agent_id: Uuid,
    pub agent_name: String,
    pub status: RunStatus,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MessageRow {
    pub id: Uuid,
    pub sequence: i32,
    pub session_id: Uuid,
    pub interaction_id: Option<Uuid>,
    pub run_id: Option<Uuid>,
    pub agent_id: Uuid,
    pub role: String,
    #[serde(rename = "type")]
    pub msg_type: String,
    pub content: Value,
    pub request_id: Option<Uuid>,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ActionRow {
    pub id: Uuid,
    pub session_id: Uuid,
    pub interaction_id: Uuid,
    pub run_id: Uuid,
    pub agent_id: Uuid,
    pub action_type: String,
    pub status: ActionStatus,
    pub payload: Value,
    pub result: Option<Value>,
    pub resolved_at: Option<DateTime<Utc>>,
}
