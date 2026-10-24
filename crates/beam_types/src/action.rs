use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ActionResponseStatus {
    #[default]
    Submitted,
    Cancelled,
    Expired,
    Invalid,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActionResponseEnvelope {
    pub action_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lc_action_id: Option<String>,
    pub kind: String,
    pub request_id: String,
    #[serde(default)]
    pub status: ActionResponseStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub responder_id: Option<String>,
    #[serde(default = "utc_now", deserialize_with = "crate::time::datetime")]
    pub responded_at: DateTime<Utc>,
}

fn utc_now() -> DateTime<Utc> {
    Utc::now()
}

/// HITL action kind. Mirrors Python `ActionType` (rc8 adds the two tool kinds).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionType {
    Confirmation,
    Form,
    Choice,
    Approval,
    /// A tool call that waits for a human (`beam.tool_approval`).
    ToolApproval,
    /// A tool the calling client runs (`beam.client_tool`).
    ClientTool,
}

/// Canonical action request emitted by an agent. Mirrors Python `ActionRequestEnvelope`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActionRequestEnvelope {
    pub kind: String,
    pub request_id: String,
    pub title: String,
    pub description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action_type: Option<ActionType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submit_label: Option<String>,
    #[serde(default = "crate::content::empty_object")]
    pub payload: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_schema: Option<Value>,
    #[serde(default = "crate::content::empty_object")]
    pub metadata: Value,
}
