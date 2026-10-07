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
    #[serde(default = "utc_now")]
    pub responded_at: DateTime<Utc>,
}

fn utc_now() -> DateTime<Utc> {
    Utc::now()
}
