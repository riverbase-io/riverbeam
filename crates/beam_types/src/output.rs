use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// JSON keys required on `AgentResult.metadata["output"]`. Mirrors Python `AgentOutputContract`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct AgentOutputContract {
    #[serde(default)]
    pub required: Vec<String>,
}

/// A media artifact produced by an execution. Mirrors Python `Artifact`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Artifact {
    #[serde(default = "Uuid::new_v4")]
    pub id: Uuid,
    pub execution_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<Uuid>,
    pub media_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default = "Utc::now", deserialize_with = "crate::time::datetime")]
    pub created_at: DateTime<Utc>,
}
