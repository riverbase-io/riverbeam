use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::action::ActionResponseEnvelope;
use crate::content::ContentPart;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InvokeAuthContext {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        alias = "organization_id"
    )]
    pub org_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InvokeRequest {
    pub agent_name: String,
    pub content: Vec<ContentPart>,
    pub session_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_override: Option<String>,
    #[serde(default, skip_serializing_if = "is_empty_object")]
    pub metadata: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interaction_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<Uuid>,
    #[serde(default)]
    pub stream: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actions: Option<Vec<ActionResponseEnvelope>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_context: Option<InvokeAuthContext>,
}

fn is_empty_object(v: &Value) -> bool {
    v.is_null() || (v.is_object() && v.as_object().is_some_and(|m| m.is_empty()))
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChatRequest {
    pub agent_name: String,
    pub message: String,
    pub session_id: Uuid,
    #[serde(default, skip_serializing_if = "is_empty_object")]
    pub metadata: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<Uuid>,
    #[serde(default)]
    pub stream: bool,
}
