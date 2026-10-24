use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::action::ActionResponseEnvelope;
use crate::content::ContentPart;
use crate::context::RequestContext;

#[derive(Clone, PartialEq, Serialize, Deserialize)]
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

/// The MCP token is a credential: never printed.
impl std::fmt::Debug for InvokeAuthContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InvokeAuthContext")
            .field("profile_id", &self.profile_id)
            .field("user_id", &self.user_id)
            .field("org_id", &self.org_id)
            .field("mcp_token", &self.mcp_token.as_ref().map(|_| "***"))
            .finish()
    }
}

/// What the calling client can run right now; sent on invoke and resume
/// (Python rc8 `ClientCapabilities`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientCapabilities {
    pub client_id: String,
    #[serde(default = "web")]
    pub kind: String,
    #[serde(default)]
    pub tools: Vec<String>,
}

fn web() -> String {
    "web".into()
}

impl ClientCapabilities {
    /// Whether this client declared `tool`.
    #[must_use]
    pub fn runs(&self, tool: &str) -> bool {
        self.tools.iter().any(|t| t == tool)
    }
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
    pub execution_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub stream: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actions: Option<Vec<ActionResponseEnvelope>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_context: Option<InvokeAuthContext>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<RequestContext>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client: Option<ClientCapabilities>,
}

fn is_empty_object(v: &Value) -> bool {
    v.is_null() || (v.is_object() && v.as_object().is_some_and(serde_json::Map::is_empty))
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
