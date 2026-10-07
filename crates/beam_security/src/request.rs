use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::enums::SecurityStage;

/// Identity/tenant context carried with a security request.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SecurityContext {
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub user_id: Option<String>,
    #[serde(default)]
    pub interaction_id: Option<String>,
}

/// A stage-tagged security evaluation request (ports `SecurityRequest`).
#[derive(Debug, Clone)]
pub struct SecurityRequest {
    pub stage: SecurityStage,
    pub payload: Map<String, Value>,
    pub security_context: Option<SecurityContext>,
}

impl SecurityRequest {
    #[must_use]
    pub fn new(stage: SecurityStage) -> Self {
        Self {
            stage,
            payload: Map::new(),
            security_context: None,
        }
    }

    #[must_use]
    pub fn with_payload(mut self, payload: Map<String, Value>) -> Self {
        self.payload = payload;
        self
    }

    #[must_use]
    pub fn with_context(mut self, ctx: SecurityContext) -> Self {
        self.security_context = Some(ctx);
        self
    }

    #[must_use]
    pub fn str_field(&self, key: &str) -> String {
        self.payload
            .get(key)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    }
}
