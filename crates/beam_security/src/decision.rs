use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::enums::{SecurityAction, SecuritySeverity, ViolationType};

/// Normalized outcome of a security evaluation (ports `SecurityDecision`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityDecision {
    pub action: SecurityAction,
    pub severity: SecuritySeverity,
    #[serde(default)]
    pub matched_rules: Vec<String>,
    #[serde(default)]
    pub narration: String,
    #[serde(default)]
    pub user_message: Option<String>,
    #[serde(default)]
    pub metadata: Map<String, Value>,
    #[serde(default)]
    pub violation_type: Option<ViolationType>,
    #[serde(default)]
    pub score: f64,
    #[serde(default)]
    pub sanitized_content: Option<String>,
}

impl Default for SecurityDecision {
    fn default() -> Self {
        Self::allow()
    }
}

impl SecurityDecision {
    #[must_use]
    pub fn allow() -> Self {
        Self {
            action: SecurityAction::Allow,
            severity: SecuritySeverity::Low,
            matched_rules: Vec::new(),
            narration: String::new(),
            user_message: None,
            metadata: Map::new(),
            violation_type: None,
            score: 0.0,
            sanitized_content: None,
        }
    }

    #[must_use]
    pub fn denies(&self) -> bool {
        self.action.denies()
    }

    #[must_use]
    pub fn requires_hitl(&self) -> bool {
        self.action == SecurityAction::RequireApproval
            || self
                .metadata
                .get("hitl_required")
                .and_then(Value::as_bool)
                .unwrap_or(false)
    }
}
