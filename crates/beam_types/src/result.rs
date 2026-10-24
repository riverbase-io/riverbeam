use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::content::ContentPart;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct TokenUsage {
    #[serde(default)]
    pub prompt_tokens: u32,
    #[serde(default)]
    pub completion_tokens: u32,
    #[serde(default)]
    pub total_tokens: u32,
    /// Prompt tokens read from a provider cache (rc8; plan 023). Left out
    /// of the JSON when 0, so rc1-shaped usage serializes as before.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub cache_read_tokens: u32,
    /// Prompt tokens written to a provider cache (rc8; plan 023).
    #[serde(default, skip_serializing_if = "is_zero")]
    pub cache_write_tokens: u32,
}

impl TokenUsage {
    /// Add `other`, field by field (saturating).
    pub fn add(&mut self, other: &Self) {
        self.prompt_tokens = self.prompt_tokens.saturating_add(other.prompt_tokens);
        self.completion_tokens = self
            .completion_tokens
            .saturating_add(other.completion_tokens);
        self.total_tokens = self.total_tokens.saturating_add(other.total_tokens);
        self.cache_read_tokens = self
            .cache_read_tokens
            .saturating_add(other.cache_read_tokens);
        self.cache_write_tokens = self
            .cache_write_tokens
            .saturating_add(other.cache_write_tokens);
    }

    /// The larger of each field of `self` and `other`.
    #[must_use]
    pub fn max_fields(&self, other: &Self) -> Self {
        Self {
            prompt_tokens: self.prompt_tokens.max(other.prompt_tokens),
            completion_tokens: self.completion_tokens.max(other.completion_tokens),
            total_tokens: self.total_tokens.max(other.total_tokens),
            cache_read_tokens: self.cache_read_tokens.max(other.cache_read_tokens),
            cache_write_tokens: self.cache_write_tokens.max(other.cache_write_tokens),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolCallRecord {
    pub tool_name: String,
    #[serde(default, skip_serializing_if = "is_empty_object")]
    pub args: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage_metadata: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_step_id: Option<String>,
}

// serde's `skip_serializing_if` passes a reference.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_zero(n: &u32) -> bool {
    *n == 0
}

fn is_empty_object(v: &Value) -> bool {
    v.is_null() || (v.is_object() && v.as_object().is_some_and(serde_json::Map::is_empty))
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentResult {
    pub content: Vec<ContentPart>,
    pub agent_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_used: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interaction_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_usage: Option<TokenUsage>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ToolCallRecord>,
    #[serde(default, skip_serializing_if = "is_empty_object")]
    pub metadata: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<f64>,
    #[serde(default)]
    pub interrupted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action_required: Option<Vec<Value>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub citations: Vec<Value>,
}

impl AgentResult {
    /// The text parts joined by a space.
    #[must_use]
    pub fn text_content(&self) -> String {
        self.content
            .iter()
            .filter_map(|p| p.text.as_deref())
            .filter(|t| !t.is_empty())
            .collect::<Vec<_>>()
            .join(" ")
    }
}
