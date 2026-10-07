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

fn is_empty_object(v: &Value) -> bool {
    v.is_null() || (v.is_object() && v.as_object().is_some_and(|m| m.is_empty()))
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
}
