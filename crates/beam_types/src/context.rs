use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// How sensitive a request fact or resource is. Mirrors Python `Sensitivity`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Sensitivity {
    Public,
    #[default]
    Internal,
    Confidential,
    Restricted,
}

/// Per-request knowledge mode. Mirrors Python `KnowledgeMode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KnowledgeMode {
    #[default]
    Inherit,
    Off,
    Auto,
    Tool,
}

/// Per-request memory mode. Mirrors Python `MemoryMode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryMode {
    #[default]
    Inherit,
    Off,
    Session,
    SessionAndLongTerm,
}

/// One untrusted fact supplied with a request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RequestFact {
    pub key: String,
    pub value: Value,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::time::opt_datetime"
    )]
    pub expiry: Option<DateTime<Utc>>,
    #[serde(default)]
    pub sensitivity: Sensitivity,
}

/// One resource reference supplied with a request; resolved by the context pipeline.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResourceRef {
    pub uri: String,
    #[serde(default = "default_resource_kind")]
    pub kind: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::time::opt_datetime"
    )]
    pub expiry: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_text: Option<String>,
}

fn default_resource_kind() -> String {
    "resource".into()
}

/// Request-scoped context inputs. Mirrors Python `RequestContext`.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct RequestContext {
    #[serde(default)]
    pub facts: Vec<RequestFact>,
    #[serde(default)]
    pub resources: Vec<ResourceRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile_id: Option<String>,
    #[serde(default)]
    pub knowledge_mode: KnowledgeMode,
    #[serde(default)]
    pub memory_mode: MemoryMode,
    /// rc8: send one durable `context_usage` frame per model call. Left out
    /// of the JSON when off, so rc1-shaped requests serialize as before.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub emit_context_usage: bool,
    /// rc8: an input budget below the model's (the smaller one applies).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_budget: Option<u64>,
}

/// Tokens one model call sends, by category, plus the reserved and free
/// space; no content (Python rc8 `ContextUsage`, plan 023 A2). Categories:
/// `system_prompt`, `tools`, `memory`, `knowledge`, `notes`, `summary`,
/// `messages`, `tool_results`.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ContextUsage {
    pub window_tokens: u64,
    pub output_tokens: u64,
    pub reserve_tokens: u64,
    #[serde(default)]
    pub compact_reserve_tokens: u64,
    #[serde(default)]
    pub by_category: std::collections::BTreeMap<String, u64>,
    #[serde(default)]
    pub deferred_tools: u64,
    #[serde(default)]
    pub used_tokens: u64,
    /// Negative when the call is over the window.
    #[serde(default)]
    pub free_tokens: i64,
    #[serde(default)]
    pub cached_read_tokens: Option<u64>,
    #[serde(default = "conservative")]
    pub counter_method: String,
    /// The model declares no window: the window is a guess, and reducers are off.
    #[serde(default)]
    pub window_assumed: bool,
}

fn conservative() -> String {
    "estimated_conservative".into()
}
