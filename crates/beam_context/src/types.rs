//! Context records (Python `kernel/contracts/context.py`): the source table,
//! items, decisions, manifests and envelopes.

use std::collections::BTreeMap;

use beam_types::{AgentMessage, ContentPart, InvokeAuthContext, Sensitivity};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// A context source's name, rank and whether it does I/O.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceDescriptor {
    pub name: &'static str,
    /// `pure` sources run in order; `io` sources run concurrently.
    pub io: bool,
    pub required: bool,
    pub compose_index: usize,
    pub default_index: Option<usize>,
}

const fn source(
    name: &'static str,
    io: bool,
    required: bool,
    compose_index: usize,
    default_index: Option<usize>,
) -> SourceDescriptor {
    SourceDescriptor {
        name,
        io,
        required,
        compose_index,
        default_index,
    }
}

/// Python `SOURCE_DESCRIPTORS`, in compose (registry) order.
pub const SOURCE_DESCRIPTORS: [SourceDescriptor; 11] = [
    source("instructions", false, true, 0, Some(0)),
    source("current", false, true, 1, Some(2)),
    source("workflow", false, true, 2, Some(8)),
    source("tools", false, false, 3, Some(9)),
    source("history", true, false, 4, Some(3)),
    // Enabled with `history` (`alias_enable`).
    source("summary", true, false, 5, None),
    source("knowledge", true, false, 6, Some(4)),
    source("ltm", true, false, 7, Some(5)),
    source("profile", true, false, 8, Some(1)),
    source("facts", false, false, 9, Some(6)),
    source("resources", true, false, 10, Some(7)),
];

/// The sources in registry order.
#[must_use]
pub fn registry_order() -> Vec<&'static str> {
    SOURCE_DESCRIPTORS.iter().map(|d| d.name).collect()
}

/// The defaulted sources by `default_index`.
#[must_use]
pub fn default_sources() -> Vec<String> {
    let mut rows: Vec<&SourceDescriptor> = SOURCE_DESCRIPTORS
        .iter()
        .filter(|d| d.default_index.is_some())
        .collect();
    rows.sort_by_key(|d| d.default_index);
    rows.into_iter().map(|d| d.name.to_owned()).collect()
}

/// The sources every build includes.
#[must_use]
pub fn required_sources() -> Vec<String> {
    SOURCE_DESCRIPTORS
        .iter()
        .filter(|d| d.required)
        .map(|d| d.name.to_owned())
        .collect()
}

/// Whether `name` is an I/O source.
#[must_use]
pub fn is_io_source(name: &str) -> bool {
    SOURCE_DESCRIPTORS.iter().any(|d| d.name == name && d.io)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Trust {
    System,
    Developer,
    User,
    Untrusted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ContextRole {
    System,
    Developer,
    Human,
    Ai,
    Tool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DecisionAction {
    Selected,
    Dropped,
    Transformed,
}

/// The rank of a sensitivity label (higher is more sensitive).
fn sensitivity_rank(s: Sensitivity) -> u8 {
    match s {
        Sensitivity::Public => 0,
        Sensitivity::Internal => 1,
        Sensitivity::Confidential => 2,
        Sensitivity::Restricted => 3,
    }
}

/// Whether `item` is more sensitive than `ceiling` allows.
#[must_use]
pub fn sensitivity_exceeds(item: Sensitivity, ceiling: Sensitivity) -> bool {
    sensitivity_rank(item) > sensitivity_rank(ceiling)
}

/// One candidate a source returns.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextItem {
    pub id: String,
    pub content: String,
    pub parts: Vec<ContentPart>,
    pub role: ContextRole,
    pub source: String,
    pub trust: Trust,
    pub priority: i64,
    pub required: bool,
    pub provenance: Option<String>,
    pub sensitivity: Sensitivity,
    pub tenant_scope: Option<String>,
    pub cacheable: bool,
    pub expiry: Option<DateTime<Utc>>,
    pub token_estimate: u64,
    pub unit_id: Option<String>,
    pub name: Option<String>,
    pub tool_call_id: Option<String>,
    pub tool_calls: Vec<Value>,
    pub tool_result: Option<Value>,
    pub citation_id: Option<String>,
    pub memory_id: Option<String>,
    pub memory_type: Option<String>,
    pub confidence: Option<f64>,
    pub consent: Option<bool>,
}

impl ContextItem {
    /// An item with Python's field defaults; `content` and `parts` are synced
    /// as Python's `_sync_parts` validator does.
    #[must_use]
    pub fn new(id: impl Into<String>, role: ContextRole, source: &str, trust: Trust) -> Self {
        Self {
            id: id.into(),
            content: String::new(),
            parts: Vec::new(),
            role,
            source: source.to_owned(),
            trust,
            priority: 0,
            required: false,
            provenance: None,
            sensitivity: Sensitivity::Internal,
            tenant_scope: None,
            cacheable: false,
            expiry: None,
            token_estimate: 0,
            unit_id: None,
            name: None,
            tool_call_id: None,
            tool_calls: Vec::new(),
            tool_result: None,
            citation_id: None,
            memory_id: None,
            memory_type: None,
            confidence: None,
            consent: None,
        }
    }

    /// Python `_sync_parts`.
    #[must_use]
    pub fn synced(mut self) -> Self {
        if self.parts.is_empty() && !self.content.is_empty() {
            self.parts = vec![ContentPart::text(self.content.clone())];
        } else if !self.parts.is_empty() && self.content.is_empty() {
            self.content = self
                .parts
                .iter()
                .filter_map(|p| p.text.as_deref().filter(|t| !t.is_empty()))
                .collect::<Vec<_>>()
                .join(" ");
        }
        self
    }
}

/// One manifest entry: an item selected, dropped or transformed, and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestDecision {
    pub item_id: String,
    pub source: String,
    pub action: DecisionAction,
    pub reason: String,
    pub tokens: u64,
    pub content_hash: String,
    pub trust: Trust,
}

/// What was selected and dropped, and why.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextManifest {
    #[serde(default = "schema_version")]
    pub schema_version: String,
    #[serde(default)]
    pub decisions: Vec<ManifestDecision>,
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
    #[serde(default)]
    pub reserve_tokens: u64,
    #[serde(default)]
    pub window_tokens: u64,
    #[serde(default)]
    pub policy_revision: Option<String>,
    #[serde(default)]
    pub revision_id: Option<String>,
    #[serde(default = "conservative")]
    pub counter_method: String,
    #[serde(default)]
    pub tokenizer_revision: Option<String>,
    #[serde(default)]
    pub serializer_revision: Option<String>,
    #[serde(default)]
    pub operation_id: Option<String>,
    #[serde(default)]
    pub parent_execution_id: Option<String>,
    /// The invoke's manifest a model call's (`{operation_id}:{step}`) belongs to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_operation_id: Option<String>,
    #[serde(default = "invoke")]
    pub operation_kind: String,
    /// The model call (1..N); 0 for the invoke's composed envelope.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub step: u32,
    /// What one model call sent, by category (rc8; plan 023 A2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<beam_types::ContextUsage>,
    #[serde(default)]
    pub source_timings: BTreeMap<String, f64>,
    #[serde(default)]
    pub actual_prompt_tokens: Option<u64>,
    #[serde(default)]
    pub estimate_drift: Option<f64>,
}

// serde's `skip_serializing_if` passes a reference.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_zero(n: &u32) -> bool {
    *n == 0
}

fn schema_version() -> String {
    "1".into()
}

fn conservative() -> String {
    beam_gateway::tokens::CONSERVATIVE.into()
}

fn invoke() -> String {
    "invoke".into()
}

impl Default for ContextManifest {
    fn default() -> Self {
        Self {
            schema_version: schema_version(),
            decisions: Vec::new(),
            input_tokens: 0,
            output_tokens: 0,
            reserve_tokens: 0,
            window_tokens: 0,
            policy_revision: None,
            revision_id: None,
            counter_method: conservative(),
            tokenizer_revision: None,
            serializer_revision: None,
            operation_id: None,
            parent_execution_id: None,
            parent_operation_id: None,
            operation_kind: invoke(),
            step: 0,
            usage: None,
            source_timings: BTreeMap::new(),
            actual_prompt_tokens: None,
            estimate_drift: None,
        }
    }
}

/// Record the provider's prompt tokens and the drift
/// from the estimate.
#[must_use]
pub fn reconcile_usage(manifest: &ContextManifest, prompt_tokens: u64) -> ContextManifest {
    #[allow(clippy::cast_precision_loss)]
    let drift = if manifest.input_tokens == 0 {
        0.0
    } else {
        (prompt_tokens as f64 - manifest.input_tokens as f64) / manifest.input_tokens as f64
    };
    ContextManifest {
        actual_prompt_tokens: Some(prompt_tokens),
        estimate_drift: Some(drift),
        ..manifest.clone()
    }
}

pub const ENVELOPE_SERIALIZER: &str = "beam-envelope-v1";

/// The only model-visible input.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextEnvelope {
    pub messages: Vec<AgentMessage>,
    pub tool_schemas: Vec<Value>,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub reserve_tokens: u64,
    pub window_tokens: u64,
    pub counter_method: String,
    pub serializer_revision: String,
    pub cache_prefix_tokens: u64,
}

impl Default for ContextEnvelope {
    fn default() -> Self {
        Self {
            messages: Vec::new(),
            tool_schemas: Vec::new(),
            input_tokens: 0,
            output_tokens: 0,
            reserve_tokens: 0,
            window_tokens: 0,
            counter_method: conservative(),
            serializer_revision: ENVELOPE_SERIALIZER.into(),
            cache_prefix_tokens: 0,
        }
    }
}

/// Non-prompt handles for sources and resolvers.
#[derive(Debug, Clone, Default)]
pub struct ExecutionContext {
    pub auth: Option<InvokeAuthContext>,
    pub tenant_id: Option<String>,
    pub user_id: Option<String>,
    pub execution_id: Option<Uuid>,
    pub run_id: Option<Uuid>,
    pub deadline: Option<DateTime<Utc>>,
    pub cancelled: bool,
}

pub use beam_execution::identity::normalize_identity;
