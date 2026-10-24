//! The publishable agent contract (Python `agents/contract.py` `AgentSpec`).
//!
//! Serialization equals Python's `model_dump(mode="json")`: every field is
//! present (`None` as `null`), so [`AgentSpec::canonical_payload`] and the
//! revision hashes built on it match across the stacks.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Map, Value};

use crate::model::CredentialRef;

/// The agent spec schema version this build writes.
pub const SPEC_SCHEMA_VERSION: &str = "1";
/// Isolation modes a spec may name.
pub const SUPPORTED_ISOLATION: [&str; 1] = ["org"];
/// Knowledge modes a spec may name.
pub const SUPPORTED_KNOWLEDGE_MODES: [&str; 3] = ["off", "auto", "tool"];
/// Memory backends a spec may name.
pub const SUPPORTED_MEMORY_BACKENDS: [&str; 6] = [
    "session",
    "session_and_long_term",
    "off",
    "none",
    "long_term",
    "ltm",
];

/// How a resume treats a newer revision.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompatibilityPolicy {
    #[default]
    Strict,
    Compatible,
    Migratable,
}

impl CompatibilityPolicy {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Strict => "strict",
            Self::Compatible => "compatible",
            Self::Migratable => "migratable",
        }
    }
}

/// Where a revision is in its lifecycle.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RevisionStatus {
    #[default]
    Draft,
    Validating,
    Active,
    Deprecated,
    Retired,
}

impl RevisionStatus {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::Validating => "validating",
            Self::Active => "active",
            Self::Deprecated => "deprecated",
            Self::Retired => "retired",
        }
    }

    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "draft" => Some(Self::Draft),
            "validating" => Some(Self::Validating),
            "active" => Some(Self::Active),
            "deprecated" => Some(Self::Deprecated),
            "retired" => Some(Self::Retired),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[allow(clippy::struct_excessive_bools)] // Python's policy fields.
pub struct RuntimePolicy {
    pub name: String,
    #[serde(default)]
    pub require_streaming: bool,
    #[serde(default)]
    pub require_resume: bool,
    #[serde(default)]
    pub require_cancellation: bool,
    #[serde(default)]
    pub require_checkpointing: bool,
    #[serde(default)]
    pub require_structured_output: bool,
    #[serde(default)]
    pub require_native_tools: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PromptPolicy {
    #[serde(default)]
    pub instructions: String,
    #[serde(default)]
    pub revision: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SchemaPolicy {
    #[serde(default)]
    pub input_schema: Option<Map<String, Value>>,
    #[serde(default)]
    pub output_schema: Option<Map<String, Value>>,
}

/// `CredentialRef` as pydantic dumps it: every key, `null` when unset.
#[allow(clippy::ref_option)] // serde's `serialize_with` passes `&Option<T>`.
fn full_credential<S: Serializer>(
    value: &Option<CredentialRef>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match value {
        None => serializer.serialize_none(),
        Some(r) => serde_json::json!({"name": r.name, "provider": r.provider, "env": r.env})
            .serialize(serializer),
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ModelPolicy {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub route: Option<String>,
    #[serde(default, serialize_with = "full_credential")]
    pub credential_ref: Option<CredentialRef>,
    #[serde(default)]
    pub revision: Option<String>,
}

fn restricted() -> String {
    "restricted".into()
}

fn web_schemes() -> Vec<String> {
    vec!["https".into(), "http".into()]
}

fn one_megabyte() -> u64 {
    1_000_000
}

/// A spec's context section.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentContextPolicySpec {
    #[serde(default)]
    pub providers: Vec<String>,
    #[serde(default)]
    pub token_budget: Option<u64>,
    #[serde(default = "restricted")]
    pub max_sensitivity: String,
    /// Source name to `fail_open` / `fail_closed`.
    #[serde(default)]
    pub source_failure: BTreeMap<String, String>,
    #[serde(default)]
    pub promote_ltm_trust: bool,
    #[serde(default = "web_schemes")]
    pub resource_schemes: Vec<String>,
    #[serde(default)]
    pub resource_hosts: Vec<String>,
    #[serde(default = "one_megabyte")]
    pub max_resource_bytes: u64,
    /// How each model call is fitted to its budget (rc8; plan 023 B1). Absent
    /// means [`CompactionPolicy::default`]: on (D4). Left out of the JSON
    /// when absent, so specs written before keep their hash.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compaction: Option<CompactionPolicy>,
}

/// Python rc8 `CompactionPolicy`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompactionPolicy {
    #[serde(default = "yes")]
    pub enabled: bool,
    #[serde(default = "soft_ratio")]
    pub soft_ratio: f64,
    #[serde(default = "hard_ratio")]
    pub hard_ratio: f64,
    #[serde(default = "target_ratio")]
    pub target_ratio: f64,
    #[serde(default = "compact_reserve")]
    pub reserve_tokens: u64,
    #[serde(default = "keep_recent_units")]
    pub keep_recent_units: usize,
    #[serde(default = "keep_tool_results")]
    pub keep_tool_results: usize,
    /// `heuristic` (default) or `model` (the agent's own model, metered).
    #[serde(default = "heuristic")]
    pub summarizer: String,
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            soft_ratio: soft_ratio(),
            hard_ratio: hard_ratio(),
            target_ratio: target_ratio(),
            reserve_tokens: compact_reserve(),
            keep_recent_units: keep_recent_units(),
            keep_tool_results: keep_tool_results(),
            summarizer: heuristic(),
        }
    }
}

fn soft_ratio() -> f64 {
    0.70
}

fn hard_ratio() -> f64 {
    0.85
}

fn target_ratio() -> f64 {
    0.50
}

fn compact_reserve() -> u64 {
    2000
}

fn keep_recent_units() -> usize {
    6
}

fn keep_tool_results() -> usize {
    3
}

fn heuristic() -> String {
    "heuristic".into()
}

impl Default for AgentContextPolicySpec {
    fn default() -> Self {
        Self {
            providers: Vec::new(),
            token_budget: None,
            max_sensitivity: restricted(),
            source_failure: BTreeMap::new(),
            promote_ltm_trust: false,
            resource_schemes: web_schemes(),
            resource_hosts: Vec::new(),
            max_resource_bytes: one_megabyte(),
            compaction: None,
        }
    }
}

fn session() -> String {
    "session".into()
}

fn auto() -> String {
    "auto".into()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemoryPolicy {
    #[serde(default = "session")]
    pub backend: String,
    #[serde(default)]
    pub revision: Option<String>,
}

impl Default for MemoryPolicy {
    fn default() -> Self {
        Self {
            backend: session(),
            revision: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KnowledgePolicy {
    #[serde(default)]
    pub collections: Vec<String>,
    #[serde(default = "auto")]
    pub mode: String,
    #[serde(default)]
    pub revision: Option<String>,
}

impl Default for KnowledgePolicy {
    fn default() -> Self {
        Self {
            collections: Vec::new(),
            mode: auto(),
            revision: None,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ToolPolicy {
    #[serde(default)]
    pub allowlist: Vec<String>,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub revision: Option<String>,
}

fn yes() -> bool {
    true
}

fn org() -> String {
    "org".into()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SecurityTenancyPolicy {
    #[serde(default = "yes")]
    pub enforce: bool,
    #[serde(default = "org")]
    pub isolation: String,
    #[serde(default)]
    pub revision: Option<String>,
}

impl Default for SecurityTenancyPolicy {
    fn default() -> Self {
        Self {
            enforce: true,
            isolation: org(),
            revision: None,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ExecutionPolicy {
    #[serde(default)]
    pub timeout_s: Option<i64>,
    #[serde(default)]
    pub max_steps: Option<i64>,
    #[serde(default)]
    pub compatibility: CompatibilityPolicy,
}

fn schema_version() -> String {
    SPEC_SCHEMA_VERSION.into()
}

fn one_oh() -> String {
    "1.0.0".into()
}

/// The immutable publishable agent contract.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentSpec {
    #[serde(default = "schema_version")]
    pub schema_version: String,
    pub name: String,
    #[serde(default = "one_oh")]
    pub version: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub tags: Vec<String>,
    pub runtime: RuntimePolicy,
    #[serde(default)]
    pub prompt: PromptPolicy,
    #[serde(default)]
    pub schemas: SchemaPolicy,
    #[serde(default)]
    pub model: ModelPolicy,
    #[serde(default)]
    pub context: AgentContextPolicySpec,
    #[serde(default)]
    pub memory: MemoryPolicy,
    #[serde(default)]
    pub knowledge: KnowledgePolicy,
    #[serde(default)]
    pub tools: ToolPolicy,
    #[serde(default)]
    pub security: SecurityTenancyPolicy,
    #[serde(default)]
    pub execution: ExecutionPolicy,
}

impl AgentSpec {
    /// A spec with Python's defaults for `name` on `runtime`.
    #[must_use]
    pub fn new(name: impl Into<String>, runtime: impl Into<String>) -> Self {
        Self {
            schema_version: schema_version(),
            name: name.into(),
            version: one_oh(),
            description: String::new(),
            tags: Vec::new(),
            runtime: RuntimePolicy {
                name: runtime.into(),
                ..RuntimePolicy::default()
            },
            prompt: PromptPolicy::default(),
            schemas: SchemaPolicy::default(),
            model: ModelPolicy::default(),
            context: AgentContextPolicySpec::default(),
            memory: MemoryPolicy::default(),
            knowledge: KnowledgePolicy::default(),
            tools: ToolPolicy::default(),
            security: SecurityTenancyPolicy::default(),
            execution: ExecutionPolicy::default(),
        }
    }

    /// Python `canonical_spec_payload` (`model_dump(mode="json")`).
    #[must_use]
    pub fn canonical_payload(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }
}

/// One difference between two specs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpecChange {
    pub path: String,
    pub left: Value,
    pub right: Value,
}

/// Changed leaves by dotted path, keys sorted.
#[must_use]
pub fn diff_specs(left: &AgentSpec, right: &AgentSpec) -> Vec<SpecChange> {
    let mut out = Vec::new();
    walk(
        &left.canonical_payload(),
        &right.canonical_payload(),
        "",
        &mut out,
    );
    out
}

fn walk(left: &Value, right: &Value, prefix: &str, out: &mut Vec<SpecChange>) {
    if let (Value::Object(l), Value::Object(r)) = (left, right) {
        let mut keys: Vec<&String> = l.keys().chain(r.keys()).collect();
        keys.sort();
        keys.dedup();
        for key in keys {
            let path = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{prefix}.{key}")
            };
            match (l.get(key), r.get(key)) {
                (None, Some(rv)) => out.push(SpecChange {
                    path,
                    left: Value::Null,
                    right: rv.clone(),
                }),
                (Some(lv), None) => out.push(SpecChange {
                    path,
                    left: lv.clone(),
                    right: Value::Null,
                }),
                (Some(lv), Some(rv)) => walk(lv, rv, &path, out),
                (None, None) => {}
            }
        }
        return;
    }
    if left != right {
        out.push(SpecChange {
            path: prefix.to_owned(),
            left: left.clone(),
            right: right.clone(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_dump_like_pydantic() {
        let spec: AgentSpec =
            serde_json::from_value(serde_json::json!({"name": "a", "runtime": {"name": "rig"}}))
                .expect("spec");
        let dumped = spec.canonical_payload();
        assert_eq!(dumped["model"]["credential_ref"], Value::Null);
        assert_eq!(
            dumped["context"]["resource_schemes"],
            serde_json::json!(["https", "http"])
        );
        assert_eq!(dumped["execution"]["compatibility"], "strict");
        assert_eq!(dumped["security"]["isolation"], "org");
        let mut other = spec.clone();
        other.tools.allowlist.push("search".into());
        let changes = diff_specs(&spec, &other);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].path, "tools.allowlist");
    }
}
