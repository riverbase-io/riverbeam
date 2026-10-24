//! Compile a pinned agent revision into an immutable context policy (Python
//! `context/policy.py`).

use std::collections::BTreeMap;

use beam_types::{pyjson, KnowledgeMode, MemoryMode, RequestContext, Sensitivity};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::types::{default_sources, required_sources, ENVELOPE_SERIALIZER};

const KNOWLEDGE_ORDER: [&str; 3] = ["off", "tool", "auto"];
const MEMORY_ORDER: [&str; 3] = ["off", "session", "session_and_long_term"];
pub const DEFAULT_MAX_RESOURCE_BYTES: u64 = 1_000_000;
pub const DEFAULT_RESERVE_TOKENS: u64 = 64;

/// What a source failure does: drop the source (`fail_open`) or fail the build.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailMode {
    FailOpen,
    FailClosed,
}

/// The parts of an agent spec the context policy reads (`spec.context`,
/// `spec.knowledge`, `spec.memory`, `spec.tools`, `spec.prompt`,
/// `spec.security`). A pinned revision supplies one; an invoke whose run has
/// no pinned spec compiles without it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ContextSpec {
    /// `spec.prompt.instructions`.
    #[serde(default)]
    pub instructions: Option<String>,
    #[serde(default)]
    pub prompt_revision: Option<String>,
    /// `spec.context.providers`; empty means the default sources.
    #[serde(default)]
    pub providers: Vec<String>,
    #[serde(default)]
    pub token_budget: Option<u64>,
    #[serde(default)]
    pub max_sensitivity: Option<Sensitivity>,
    #[serde(default)]
    pub source_failure: BTreeMap<String, FailMode>,
    #[serde(default)]
    pub promote_ltm_trust: bool,
    #[serde(default)]
    pub resource_schemes: Vec<String>,
    #[serde(default)]
    pub resource_hosts: Vec<String>,
    #[serde(default)]
    pub max_resource_bytes: Option<u64>,
    /// `spec.knowledge.mode`.
    #[serde(default)]
    pub knowledge_mode: Option<String>,
    #[serde(default)]
    pub collections: Vec<String>,
    #[serde(default)]
    pub knowledge_revision: Option<String>,
    /// `spec.memory.backend`.
    #[serde(default)]
    pub memory_backend: Option<String>,
    #[serde(default)]
    pub memory_revision: Option<String>,
    /// `spec.tools.allowlist`.
    #[serde(default)]
    pub tool_allowlist: Vec<String>,
    #[serde(default)]
    pub tool_revision: Option<String>,
    #[serde(default)]
    pub security_revision: Option<String>,
}

/// A compiled context policy: what a turn's context may hold.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResolvedContextPolicy {
    pub enable: Vec<String>,
    pub required_sources: Vec<String>,
    pub provider_reserve_tokens: u64,
    pub fail_on_overflow: bool,
    pub max_sensitivity: Sensitivity,
    pub token_budget: Option<u64>,
    pub revision: String,
    pub knowledge_mode: String,
    pub memory_mode: String,
    pub collections: Vec<String>,
    pub tool_allowlist: Vec<String>,
    pub prompt_revision: Option<String>,
    pub memory_revision: Option<String>,
    pub knowledge_revision: Option<String>,
    pub tool_revision: Option<String>,
    pub security_revision: Option<String>,
    pub promote_ltm_trust: bool,
    pub source_failure: BTreeMap<String, FailMode>,
    pub resource_schemes: Vec<String>,
    pub resource_hosts: Vec<String>,
    pub max_resource_bytes: u64,
    pub serializer_revision: String,
    pub tokenizer_revision: String,
}

impl Default for ResolvedContextPolicy {
    /// The defaults, with no compiled revision.
    fn default() -> Self {
        Self {
            enable: default_sources(),
            required_sources: required_sources(),
            provider_reserve_tokens: DEFAULT_RESERVE_TOKENS,
            fail_on_overflow: true,
            max_sensitivity: Sensitivity::Restricted,
            token_budget: None,
            revision: String::new(),
            knowledge_mode: "auto".into(),
            memory_mode: "session".into(),
            collections: Vec::new(),
            tool_allowlist: Vec::new(),
            prompt_revision: None,
            memory_revision: None,
            knowledge_revision: None,
            tool_revision: None,
            security_revision: None,
            promote_ltm_trust: false,
            source_failure: BTreeMap::new(),
            resource_schemes: vec!["https".into(), "http".into()],
            resource_hosts: Vec::new(),
            max_resource_bytes: DEFAULT_MAX_RESOURCE_BYTES,
            serializer_revision: ENVELOPE_SERIALIZER.into(),
            tokenizer_revision: beam_gateway::tokens::CONSERVATIVE.into(),
        }
    }
}

impl ResolvedContextPolicy {
    #[must_use]
    pub fn fail_mode(&self, source: &str) -> FailMode {
        self.source_failure
            .get(source)
            .copied()
            .unwrap_or(FailMode::FailOpen)
    }

    #[must_use]
    pub fn enables(&self, source: &str) -> bool {
        self.enable.iter().any(|s| s == source)
    }
}

/// A request may only narrow the compiled mode.
#[must_use]
pub fn resolve_inherit_mode(request_mode: &str, compiled_mode: &str, order: &[&str]) -> String {
    let index = |m: &str| order.iter().position(|o| *o == m);
    if request_mode == "inherit" {
        return if index(compiled_mode).is_some() {
            compiled_mode.to_owned()
        } else {
            order.last().copied().unwrap_or_default().to_owned()
        };
    }
    match (index(request_mode), index(compiled_mode)) {
        (None, _) => compiled_mode.to_owned(),
        (Some(_), None) => request_mode.to_owned(),
        (Some(r), Some(c)) => order[r.min(c)].to_owned(),
    }
}

fn knowledge_label(mode: KnowledgeMode) -> &'static str {
    match mode {
        KnowledgeMode::Inherit => "inherit",
        KnowledgeMode::Off => "off",
        KnowledgeMode::Auto => "auto",
        KnowledgeMode::Tool => "tool",
    }
}

fn memory_label(mode: MemoryMode) -> &'static str {
    match mode {
        MemoryMode::Inherit => "inherit",
        MemoryMode::Off => "off",
        MemoryMode::Session => "session",
        MemoryMode::SessionAndLongTerm => "session_and_long_term",
    }
}

fn sensitivity_label(s: Sensitivity) -> &'static str {
    match s {
        Sensitivity::Public => "public",
        Sensitivity::Internal => "internal",
        Sensitivity::Confidential => "confidential",
        Sensitivity::Restricted => "restricted",
    }
}

fn compiled_knowledge_mode(mode: Option<&str>) -> &str {
    match mode {
        Some(m) if KNOWLEDGE_ORDER.contains(&m) => m,
        _ => "auto",
    }
}

fn compiled_memory_mode(backend: Option<&str>) -> &'static str {
    match backend {
        Some("off" | "none") => "off",
        Some("session_and_long_term" | "long_term" | "ltm") => "session_and_long_term",
        _ => "session",
    }
}

/// Python `compile_context_policy`. The revision is the sha256 of the policy
/// payload as `json.dumps(sort_keys=True, separators=(",", ":"))`.
#[must_use]
pub fn compile_context_policy(
    spec: Option<&ContextSpec>,
    request: Option<&RequestContext>,
    extra_tools: &[String],
) -> ResolvedContextPolicy {
    let default_spec = ContextSpec::default();
    let spec_ref = spec.unwrap_or(&default_spec);
    let mut enable = if spec_ref.providers.is_empty() {
        default_sources()
    } else {
        spec_ref.providers.clone()
    };
    let compiled_knowledge = compiled_knowledge_mode(spec_ref.knowledge_mode.as_deref());
    let compiled_memory = compiled_memory_mode(spec_ref.memory_backend.as_deref());
    let default_request = RequestContext::default();
    let req = request.unwrap_or(&default_request);
    let knowledge_mode = resolve_inherit_mode(
        knowledge_label(req.knowledge_mode),
        compiled_knowledge,
        &KNOWLEDGE_ORDER,
    );
    let memory_mode = resolve_inherit_mode(
        memory_label(req.memory_mode),
        compiled_memory,
        &MEMORY_ORDER,
    );

    let mut tools = if spec_ref.tool_allowlist.is_empty() {
        extra_tools.to_vec()
    } else {
        spec_ref.tool_allowlist.clone()
    };
    if knowledge_mode == "off" || knowledge_mode == "tool" {
        enable.retain(|s| s != "knowledge");
    }
    if knowledge_mode == "tool" && !tools.iter().any(|t| t == "knowledge_search") {
        tools.push("knowledge_search".into());
    }
    if memory_mode == "off" {
        enable.retain(|s| s != "history" && s != "ltm");
    } else if memory_mode == "session" {
        enable.retain(|s| s != "ltm");
    }

    let resource_schemes = if spec_ref.resource_schemes.is_empty() {
        vec!["https".to_owned(), "http".to_owned()]
    } else {
        spec_ref.resource_schemes.clone()
    };
    let max_resource_bytes = spec_ref
        .max_resource_bytes
        .filter(|b| *b > 0)
        .unwrap_or(DEFAULT_MAX_RESOURCE_BYTES);
    let max_sensitivity = spec_ref.max_sensitivity.unwrap_or(Sensitivity::Restricted);
    let payload = json!({
        "enable": enable,
        "knowledge_mode": knowledge_mode,
        "memory_mode": memory_mode,
        "collections": spec_ref.collections,
        "tool_allowlist": tools,
        "token_budget": spec_ref.token_budget,
        "max_sensitivity": sensitivity_label(max_sensitivity),
        "source_failure": spec_ref.source_failure,
        "promote_ltm_trust": spec_ref.promote_ltm_trust,
        "resource_schemes": resource_schemes,
        "resource_hosts": spec_ref.resource_hosts,
        "max_resource_bytes": max_resource_bytes,
        "prompt_revision": spec_ref.prompt_revision,
        "memory_revision": spec_ref.memory_revision,
        "knowledge_revision": spec_ref.knowledge_revision,
        "tool_revision": spec_ref.tool_revision,
        "security_revision": spec_ref.security_revision,
    });
    let revision = format!(
        "{:x}",
        Sha256::digest(pyjson::dumps_sorted_compact(&payload).as_bytes())
    );
    let required = required_sources()
        .into_iter()
        .filter(|s| enable.contains(s))
        .collect();
    ResolvedContextPolicy {
        enable,
        required_sources: required,
        revision,
        knowledge_mode,
        memory_mode,
        collections: spec_ref.collections.clone(),
        tool_allowlist: tools,
        token_budget: spec_ref.token_budget,
        prompt_revision: spec_ref.prompt_revision.clone(),
        memory_revision: spec_ref.memory_revision.clone(),
        knowledge_revision: spec_ref.knowledge_revision.clone(),
        tool_revision: spec_ref.tool_revision.clone(),
        security_revision: spec_ref.security_revision.clone(),
        max_sensitivity,
        promote_ltm_trust: spec_ref.promote_ltm_trust,
        source_failure: spec_ref.source_failure.clone(),
        resource_schemes,
        resource_hosts: spec_ref.resource_hosts.clone(),
        max_resource_bytes,
        ..ResolvedContextPolicy::default()
    }
}
