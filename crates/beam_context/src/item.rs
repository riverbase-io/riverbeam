//! The shared `ContextItem` constructor (Python `context/item_factory.py`) and
//! privacy-safe drop decisions (Python `context/decisions.py`).

use std::collections::HashSet;

use beam_gateway::TokenCounter;
use beam_types::{pyjson, ContentPart, Sensitivity};
use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::types::{ContextItem, ContextRole, DecisionAction, ManifestDecision, Trust};

/// Python `make_context_item`'s keyword arguments.
#[derive(Debug, Clone)]
pub struct NewItem {
    pub id: String,
    pub content: String,
    pub role: ContextRole,
    pub source: &'static str,
    pub trust: Trust,
    pub required: bool,
    pub priority: i64,
    pub provenance: Option<String>,
    pub sensitivity: Sensitivity,
    pub tenant_scope: Option<String>,
    pub parts: Vec<ContentPart>,
    pub unit_id: Option<String>,
    pub name: Option<String>,
    pub tool_call_id: Option<String>,
    pub tool_calls: Vec<Value>,
    pub tool_result: Option<Value>,
    pub citation_id: Option<String>,
    pub expiry: Option<DateTime<Utc>>,
    pub memory_id: Option<String>,
    pub memory_type: Option<String>,
    pub confidence: Option<f64>,
    pub consent: Option<bool>,
    /// Count with this model's tokenizer (only the current turn passes one).
    pub model: Option<String>,
}

impl NewItem {
    #[must_use]
    pub fn new(
        id: impl Into<String>,
        content: impl Into<String>,
        role: ContextRole,
        source: &'static str,
        trust: Trust,
    ) -> Self {
        Self {
            id: id.into(),
            content: content.into(),
            role,
            source,
            trust,
            required: false,
            priority: 0,
            provenance: None,
            sensitivity: Sensitivity::Internal,
            tenant_scope: None,
            parts: Vec::new(),
            unit_id: None,
            name: None,
            tool_call_id: None,
            tool_calls: Vec::new(),
            tool_result: None,
            citation_id: None,
            expiry: None,
            memory_id: None,
            memory_type: None,
            confidence: None,
            consent: None,
            model: None,
        }
    }

    /// Count parts (plus `str(tool_calls)`) and build the item.
    #[must_use]
    pub fn build(self, counter: &TokenCounter) -> ContextItem {
        let parts = if self.parts.is_empty() {
            vec![ContentPart::text(self.content.clone())]
        } else {
            self.parts
        };
        let model = self.model.as_deref();
        let mut estimate = counter.count_parts(&parts, model).tokens;
        if !self.tool_calls.is_empty() {
            let text = pyjson::py_repr(&Value::Array(self.tool_calls.clone()));
            estimate += counter.count_text(&text, model).tokens;
        }
        ContextItem {
            content: self.content,
            parts,
            priority: self.priority,
            required: self.required,
            provenance: self.provenance,
            sensitivity: self.sensitivity,
            tenant_scope: self.tenant_scope,
            expiry: self.expiry,
            token_estimate: estimate,
            unit_id: self.unit_id,
            name: self.name,
            tool_call_id: self.tool_call_id,
            tool_calls: self.tool_calls,
            tool_result: self.tool_result,
            citation_id: self.citation_id,
            memory_id: self.memory_id,
            memory_type: self.memory_type,
            confidence: self.confidence,
            consent: self.consent,
            ..ContextItem::new(self.id, self.role, self.source, self.trust)
        }
    }
}

const ZERO_HASH: &str = "0000000000000000";

/// The manifest decision for a source that produced nothing.
#[must_use]
pub fn dropped_source(source: &str, reason: &str) -> ManifestDecision {
    ManifestDecision {
        item_id: format!("source:{source}"),
        source: source.to_owned(),
        action: DecisionAction::Dropped,
        reason: reason.to_owned(),
        tokens: 0,
        content_hash: ZERO_HASH.into(),
        trust: Trust::Untrusted,
    }
}

/// The manifest decision for an item left out.
#[must_use]
pub fn dropped_item(item: &ContextItem, reason: &str) -> ManifestDecision {
    ManifestDecision {
        item_id: item.id.clone(),
        source: item.source.clone(),
        action: DecisionAction::Dropped,
        reason: reason.to_owned(),
        tokens: item.token_estimate,
        content_hash: ZERO_HASH.into(),
        trust: item.trust,
    }
}

/// The manifest decision for a resource that did not resolve.
#[must_use]
pub fn dropped_resource(item_id: &str, reason: &str) -> ManifestDecision {
    ManifestDecision {
        item_id: item_id.to_owned(),
        source: "resources".into(),
        action: DecisionAction::Dropped,
        reason: reason.to_owned(),
        tokens: 0,
        content_hash: ZERO_HASH.into(),
        trust: Trust::Untrusted,
    }
}

/// First decision per `(item_id, reason)`.
#[must_use]
pub fn unique_decisions(rows: Vec<ManifestDecision>) -> Vec<ManifestDecision> {
    let mut seen = HashSet::new();
    rows.into_iter()
        .filter(|row| seen.insert((row.item_id.clone(), row.reason.clone())))
        .collect()
}
