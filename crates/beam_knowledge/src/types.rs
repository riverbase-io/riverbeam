use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Namespace {
    Knowledge,
    Memory,
}

impl Default for Namespace {
    fn default() -> Self {
        Self::Knowledge
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct KnowledgeScope {
    pub name: String,
    #[serde(default)]
    pub id: String,
}

impl KnowledgeScope {
    #[must_use]
    pub fn new(name: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            id: id.into(),
        }
    }

    #[must_use]
    pub fn system() -> Self {
        Self::new("system", "")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Chunk {
    pub text: String,
    #[serde(default)]
    pub chunk_index: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_count: Option<usize>,
    #[serde(default)]
    pub metadata: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VectorRecord {
    pub id: String,
    #[serde(default)]
    pub namespace: Namespace,
    pub text: String,
    pub embedding: Vec<f32>,
    #[serde(default)]
    pub metadata: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<KnowledgeScope>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub document_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_index: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_count: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedder_name: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct RetrievalFilter {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<KnowledgeScope>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub document_ids: Option<Vec<String>>,
}

impl RetrievalFilter {
    #[must_use]
    pub fn matches(&self, record: &VectorRecord) -> bool {
        if let Some(scope) = &self.scope {
            if record.scope.as_ref() != Some(scope) {
                return false;
            }
        }
        if let Some(agent_id) = &self.agent_id {
            if record.agent_id.as_deref() != Some(agent_id.as_str()) {
                return false;
            }
        }
        if let Some(user_id) = &self.user_id {
            if record.user_id.as_deref() != Some(user_id.as_str()) {
                return false;
            }
        }
        if let Some(document_ids) = &self.document_ids {
            match &record.document_id {
                Some(doc) if document_ids.iter().any(|d| d == doc) => {}
                _ => return false,
            }
        }
        true
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Hit {
    pub id: String,
    pub text: String,
    pub score: f32,
    #[serde(default)]
    pub metadata: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub document_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_index: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<KnowledgeScope>,
}
