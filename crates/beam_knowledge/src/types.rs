//! Knowledge records (Python `knowledge/contract.py` and the kernel enums).

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Namespace {
    #[default]
    Knowledge,
    Memory,
}

/// Who may see a record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KnowledgeScope {
    #[default]
    Global,
    Org,
    Agent,
    User,
}

impl KnowledgeScope {
    /// The Python value (`str(scope)`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Global => "global",
            Self::Org => "org",
            Self::Agent => "agent",
            Self::User => "user",
        }
    }

    /// The `knowledgescope` enum label (the Python member name).
    #[must_use]
    pub fn db_label(self) -> &'static str {
        match self {
            Self::Global => "GLOBAL",
            Self::Org => "ORG",
            Self::Agent => "AGENT",
            Self::User => "USER",
        }
    }

    /// From a Python value or member name.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "global" => Some(Self::Global),
            "org" => Some(Self::Org),
            "agent" => Some(Self::Agent),
            "user" => Some(Self::User),
            _ => None,
        }
    }
}

/// The kind of a long-term memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MemoryType {
    #[default]
    Semantic,
    Preference,
    Episodic,
}

impl MemoryType {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Semantic => "semantic",
            Self::Preference => "preference",
            Self::Episodic => "episodic",
        }
    }

    #[must_use]
    pub fn db_label(self) -> &'static str {
        match self {
            Self::Semantic => "SEMANTIC",
            Self::Preference => "PREFERENCE",
            Self::Episodic => "EPISODIC",
        }
    }

    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "semantic" => Some(Self::Semantic),
            "preference" => Some(Self::Preference),
            "episodic" => Some(Self::Episodic),
            _ => None,
        }
    }
}

/// `PENDING → INDEXING → INDEXED | FAILED`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DocumentStatus {
    #[default]
    Pending,
    Indexing,
    Indexed,
    Failed,
}

impl DocumentStatus {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Indexing => "indexing",
            Self::Indexed => "indexed",
            Self::Failed => "failed",
        }
    }

    #[must_use]
    pub fn db_label(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Indexing => "INDEXING",
            Self::Indexed => "INDEXED",
            Self::Failed => "FAILED",
        }
    }

    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "pending" => Some(Self::Pending),
            "indexing" => Some(Self::Indexing),
            "indexed" => Some(Self::Indexed),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

/// One logical block before chunking.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ParsedBlock {
    pub text: String,
    #[serde(default)]
    pub metadata: Map<String, Value>,
}

impl ParsedBlock {
    #[must_use]
    pub fn new(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            metadata: Map::new(),
        }
    }
}

/// One chunk of a document, with its position and text.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Chunk {
    pub text: String,
    #[serde(default)]
    pub chunk_index: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_count: Option<usize>,
    #[serde(default)]
    pub metadata: Map<String, Value>,
}

/// A row for a [`crate::VectorStore`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VectorRecord {
    pub id: String,
    #[serde(default)]
    pub namespace: Namespace,
    pub text: String,
    pub embedding: Vec<f32>,
    #[serde(default)]
    pub metadata: Map<String, Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<KnowledgeScope>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub org_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub document_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_index: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_count: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedder_name: Option<String>,
}

impl VectorRecord {
    /// A record with no scope, owners or provenance.
    #[must_use]
    pub fn new(
        id: impl Into<String>,
        namespace: Namespace,
        text: impl Into<String>,
        embedding: Vec<f32>,
    ) -> Self {
        Self {
            id: id.into(),
            namespace,
            text: text.into(),
            embedding,
            metadata: Map::new(),
            scope: None,
            agent_id: None,
            user_id: None,
            org_id: None,
            document_id: None,
            chunk_index: None,
            source: None,
            content_hash: None,
            token_count: None,
            embedder_name: None,
        }
    }
}

/// The ACL and scope filter applied at query time.
/// `extra` keys must all match the record's metadata.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RetrievalFilter {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<KnowledgeScope>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scopes: Option<Vec<KnowledgeScope>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub org_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub document_ids: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_types: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// One retrieval result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Hit {
    pub id: String,
    pub text: String,
    pub score: f64,
    #[serde(default)]
    pub metadata: Map<String, Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub document_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_index: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<KnowledgeScope>,
}

impl Hit {
    /// A hit for `record` with `score`.
    #[must_use]
    pub fn from_record(record: &VectorRecord, score: f64) -> Self {
        Self {
            id: record.id.clone(),
            text: record.text.clone(),
            score,
            metadata: record.metadata.clone(),
            document_id: record.document_id.clone(),
            chunk_index: record.chunk_index,
            source: record.source.clone(),
            scope: record.scope,
        }
    }
}
