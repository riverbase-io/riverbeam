//! Citations from retrieval hits (Python `knowledge/provenance.py`) and
//! embedding revisions (Python `knowledge/revisions.py`).

use std::collections::HashMap;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::types::Hit;

/// Python `Citation.model_dump(mode="json")` for one hit, or `None` when the
/// hit names no UUID document.
#[must_use]
pub fn citation_from_hit(hit: &Hit) -> Option<Value> {
    let document_id = Uuid::parse_str(hit.document_id.as_deref()?.trim()).ok()?;
    let meta = |key: &str| hit.metadata.get(key).filter(|v| !v.is_null()).cloned();
    let truthy = |v: &Value| {
        !matches!(v, Value::Null | Value::Bool(false)) && v.as_str().is_none_or(|s| !s.is_empty())
    };
    let revision = meta("document_revision")
        .filter(truthy)
        .or_else(|| meta("embedding_revision"));
    Some(json!({
        "document_id": document_id.to_string(),
        "document_revision": revision,
        "chunk_id": hit.id,
        "chunk_index": hit.chunk_index.unwrap_or(0),
        "source": hit.source,
        "title": meta("title"),
        "score": hit.score,
        "rerank_score": meta("rerank_score"),
    }))
}

/// Citations for retrieval hits.
#[must_use]
pub fn citations_from_hits(hits: &[Hit]) -> Vec<Value> {
    hits.iter().filter_map(citation_from_hit).collect()
}

/// Which embedder and settings produced a document's vectors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmbeddingRevision {
    pub name: String,
    pub embedder_name: String,
    pub embedder_version: String,
    pub dimension: usize,
    pub chunker_name: String,
    pub chunker_version: String,
    /// `indexing`, `active` or `retired`.
    #[serde(default = "indexing")]
    pub status: String,
}

fn indexing() -> String {
    "indexing".into()
}

#[derive(Debug, Default)]
struct RevisionRows {
    by_name: HashMap<String, EmbeddingRevision>,
    active: Option<String>,
}

/// Retrieval reads only the active revision,
/// so a new embedder's vectors (another width) never mix with the old ones.
#[derive(Debug, Default)]
pub struct EmbeddingRevisionBook {
    rows: Mutex<RevisionRows>,
}

impl EmbeddingRevisionBook {
    pub fn register(&self, revision: EmbeddingRevision) -> EmbeddingRevision {
        let active = revision.status == "active";
        let name = revision.name.clone();
        if let Ok(mut rows) = self.rows.lock() {
            rows.by_name.insert(name.clone(), revision.clone());
        }
        if active {
            self.activate(&name);
        }
        revision
    }

    /// Make `name` the active revision; the previous one is retired.
    pub fn activate(&self, name: &str) -> Option<EmbeddingRevision> {
        let mut rows = self.rows.lock().ok()?;
        rows.by_name.get(name)?;
        for other in rows.by_name.values_mut() {
            if other.status == "active" && other.name != name {
                other.status = "retired".into();
            }
        }
        let row = rows.by_name.get_mut(name)?;
        row.status = "active".into();
        let row = row.clone();
        rows.active = Some(name.to_owned());
        Some(row)
    }

    #[must_use]
    pub fn get(&self, name: &str) -> Option<EmbeddingRevision> {
        self.rows.lock().ok()?.by_name.get(name).cloned()
    }

    #[must_use]
    pub fn active(&self) -> Option<EmbeddingRevision> {
        let rows = self.rows.lock().ok()?;
        rows.by_name.get(rows.active.as_deref()?).cloned()
    }
}
