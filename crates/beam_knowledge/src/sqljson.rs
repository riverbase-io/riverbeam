//! JSON-embedding vector store for Postgres without pgvector (Python
//! `knowledge/stores/sqljson.py`, table `beam_vector_json`): embeddings are
//! JSONB and scored in process, with the same visibility rules.

use async_trait::async_trait;
use beam_store::repo::vector_json::JsonVectorRow;
use beam_store::{Db, PgBeamStore};
use serde_json::Value;
use uuid::Uuid;

use riverbase_core::RiverbaseResult;

use crate::store::{cosine, VectorStore};
use crate::types::{Hit, KnowledgeScope, Namespace, RetrievalFilter, VectorRecord};
use crate::visibility::record_is_visible;

/// Vectors as JSON rows in Postgres, scored in process: an adapter on
/// `db.vector_json()`.
#[derive(Clone)]
pub struct SqlJsonVectorStore {
    db: Db,
}

fn namespace_label(namespace: Namespace) -> &'static str {
    match namespace {
        Namespace::Knowledge => "knowledge",
        Namespace::Memory => "memory",
    }
}

/// Metadata key that carries the scope. The table has no scope column, and
/// without it an owned scope with no owner id would read back as unowned and
/// visible to everyone (Python's JSON store has that hole).
const SCOPE_KEY: &str = "knowledge_scope";

/// Python `_row_to_record`, with the scope read back from the metadata.
fn row_record(row: JsonVectorRow) -> VectorRecord {
    #[allow(clippy::cast_possible_truncation)]
    let embedding = row
        .embedding
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(Value::as_f64)
                .map(|v| v as f32)
                .collect()
        })
        .unwrap_or_default();
    let namespace = if row.namespace == "memory" {
        Namespace::Memory
    } else {
        Namespace::Knowledge
    };
    let mut record = VectorRecord::new(row.id.to_string(), namespace, row.text, embedding);
    record.metadata = row.metadata_json.as_object().cloned().unwrap_or_default();
    record.scope = record
        .metadata
        .remove(SCOPE_KEY)
        .and_then(|v| v.as_str().and_then(KnowledgeScope::parse));
    record.user_id = row.user_id;
    record.agent_id = row.agent_id;
    record.org_id = row.org_id;
    record.document_id = row.document_id;
    record.source = row.source;
    record
}

/// A record as its row, the scope kept in the metadata.
fn record_row(r: &VectorRecord) -> JsonVectorRow {
    let embedding: Vec<Value> = r
        .embedding
        .iter()
        .map(|v| Value::from(f64::from(*v)))
        .collect();
    let mut metadata = r.metadata.clone();
    if let Some(scope) = r.scope {
        metadata.insert(SCOPE_KEY.into(), Value::String(scope.as_str().into()));
    }
    JsonVectorRow {
        id: record_uuid(&r.id),
        namespace: namespace_label(r.namespace).to_owned(),
        text: r.text.clone(),
        embedding: Value::Array(embedding),
        metadata_json: Value::Object(metadata),
        user_id: r.user_id.clone(),
        agent_id: r.agent_id.clone(),
        org_id: r.org_id.clone(),
        document_id: r.document_id.clone(),
        source: r.source.clone(),
    }
}

fn record_uuid(id: &str) -> Uuid {
    Uuid::parse_str(id).unwrap_or_else(|_| Uuid::new_v5(&Uuid::NAMESPACE_OID, id.as_bytes()))
}

fn store_err(e: beam_store::DbError) -> riverbase_core::RiverbaseError {
    e.or(crate::BEM_244)
}

impl SqlJsonVectorStore {
    /// The store on `db`.
    #[must_use]
    pub fn from_db(db: Db) -> Self {
        Self { db }
    }

    /// The store on `store`'s database.
    #[must_use]
    pub fn for_store(store: &PgBeamStore) -> Self {
        Self::from_db(store.db().clone())
    }

    async fn rows(&self, namespace: Namespace) -> RiverbaseResult<Vec<VectorRecord>> {
        let rows = self
            .db
            .vector_json()
            .list(namespace_label(namespace))
            .await
            .map_err(store_err)?;
        Ok(rows.into_iter().map(row_record).collect())
    }
}

#[async_trait]
impl VectorStore for SqlJsonVectorStore {
    async fn upsert(&self, records: Vec<VectorRecord>) -> RiverbaseResult<()> {
        let rows: Vec<JsonVectorRow> = records.iter().map(record_row).collect();
        self.db.vector_json().upsert(&rows).await.map_err(store_err)
    }

    async fn query(
        &self,
        vector: &[f32],
        k: usize,
        filters: Option<&RetrievalFilter>,
        namespace: Namespace,
    ) -> RiverbaseResult<Vec<Hit>> {
        let mut scored: Vec<(VectorRecord, f64)> = self
            .rows(namespace)
            .await?
            .into_iter()
            .filter(|r| filters.is_none_or(|f| record_is_visible(r, Some(f))))
            .map(|r| {
                let score = cosine(vector, &r.embedding);
                (r, score)
            })
            .collect();
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        Ok(scored
            .into_iter()
            .take(k)
            .map(|(r, score)| Hit {
                chunk_index: None,
                ..Hit::from_record(&r, score)
            })
            .collect())
    }

    async fn delete(
        &self,
        ids: Option<&[String]>,
        document_id: Option<&str>,
        namespace: Namespace,
    ) -> RiverbaseResult<usize> {
        let table = self.db.vector_json();
        let namespace = namespace_label(namespace);
        if let Some(ids) = ids.filter(|i| !i.is_empty()) {
            let ids: Vec<Uuid> = ids.iter().map(|i| record_uuid(i)).collect();
            table.delete_ids(namespace, ids).await.map_err(store_err)
        } else if let Some(document) = document_id {
            table
                .delete_document(namespace, document)
                .await
                .map_err(store_err)
        } else {
            Ok(0)
        }
    }

    async fn get(&self, id: &str, namespace: Namespace) -> RiverbaseResult<Option<VectorRecord>> {
        let row = self
            .db
            .vector_json()
            .get(record_uuid(id))
            .await
            .map_err(store_err)?;
        Ok(row.map(row_record).filter(|r| r.namespace == namespace))
    }
}
