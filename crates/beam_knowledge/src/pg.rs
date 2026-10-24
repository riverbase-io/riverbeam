//! Postgres + pgvector [`VectorStore`] and document rows (Python
//! `knowledge/stores/pgvector.py` and `knowledge/stores/sql.py`). Behind the
//! `postgres` feature.
//!
//! The tables are fluvius-beam's (`agent_knowledge_document`,
//! `agent_knowledge_chunk`, `agent_memory`), created by `beam_store::migrate`;
//! this module never runs DDL. The `knowledge` namespace maps to
//! `agent_knowledge_chunk`, `memory` to `agent_memory`.

use async_trait::async_trait;
use beam_store::repo::knowledge::{
    ChunkFilter, ChunkWrite, DocumentRecord, Fingerprint, MemoryFilter, MemoryWrite,
};
use beam_store::{Db, PgBeamStore};
use chrono::{DateTime, Utc};
use serde_json::{json, Map, Value};
use uuid::Uuid;

use riverbase_core::{RiverbaseError, RiverbaseResult};

use crate::builder::{DocumentRecords, DocumentRow};
use crate::store::VectorStore;
use crate::types::{
    DocumentStatus, Hit, KnowledgeScope, MemoryType, Namespace, RetrievalFilter, VectorRecord,
};

/// Cosine-similarity ANN store on `<schema>.agent_knowledge_chunk` /
/// `agent_memory`: an adapter on `db.knowledge()`.
#[derive(Clone)]
pub struct PgVectorStore {
    db: Db,
}

impl PgVectorStore {
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

    /// Connect and check that pgvector and the migrated knowledge tables exist
    /// in the beam schema. Returns `Ok(None)` when they do not.
    ///
    /// # Errors
    /// Returns a store error if the pool cannot be established.
    pub async fn connect(database_url: &str) -> RiverbaseResult<Option<Self>> {
        let db = Db::connect(database_url)
            .await
            .map_err(|e| crate::BEM_203.with_data(e.to_string()))?;
        if db
            .knowledge_ready()
            .await
            .map_err(|e| e.or(crate::BEM_205))?
        {
            Ok(Some(Self::from_db(db)))
        } else {
            Ok(None)
        }
    }

    /// The database the store is on.
    #[must_use]
    pub fn db(&self) -> &Db {
        &self.db
    }
}

fn vector_literal(values: &[f32]) -> String {
    let inner = values
        .iter()
        .map(|v| {
            if v.is_finite() {
                format!("{v}")
            } else {
                "0".into()
            }
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("[{inner}]")
}

/// pgvector's text form back to floats.
fn parse_vector(text: &str) -> Vec<f32> {
    text.trim_matches(|c| c == '[' || c == ']')
        .split(',')
        .filter_map(|v| v.trim().parse().ok())
        .collect()
}

fn record_uuid(id: &str) -> Uuid {
    Uuid::parse_str(id).unwrap_or_else(|_| Uuid::new_v5(&Uuid::NAMESPACE_OID, id.as_bytes()))
}

/// An owner or document id for a `uuid` column. A value that is not a UUID is
/// refused (Python's insert fails too), so ownership is never silently dropped.
fn owner_uuid(value: Option<&str>) -> RiverbaseResult<Option<Uuid>> {
    match value.map(str::trim).filter(|v| !v.is_empty()) {
        None => Ok(None),
        Some(v) => Uuid::parse_str(v)
            .map(Some)
            .map_err(|_| crate::BEM_240.with_data(v.to_owned())),
    }
}

/// A non-UUID org is no org.
fn coerce_org(value: Option<&str>) -> Option<Uuid> {
    value.and_then(|v| Uuid::parse_str(v.trim()).ok())
}

fn embedder_meta(record: &VectorRecord) -> Map<String, Value> {
    let mut metadata = record.metadata.clone();
    if let Some(name) = &record.embedder_name {
        metadata.insert("embedder_name".into(), Value::String(name.clone()));
    }
    metadata
}

fn distance_to_score(distance: Option<f64>) -> f64 {
    distance.map_or(0.0, |d| 1.0 - d)
}

fn store_err(e: beam_store::DbError) -> RiverbaseError {
    e.or(crate::BEM_243)
}

fn memory_type_value(label: &str) -> String {
    MemoryType::parse(label).map_or_else(|| label.to_lowercase(), |t| t.as_str().to_owned())
}

/// `metadata.expires_at` (an ISO string) as a timestamp.
fn parse_expiry(value: Option<&Value>) -> Option<DateTime<Utc>> {
    let raw = value?.as_str()?;
    DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|t| t.with_timezone(&Utc))
}

/// A knowledge record as the chunk it writes.
fn chunk_write(r: &VectorRecord) -> RiverbaseResult<ChunkWrite> {
    let document_id =
        owner_uuid(r.document_id.as_deref())?.ok_or_else(|| crate::BEM_207.raise())?;
    let revision = r
        .metadata
        .get("embedding_revision")
        .and_then(Value::as_str)
        .map(str::to_owned);
    Ok(ChunkWrite {
        id: record_uuid(&r.id),
        document_id,
        scope: r.scope.map(|s| s.db_label().to_owned()),
        agent_id: owner_uuid(r.agent_id.as_deref())?,
        user_id: owner_uuid(r.user_id.as_deref())?,
        org_id: coerce_org(r.org_id.as_deref()),
        source: r.source.clone(),
        chunk_index: i32::try_from(r.chunk_index.unwrap_or(0)).unwrap_or(i32::MAX),
        token_count: r.token_count.and_then(|t| i32::try_from(t).ok()),
        content_hash: r.content_hash.clone(),
        content: r.text.clone(),
        embedding: vector_literal(&r.embedding),
        metadata: Value::Object(embedder_meta(r)),
        embedding_revision: revision,
    })
}

/// A memory record as the row it writes: `type`, `importance` and
/// `expires_at` move from the metadata to their columns.
fn memory_write(r: &VectorRecord) -> RiverbaseResult<MemoryWrite> {
    let mut metadata = r.metadata.clone();
    let memory_type = metadata
        .remove("type")
        .and_then(|v| v.as_str().and_then(MemoryType::parse))
        .unwrap_or_default();
    let importance = metadata
        .remove("importance")
        .and_then(|v| v.as_i64())
        .unwrap_or(1);
    let expires_at = parse_expiry(metadata.remove("expires_at").as_ref());
    let version = metadata.get("version").and_then(Value::as_i64).unwrap_or(1);
    let confidence = metadata.get("confidence").and_then(Value::as_f64);
    let user_id = owner_uuid(r.user_id.as_deref())?
        .ok_or_else(|| crate::BEM_240.with_data("memory user_id"))?;
    Ok(MemoryWrite {
        id: record_uuid(&r.id),
        agent_id: owner_uuid(r.agent_id.as_deref())?,
        user_id,
        org_id: coerce_org(r.org_id.as_deref()),
        memory_type: memory_type.db_label().to_owned(),
        content: r.text.clone(),
        embedding: vector_literal(&r.embedding),
        embedder_name: r.embedder_name.clone(),
        metadata: Value::Object(metadata),
        importance: i32::try_from(importance).unwrap_or(1),
        expires_at,
        version: i32::try_from(version).unwrap_or(1),
        confidence,
    })
}

/// `records` split into chunk and memory writes.
fn writes(records: &[VectorRecord]) -> RiverbaseResult<(Vec<ChunkWrite>, Vec<MemoryWrite>)> {
    let (mut chunks, mut memories) = (Vec::new(), Vec::new());
    for r in records {
        match r.namespace {
            Namespace::Knowledge => chunks.push(chunk_write(r)?),
            Namespace::Memory => memories.push(memory_write(r)?),
        }
    }
    Ok((chunks, memories))
}

impl PgVectorStore {
    /// Python `_query_knowledge` with `_apply_knowledge_filters`.
    async fn query_knowledge(
        &self,
        vector: &[f32],
        k: usize,
        filters: Option<&RetrievalFilter>,
    ) -> RiverbaseResult<Vec<Hit>> {
        let f = filters.cloned().unwrap_or_default();
        let filter = ChunkFilter {
            filtered: filters.is_some(),
            scope: f.scope.map(|s| s.db_label().to_owned()),
            scopes: f
                .scopes
                .as_ref()
                .filter(|s| !s.is_empty())
                .map(|s| s.iter().map(|x| x.db_label().to_owned()).collect()),
            agent_id: owner_uuid(f.agent_id.as_deref()).ok().flatten(),
            user_id: owner_uuid(f.user_id.as_deref()).ok().flatten(),
            org_id: coerce_org(f.org_id.as_deref()),
            document_ids: f
                .document_ids
                .as_ref()
                .filter(|d| !d.is_empty())
                .map(|ids| ids.iter().filter_map(|d| Uuid::parse_str(d).ok()).collect()),
            metadata: (!f.extra.is_empty()).then(|| Value::Object(f.extra.clone())),
        };
        let rows = self
            .db
            .knowledge()
            .search_chunks(
                &vector_literal(vector),
                i64::try_from(k).unwrap_or(i64::MAX),
                &filter,
            )
            .await
            .map_err(store_err)?;
        Ok(rows
            .into_iter()
            .map(|row| Hit {
                id: row.id.to_string(),
                text: row.content,
                score: distance_to_score(row.distance),
                metadata: row.metadata_json.as_object().cloned().unwrap_or_default(),
                document_id: row.document_id.map(|d| d.to_string()),
                chunk_index: usize::try_from(row.chunk_index).ok(),
                source: row.source,
                scope: row.scope.as_deref().and_then(KnowledgeScope::parse),
            })
            .collect())
    }

    /// Unexpired rows, then `last_accessed_at` stamped.
    /// An agent filter also keeps rows without an agent (user-wide memories,
    /// as `record_is_visible` does); Python keeps only that agent's rows.
    async fn query_memory(
        &self,
        vector: &[f32],
        k: usize,
        filters: Option<&RetrievalFilter>,
    ) -> RiverbaseResult<Vec<Hit>> {
        let f = filters.cloned().unwrap_or_default();
        // A supplied owner that is not a UUID matches no row. It must never
        // drop the filter (Python's query fails on it, which is closed too).
        let strict = |v: Option<&str>| match v {
            None => Ok(None),
            Some(id) => Uuid::parse_str(id.trim()).map(Some),
        };
        let (Ok(agent), Ok(user)) = (strict(f.agent_id.as_deref()), strict(f.user_id.as_deref()))
        else {
            return Ok(Vec::new());
        };
        let types: Option<Vec<String>> =
            f.memory_types.as_ref().filter(|t| !t.is_empty()).map(|t| {
                t.iter()
                    .map(|x| {
                        MemoryType::parse(x)
                            .map_or_else(|| x.to_uppercase(), |m| m.db_label().to_owned())
                    })
                    .collect()
            });
        let filter = MemoryFilter {
            agent_id: agent,
            user_id: user,
            types,
        };
        let rows = self
            .db
            .knowledge()
            .search_memories(
                &vector_literal(vector),
                i64::try_from(k).unwrap_or(i64::MAX),
                &filter,
            )
            .await
            .map_err(store_err)?;
        Ok(rows
            .into_iter()
            .map(|row| {
                let mut metadata = row.metadata_json.as_object().cloned().unwrap_or_default();
                metadata.insert(
                    "type".into(),
                    Value::String(memory_type_value(&row.memory_type)),
                );
                metadata.insert("importance".into(), Value::from(row.importance));
                metadata.insert(
                    "agent_id".into(),
                    row.agent_id
                        .map_or(Value::Null, |a| Value::String(a.to_string())),
                );
                metadata.insert("user_id".into(), Value::String(row.user_id.to_string()));
                Hit {
                    id: row.id.to_string(),
                    text: row.content,
                    score: distance_to_score(row.distance),
                    metadata,
                    document_id: None,
                    chunk_index: None,
                    source: None,
                    scope: None,
                }
            })
            .collect())
    }
}

#[async_trait]
impl VectorStore for PgVectorStore {
    async fn upsert(&self, records: Vec<VectorRecord>) -> RiverbaseResult<()> {
        if records.is_empty() {
            return Ok(());
        }
        let (chunks, memories) = writes(&records)?;
        self.db
            .knowledge()
            .upsert(chunks, memories)
            .await
            .map_err(store_err)
    }

    async fn query(
        &self,
        vector: &[f32],
        k: usize,
        filters: Option<&RetrievalFilter>,
        namespace: Namespace,
    ) -> RiverbaseResult<Vec<Hit>> {
        match namespace {
            Namespace::Knowledge => self.query_knowledge(vector, k, filters).await,
            Namespace::Memory => self.query_memory(vector, k, filters).await,
        }
    }

    async fn delete(
        &self,
        ids: Option<&[String]>,
        document_id: Option<&str>,
        namespace: Namespace,
    ) -> RiverbaseResult<usize> {
        let knowledge = self.db.knowledge();
        if let Some(ids) = ids.filter(|i| !i.is_empty()) {
            let ids: Vec<Uuid> = ids.iter().map(|i| record_uuid(i)).collect();
            knowledge
                .delete_ids(namespace == Namespace::Memory, ids)
                .await
                .map_err(store_err)
        } else if let (Some(document), Namespace::Knowledge) = (document_id, namespace) {
            let Some(document) = owner_uuid(Some(document))? else {
                return Ok(0);
            };
            knowledge
                .delete_document_chunks(document)
                .await
                .map_err(store_err)
        } else {
            Ok(0)
        }
    }

    async fn replace_document_chunks(
        &self,
        document_id: &str,
        records: Vec<VectorRecord>,
        namespace: Namespace,
    ) -> RiverbaseResult<usize> {
        // Only knowledge chunks belong to a document (a malformed id is
        // `BEM-240`); every record is written as `namespace`'s.
        let document = match namespace {
            Namespace::Knowledge => owner_uuid(Some(document_id))?,
            Namespace::Memory => None,
        };
        let (chunks, memories) = match namespace {
            Namespace::Knowledge => (
                records
                    .iter()
                    .map(chunk_write)
                    .collect::<RiverbaseResult<_>>()?,
                Vec::new(),
            ),
            Namespace::Memory => (
                Vec::new(),
                records
                    .iter()
                    .map(memory_write)
                    .collect::<RiverbaseResult<_>>()?,
            ),
        };
        self.db
            .knowledge()
            .replace_document(document, chunks, memories)
            .await
            .map_err(store_err)
    }

    async fn get(&self, id: &str, namespace: Namespace) -> RiverbaseResult<Option<VectorRecord>> {
        let id_uuid = record_uuid(id);
        match namespace {
            Namespace::Memory => {
                let row = self
                    .db
                    .knowledge()
                    .get_memory(id_uuid)
                    .await
                    .map_err(store_err)?;
                Ok(row.map(|row| {
                    // The columns fill missing metadata keys.
                    let mut md = row.metadata_json.as_object().cloned().unwrap_or_default();
                    md.entry("type")
                        .or_insert_with(|| Value::String(memory_type_value(&row.memory_type)));
                    md.entry("importance")
                        .or_insert_with(|| Value::from(row.importance));
                    if let Some(expires) = row.expires_at {
                        md.entry("expires_at")
                            .or_insert_with(|| Value::String(crate::longterm::iso(expires)));
                    }
                    if let Some(confidence) = row.confidence {
                        md.entry("confidence")
                            .or_insert_with(|| Value::from(confidence));
                    }
                    let mut record = VectorRecord::new(
                        row.id.to_string(),
                        Namespace::Memory,
                        row.content,
                        row.embedding
                            .as_deref()
                            .map(parse_vector)
                            .unwrap_or_default(),
                    );
                    record.metadata = md;
                    record.user_id = Some(row.user_id.to_string());
                    record.agent_id = row.agent_id.map(|a| a.to_string());
                    record.org_id = row.org_id.map(|o| o.to_string());
                    record.embedder_name = row.embedder_name;
                    record
                }))
            }
            Namespace::Knowledge => {
                let row = self
                    .db
                    .knowledge()
                    .get_chunk(id_uuid)
                    .await
                    .map_err(store_err)?;
                Ok(row.map(|row| {
                    let mut record = VectorRecord::new(
                        row.id.to_string(),
                        Namespace::Knowledge,
                        row.content,
                        parse_vector(&row.embedding),
                    );
                    record.metadata = row.metadata_json.as_object().cloned().unwrap_or_default();
                    record.document_id = Some(row.document_id.to_string());
                    record.chunk_index = usize::try_from(row.chunk_index).ok();
                    record.source = row.source;
                    record.scope = row.scope.as_deref().and_then(KnowledgeScope::parse);
                    record.user_id = row.user_id.map(|u| u.to_string());
                    record.agent_id = row.agent_id.map(|a| a.to_string());
                    record.org_id = row.org_id.map(|o| o.to_string());
                    record
                }))
            }
        }
    }
}

/// Python `KnowledgeRecords` on `<schema>.agent_knowledge_document`: an
/// adapter on `db.knowledge()`.
#[derive(Clone)]
pub struct PgDocumentRecords {
    db: Db,
}

impl PgDocumentRecords {
    /// The records on `db`.
    #[must_use]
    pub fn from_db(db: Db) -> Self {
        Self { db }
    }

    /// The records on the same database as `store`.
    #[must_use]
    pub fn for_store(store: &PgVectorStore) -> Self {
        Self::from_db(store.db.clone())
    }
}

impl From<DocumentRecord> for DocumentRow {
    fn from(r: DocumentRecord) -> Self {
        Self {
            id: r.id.to_string(),
            title: r.title,
            scope: r
                .scope
                .as_deref()
                .and_then(KnowledgeScope::parse)
                .unwrap_or_default(),
            doc_type: r.doc_type,
            agent_id: r.agent_id.map(|a| a.to_string()),
            user_id: r.user_id.map(|u| u.to_string()),
            org_id: r.org_id.map(|o| o.to_string()),
            source: r.source,
            body: r.body,
            media_id: r.media_id.map(|m| m.to_string()),
            metadata: r.metadata_json.as_object().cloned().unwrap_or_default(),
            status: DocumentStatus::parse(&r.status).unwrap_or_default(),
            content_hash: r.content_hash,
            chunk_count: usize::try_from(r.chunk_count).unwrap_or(0),
            embedder_name: r.embedder_name,
        }
    }
}

#[async_trait]
impl DocumentRecords for PgDocumentRecords {
    async fn upsert_document(&self, d: &DocumentRow) -> RiverbaseResult<()> {
        let id = owner_uuid(Some(&d.id))?.ok_or_else(|| crate::BEM_240.with_data("document id"))?;
        // Every column, as the `ON CONFLICT … DO UPDATE` set them.
        let data = json!({
            "scope": d.scope.db_label(),
            "agent_id": owner_uuid(d.agent_id.as_deref())?,
            "user_id": owner_uuid(d.user_id.as_deref())?,
            "org_id": coerce_org(d.org_id.as_deref()),
            "title": d.title,
            "source": d.source,
            "type": d.doc_type,
            "body": d.body,
            "media_id": d.media_id.as_deref().and_then(|m| Uuid::parse_str(m).ok()),
            "metadata_json": Value::Object(d.metadata.clone()),
            "status": d.status.db_label(),
            "content_hash": d.content_hash,
            "chunk_count": i32::try_from(d.chunk_count).unwrap_or(i32::MAX),
            "embedder_name": d.embedder_name,
        });
        self.db
            .knowledge()
            .upsert_document(id, &data)
            .await
            .map_err(|e| e.or(crate::BEM_208))
    }

    async fn find_by_fingerprint(
        &self,
        scope: KnowledgeScope,
        agent_id: Option<&str>,
        user_id: Option<&str>,
        org_id: Option<&str>,
        content_hash: &str,
    ) -> RiverbaseResult<Option<DocumentRow>> {
        let fingerprint = Fingerprint {
            scope: scope.db_label(),
            preferred_status: DocumentStatus::Indexed.db_label(),
            agent_id: owner_uuid(agent_id)?,
            user_id: owner_uuid(user_id)?,
            org_id: coerce_org(org_id),
            content_hash,
        };
        let row = self
            .db
            .knowledge()
            .find_by_fingerprint(fingerprint)
            .await
            .map_err(|e| e.or(crate::BEM_241))?;
        Ok(row.map(DocumentRow::from))
    }

    async fn find(
        &self,
        document_id: &str,
        content_hash: &str,
    ) -> RiverbaseResult<Option<DocumentRow>> {
        let Some(id) = owner_uuid(Some(document_id))? else {
            return Ok(None);
        };
        let row = self
            .db
            .knowledge()
            .find_document(id, content_hash)
            .await
            .map_err(|e| e.or(crate::BEM_241))?;
        Ok(row.map(DocumentRow::from))
    }

    async fn delete_document(&self, document_id: &str) -> RiverbaseResult<()> {
        let Some(id) = owner_uuid(Some(document_id))? else {
            return Ok(());
        };
        self.db
            .knowledge()
            .delete_document(id)
            .await
            .map_err(|e| e.or(crate::BEM_242))
    }

    async fn fail_stuck_indexing(&self, minutes: i64) -> RiverbaseResult<u64> {
        // The app clock's cutoff (plan 015 D4), clamped as the SQL's `i32`
        // minutes were.
        let cutoff = Utc::now()
            - chrono::Duration::minutes(minutes.clamp(i64::from(i32::MIN), i64::from(i32::MAX)));
        self.db
            .knowledge()
            .fail_stuck(
                DocumentStatus::Indexing.db_label(),
                DocumentStatus::Failed.db_label(),
                cutoff,
            )
            .await
            .map_err(|e| e.or(crate::BEM_208))
    }
}
