//! Postgres + pgvector [`VectorStore`]. Behind the `postgres` feature.

use async_trait::async_trait;
use diesel::sql_query;
use diesel::sql_types::{Float4, Integer, Jsonb, Nullable, Text, Uuid as SqlUuid};
use diesel::QueryableByName;
use diesel_async::{AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::Value;
use uuid::Uuid;

use riverbase_core::datastore::{establish_dbpool, PgPool};

use riverbase_core::RiverbaseResult;
use crate::store::VectorStore;
use crate::types::{Hit, KnowledgeScope, Namespace, RetrievalFilter, VectorRecord};

const KNOWLEDGE_DDL: &str = r"
CREATE SCHEMA IF NOT EXISTS ref_beam;

CREATE TABLE IF NOT EXISTS ref_beam.agent_knowledge_document (
    _id           UUID PRIMARY KEY,
    scope         TEXT,
    agent_id      UUID,
    user_id       UUID,
    title         TEXT NOT NULL,
    source        TEXT,
    type          TEXT NOT NULL,
    body          TEXT,
    media_id      UUID,
    status        TEXT NOT NULL DEFAULT 'pending',
    embedder_name TEXT,
    chunk_count   INTEGER NOT NULL DEFAULT 0,
    content_hash  VARCHAR(64),
    metadata_json JSONB NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS ref_beam.agent_knowledge_chunk (
    _id          UUID PRIMARY KEY,
    document_id  UUID NOT NULL REFERENCES ref_beam.agent_knowledge_document (_id),
    scope        TEXT,
    agent_id     UUID,
    user_id      UUID,
    source       TEXT,
    chunk_index  INTEGER NOT NULL DEFAULT 0,
    token_count  INTEGER,
    content_hash VARCHAR(64),
    content      TEXT NOT NULL,
    embedding    vector(1536) NOT NULL,
    metadata_json JSONB NOT NULL DEFAULT '{}',
    CONSTRAINT uq_agent_knowledge_chunk_doc_idx UNIQUE (document_id, chunk_index)
);
CREATE INDEX IF NOT EXISTS ix_agent_knowledge_chunk_embedding_hnsw
    ON ref_beam.agent_knowledge_chunk USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

CREATE TABLE IF NOT EXISTS ref_beam.agent_memory (
    _id            UUID PRIMARY KEY,
    user_id        UUID NOT NULL,
    agent_id       UUID,
    type           TEXT NOT NULL DEFAULT 'semantic',
    content        TEXT NOT NULL,
    embedding      vector(1536),
    embedder_name  TEXT,
    metadata_json  JSONB NOT NULL DEFAULT '{}',
    importance     INTEGER NOT NULL DEFAULT 1,
    last_accessed_at TIMESTAMPTZ,
    expires_at     TIMESTAMPTZ,
    version        INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS ix_agent_memory_embedding_hnsw
    ON ref_beam.agent_memory USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64)
    WHERE embedding IS NOT NULL;
";

/// Cosine-similarity ANN store on `ref_beam.agent_knowledge_chunk` / `agent_memory`.
#[derive(Clone)]
pub struct PgVectorStore {
    pool: PgPool,
}

impl PgVectorStore {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Connect, try to enable pgvector, apply knowledge DDL.
    ///
    /// # Errors
    /// Returns a store error if the pool cannot be established.
    pub async fn connect(database_url: &str) -> RiverbaseResult<Option<Self>> {
        let pool = establish_dbpool(database_url)
            .await
            .map_err(|e| crate::BEM_208.with_data(e.to_string()))?;
        if ensure_knowledge_schema(&pool).await? {
            Ok(Some(Self::new(pool)))
        } else {
            Ok(None)
        }
    }

    async fn conn(
        &self,
    ) -> RiverbaseResult<impl std::ops::DerefMut<Target = AsyncPgConnection> + Send + '_>
    {
        self.pool
            .get()
            .await
            .map_err(|e| crate::BEM_204.with_data(e.to_string()))
    }
}

/// `CREATE EXTENSION vector` then knowledge/memory tables. `Ok(false)` if the
/// extension is unavailable (stock Postgres).
///
/// # Errors
/// Returns a store error if a connection cannot be taken or DDL
/// after a successful extension create fails.
pub async fn ensure_knowledge_schema(pool: &PgPool) -> RiverbaseResult<bool> {
    let mut guard = pool
        .get()
        .await
        .map_err(|e| crate::BEM_205.with_data(e.to_string()))?;
    let conn: &mut AsyncPgConnection = &mut guard;
    if conn
        .batch_execute("CREATE EXTENSION IF NOT EXISTS vector")
        .await
        .is_err()
    {
        tracing::warn!("pgvector extension unavailable; knowledge will use in-memory store");
        return Ok(false);
    }
    conn.batch_execute(KNOWLEDGE_DDL)
        .await
        .map_err(|e| crate::BEM_206.with_data(e.to_string()))?;
    Ok(true)
}

fn vector_literal(values: &[f32]) -> String {
    let vector = pgvector::Vector::from(values.to_vec());
    let inner = vector
        .to_vec()
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

fn record_uuid(record: &VectorRecord) -> Uuid {
    Uuid::parse_str(&record.id).unwrap_or_else(|_| Uuid::new_v5(&Uuid::NAMESPACE_OID, record.id.as_bytes()))
}

fn document_uuid(record: &VectorRecord) -> RiverbaseResult<Uuid> {
    record
        .document_id
        .as_deref()
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(|| crate::BEM_207.raise())
}

fn opt_uuid(value: Option<&str>) -> Option<Uuid> {
    value.and_then(|v| Uuid::parse_str(v).ok())
}

fn scope_text(scope: Option<&KnowledgeScope>) -> Option<String> {
    scope.map(|s| {
        if s.id.is_empty() {
            s.name.clone()
        } else {
            format!("{}:{}", s.name, s.id)
        }
    })
}

fn scope_from_db(value: Option<String>) -> Option<KnowledgeScope> {
    value.map(|raw| {
        if let Some((name, id)) = raw.split_once(':') {
            KnowledgeScope::new(name, id)
        } else {
            KnowledgeScope::new(raw, "")
        }
    })
}

#[derive(QueryableByName)]
struct ChunkHitRow {
    #[diesel(sql_type = SqlUuid)]
    id: Uuid,
    #[diesel(sql_type = Nullable<SqlUuid>)]
    document_id: Option<Uuid>,
    #[diesel(sql_type = Nullable<Text>)]
    scope: Option<String>,
    #[diesel(sql_type = Integer)]
    chunk_index: i32,
    #[diesel(sql_type = Nullable<Text>)]
    source: Option<String>,
    #[diesel(sql_type = Text)]
    content: String,
    #[diesel(sql_type = Jsonb)]
    metadata_json: Value,
    #[diesel(sql_type = Float4)]
    distance: f32,
}

#[derive(QueryableByName)]
struct MemoryHitRow {
    #[diesel(sql_type = SqlUuid)]
    id: Uuid,
    #[diesel(sql_type = Text)]
    content: String,
    #[diesel(sql_type = Jsonb)]
    metadata_json: Value,
    #[diesel(sql_type = Float4)]
    distance: f32,
}

fn distance_to_score(distance: f32) -> f32 {
    1.0 - distance
}

#[async_trait]
impl VectorStore for PgVectorStore {
    async fn upsert(&self, records: Vec<VectorRecord>) -> RiverbaseResult<()> {
        if records.is_empty() {
            return Ok(());
        }
        let mut guard = self.conn().await?;
        let conn: &mut AsyncPgConnection = &mut guard;
        for record in records {
            match record.namespace {
                Namespace::Knowledge => upsert_knowledge(conn, &record).await?,
                Namespace::Memory => upsert_memory(conn, &record).await?,
            }
        }
        Ok(())
    }

    async fn query(
        &self,
        vector: &[f32],
        k: usize,
        filters: Option<&RetrievalFilter>,
        namespace: Namespace,
    ) -> RiverbaseResult<Vec<Hit>> {
        let mut guard = self.conn().await?;
        let conn: &mut AsyncPgConnection = &mut guard;
        match namespace {
            Namespace::Knowledge => query_knowledge(conn, vector, k, filters).await,
            Namespace::Memory => query_memory(conn, vector, k, filters).await,
        }
    }

    async fn delete(
        &self,
        ids: Option<&[String]>,
        document_id: Option<&str>,
        namespace: Namespace,
    ) -> RiverbaseResult<usize> {
        let mut guard = self.conn().await?;
        let conn: &mut AsyncPgConnection = &mut guard;
        match namespace {
            Namespace::Knowledge => delete_knowledge(conn, ids, document_id).await,
            Namespace::Memory => delete_memory(conn, ids).await,
        }
    }

    async fn replace_document_chunks(
        &self,
        document_id: &str,
        records: Vec<VectorRecord>,
        namespace: Namespace,
    ) -> RiverbaseResult<usize> {
        if namespace != Namespace::Knowledge {
            return self.delete(None, Some(document_id), namespace).await;
        }
        let mut guard = self.conn().await?;
        let conn: &mut AsyncPgConnection = &mut guard;
        let deleted = delete_knowledge(conn, None, Some(document_id)).await?;
        for record in records {
            upsert_knowledge(conn, &record).await?;
        }
        Ok(deleted)
    }
}

async fn upsert_knowledge(
    conn: &mut AsyncPgConnection,
    record: &VectorRecord,
) -> RiverbaseResult<()> {
    let document_id = document_uuid(record)?;
    let title = record
        .source
        .clone()
        .unwrap_or_else(|| "ingest".into());
    let embedder = record.embedder_name.clone().unwrap_or_default();
    sql_query(
        "INSERT INTO ref_beam.agent_knowledge_document \
         (_id, scope, title, source, type, status, embedder_name, chunk_count, metadata_json) \
         VALUES ($1, $2, $3, $4, 'text', 'indexed', $5, 0, '{}'::jsonb) \
         ON CONFLICT (_id) DO UPDATE SET \
           source = EXCLUDED.source, embedder_name = EXCLUDED.embedder_name, status = 'indexed'",
    )
    .bind::<SqlUuid, _>(document_id)
    .bind::<Nullable<Text>, _>(scope_text(record.scope.as_ref()))
    .bind::<Text, _>(title)
    .bind::<Nullable<Text>, _>(record.source.clone())
    .bind::<Nullable<Text>, _>(if embedder.is_empty() {
        None
    } else {
        Some(embedder)
    })
    .execute(conn)
    .await
    .map_err(|e| crate::BEM_209.with_data(e.to_string()))?;

    let chunk_id = record_uuid(record);
    let literal = vector_literal(&record.embedding);
    sql_query(
        "INSERT INTO ref_beam.agent_knowledge_chunk \
         (_id, document_id, scope, source, chunk_index, token_count, content, embedding, metadata_json) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8::vector, $9) \
         ON CONFLICT (_id) DO UPDATE SET \
           content = EXCLUDED.content, embedding = EXCLUDED.embedding, \
           metadata_json = EXCLUDED.metadata_json, chunk_index = EXCLUDED.chunk_index",
    )
    .bind::<SqlUuid, _>(chunk_id)
    .bind::<SqlUuid, _>(document_id)
    .bind::<Nullable<Text>, _>(scope_text(record.scope.as_ref()))
    .bind::<Nullable<Text>, _>(record.source.clone())
    .bind::<Integer, _>(i32::try_from(record.chunk_index.unwrap_or(0)).unwrap_or(0))
    .bind::<Nullable<Integer>, _>(
        record
            .token_count
            .and_then(|n| i32::try_from(n).ok()),
    )
    .bind::<Text, _>(record.text.clone())
    .bind::<Text, _>(literal)
    .bind::<Jsonb, _>(record.metadata.clone())
    .execute(conn)
    .await
    .map_err(|e| crate::BEM_210.with_data(e.to_string()))?;
    Ok(())
}

async fn upsert_memory(
    conn: &mut AsyncPgConnection,
    record: &VectorRecord,
) -> RiverbaseResult<()> {
    let id = record_uuid(record);
    let user_id = opt_uuid(record.user_id.as_deref()).unwrap_or(Uuid::nil());
    let mem_type = record
        .metadata
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("semantic")
        .to_string();
    let importance = record
        .metadata
        .get("importance")
        .and_then(Value::as_i64)
        .unwrap_or(1);
    let literal = vector_literal(&record.embedding);
    sql_query(
        "INSERT INTO ref_beam.agent_memory \
         (_id, user_id, agent_id, type, content, embedding, embedder_name, metadata_json, importance, last_accessed_at) \
         VALUES ($1, $2, $3, $4, $5, $6::vector, $7, $8, $9, now()) \
         ON CONFLICT (_id) DO UPDATE SET \
           content = EXCLUDED.content, embedding = EXCLUDED.embedding, \
           metadata_json = EXCLUDED.metadata_json, last_accessed_at = now()",
    )
    .bind::<SqlUuid, _>(id)
    .bind::<SqlUuid, _>(user_id)
    .bind::<Nullable<SqlUuid>, _>(opt_uuid(record.agent_id.as_deref()))
    .bind::<Text, _>(mem_type)
    .bind::<Text, _>(record.text.clone())
    .bind::<Text, _>(literal)
    .bind::<Nullable<Text>, _>(record.embedder_name.clone())
    .bind::<Jsonb, _>(record.metadata.clone())
    .bind::<Integer, _>(i32::try_from(importance).unwrap_or(1))
    .execute(conn)
    .await
    .map_err(|e| crate::BEM_211.with_data(e.to_string()))?;
    Ok(())
}

async fn query_knowledge(
    conn: &mut AsyncPgConnection,
    vector: &[f32],
    k: usize,
    filters: Option<&RetrievalFilter>,
) -> RiverbaseResult<Vec<Hit>> {
    let literal = vector_literal(vector);
    let scope = filters.and_then(|f| scope_text(f.scope.as_ref()));
    let agent_id = filters.and_then(|f| opt_uuid(f.agent_id.as_deref()));
    let user_id = filters.and_then(|f| opt_uuid(f.user_id.as_deref()));
    let limit = i32::try_from(k).unwrap_or(6);
    let rows: Vec<ChunkHitRow> = sql_query(
        "SELECT _id AS id, document_id, scope, chunk_index, source, content, metadata_json, \
         (embedding <=> $1::vector) AS distance \
         FROM ref_beam.agent_knowledge_chunk \
         WHERE ($2::text IS NULL OR scope = $2) \
           AND ($3::uuid IS NULL OR agent_id = $3 OR agent_id IS NULL) \
           AND ($4::uuid IS NULL OR user_id = $4 OR user_id IS NULL) \
         ORDER BY distance ASC LIMIT $5",
    )
    .bind::<Text, _>(literal)
    .bind::<Nullable<Text>, _>(scope)
    .bind::<Nullable<SqlUuid>, _>(agent_id)
    .bind::<Nullable<SqlUuid>, _>(user_id)
    .bind::<Integer, _>(limit)
    .load(conn)
    .await
    .map_err(|e| crate::BEM_212.with_data(e.to_string()))?;

    let doc_filter = filters.and_then(|f| f.document_ids.as_ref());
    Ok(rows
        .into_iter()
        .filter(|row| match doc_filter {
            None => true,
            Some(ids) => row
                .document_id
                .is_some_and(|id| ids.iter().any(|d| d == &id.to_string())),
        })
        .map(|row| Hit {
            id: row.id.to_string(),
            text: row.content,
            score: distance_to_score(row.distance),
            metadata: row.metadata_json,
            document_id: row.document_id.map(|id| id.to_string()),
            chunk_index: Some(usize::try_from(row.chunk_index).unwrap_or(0)),
            source: row.source,
            scope: scope_from_db(row.scope),
        })
        .collect())
}

async fn query_memory(
    conn: &mut AsyncPgConnection,
    vector: &[f32],
    k: usize,
    filters: Option<&RetrievalFilter>,
) -> RiverbaseResult<Vec<Hit>> {
    let literal = vector_literal(vector);
    let agent_id = filters.and_then(|f| opt_uuid(f.agent_id.as_deref()));
    let user_id = filters.and_then(|f| opt_uuid(f.user_id.as_deref()));
    let limit = i32::try_from(k).unwrap_or(6);
    let rows: Vec<MemoryHitRow> = sql_query(
        "SELECT _id AS id, content, metadata_json, (embedding <=> $1::vector) AS distance \
         FROM ref_beam.agent_memory \
         WHERE embedding IS NOT NULL \
           AND (expires_at IS NULL OR expires_at > now()) \
           AND ($2::uuid IS NULL OR agent_id = $2) \
           AND ($3::uuid IS NULL OR user_id = $3) \
         ORDER BY distance ASC LIMIT $4",
    )
    .bind::<Text, _>(literal)
    .bind::<Nullable<SqlUuid>, _>(agent_id)
    .bind::<Nullable<SqlUuid>, _>(user_id)
    .bind::<Integer, _>(limit)
    .load(conn)
    .await
    .map_err(|e| crate::BEM_213.with_data(e.to_string()))?;
    Ok(rows
        .into_iter()
        .map(|row| Hit {
            id: row.id.to_string(),
            text: row.content,
            score: distance_to_score(row.distance),
            metadata: row.metadata_json,
            document_id: None,
            chunk_index: None,
            source: None,
            scope: None,
        })
        .collect())
}

async fn delete_knowledge(
    conn: &mut AsyncPgConnection,
    ids: Option<&[String]>,
    document_id: Option<&str>,
) -> RiverbaseResult<usize> {
    if let Some(ids) = ids {
        let mut n = 0usize;
        for id in ids {
            if let Ok(uuid) = Uuid::parse_str(id) {
                let rows = sql_query("DELETE FROM ref_beam.agent_knowledge_chunk WHERE _id = $1")
                    .bind::<SqlUuid, _>(uuid)
                    .execute(conn)
                    .await
                    .map_err(|e| crate::BEM_214.with_data(e.to_string()))?;
                n = n.saturating_add(rows);
            }
        }
        return Ok(n);
    }
    if let Some(doc) = document_id.and_then(|s| Uuid::parse_str(s).ok()) {
        let n = sql_query("DELETE FROM ref_beam.agent_knowledge_chunk WHERE document_id = $1")
            .bind::<SqlUuid, _>(doc)
            .execute(conn)
            .await
            .map_err(|e| crate::BEM_215.with_data(e.to_string()))?;
        return Ok(n);
    }
    Ok(0)
}

async fn delete_memory(conn: &mut AsyncPgConnection, ids: Option<&[String]>) -> RiverbaseResult<usize> {
    let Some(ids) = ids else {
        return Ok(0);
    };
    let mut n = 0usize;
    for id in ids {
        if let Ok(uuid) = Uuid::parse_str(id) {
            let rows = sql_query("DELETE FROM ref_beam.agent_memory WHERE _id = $1")
                .bind::<SqlUuid, _>(uuid)
                .execute(conn)
                .await
                .map_err(|e| crate::BEM_215.with_data(e.to_string()))?;
            n = n.saturating_add(rows);
        }
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{KnowledgeManager, KnowledgeScope, MockEmbedder};

    #[tokio::test]
    async fn ingest_retrieve_delete_when_pgvector_available() {
        let url = match std::env::var("BEAM_DATABASE_URL")
            .or_else(|_| std::env::var("BEAM_SQL_DB_DSN"))
        {
            Ok(u) if !u.is_empty() => u,
            _ => return,
        };
        let Some(store) = PgVectorStore::connect(&url)
            .await
            .expect("pgvector connect")
        else {
            return;
        };
        let mgr = KnowledgeManager::new(Arc::new(MockEmbedder::new(1536)), Arc::new(store));
        let result = mgr
            .ingest(
                "Paris is the capital of France.\n\nRust is a systems language.",
                None,
                Some(KnowledgeScope::system()),
                None,
                None,
                Some("pgvector-test".into()),
            )
            .await
            .expect("ingest");
        assert!(result.chunk_count >= 1);
        let hits = mgr
            .retrieve("capital of France", 3, None)
            .await
            .expect("retrieve");
        assert!(!hits.is_empty(), "expected at least one hit");
        assert!(
            hits[0].text.to_lowercase().contains("paris"),
            "top hit should mention paris: {}",
            hits[0].text
        );
        let deleted = mgr
            .delete_document(result.document_id)
            .await
            .expect("delete");
        assert!(deleted >= 1);
    }
}
