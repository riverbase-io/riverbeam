//! Knowledge, `db.knowledge()`: pgvector chunks (`agent_knowledge_chunk`),
//! memories (`agent_memory`) and document rows (`agent_knowledge_document`),
//! Python's `knowledge/stores/pgvector.py` and `sql.py`. Vector writes,
//! similarity search and `embedding::text` reads stay SQL (pgvector); the
//! document rows are typed over the generated entity. Embeddings travel as
//! pgvector's text form (`[1,2,3]`). Each statement raises its site's code.

use chrono::{DateTime, Utc};
use diesel::sql_query;
use diesel::sql_types::{
    Array, BigInt, Bool, Double, Integer, Jsonb, Nullable, Text, Timestamptz, Uuid as SqlUuid,
};
use diesel::{
    BoolExpressionMethods, ExpressionMethods, OptionalExtension, PgExpressionMethods, QueryDsl,
    Queryable, QueryableByName, Selectable, SelectableHelper,
};
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use riverbase_core::base::ErrorSpec;
use serde_json::Value;
use uuid::Uuid;

use crate::db::{Db, DbError, DbResult};
use crate::entity::agent_knowledge_document::upsert_agent_knowledge_document;
use crate::entity::value::{enum_sql, new_etag, now};

/// SQL that sets the Fluvius update bookkeeping columns.
const TOUCH: &str = "_updated = now(), _etag = gen_random_uuid()";

fn site(e: &diesel::result::Error, spec: ErrorSpec) -> DbError {
    spec.with_data(e.to_string()).into()
}

/// A knowledge chunk to write; `scope` is its `knowledgescope` label.
#[derive(Debug, Clone)]
pub struct ChunkWrite {
    pub id: Uuid,
    pub document_id: Uuid,
    pub scope: Option<String>,
    pub agent_id: Option<Uuid>,
    pub user_id: Option<Uuid>,
    pub org_id: Option<Uuid>,
    pub source: Option<String>,
    pub chunk_index: i32,
    pub token_count: Option<i32>,
    pub content_hash: Option<String>,
    pub content: String,
    pub embedding: String,
    pub metadata: Value,
    pub embedding_revision: Option<String>,
}

/// A memory to write; `memory_type` is its `memorytype` label.
#[derive(Debug, Clone)]
pub struct MemoryWrite {
    pub id: Uuid,
    pub agent_id: Option<Uuid>,
    pub user_id: Uuid,
    pub org_id: Option<Uuid>,
    pub memory_type: String,
    pub content: String,
    pub embedding: String,
    pub embedder_name: Option<String>,
    pub metadata: Value,
    pub importance: i32,
    pub expires_at: Option<DateTime<Utc>>,
    pub version: i32,
    pub confidence: Option<f64>,
}

/// A chunk search's filters (Python `_apply_knowledge_filters`). Only
/// applied when `filtered`; owner columns fail closed (a missing identity
/// hides that scope's rows).
#[derive(Debug, Clone, Default)]
pub struct ChunkFilter {
    pub filtered: bool,
    pub scope: Option<String>,
    pub scopes: Option<Vec<String>>,
    pub agent_id: Option<Uuid>,
    pub user_id: Option<Uuid>,
    pub org_id: Option<Uuid>,
    pub document_ids: Option<Vec<Uuid>>,
    pub metadata: Option<Value>,
}

/// A memory search's filters; `types` are `memorytype` labels.
#[derive(Debug, Clone, Default)]
pub struct MemoryFilter {
    pub agent_id: Option<Uuid>,
    pub user_id: Option<Uuid>,
    pub types: Option<Vec<String>>,
}

/// A chunk found by similarity; `distance` is cosine distance.
#[derive(Debug, Clone, QueryableByName)]
pub struct ChunkHit {
    #[diesel(sql_type = SqlUuid)]
    pub id: Uuid,
    #[diesel(sql_type = Nullable<SqlUuid>)]
    pub document_id: Option<Uuid>,
    #[diesel(sql_type = Nullable<Text>)]
    pub scope: Option<String>,
    #[diesel(sql_type = Integer)]
    pub chunk_index: i32,
    #[diesel(sql_type = Nullable<Text>)]
    pub source: Option<String>,
    #[diesel(sql_type = Text)]
    pub content: String,
    #[diesel(sql_type = Jsonb)]
    pub metadata_json: Value,
    #[diesel(sql_type = Nullable<Double>)]
    pub distance: Option<f64>,
}

/// A memory found by similarity.
#[derive(Debug, Clone, QueryableByName)]
pub struct MemoryHit {
    #[diesel(sql_type = SqlUuid)]
    pub id: Uuid,
    #[diesel(sql_type = Nullable<SqlUuid>)]
    pub agent_id: Option<Uuid>,
    #[diesel(sql_type = SqlUuid)]
    pub user_id: Uuid,
    #[diesel(sql_type = Text)]
    pub memory_type: String,
    #[diesel(sql_type = Text)]
    pub content: String,
    #[diesel(sql_type = Jsonb)]
    pub metadata_json: Value,
    #[diesel(sql_type = Integer)]
    pub importance: i32,
    #[diesel(sql_type = Nullable<Double>)]
    pub distance: Option<f64>,
}

/// One chunk row; `embedding` in pgvector's text form.
#[derive(Debug, Clone, QueryableByName)]
pub struct ChunkRow {
    #[diesel(sql_type = SqlUuid)]
    pub id: Uuid,
    #[diesel(sql_type = SqlUuid)]
    pub document_id: Uuid,
    #[diesel(sql_type = Nullable<Text>)]
    pub scope: Option<String>,
    #[diesel(sql_type = Nullable<SqlUuid>)]
    pub agent_id: Option<Uuid>,
    #[diesel(sql_type = Nullable<SqlUuid>)]
    pub user_id: Option<Uuid>,
    #[diesel(sql_type = Nullable<SqlUuid>)]
    pub org_id: Option<Uuid>,
    #[diesel(sql_type = Nullable<Text>)]
    pub source: Option<String>,
    #[diesel(sql_type = Integer)]
    pub chunk_index: i32,
    #[diesel(sql_type = Text)]
    pub content: String,
    #[diesel(sql_type = Text)]
    pub embedding: String,
    #[diesel(sql_type = Jsonb)]
    pub metadata_json: Value,
}

/// One memory row; `embedding` in pgvector's text form.
#[derive(Debug, Clone, QueryableByName)]
pub struct MemoryRow {
    #[diesel(sql_type = SqlUuid)]
    pub id: Uuid,
    #[diesel(sql_type = Nullable<SqlUuid>)]
    pub agent_id: Option<Uuid>,
    #[diesel(sql_type = SqlUuid)]
    pub user_id: Uuid,
    #[diesel(sql_type = Nullable<SqlUuid>)]
    pub org_id: Option<Uuid>,
    #[diesel(sql_type = Text)]
    pub memory_type: String,
    #[diesel(sql_type = Text)]
    pub content: String,
    #[diesel(sql_type = Nullable<Text>)]
    pub embedding: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    pub embedder_name: Option<String>,
    #[diesel(sql_type = Jsonb)]
    pub metadata_json: Value,
    #[diesel(sql_type = Integer)]
    pub importance: i32,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    pub expires_at: Option<DateTime<Utc>>,
    #[diesel(sql_type = Nullable<Double>)]
    pub confidence: Option<f64>,
}

/// A document row (typed over the generated entity's table; `scope` and
/// `status` read as their labels).
#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = crate::entity::agent_knowledge_document::agent_knowledge_document)]
pub struct DocumentRecord {
    #[diesel(column_name = _id)]
    pub id: Uuid,
    pub scope: Option<String>,
    pub agent_id: Option<Uuid>,
    pub user_id: Option<Uuid>,
    pub org_id: Option<Uuid>,
    pub title: String,
    pub source: Option<String>,
    #[diesel(column_name = document_type)]
    pub doc_type: String,
    pub body: Option<String>,
    pub media_id: Option<Uuid>,
    pub metadata_json: Value,
    pub status: String,
    pub content_hash: Option<String>,
    pub chunk_count: i32,
    pub embedder_name: Option<String>,
}

/// A document's fingerprint: its scope label, owners and content hash;
/// `preferred_status` is the status label ranked first.
#[derive(Debug, Clone)]
pub struct Fingerprint<'f> {
    pub scope: &'f str,
    pub preferred_status: &'f str,
    pub agent_id: Option<Uuid>,
    pub user_id: Option<Uuid>,
    pub org_id: Option<Uuid>,
    pub content_hash: &'f str,
}

#[derive(QueryableByName)]
struct Present {
    #[diesel(sql_type = Integer)]
    #[allow(dead_code)]
    present: i32,
}

/// Knowledge on a [`Db`].
pub struct Knowledge<'d> {
    db: &'d Db,
}

impl Db {
    /// Knowledge chunks, memories and documents.
    #[must_use]
    pub fn knowledge(&self) -> Knowledge<'_> {
        Knowledge { db: self }
    }

    /// Whether the `vector` extension and the schema's
    /// `agent_knowledge_chunk` exist (a catalog check, across orgs).
    ///
    /// # Errors
    /// `BEM-205` when no connection can be had; `BEM-206` when the check fails.
    pub async fn knowledge_ready(&self) -> DbResult<bool> {
        let schema = self.schema().as_str();
        self.at(crate::BEM_205)
            .cross_org(|conn| {
                async move {
                    let rows: Vec<Present> = sql_query(
                        "SELECT 1 AS present FROM pg_extension e, information_schema.tables t \
                         WHERE e.extname = 'vector' AND t.table_schema = $1 AND t.table_name = 'agent_knowledge_chunk'",
                    )
                    .bind::<Text, _>(schema)
                    .load(conn)
                    .await
                    .map_err(|e| site(&e, crate::BEM_206))?;
                    Ok(!rows.is_empty())
                }
                .scope_boxed()
            })
            .await
    }
}

async fn write_chunks(
    conn: &mut AsyncPgConnection,
    s: &crate::SchemaName,
    chunks: &[ChunkWrite],
) -> DbResult<()> {
    for r in chunks {
        sql_query(format!(
            "INSERT INTO {s}.agent_knowledge_chunk \
             (_id, document_id, scope, agent_id, user_id, org_id, source, chunk_index, token_count, \
              content_hash, content, embedding, metadata_json, embedding_revision) \
             VALUES ($1, $2, CAST($3 AS {s}.knowledgescope), $4, $5, $6, $7, $8, $9, $10, $11, $12::vector, $13, $14) \
             ON CONFLICT (_id) DO UPDATE SET document_id = EXCLUDED.document_id, scope = EXCLUDED.scope, \
             agent_id = EXCLUDED.agent_id, user_id = EXCLUDED.user_id, org_id = EXCLUDED.org_id, \
             source = EXCLUDED.source, chunk_index = EXCLUDED.chunk_index, token_count = EXCLUDED.token_count, \
             content_hash = EXCLUDED.content_hash, content = EXCLUDED.content, embedding = EXCLUDED.embedding, \
             metadata_json = EXCLUDED.metadata_json, embedding_revision = EXCLUDED.embedding_revision, {TOUCH}"
        ))
        .bind::<SqlUuid, _>(r.id)
        .bind::<SqlUuid, _>(r.document_id)
        .bind::<Nullable<Text>, _>(r.scope.as_deref())
        .bind::<Nullable<SqlUuid>, _>(r.agent_id)
        .bind::<Nullable<SqlUuid>, _>(r.user_id)
        .bind::<Nullable<SqlUuid>, _>(r.org_id)
        .bind::<Nullable<Text>, _>(r.source.as_deref())
        .bind::<Integer, _>(r.chunk_index)
        .bind::<Nullable<Integer>, _>(r.token_count)
        .bind::<Nullable<Text>, _>(r.content_hash.as_deref())
        .bind::<Text, _>(&r.content)
        .bind::<Text, _>(&r.embedding)
        .bind::<Jsonb, _>(&r.metadata)
        .bind::<Nullable<Text>, _>(r.embedding_revision.as_deref())
        .execute(&mut *conn)
        .await
        .map_err(|e| site(&e, crate::BEM_209))?;
    }
    Ok(())
}

async fn write_memories(
    conn: &mut AsyncPgConnection,
    s: &crate::SchemaName,
    memories: &[MemoryWrite],
) -> DbResult<()> {
    for r in memories {
        sql_query(format!(
            "INSERT INTO {s}.agent_memory \
             (_id, agent_id, user_id, org_id, type, content, embedding, embedder_name, metadata_json, \
              importance, expires_at, last_accessed_at, version, confidence) \
             VALUES ($1, $2, $3, $4, CAST($5 AS {s}.memorytype), $6, $7::vector, $8, $9, $10, $11, now(), $12, $13) \
             ON CONFLICT (_id) DO UPDATE SET agent_id = EXCLUDED.agent_id, user_id = EXCLUDED.user_id, \
             org_id = EXCLUDED.org_id, type = EXCLUDED.type, content = EXCLUDED.content, \
             embedding = EXCLUDED.embedding, embedder_name = EXCLUDED.embedder_name, \
             metadata_json = EXCLUDED.metadata_json, importance = EXCLUDED.importance, \
             expires_at = EXCLUDED.expires_at, last_accessed_at = EXCLUDED.last_accessed_at, \
             version = EXCLUDED.version, confidence = EXCLUDED.confidence, {TOUCH}"
        ))
        .bind::<SqlUuid, _>(r.id)
        .bind::<Nullable<SqlUuid>, _>(r.agent_id)
        .bind::<SqlUuid, _>(r.user_id)
        .bind::<Nullable<SqlUuid>, _>(r.org_id)
        .bind::<Text, _>(&r.memory_type)
        .bind::<Text, _>(&r.content)
        .bind::<Text, _>(&r.embedding)
        .bind::<Nullable<Text>, _>(r.embedder_name.as_deref())
        .bind::<Jsonb, _>(&r.metadata)
        .bind::<Integer, _>(r.importance)
        .bind::<Nullable<Timestamptz>, _>(r.expires_at)
        .bind::<Integer, _>(r.version)
        .bind::<Nullable<Double>, _>(r.confidence)
        .execute(&mut *conn)
        .await
        .map_err(|e| site(&e, crate::BEM_210))?;
    }
    Ok(())
}

async fn delete_chunks_of(conn: &mut AsyncPgConnection, document_id: Uuid) -> DbResult<usize> {
    use crate::entity::agent_knowledge_chunk::agent_knowledge_chunk as chunk;
    diesel::delete(chunk::table.filter(chunk::document_id.eq(document_id)))
        .execute(conn)
        .await
        .map_err(|e| site(&e, crate::BEM_214))
}

impl Knowledge<'_> {
    /// Write `chunks` and `memories` in one transaction.
    ///
    /// # Errors
    /// `BEM-209`/`BEM-210` when a write fails (rolled back); a failed
    /// checkout or transaction.
    pub async fn upsert(
        &self,
        chunks: Vec<ChunkWrite>,
        memories: Vec<MemoryWrite>,
    ) -> DbResult<()> {
        let s = self.db.schema();
        self.db
            .transaction::<_, DbError, _>(|tx| {
                Box::pin(async move {
                    write_chunks(tx, s, &chunks).await?;
                    write_memories(tx, s, &memories).await
                })
            })
            .await
    }

    /// In one transaction: delete the chunks of `document_id` (when given),
    /// then write `chunks` and `memories`, so a failed write leaves the old
    /// chunks in place. Returns how many were deleted.
    ///
    /// # Errors
    /// `BEM-214` when the delete fails; as [`Self::upsert`].
    pub async fn replace_document(
        &self,
        document_id: Option<Uuid>,
        chunks: Vec<ChunkWrite>,
        memories: Vec<MemoryWrite>,
    ) -> DbResult<usize> {
        let s = self.db.schema();
        self.db
            .transaction::<_, DbError, _>(|tx| {
                Box::pin(async move {
                    let deleted = match document_id {
                        Some(document) => delete_chunks_of(tx, document).await?,
                        None => 0,
                    };
                    write_chunks(tx, s, &chunks).await?;
                    write_memories(tx, s, &memories).await?;
                    Ok(deleted)
                })
            })
            .await
    }

    /// Delete chunks (or, with `memories`, memories) `ids`; how many.
    ///
    /// # Errors
    /// `BEM-213` when the delete fails; a failed checkout.
    pub async fn delete_ids(&self, memories: bool, ids: Vec<Uuid>) -> DbResult<usize> {
        use crate::entity::agent_knowledge_chunk::agent_knowledge_chunk as chunk;
        use crate::entity::agent_memory::agent_memory as memory;
        self.db
            .scoped(|conn| {
                async move {
                    if memories {
                        diesel::delete(memory::table.filter(memory::_id.eq_any(ids)))
                            .execute(conn)
                            .await
                    } else {
                        diesel::delete(chunk::table.filter(chunk::_id.eq_any(ids)))
                            .execute(conn)
                            .await
                    }
                    .map_err(|e| site(&e, crate::BEM_213))
                }
                .scope_boxed()
            })
            .await
    }

    /// Delete the chunks of `document_id`; how many.
    ///
    /// # Errors
    /// `BEM-214` when the delete fails; a failed checkout.
    pub async fn delete_document_chunks(&self, document_id: Uuid) -> DbResult<usize> {
        self.db
            .scoped(|conn| async move { delete_chunks_of(conn, document_id).await }.scope_boxed())
            .await
    }

    /// The `k` chunks nearest `embedding` that pass `filter`.
    ///
    /// # Errors
    /// `BEM-211` when the search fails; a failed checkout.
    pub async fn search_chunks(
        &self,
        embedding: &str,
        k: i64,
        filter: &ChunkFilter,
    ) -> DbResult<Vec<ChunkHit>> {
        let s = self.db.schema();
        let owned = |col: &str, param: usize, scope: &str| {
            format!(
                "(NOT $10 OR (CASE WHEN ${param}::uuid IS NULL THEN ({col} IS NULL AND scope != '{scope}') \
                 ELSE ({col} = ${param} OR ({col} IS NULL AND scope != '{scope}')) END))"
            )
        };
        let sql = format!(
            "SELECT _id AS id, document_id, scope::text AS scope, chunk_index, source, content, metadata_json, \
             (embedding <=> $1::vector)::float8 AS distance FROM {s}.agent_knowledge_chunk \
             WHERE embedding IS NOT NULL \
             AND (NOT $10 OR $2::text IS NULL OR scope = CAST($2 AS {s}.knowledgescope)) \
             AND (NOT $10 OR $3::text[] IS NULL OR scope::text = ANY($3)) \
             AND {agent} AND {user} AND {org} \
             AND (NOT $10 OR $7::uuid[] IS NULL OR document_id = ANY($7)) \
             AND (NOT $10 OR $8::jsonb IS NULL OR metadata_json @> $8) \
             ORDER BY distance ASC LIMIT $9",
            agent = owned("agent_id", 4, "AGENT"),
            user = owned("user_id", 5, "USER"),
            org = owned("org_id", 6, "ORG"),
        );
        self.db
            .scoped(|conn| {
                async move {
                    sql_query(sql)
                        .bind::<Text, _>(embedding)
                        .bind::<Nullable<Text>, _>(filter.scope.as_deref())
                        .bind::<Nullable<Array<Text>>, _>(filter.scopes.clone())
                        .bind::<Nullable<SqlUuid>, _>(filter.agent_id)
                        .bind::<Nullable<SqlUuid>, _>(filter.user_id)
                        .bind::<Nullable<SqlUuid>, _>(filter.org_id)
                        .bind::<Nullable<Array<SqlUuid>>, _>(filter.document_ids.clone())
                        .bind::<Nullable<Jsonb>, _>(filter.metadata.clone())
                        .bind::<BigInt, _>(k)
                        .bind::<Bool, _>(filter.filtered)
                        .load(conn)
                        .await
                        .map_err(|e| site(&e, crate::BEM_211))
                }
                .scope_boxed()
            })
            .await
    }

    /// The `k` unexpired memories nearest `embedding` that pass `filter`,
    /// then their `last_accessed_at` stamped, in one transaction. An agent
    /// filter also keeps memories without an agent (user-wide ones).
    ///
    /// # Errors
    /// `BEM-212` when the search or stamp fails; a failed checkout.
    pub async fn search_memories(
        &self,
        embedding: &str,
        k: i64,
        filter: &MemoryFilter,
    ) -> DbResult<Vec<MemoryHit>> {
        use crate::entity::agent_memory::agent_memory as memory;
        let s = self.db.schema();
        let sql = format!(
            "SELECT _id AS id, agent_id, user_id, type::text AS memory_type, content, metadata_json, importance, \
             (embedding <=> $1::vector)::float8 AS distance FROM {s}.agent_memory \
             WHERE embedding IS NOT NULL \
             AND ($2::uuid IS NULL OR agent_id = $2 OR agent_id IS NULL) \
             AND ($3::uuid IS NULL OR user_id = $3) \
             AND ($4::text[] IS NULL OR type::text = ANY($4)) \
             AND (expires_at IS NULL OR expires_at > now()) \
             ORDER BY distance ASC LIMIT $5"
        );
        self.db
            .transaction::<_, DbError, _>(|tx| {
                Box::pin(async move {
                    let rows: Vec<MemoryHit> = sql_query(sql)
                        .bind::<Text, _>(embedding)
                        .bind::<Nullable<SqlUuid>, _>(filter.agent_id)
                        .bind::<Nullable<SqlUuid>, _>(filter.user_id)
                        .bind::<Nullable<Array<Text>>, _>(filter.types.clone())
                        .bind::<BigInt, _>(k)
                        .load(tx)
                        .await
                        .map_err(|e| site(&e, crate::BEM_212))?;
                    if !rows.is_empty() {
                        let ids: Vec<Uuid> = rows.iter().map(|r| r.id).collect();
                        diesel::update(memory::table.filter(memory::_id.eq_any(ids)))
                            .set(memory::last_accessed_at.eq(now()))
                            .execute(tx)
                            .await
                            .map_err(|e| site(&e, crate::BEM_212))?;
                    }
                    Ok(rows)
                })
            })
            .await
    }

    /// Chunk `id`.
    ///
    /// # Errors
    /// `BEM-211` when the read fails; a failed checkout.
    pub async fn get_chunk(&self, id: Uuid) -> DbResult<Option<ChunkRow>> {
        let s = self.db.schema();
        self.db
            .scoped(|conn| {
                async move {
                    let rows: Vec<ChunkRow> = sql_query(format!(
                        "SELECT _id AS id, document_id, scope::text AS scope, agent_id, user_id, org_id, source, chunk_index, \
                         content, embedding::text AS embedding, metadata_json FROM {s}.agent_knowledge_chunk WHERE _id = $1"
                    ))
                    .bind::<SqlUuid, _>(id)
                    .load(conn)
                    .await
                    .map_err(|e| site(&e, crate::BEM_211))?;
                    Ok(rows.into_iter().next())
                }
                .scope_boxed()
            })
            .await
    }

    /// Memory `id`.
    ///
    /// # Errors
    /// `BEM-212` when the read fails; a failed checkout.
    pub async fn get_memory(&self, id: Uuid) -> DbResult<Option<MemoryRow>> {
        let s = self.db.schema();
        self.db
            .scoped(|conn| {
                async move {
                    let rows: Vec<MemoryRow> = sql_query(format!(
                        "SELECT _id AS id, agent_id, user_id, org_id, type::text AS memory_type, content, \
                         embedding::text AS embedding, embedder_name, metadata_json, importance, expires_at, confidence \
                         FROM {s}.agent_memory WHERE _id = $1"
                    ))
                    .bind::<SqlUuid, _>(id)
                    .load(conn)
                    .await
                    .map_err(|e| site(&e, crate::BEM_212))?;
                    Ok(rows.into_iter().next())
                }
                .scope_boxed()
            })
            .await
    }

    /// Write document `id` through its entity (`data` keyed by column).
    ///
    /// # Errors
    /// `BEM-208` when the write fails; a failed checkout.
    pub async fn upsert_document(&self, id: Uuid, data: &Value) -> DbResult<()> {
        self.db
            .scoped(|conn| {
                async move {
                    upsert_agent_knowledge_document(conn, id, data)
                        .await
                        .map_err(|e| crate::BEM_208.with_data(e.to_string()).into())
                }
                .scope_boxed()
            })
            .await
    }

    /// The document with `fingerprint`: a NULL-safe owner match, a row in
    /// the preferred status (`INDEXED`) first, then by id (Python `_rank`; a UUID's byte order
    /// is its text order). No `_deleted` filter.
    ///
    /// # Errors
    /// `BEM-241` when the read fails; a malformed label; a failed checkout.
    pub async fn find_by_fingerprint(
        &self,
        f: Fingerprint<'_>,
    ) -> DbResult<Option<DocumentRecord>> {
        use crate::entity::agent_knowledge_document::{
            agent_knowledge_document as t, KNOWLEDGEDOCUMENTSTATUS, KNOWLEDGESCOPE,
        };
        let scope =
            enum_sql::<Nullable<Text>>(Some(f.scope), "scope", "knowledgescope", KNOWLEDGESCOPE)?;
        let indexed = enum_sql::<Text>(
            Some(f.preferred_status),
            "status",
            "knowledgedocumentstatus",
            KNOWLEDGEDOCUMENTSTATUS,
        )?;
        let Fingerprint {
            agent_id,
            user_id,
            org_id,
            content_hash,
            ..
        } = f;
        self.db
            .scoped(|conn| {
                async move {
                    t::table
                        .filter(t::scope.eq(scope))
                        .filter(t::content_hash.eq(content_hash))
                        .filter(t::agent_id.is_not_distinct_from(agent_id))
                        .filter(t::user_id.is_not_distinct_from(user_id))
                        .filter(t::org_id.is_not_distinct_from(org_id))
                        .order((t::status.eq(indexed).desc(), t::_id.asc()))
                        .select(DocumentRecord::as_select())
                        .first(conn)
                        .await
                        .optional()
                        .map_err(|e| site(&e, crate::BEM_241))
                }
                .scope_boxed()
            })
            .await
    }

    /// Live document `id` with `content_hash`.
    ///
    /// # Errors
    /// `BEM-241` when the read fails; a failed checkout.
    pub async fn find_document(
        &self,
        id: Uuid,
        content_hash: &str,
    ) -> DbResult<Option<DocumentRecord>> {
        use crate::entity::agent_knowledge_document::agent_knowledge_document as t;
        self.db
            .scoped(|conn| {
                async move {
                    t::table
                        .filter(t::_id.eq(id))
                        .filter(t::content_hash.eq(content_hash))
                        .filter(t::_deleted.is_null())
                        .select(DocumentRecord::as_select())
                        .first(conn)
                        .await
                        .optional()
                        .map_err(|e| site(&e, crate::BEM_241))
                }
                .scope_boxed()
            })
            .await
    }

    /// Hard-delete document `id`: dedupe and re-ingest expect the row gone.
    ///
    /// # Errors
    /// `BEM-242` when the delete fails; a failed checkout.
    pub async fn delete_document(&self, id: Uuid) -> DbResult<()> {
        use crate::entity::agent_knowledge_document::agent_knowledge_document as t;
        self.db
            .scoped(|conn| {
                async move {
                    diesel::delete(t::table.filter(t::_id.eq(id)))
                        .execute(conn)
                        .await
                        .map_err(|e| site(&e, crate::BEM_242))?;
                    Ok(())
                }
                .scope_boxed()
            })
            .await
    }

    /// Move documents in status `stuck` since before `cutoff` (on
    /// `COALESCE(_updated, _created)`) to status `failed`; how many.
    ///
    /// # Errors
    /// `BEM-208` when the update fails; a malformed label; a failed checkout.
    pub async fn fail_stuck(
        &self,
        stuck: &str,
        failed: &str,
        cutoff: DateTime<Utc>,
    ) -> DbResult<u64> {
        use crate::entity::agent_knowledge_document::{
            agent_knowledge_document as t, KNOWLEDGEDOCUMENTSTATUS,
        };
        let label = |status: &str| {
            enum_sql::<Text>(
                Some(status),
                "status",
                "knowledgedocumentstatus",
                KNOWLEDGEDOCUMENTSTATUS,
            )
        };
        let (indexing, failed) = (label(stuck)?, label(failed)?);
        self.db
            .scoped(|conn| {
                async move {
                    let n = diesel::update(
                        t::table.filter(t::status.eq(indexing)).filter(
                            t::_updated
                                .le(cutoff)
                                .or(t::_updated.is_null().and(t::_created.le(cutoff))),
                        ),
                    )
                    .set((
                        t::status.eq(failed),
                        t::_updated.eq(now()),
                        t::_etag.eq(new_etag()),
                    ))
                    .execute(conn)
                    .await
                    .map_err(|e| site(&e, crate::BEM_208))?;
                    Ok(u64::try_from(n).unwrap_or(0))
                }
                .scope_boxed()
            })
            .await
    }
}
