//! JSON-embedding vectors, `db.vector_json()`: the `beam_vector_json`
//! table the knowledge store falls back to without pgvector (Python
//! `knowledge/stores/sqljson.py`). Embeddings are JSONB, scored by the
//! caller; ids and owners are text, so the table has no `_tenant`.

use diesel::sql_query;
use diesel::sql_types::{Array, Jsonb, Nullable, Text, Uuid as SqlUuid};
use diesel::QueryableByName;
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::RunQueryDsl;
use serde_json::Value;
use uuid::Uuid;

use crate::db::{Db, DbResult};

const TOUCH: &str = "_updated = now(), _etag = gen_random_uuid()";

/// One stored vector.
#[derive(Debug, Clone, QueryableByName)]
pub struct JsonVectorRow {
    #[diesel(sql_type = SqlUuid)]
    pub id: Uuid,
    /// `knowledge` or `memory`.
    #[diesel(sql_type = Text)]
    pub namespace: String,
    #[diesel(sql_type = Text)]
    pub text: String,
    /// A JSON array of numbers.
    #[diesel(sql_type = Jsonb)]
    pub embedding: Value,
    #[diesel(sql_type = Jsonb)]
    pub metadata_json: Value,
    #[diesel(sql_type = Nullable<Text>)]
    pub user_id: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    pub agent_id: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    pub org_id: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    pub document_id: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    pub source: Option<String>,
}

/// The JSON vectors on a [`Db`].
pub struct VectorJson<'d> {
    db: &'d Db,
}

impl Db {
    /// The JSON-embedding vector table.
    #[must_use]
    pub fn vector_json(&self) -> VectorJson<'_> {
        VectorJson { db: self }
    }
}

fn bem(e: &diesel::result::Error) -> crate::db::DbError {
    crate::BEM_244.with_data(e.to_string()).into()
}

impl VectorJson<'_> {
    /// Insert or replace each row.
    ///
    /// # Errors
    /// `BEM-244` when a write fails; a failed checkout.
    pub async fn upsert(&self, rows: &[JsonVectorRow]) -> DbResult<()> {
        let s = self.db.schema();
        self.db
            .scoped(|conn| {
                async move {
                    for r in rows {
                        sql_query(format!(
                            "INSERT INTO {s}.beam_vector_json \
                             (_id, namespace, text, embedding, metadata_json, user_id, agent_id, org_id, document_id, source) \
                             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
                             ON CONFLICT (_id) DO UPDATE SET namespace = EXCLUDED.namespace, text = EXCLUDED.text, \
                             embedding = EXCLUDED.embedding, metadata_json = EXCLUDED.metadata_json, user_id = EXCLUDED.user_id, \
                             agent_id = EXCLUDED.agent_id, org_id = EXCLUDED.org_id, document_id = EXCLUDED.document_id, \
                             source = EXCLUDED.source, {TOUCH}"
                        ))
                        .bind::<SqlUuid, _>(r.id)
                        .bind::<Text, _>(&r.namespace)
                        .bind::<Text, _>(&r.text)
                        .bind::<Jsonb, _>(&r.embedding)
                        .bind::<Jsonb, _>(&r.metadata_json)
                        .bind::<Nullable<Text>, _>(r.user_id.as_deref())
                        .bind::<Nullable<Text>, _>(r.agent_id.as_deref())
                        .bind::<Nullable<Text>, _>(r.org_id.as_deref())
                        .bind::<Nullable<Text>, _>(r.document_id.as_deref())
                        .bind::<Nullable<Text>, _>(r.source.as_deref())
                        .execute(conn)
                        .await
                        .map_err(|e| bem(&e))?;
                    }
                    Ok(())
                }
                .scope_boxed()
            })
            .await
    }

    /// Every row of `namespace`, oldest first.
    ///
    /// # Errors
    /// `BEM-244` when the read fails; a failed checkout.
    pub async fn list(&self, namespace: &str) -> DbResult<Vec<JsonVectorRow>> {
        let s = self.db.schema();
        self.db
            .scoped(|conn| {
                async move {
                    sql_query(format!(
                        "SELECT _id AS id, namespace, text, embedding, metadata_json, user_id, agent_id, org_id, document_id, source \
                         FROM {s}.beam_vector_json WHERE namespace = $1 ORDER BY _created, _id"
                    ))
                    .bind::<Text, _>(namespace)
                    .load(conn)
                    .await
                    .map_err(|e| bem(&e))
                }
                .scope_boxed()
            })
            .await
    }

    /// Delete rows `ids` of `namespace`; how many.
    ///
    /// # Errors
    /// `BEM-244` when the delete fails; a failed checkout.
    pub async fn delete_ids(&self, namespace: &str, ids: Vec<Uuid>) -> DbResult<usize> {
        let s = self.db.schema();
        self.db
            .scoped(|conn| {
                async move {
                    sql_query(format!(
                        "DELETE FROM {s}.beam_vector_json WHERE namespace = $1 AND _id = ANY($2)"
                    ))
                    .bind::<Text, _>(namespace)
                    .bind::<Array<SqlUuid>, _>(ids)
                    .execute(conn)
                    .await
                    .map_err(|e| bem(&e))
                }
                .scope_boxed()
            })
            .await
    }

    /// Delete the rows of `namespace` from `document_id`; how many.
    ///
    /// # Errors
    /// `BEM-244` when the delete fails; a failed checkout.
    pub async fn delete_document(&self, namespace: &str, document_id: &str) -> DbResult<usize> {
        let s = self.db.schema();
        self.db
            .scoped(|conn| {
                async move {
                    sql_query(format!(
                        "DELETE FROM {s}.beam_vector_json WHERE namespace = $1 AND document_id = $2"
                    ))
                    .bind::<Text, _>(namespace)
                    .bind::<Text, _>(document_id)
                    .execute(conn)
                    .await
                    .map_err(|e| bem(&e))
                }
                .scope_boxed()
            })
            .await
    }

    /// Row `id`, in any namespace.
    ///
    /// # Errors
    /// `BEM-244` when the read fails; a failed checkout.
    pub async fn get(&self, id: Uuid) -> DbResult<Option<JsonVectorRow>> {
        let s = self.db.schema();
        self.db
            .scoped(|conn| {
                async move {
                    let rows: Vec<JsonVectorRow> = sql_query(format!(
                        "SELECT _id AS id, namespace, text, embedding, metadata_json, user_id, agent_id, org_id, document_id, source \
                         FROM {s}.beam_vector_json WHERE _id = $1"
                    ))
                    .bind::<SqlUuid, _>(id)
                    .load(conn)
                    .await
                    .map_err(|e| bem(&e))?;
                    Ok(rows.into_iter().next())
                }
                .scope_boxed()
            })
            .await
    }
}
