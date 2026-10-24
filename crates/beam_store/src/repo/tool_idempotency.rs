//! Remembered tool results, `db.tool_idempotency()`: the
//! `tool_call_idempotency` rows a side effect is replayed from after a worker
//! restart, shared with Python. Keyed by execution, call and tool revision.

use std::time::Duration;

use diesel::sql_query;
use diesel::sql_types::{Double, Jsonb, Text};
use diesel::QueryableByName;
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::RunQueryDsl;
use serde_json::Value;

use crate::db::{Db, DbResult};

/// One remembered result's key.
#[derive(Debug, Clone, Copy)]
pub struct ToolCallKey<'k> {
    pub execution_id: &'k str,
    pub call_id: &'k str,
    pub tool_revision: &'k str,
}

/// The remembered tool results on a [`Db`].
pub struct ToolIdempotency<'d> {
    db: &'d Db,
}

impl Db {
    /// Remembered tool results.
    #[must_use]
    pub fn tool_idempotency(&self) -> ToolIdempotency<'_> {
        ToolIdempotency { db: self }
    }
}

#[derive(QueryableByName)]
struct ResultRow {
    #[diesel(sql_type = Jsonb)]
    result_json: Value,
}

impl ToolIdempotency<'_> {
    /// The unexpired result stored for `key`.
    ///
    /// # Errors
    /// A failed checkout or read.
    pub async fn get(&self, key: ToolCallKey<'_>) -> DbResult<Option<Value>> {
        let s = self.db.schema();
        self.db
            .scoped(|conn| {
                async move {
                    let rows: Vec<ResultRow> = sql_query(format!(
                        "SELECT result_json FROM {s}.tool_call_idempotency \
                         WHERE execution_id = $1 AND call_id = $2 AND tool_revision = $3 \
                         AND (expires_at IS NULL OR expires_at > now())"
                    ))
                    .bind::<Text, _>(key.execution_id)
                    .bind::<Text, _>(key.call_id)
                    .bind::<Text, _>(key.tool_revision)
                    .load(conn)
                    .await?;
                    Ok(rows.into_iter().next().map(|r| r.result_json))
                }
                .scope_boxed()
            })
            .await
    }

    /// Store `result` for `key`, kept for `ttl` (a new result replaces it).
    ///
    /// # Errors
    /// A failed checkout or write.
    pub async fn put(&self, key: ToolCallKey<'_>, result: &Value, ttl: Duration) -> DbResult<()> {
        let s = self.db.schema();
        self.db
            .scoped(|conn| {
                async move {
                    sql_query(format!(
                        "INSERT INTO {s}.tool_call_idempotency (_id, execution_id, call_id, tool_revision, result_json, expires_at) \
                         VALUES (gen_random_uuid(), $1, $2, $3, $4, now() + make_interval(secs => $5)) \
                         ON CONFLICT (execution_id, call_id, tool_revision) DO UPDATE SET \
                         result_json = EXCLUDED.result_json, expires_at = EXCLUDED.expires_at, \
                         _updated = now(), _etag = gen_random_uuid()"
                    ))
                    .bind::<Text, _>(key.execution_id)
                    .bind::<Text, _>(key.call_id)
                    .bind::<Text, _>(key.tool_revision)
                    .bind::<Jsonb, _>(result)
                    .bind::<Double, _>(ttl.as_secs_f64())
                    .execute(conn)
                    .await?;
                    Ok(())
                }
                .scope_boxed()
            })
            .await
    }
}
