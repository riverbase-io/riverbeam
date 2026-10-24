//! Saved runtime state, `db.runtime_state()`: the `runtime_state` table
//! (migration 0003), Rust-owned and keyed by `state_id`, so not in
//! riverbase's entity shape. Its plain writes are typed over the table
//! below (plan 015 D3); the fenced save and the status compare-and-set stay
//! SQL.

use std::time::Duration;

use chrono::{DateTime, Utc};
use diesel::sql_query;
use diesel::sql_types::{Double, Integer, Jsonb, Text, Uuid as SqlUuid};
use diesel::{
    ExpressionMethods, OptionalExtension, QueryDsl, Queryable, QueryableByName, Selectable,
    SelectableHelper,
};
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::RunQueryDsl;
use serde_json::Value;
use uuid::Uuid;

use crate::db::{Db, DbError, DbResult};
use crate::entity::value::now;

// The unqualified name resolves through the pool's `search_path`.
diesel::table! {
    runtime_state (state_id) {
        state_id -> Uuid,
        execution_id -> Uuid,
        run_id -> Uuid,
        runtime_name -> Text,
        run_format -> Int4,
        adapter_version -> Text,
        library_version -> Text,
        status -> Text,
        turn -> Int4,
        state -> Jsonb,
        _created -> Timestamptz,
        _updated -> Timestamptz,
    }
}

/// One saved run.
#[derive(Debug, Clone, PartialEq, Queryable, Selectable)]
#[diesel(table_name = runtime_state)]
pub struct RuntimeStateRow {
    pub state_id: Uuid,
    pub execution_id: Uuid,
    pub run_id: Uuid,
    pub runtime_name: String,
    pub run_format: i32,
    pub adapter_version: String,
    pub library_version: String,
    pub status: String,
    pub turn: i32,
    pub state: Value,
    #[diesel(column_name = _updated)]
    pub updated_at: DateTime<Utc>,
}

/// A run's lease, as a fenced write checks it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fence {
    pub run_id: Uuid,
    pub lease_id: Uuid,
    pub fence: i32,
}

#[derive(QueryableByName)]
struct One {
    #[diesel(sql_type = Integer)]
    #[allow(dead_code)]
    one: i32,
}

/// The saved runtime state on a [`Db`].
pub struct RuntimeState<'d> {
    db: &'d Db,
}

impl Db {
    /// Saved runtime state.
    #[must_use]
    pub fn runtime_state(&self) -> RuntimeState<'_> {
        RuntimeState { db: self }
    }
}

impl RuntimeState<'_> {
    /// Insert or replace `row`. With `fence`, only while that lease is still
    /// the run's current one: the run is read `FOR SHARE`, which holds off a
    /// new claim (an update of the run) until this write commits, so the new
    /// owner reads it. Returns `false`, writing nothing, when the lease is
    /// stale.
    ///
    /// # Errors
    /// A failed checkout or statement (rolled back).
    pub async fn save(&self, row: &RuntimeStateRow, fence: Option<Fence>) -> DbResult<bool> {
        let s = self.db.schema();
        self.db
            .transaction::<_, DbError, _>(|tx| {
                Box::pin(async move {
                    if let Some(fence) = fence {
                        let owned: Vec<One> = sql_query(format!(
                            "SELECT 1 AS one FROM {s}.agent_run \
                             WHERE _id = $1 AND lease_id = $2 AND lease_fence = $3 FOR SHARE"
                        ))
                        .bind::<SqlUuid, _>(fence.run_id)
                        .bind::<SqlUuid, _>(fence.lease_id)
                        .bind::<Integer, _>(fence.fence)
                        .load(tx)
                        .await?;
                        if owned.is_empty() {
                            return Ok(false);
                        }
                    }
                    sql_query(format!(
                        "INSERT INTO {s}.runtime_state (state_id, execution_id, run_id, runtime_name, \
                         run_format, adapter_version, library_version, status, turn, state) \
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
                         ON CONFLICT (state_id) DO UPDATE SET run_id = EXCLUDED.run_id, \
                         runtime_name = EXCLUDED.runtime_name, run_format = EXCLUDED.run_format, \
                         adapter_version = EXCLUDED.adapter_version, \
                         library_version = EXCLUDED.library_version, status = EXCLUDED.status, \
                         turn = EXCLUDED.turn, state = EXCLUDED.state, _updated = now()"
                    ))
                    .bind::<SqlUuid, _>(row.state_id)
                    .bind::<SqlUuid, _>(row.execution_id)
                    .bind::<SqlUuid, _>(row.run_id)
                    .bind::<Text, _>(&row.runtime_name)
                    .bind::<Integer, _>(row.run_format)
                    .bind::<Text, _>(&row.adapter_version)
                    .bind::<Text, _>(&row.library_version)
                    .bind::<Text, _>(&row.status)
                    .bind::<Integer, _>(row.turn)
                    .bind::<Jsonb, _>(&row.state)
                    .execute(tx)
                    .await?;
                    Ok(true)
                })
            })
            .await
    }

    /// Saved run `state_id`.
    ///
    /// # Errors
    /// A failed checkout or read.
    pub async fn load(&self, state_id: Uuid) -> DbResult<Option<RuntimeStateRow>> {
        use runtime_state::dsl as t;
        self.db
            .scoped(|conn| {
                async move {
                    Ok(t::runtime_state
                        .filter(t::state_id.eq(state_id))
                        .select(RuntimeStateRow::as_select())
                        .first(conn)
                        .await
                        .optional()?)
                }
                .scope_boxed()
            })
            .await
    }

    /// Move run `state_id` from status `from` to `to`, recording `run_id` as
    /// its writer; `false`, changing nothing, when it is not in `from`.
    ///
    /// # Errors
    /// A failed checkout or update.
    pub async fn claim(
        &self,
        state_id: Uuid,
        from: &str,
        to: &str,
        run_id: Uuid,
    ) -> DbResult<bool> {
        let s = self.db.schema();
        self.db
            .scoped(|conn| {
                async move {
                    let n = sql_query(format!(
                        "UPDATE {s}.runtime_state SET status = $3, run_id = $4, _updated = now() \
                         WHERE state_id = $1 AND status = $2"
                    ))
                    .bind::<SqlUuid, _>(state_id)
                    .bind::<Text, _>(from)
                    .bind::<Text, _>(to)
                    .bind::<SqlUuid, _>(run_id)
                    .execute(conn)
                    .await?;
                    Ok(n > 0)
                }
                .scope_boxed()
            })
            .await
    }

    /// Change only the status; whether the row exists.
    ///
    /// # Errors
    /// A failed checkout or update.
    pub async fn set_status(&self, state_id: Uuid, status: &str) -> DbResult<bool> {
        use runtime_state::dsl as t;
        self.db
            .scoped(|conn| {
                async move {
                    let n = diesel::update(t::runtime_state.filter(t::state_id.eq(state_id)))
                        .set((t::status.eq(status), t::_updated.eq(now())))
                        .execute(conn)
                        .await?;
                    Ok(n > 0)
                }
                .scope_boxed()
            })
            .await
    }

    /// Delete run `state_id`; whether it existed.
    ///
    /// # Errors
    /// A failed checkout or delete.
    pub async fn purge(&self, state_id: Uuid) -> DbResult<bool> {
        use runtime_state::dsl as t;
        self.db
            .scoped(|conn| {
                async move {
                    let n = diesel::delete(t::runtime_state.filter(t::state_id.eq(state_id)))
                        .execute(conn)
                        .await?;
                    Ok(n > 0)
                }
                .scope_boxed()
            })
            .await
    }

    /// Runs in one of `statuses` not written for `older_than`, by the
    /// database clock (as for leases: worker clock skew cannot purge live
    /// state).
    ///
    /// # Errors
    /// A failed checkout or read.
    pub async fn retention_scan(
        &self,
        statuses: &[&str],
        older_than: Duration,
    ) -> DbResult<Vec<Uuid>> {
        use runtime_state::dsl as t;
        self.db
            .scoped(|conn| {
                async move {
                    let old_enough = diesel::dsl::sql::<diesel::sql_types::Bool>(
                        "_updated <= now() - make_interval(secs => ",
                    )
                    .bind::<Double, _>(older_than.as_secs_f64())
                    .sql(")");
                    Ok(t::runtime_state
                        .filter(t::status.eq_any(statuses))
                        .filter(old_enough)
                        .select(t::state_id)
                        .load::<Uuid>(conn)
                        .await?)
                }
                .scope_boxed()
            })
            .await
    }

    /// Read the table once (liveness).
    ///
    /// # Errors
    /// A failed checkout or read.
    pub async fn probe(&self) -> DbResult<()> {
        let s = self.db.schema();
        self.db
            .scoped(|conn| {
                async move {
                    sql_query(format!("SELECT 1 AS one FROM {s}.runtime_state LIMIT 1"))
                        .load::<One>(conn)
                        .await?;
                    Ok(())
                }
                .scope_boxed()
            })
            .await
    }
}
