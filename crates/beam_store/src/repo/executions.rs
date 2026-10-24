//! Executions and their run leases, `db.executions()`: a port of
//! fluvius-beam's SQL execution store.
//!
//! Statement order and advisory-lock keys match Python's
//! (`execution/stores/sql/executions.py`, `runs.py`), so both stacks serialize
//! on the same locks when they share a database:
//! - start-or-get: `pg_advisory_xact_lock(hashtext(key), hashtext(user \x1f agent))`,
//!   idempotency lookup, execution insert, idempotency insert, then the slot;
//! - concurrency slot: `pg_advisory_xact_lock(hashtext('beam:conc'), hashtext(scope_key))`.
//!
//! One deliberate difference: lease expiry uses the database clock (`now()`),
//! not the worker clock, so clock skew between workers cannot steal a lease.
//!
//! Status changes lock the execution row (`FOR UPDATE`) and let the caller
//! decide the change from what it read, with a plain function: no other
//! work can run while the row is locked.

use std::time::Duration;

use chrono::{DateTime, Utc};
use diesel::sql_query;
use diesel::sql_types::{Bool, Integer, Nullable, Text, Timestamptz, Uuid as SqlUuid};
use diesel::{
    ExpressionMethods, OptionalExtension, QueryDsl, Queryable, QueryableByName, Selectable,
    SelectableHelper,
};
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use riverbase_core::RiverbaseError;
use serde_json::{json, Value};
use uuid::Uuid;

use super::runtime_state::Fence;
use crate::db::{Db, DbError, DbResult};
use crate::entity::agent_execution::upsert_agent_execution;
use crate::entity::agent_run::upsert_agent_run;
use crate::entity::value::enum_sql;
use crate::entity::write;
use crate::repo::rows::Counted;
use crate::schema::SchemaName;

/// The concurrency slot's advisory-lock namespace (Python's).
pub const CONCURRENCY_LOCK_NAMESPACE: &str = "beam:conc";

const TOUCH: &str = "_updated = now(), _etag = gen_random_uuid()";

/// An `agent_execution` row (typed over the generated entity's table;
/// `status` reads as its label).
#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = crate::entity::agent_execution::agent_execution)]
pub struct ExecutionRecord {
    #[diesel(column_name = _id)]
    pub id: Uuid,
    pub session_id: Uuid,
    pub agent_id: Uuid,
    pub agent_revision_id: Option<Uuid>,
    pub status: String,
    pub runtime_name: Option<String>,
    pub runtime_ref: Option<Value>,
    pub concurrency_key: Option<String>,
    pub idempotency_key: Option<String>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub timeout_at: Option<DateTime<Utc>>,
    pub current_attempt: i32,
    pub last_error_code: Option<String>,
    pub last_error_message: Option<String>,
    pub metadata_json: Value,
    pub org_id: Option<Uuid>,
    pub runtime_adapter_version: Option<String>,
    pub runtime_library_version: Option<String>,
}

/// A new execution. `status` is its first status's label; `slot_scope` its
/// normalized concurrency scope, when it takes a slot.
#[derive(Debug, Clone)]
pub struct NewExecutionRecord {
    pub id: Uuid,
    pub session_id: Uuid,
    pub agent_id: Uuid,
    pub org_id: Option<Uuid>,
    pub status: String,
    pub runtime_name: Option<String>,
    pub runtime_ref: Option<Value>,
    pub concurrency_key: Option<String>,
    pub timeout_at: Option<DateTime<Utc>>,
    pub metadata: Value,
    pub slot_scope: Option<String>,
}

/// An idempotency scope: its key, user and agent, and the two strings
/// Python's lock hashes.
#[derive(Debug, Clone)]
pub struct IdempotencyRecord {
    pub key: String,
    pub user_id: Uuid,
    pub agent_name: String,
    pub lock: (String, String),
}

/// What a status change reads from the locked execution row.
#[derive(Debug, Clone, QueryableByName)]
pub struct LockedExecution {
    #[diesel(sql_type = Text)]
    pub status: String,
    #[diesel(sql_type = Integer)]
    pub current_attempt: i32,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    pub started_at: Option<DateTime<Utc>>,
}

/// A decided status change: the columns to set (keyed by column, as the
/// entity write takes them), whether the execution gives up its slot, and
/// what the caller gets back.
#[derive(Debug, Clone)]
pub struct Change<T> {
    pub patch: Option<Value>,
    pub release_slot: bool,
    pub output: T,
}

/// A run lease claim's outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseClaim {
    /// Claimed, with the new fence.
    Claimed(i32),
    /// Another worker holds a live lease.
    HeldByOther,
    /// No such run.
    Missing,
}

#[derive(QueryableByName)]
struct IdRow {
    #[diesel(sql_type = Nullable<SqlUuid>)]
    id: Option<Uuid>,
}

#[derive(QueryableByName)]
struct LeaseRow {
    #[diesel(sql_type = Nullable<Text>)]
    worker_id: Option<String>,
    #[diesel(sql_type = Bool)]
    live: bool,
    #[diesel(sql_type = Integer)]
    lease_fence: i32,
}

#[derive(QueryableByName)]
struct ExpiredRow {
    #[diesel(sql_type = SqlUuid, column_name = _id)]
    id: Uuid,
    #[diesel(sql_type = Nullable<SqlUuid>)]
    execution_id: Option<Uuid>,
}

// `execution_idempotency` has no entity: the columns the lookup reads.
diesel::table! {
    execution_idempotency (_id) {
        _id -> diesel::sql_types::Uuid,
        key -> diesel::sql_types::Text,
        user_id -> diesel::sql_types::Uuid,
        agent_name -> diesel::sql_types::Text,
        execution_id -> diesel::sql_types::Nullable<diesel::sql_types::Uuid>,
        _deleted -> diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>,
    }
}

fn interval(d: Duration) -> String {
    format!("{} milliseconds", d.as_millis())
}

fn beam(e: RiverbaseError) -> DbError {
    DbError::Beam(e)
}

/// Lock execution `$1`'s row for a status change, and read what it needs.
fn lock_execution(schema: &SchemaName) -> String {
    format!(
        "SELECT status::text AS status, current_attempt, started_at FROM {schema}.agent_execution \
         WHERE _id = $1 AND _deleted IS NULL FOR UPDATE"
    )
}

async fn locked(
    conn: &mut AsyncPgConnection,
    schema: &SchemaName,
    id: Uuid,
) -> DbResult<Option<LockedExecution>> {
    let rows: Vec<LockedExecution> = sql_query(lock_execution(schema))
        .bind::<SqlUuid, _>(id)
        .load(conn)
        .await?;
    Ok(rows.into_iter().next())
}

async fn patch_execution(conn: &mut AsyncPgConnection, id: Uuid, data: &Value) -> DbResult<()> {
    upsert_agent_execution(conn, id, data)
        .await
        .map_err(|e| DbError::Query(e.to_string()))
}

async fn load_execution(
    conn: &mut AsyncPgConnection,
    id: Uuid,
) -> DbResult<Option<ExecutionRecord>> {
    use crate::entity::agent_execution::agent_execution as e;
    Ok(e::table
        .filter(e::_id.eq(id))
        .filter(e::_deleted.is_null())
        .select(ExecutionRecord::as_select())
        .first(conn)
        .await
        .optional()?)
}

async fn insert_execution(
    conn: &mut AsyncPgConnection,
    new: &NewExecutionRecord,
    idempotency_key: Option<&str>,
) -> DbResult<()> {
    use crate::entity::agent_execution::{agent_execution as t, EXECUTIONSTATUS};
    // The session's owner, as the execution's rows copy it (plan 008 D1); a
    // caller's org wins.
    let owner = write::session_owner_on(conn, new.session_id)
        .await
        .map_err(|e| DbError::Query(e.to_string()))?;
    let status = enum_sql::<Text>(
        Some(&new.status),
        "status",
        "executionstatus",
        EXECUTIONSTATUS,
    )?;
    // A typed insert over the entity's schema, not the entity write: its
    // `ON CONFLICT (_id) DO NOTHING` would let two `ensure` calls with one
    // caller-chosen id both "create" it. A duplicate id must fail, as it did.
    diesel::insert_into(t::table)
        .values((
            t::_id.eq(new.id),
            t::session_id.eq(new.session_id),
            t::agent_id.eq(new.agent_id),
            t::org_id.eq(new.org_id.or(owner.org_id)),
            t::user_id.eq(owner.user_id),
            t::status.eq(status),
            t::runtime_name.eq(new.runtime_name.as_deref()),
            t::runtime_ref.eq(new.runtime_ref.as_ref()),
            t::concurrency_key.eq(new.concurrency_key.as_deref()),
            t::idempotency_key.eq(idempotency_key),
            t::timeout_at.eq(new.timeout_at),
            t::created_at.eq(Utc::now()),
            t::current_attempt.eq(0),
            t::metadata_json.eq(&new.metadata),
        ))
        .execute(conn)
        .await?;
    Ok(())
}

/// Python `_acquire_concurrency_slot`, inside the caller's transaction.
async fn acquire_slot(
    conn: &mut AsyncPgConnection,
    schema: &SchemaName,
    new: &NewExecutionRecord,
) -> DbResult<()> {
    let Some(scope) = new.slot_scope.as_deref() else {
        return Ok(());
    };
    sql_query("SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))")
        .bind::<Text, _>(CONCURRENCY_LOCK_NAMESPACE)
        .bind::<Text, _>(scope)
        .execute(&mut *conn)
        .await?;
    let owner: Vec<IdRow> = sql_query(format!(
        "SELECT execution_id AS id FROM {schema}.execution_concurrency_slot WHERE scope_key = $1 AND _deleted IS NULL"
    ))
    .bind::<Text, _>(scope)
    .load(&mut *conn)
    .await?;
    match owner.into_iter().next().and_then(|r| r.id) {
        Some(id) if id != new.id => Err(beam(crate::BEM_372.with_data(scope.to_owned()))),
        Some(_) => Ok(()),
        None => {
            let inserted = sql_query(format!(
                "INSERT INTO {schema}.execution_concurrency_slot (_id, scope_key, execution_id, org_id) \
                 VALUES (gen_random_uuid(), $1, $2, $3)"
            ))
            .bind::<Text, _>(scope)
            .bind::<SqlUuid, _>(new.id)
            .bind::<Nullable<SqlUuid>, _>(new.org_id)
            .execute(conn)
            .await;
            match inserted.map_err(DbError::from) {
                Ok(_) => Ok(()),
                Err(DbError::Conflict(_)) => Err(beam(crate::BEM_372.with_data(scope.to_owned()))),
                Err(e) => Err(e),
            }
        }
    }
}

async fn lookup_idempotency_row(
    conn: &mut AsyncPgConnection,
    key: &str,
    user_id: Uuid,
    agent_name: &str,
) -> DbResult<Option<Uuid>> {
    use execution_idempotency as i;
    let row: Option<Option<Uuid>> = i::table
        .filter(i::key.eq(key))
        .filter(i::user_id.eq(user_id))
        .filter(i::agent_name.eq(agent_name))
        .filter(i::_deleted.is_null())
        .select(i::execution_id)
        .first(conn)
        .await
        .optional()?;
    Ok(row.flatten())
}

/// The executions on a [`Db`].
pub struct Executions<'d> {
    db: &'d Db,
}

impl Db {
    /// Executions and their run leases.
    #[must_use]
    pub fn executions(&self) -> Executions<'_> {
        Executions { db: self }
    }
}

impl Executions<'_> {
    /// Execution `id`.
    ///
    /// # Errors
    /// A failed checkout or read.
    pub async fn get(&self, id: Uuid) -> DbResult<Option<ExecutionRecord>> {
        self.db
            .scoped(|conn| async move { load_execution(conn, id).await }.scope_boxed())
            .await
    }

    /// Set the columns `data` names on execution `id` (the entity write's
    /// `_update`).
    ///
    /// # Errors
    /// A failed checkout or write.
    pub async fn patch(&self, id: Uuid, data: &Value) -> DbResult<()> {
        self.db
            .scoped(|conn| async move { patch_execution(conn, id, data).await }.scope_boxed())
            .await
    }

    /// The session's executions, oldest first.
    ///
    /// # Errors
    /// A failed checkout or read.
    pub async fn list_for_session(&self, session_id: Uuid) -> DbResult<Vec<ExecutionRecord>> {
        use crate::entity::agent_execution::agent_execution as e;
        self.db
            .scoped(|conn| {
                async move {
                    Ok(e::table
                        .filter(e::session_id.eq(session_id))
                        .filter(e::_deleted.is_null())
                        .order((e::created_at.asc(), e::_id.asc()))
                        .select(ExecutionRecord::as_select())
                        .load(conn)
                        .await?)
                }
                .scope_boxed()
            })
            .await
    }

    /// The execution an idempotency key names.
    ///
    /// # Errors
    /// A failed checkout or read.
    pub async fn lookup_idempotency(
        &self,
        key: &str,
        user_id: Uuid,
        agent_name: &str,
    ) -> DbResult<Option<Uuid>> {
        self.db
            .scoped(|conn| {
                async move { lookup_idempotency_row(conn, key, user_id, agent_name).await }
                    .scope_boxed()
            })
            .await
    }

    /// The execution action `action_id` belongs to.
    ///
    /// # Errors
    /// A failed checkout or read.
    pub async fn execution_for_action(&self, action_id: Uuid) -> DbResult<Option<Uuid>> {
        use crate::entity::agent_action::agent_action as a;
        self.db
            .scoped(|conn| {
                async move {
                    let execution: Option<Option<Uuid>> = a::table
                        .filter(a::_id.eq(action_id))
                        .filter(a::_deleted.is_null())
                        .select(a::execution_id)
                        .first(conn)
                        .await
                        .optional()?;
                    Ok(execution.flatten())
                }
                .scope_boxed()
            })
            .await
    }

    /// Insert `new` and take its slot, in one transaction; the stored row.
    ///
    /// # Errors
    /// `BEM-372` when another execution holds the slot; `Conflict` for a
    /// duplicate id; a failed checkout or statement (rolled back).
    pub async fn create(&self, new: &NewExecutionRecord) -> DbResult<Option<ExecutionRecord>> {
        let s = self.db.schema();
        self.db
            .transaction::<_, DbError, _>(|tx| {
                Box::pin(async move {
                    insert_execution(tx, new, None).await?;
                    acquire_slot(tx, s, new).await?;
                    load_execution(tx, new.id).await
                })
            })
            .await
    }

    /// Python's start-or-get, in one transaction under the idempotency lock:
    /// the execution the key already names, or `new` inserted with its key
    /// and slot. A lost race is `Conflict`; the caller then reads the winner.
    ///
    /// # Errors
    /// `BEM-371` when the key's execution is missing; `BEM-372` when the slot
    /// is taken; `Conflict`; a failed checkout or statement (rolled back).
    pub async fn start_or_get(
        &self,
        new: &NewExecutionRecord,
        idempotency: &IdempotencyRecord,
    ) -> DbResult<Option<ExecutionRecord>> {
        let s = self.db.schema();
        self.db
            .transaction::<_, DbError, _>(|tx| {
                Box::pin(async move {
                    let (a, b) = &idempotency.lock;
                    sql_query("SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))")
                        .bind::<Text, _>(a)
                        .bind::<Text, _>(b)
                        .execute(&mut *tx)
                        .await?;
                    if let Some(existing) = lookup_idempotency_row(
                        tx,
                        &idempotency.key,
                        idempotency.user_id,
                        &idempotency.agent_name,
                    )
                    .await?
                    {
                        return match load_execution(tx, existing).await? {
                            Some(row) => Ok(Some(row)),
                            None => Err(beam(
                                crate::BEM_371.with_data("idempotency winner is missing"),
                            )),
                        };
                    }
                    insert_execution(tx, new, Some(&idempotency.key)).await?;
                    sql_query(format!(
                        "INSERT INTO {s}.execution_idempotency (_id, key, user_id, agent_name, execution_id) \
                         VALUES (gen_random_uuid(), $1, $2, $3, $4)"
                    ))
                    .bind::<Text, _>(&idempotency.key)
                    .bind::<SqlUuid, _>(idempotency.user_id)
                    .bind::<Text, _>(&idempotency.agent_name)
                    .bind::<SqlUuid, _>(new.id)
                    .execute(&mut *tx)
                    .await?;
                    acquire_slot(tx, s, new).await?;
                    load_execution(tx, new.id).await
                })
            })
            .await
    }

    /// Lock execution `id`, let `decide` choose the change from what it
    /// read, and apply it (the patch, then the slot release) in the same
    /// transaction. `None` when there is no such execution.
    ///
    /// # Errors
    /// `decide`'s error (rolled back); a failed checkout or statement.
    pub async fn update_locked<T, F>(&self, id: Uuid, decide: F) -> DbResult<Option<T>>
    where
        T: Send + 'static,
        F: FnOnce(LockedExecution) -> Result<Change<T>, RiverbaseError> + Send,
    {
        let s = self.db.schema();
        self.db
            .transaction::<_, DbError, _>(|tx| {
                Box::pin(async move {
                    let Some(row) = locked(tx, s, id).await? else {
                        return Ok(None);
                    };
                    let change = decide(row).map_err(beam)?;
                    if let Some(patch) = &change.patch {
                        patch_execution(tx, id, patch).await?;
                    }
                    if change.release_slot {
                        sql_query(format!(
                            "DELETE FROM {s}.execution_concurrency_slot WHERE execution_id = $1"
                        ))
                        .bind::<SqlUuid, _>(id)
                        .execute(tx)
                        .await?;
                    }
                    Ok(Some(change.output))
                })
            })
            .await
    }

    /// Move execution `id` back to `waiting` when it is still `running`
    /// attempt `attempt`; whether it moved. One statement: the row lock
    /// orders it against a competing attempt's increment.
    ///
    /// # Errors
    /// A failed checkout or update.
    pub async fn return_to_waiting(
        &self,
        id: Uuid,
        attempt: i32,
        waiting: &str,
        running: &str,
    ) -> DbResult<bool> {
        let s = self.db.schema();
        self.db
            .scoped(|conn| {
                async move {
                    let moved: Vec<IdRow> = sql_query(format!(
                        "UPDATE {s}.agent_execution SET status = CAST($3 AS {s}.executionstatus), \
                         interrupted_at = now(), {TOUCH} \
                         WHERE _id = $1 AND status = CAST($4 AS {s}.executionstatus) \
                         AND current_attempt = $2 AND _deleted IS NULL \
                         RETURNING _id AS id"
                    ))
                    .bind::<SqlUuid, _>(id)
                    .bind::<Integer, _>(attempt)
                    .bind::<Text, _>(waiting)
                    .bind::<Text, _>(running)
                    .load(conn)
                    .await?;
                    Ok(!moved.is_empty())
                }
                .scope_boxed()
            })
            .await
    }

    /// Mark execution `id` retrying, in one transaction. `decide` reads its
    /// locked status: skip it, retry it, or retry it unless `run_id` still
    /// holds a live lease. With `run_id`, the run is interrupted and its
    /// lease cleared. Whether it was marked.
    ///
    /// # Errors
    /// `decide`'s error (rolled back); a failed checkout or statement.
    pub async fn mark_retrying<F>(
        &self,
        id: Uuid,
        run_id: Option<Uuid>,
        labels: RetryLabels<'_>,
        decide: F,
    ) -> DbResult<bool>
    where
        F: FnOnce(&str) -> Result<RetryStep, RiverbaseError> + Send,
    {
        let s = self.db.schema();
        self.db
            .transaction::<_, DbError, _>(|tx| {
                Box::pin(async move {
                    let Some(row) = locked(tx, s, id).await? else {
                        return Ok(false);
                    };
                    match decide(&row.status).map_err(beam)? {
                        RetryStep::Skip => return Ok(false),
                        RetryStep::Retry => {}
                        RetryStep::UnlessLeased => {
                            if let Some(run) = run_id {
                                let live: Vec<Counted> = sql_query(format!(
                                    "SELECT count(*) AS n FROM {s}.agent_run WHERE _id = $1 AND lease_expires_at > now()"
                                ))
                                .bind::<SqlUuid, _>(run)
                                .load(&mut *tx)
                                .await?;
                                if <[Counted]>::first(&live).is_some_and(|c| c.n > 0) {
                                    return Ok(false);
                                }
                            }
                        }
                    }
                    patch_execution(tx, id, &json!({"_update": true, "status": labels.retrying}))
                        .await?;
                    if let Some(run) = run_id {
                        let data = json!({
                            "_update": true,
                            "status": labels.interrupted_run,
                            "ended_at": Utc::now(),
                            "worker_id": null,
                            "lease_id": null,
                            "lease_expires_at": null,
                        });
                        upsert_agent_run(tx, run, &data)
                            .await
                            .map_err(|e| DbError::Query(e.to_string()))?;
                    }
                    Ok(true)
                })
            })
            .await
    }

    /// Expire the pending actions of execution `id`; how many.
    ///
    /// # Errors
    /// A failed checkout or update.
    pub async fn expire_pending_actions(&self, id: Uuid) -> DbResult<u64> {
        self.db
            .scoped(|conn| {
                async move {
                    write::expire_pending_actions(conn, id)
                        .await
                        .map_err(|e| DbError::Query(e.to_string()))
                }
                .scope_boxed()
            })
            .await
    }

    /// Claim run `run_id`'s lease for `worker_id` in one transaction: the
    /// run is locked, refused when another worker's lease is live, and its
    /// fence moves on.
    ///
    /// # Errors
    /// A failed checkout or statement.
    pub async fn claim_run_lease(
        &self,
        run_id: Uuid,
        worker_id: &str,
        lease_id: Uuid,
        ttl: Duration,
    ) -> DbResult<LeaseClaim> {
        let s = self.db.schema();
        self.db
            .transaction::<_, DbError, _>(|tx| {
                Box::pin(async move {
                    let rows: Vec<LeaseRow> = sql_query(format!(
                        "SELECT worker_id, (lease_id IS NOT NULL AND lease_expires_at > now()) AS live, lease_fence \
                         FROM {s}.agent_run WHERE _id = $1 AND _deleted IS NULL FOR UPDATE"
                    ))
                    .bind::<SqlUuid, _>(run_id)
                    .load(&mut *tx)
                    .await?;
                    let Some(row) = rows.into_iter().next() else {
                        return Ok(LeaseClaim::Missing);
                    };
                    if row.live && row.worker_id.as_deref() != Some(worker_id) {
                        return Ok(LeaseClaim::HeldByOther);
                    }
                    let fence = row.lease_fence + 1;
                    sql_query(format!(
                        "UPDATE {s}.agent_run SET worker_id = $2, lease_id = $3, \
                         lease_expires_at = now() + CAST($4 AS interval), retry_reason = NULL, lease_fence = $5, {TOUCH} \
                         WHERE _id = $1"
                    ))
                    .bind::<SqlUuid, _>(run_id)
                    .bind::<Text, _>(worker_id)
                    .bind::<SqlUuid, _>(lease_id)
                    .bind::<Text, _>(interval(ttl))
                    .bind::<Integer, _>(fence)
                    .execute(tx)
                    .await?;
                    Ok(LeaseClaim::Claimed(fence))
                })
            })
            .await
    }

    /// Extend `fence`'s lease by `ttl`; `false` when it is no longer the
    /// run's (another claim moved the fence).
    ///
    /// # Errors
    /// A failed checkout or update.
    pub async fn heartbeat(&self, fence: Fence, worker_id: &str, ttl: Duration) -> DbResult<bool> {
        let s = self.db.schema();
        self.db
            .scoped(|conn| {
                async move {
                    let n = sql_query(format!(
                        "UPDATE {s}.agent_run SET lease_expires_at = now() + CAST($5 AS interval) \
                         WHERE _id = $1 AND lease_id = $2 AND worker_id = $3 AND lease_fence = $4"
                    ))
                    .bind::<SqlUuid, _>(fence.run_id)
                    .bind::<SqlUuid, _>(fence.lease_id)
                    .bind::<Text, _>(worker_id)
                    .bind::<Integer, _>(fence.fence)
                    .bind::<Text, _>(interval(ttl))
                    .execute(conn)
                    .await?;
                    Ok(n > 0)
                }
                .scope_boxed()
            })
            .await
    }

    /// Release `fence`'s lease (no-op when it is no longer the run's).
    ///
    /// # Errors
    /// A failed checkout or update.
    pub async fn release_lease(&self, fence: Fence) -> DbResult<()> {
        let s = self.db.schema();
        self.db
            .scoped(|conn| {
                async move {
                    sql_query(format!(
                        "UPDATE {s}.agent_run SET worker_id = NULL, lease_id = NULL, lease_expires_at = NULL, {TOUCH} \
                         WHERE _id = $1 AND lease_id = $2 AND lease_fence = $3"
                    ))
                    .bind::<SqlUuid, _>(fence.run_id)
                    .bind::<SqlUuid, _>(fence.lease_id)
                    .bind::<Integer, _>(fence.fence)
                    .execute(conn)
                    .await?;
                    Ok(())
                }
                .scope_boxed()
            })
            .await
    }

    /// Whether `fence` is still the run's lease.
    ///
    /// # Errors
    /// A failed checkout or read.
    pub async fn fence_holds(&self, fence: Fence) -> DbResult<bool> {
        let s = self.db.schema();
        self.db
            .scoped(|conn| {
                async move {
                    let rows: Vec<Counted> = sql_query(format!(
                        "SELECT count(*) AS n FROM {s}.agent_run WHERE _id = $1 AND lease_id = $2 AND lease_fence = $3"
                    ))
                    .bind::<SqlUuid, _>(fence.run_id)
                    .bind::<SqlUuid, _>(fence.lease_id)
                    .bind::<Integer, _>(fence.fence)
                    .load(conn)
                    .await?;
                    Ok(<[Counted]>::first(&rows).is_some_and(|c| c.n > 0))
                }
                .scope_boxed()
            })
            .await
    }

    /// Running runs whose lease expired, by the database clock, oldest
    /// first: `(run, execution)`.
    ///
    /// # Errors
    /// A failed checkout or read.
    pub async fn expired_leases(&self) -> DbResult<Vec<(Uuid, Option<Uuid>)>> {
        let s = self.db.schema();
        self.db
            .scoped(|conn| {
                async move {
                    let rows: Vec<ExpiredRow> = sql_query(format!(
                        "SELECT _id, execution_id FROM {s}.agent_run WHERE status = CAST('RUNNING' AS {s}.runstatus) \
                         AND lease_expires_at <= now() AND _deleted IS NULL ORDER BY lease_expires_at"
                    ))
                    .load(conn)
                    .await?;
                    Ok(rows.into_iter().map(|r| (r.id, r.execution_id)).collect())
                }
                .scope_boxed()
            })
            .await
    }
}

/// What [`Executions::mark_retrying`] does with an execution's status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryStep {
    /// Leave it.
    Skip,
    /// Mark it retrying.
    Retry,
    /// Mark it retrying unless its run holds a live lease.
    UnlessLeased,
}

/// The status labels [`Executions::mark_retrying`] writes.
#[derive(Debug, Clone, Copy)]
pub struct RetryLabels<'l> {
    pub retrying: &'l str,
    /// The run's status once interrupted.
    pub interrupted_run: &'l str,
}
