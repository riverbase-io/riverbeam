//! Writing through the beam entities outside a riverbase command:
//! a unit of work of its own, scoped by the running task's org (optional
//! RLS), committed when the write succeeds and rolled back when it fails.
//!
//! Riverbase's `PgTransaction` has no `Drop`: a unit of work dropped before
//! `COMMIT` / `ROLLBACK` would go back to the pool still open, holding its
//! locks, for the next borrower to write into. The writes here run inside
//! futures that are cancelled (a run stopped or timed out, a lost lease), so
//! [`UnitOfWork`] rolls itself back when it is dropped unfinished.

use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};

use diesel::sql_types::Text;
use diesel::{ExpressionMethods, OptionalExtension, QueryDsl};
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use riverbase_core::datastore::{CommandUnitOfWork, DataStore, PgDataStore};
use riverbase_core::RiverbaseResult;
use serde_json::Value;
use uuid::Uuid;

use super::value::{enum_sql, new_etag, now};
use crate::rows::{ActionStatus, DbEnum};

/// A unit of work that is always finished: [`finish`] commits or rolls it
/// back, and dropping it unfinished (a cancelled future) rolls it back on a
/// spawned task, so its connection only returns to the pool afterwards.
pub struct UnitOfWork {
    inner: CommandUnitOfWork,
    finished: AtomicBool,
}

impl Deref for UnitOfWork {
    type Target = CommandUnitOfWork;

    fn deref(&self) -> &CommandUnitOfWork {
        &self.inner
    }
}

impl Drop for UnitOfWork {
    fn drop(&mut self) {
        // Fully qualified: `RunQueryDsl::load` (in scope, implemented for every
        // type, by value) would win method lookup over `AtomicBool::load`.
        if AtomicBool::load(&self.finished, Ordering::Acquire) {
            return;
        }
        // The clone keeps the transaction's connection until the rollback.
        let uow = self.inner.clone();
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn(async move { rollback(&uow).await });
            }
            Err(_) => {
                tracing::warn!("beam entity write dropped outside a runtime; not rolled back");
            }
        }
    }
}

/// A new unit of work on `datastore`, with the task's org set for its
/// transaction (`org_scope::apply`; see [`crate::Db`]).
///
/// # Errors
/// When the transaction cannot begin, or the scope cannot be set (the
/// transaction is rolled back then).
pub async fn begin(datastore: &PgDataStore) -> RiverbaseResult<UnitOfWork> {
    let uow = UnitOfWork {
        inner: datastore.begin_command_transaction().await?,
        finished: AtomicBool::new(false),
    };
    let scoped = async {
        let mut conn = uow.postgres_connection().await?;
        crate::org_scope::apply(&mut conn, crate::org_scope::current_org())
            .await
            .map_err(|e| crate::BEM_137.with_data(e.to_string()))
    }
    .await;
    match scoped {
        Ok(()) => Ok(uow),
        Err(e) => finish(&uow, Err(e)).await,
    }
}

async fn rollback(uow: &CommandUnitOfWork) {
    if let Err(e) = uow.rollback().await {
        tracing::warn!(error = %e, "beam entity write rollback failed");
    }
}

/// Commit `uow` when `result` is `Ok`, else roll it back and return the
/// error.
///
/// # Errors
/// `result`'s error, or the commit's.
pub async fn finish<T>(uow: &UnitOfWork, result: RiverbaseResult<T>) -> RiverbaseResult<T> {
    let finished = match result {
        Ok(value) => uow.commit().await.map(|()| value),
        Err(e) => {
            rollback(uow).await;
            Err(e)
        }
    };
    // Done either way: a failed COMMIT ends the transaction too.
    uow.finished.store(true, Ordering::Release);
    finished
}

/// Write `data` (keyed by physical column) to row `id` of `table` through
/// its entity on `uow`; see the generated `upsert_<table>` for `_insert` and
/// `_update`.
///
/// # Errors
/// `BEM-146` for a malformed value; store errors.
pub async fn put(
    datastore: &PgDataStore,
    uow: &CommandUnitOfWork,
    table: &str,
    id: Uuid,
    data: Value,
) -> RiverbaseResult<()> {
    let resource = datastore.registry().parse(table)?;
    datastore.upsert(uow, resource, &id.to_string(), data).await
}

/// A session's owner, which the rows of the session copy (plan 008 D1).
#[derive(Debug, Clone, Copy, Default)]
pub struct Owner {
    pub user_id: Option<Uuid>,
    pub org_id: Option<Uuid>,
}

/// The owner of session `session_id`, read on `uow`; no owner for a session
/// that is not stored.
///
/// # Errors
/// Store errors.
pub async fn session_owner(uow: &CommandUnitOfWork, session_id: Uuid) -> RiverbaseResult<Owner> {
    let mut conn = uow.postgres_connection().await?;
    session_owner_on(&mut conn, session_id).await
}

/// [`session_owner`] on a connection of the caller's (a store's own
/// transaction).
///
/// # Errors
/// Store errors.
pub(crate) async fn session_owner_on(
    conn: &mut AsyncPgConnection,
    session_id: Uuid,
) -> RiverbaseResult<Owner> {
    use crate::entity::agent_session::agent_session as t;
    let owner: Option<(Option<Uuid>, Option<Uuid>)> = t::table
        .filter(t::_id.eq(session_id))
        .select((t::user_id, t::org_id))
        .first(conn)
        .await
        .optional()?;
    Ok(
        owner.map_or_else(Owner::default, |(user_id, org_id)| Owner {
            user_id,
            org_id,
        }),
    )
}

/// Expire the pending actions of execution `execution_id` on `conn`, with a
/// new `_updated` and `_etag` each; the number expired. Shared by the beam
/// store and the execution records.
///
/// # Errors
/// Store errors.
pub(crate) async fn expire_pending_actions(
    conn: &mut AsyncPgConnection,
    execution_id: Uuid,
) -> RiverbaseResult<u64> {
    use crate::entity::agent_action::{agent_action as t, ACTIONSTATUS};
    let label = |status: ActionStatus| {
        enum_sql::<Text>(
            Some(status.db_label()),
            "status",
            "actionstatus",
            ACTIONSTATUS,
        )
    };
    let (pending, expired) = (label(ActionStatus::Pending)?, label(ActionStatus::Expired)?);
    let n = diesel::update(
        t::table
            .filter(t::execution_id.eq(execution_id))
            .filter(t::status.eq(pending))
            .filter(t::_deleted.is_null()),
    )
    .set((
        t::status.eq(expired),
        t::_updated.eq(now()),
        t::_etag.eq(new_etag()),
    ))
    .execute(conn)
    .await?;
    Ok(u64::try_from(n).unwrap_or(0))
}

/// A table [`exists`] checks.
#[derive(Debug, Clone, Copy)]
pub enum Table {
    AgentAction,
    AgentRevision,
}

/// Whether `table` has a row `id` (soft-deleted or not), read on `uow`.
///
/// # Errors
/// Store errors.
pub async fn exists(uow: &CommandUnitOfWork, table: Table, id: Uuid) -> RiverbaseResult<bool> {
    use crate::entity::agent_action::agent_action;
    use crate::entity::agent_revision::agent_revision;
    let mut conn = uow.postgres_connection().await?;
    let found = match table {
        Table::AgentAction => {
            diesel::select(diesel::dsl::exists(
                agent_action::table.filter(agent_action::_id.eq(id)),
            ))
            .get_result(&mut *conn)
            .await?
        }
        Table::AgentRevision => {
            diesel::select(diesel::dsl::exists(
                agent_revision::table.filter(agent_revision::_id.eq(id)),
            ))
            .get_result(&mut *conn)
            .await?
        }
    };
    Ok(found)
}
