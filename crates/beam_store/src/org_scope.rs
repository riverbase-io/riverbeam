//! Org scoping for the optional row-level security (`crates/beam_store/sql/rls.sql`).
//!
//! With RLS on (`BEAM_RLS_ENABLED=true`, or [`set_rls_enabled`]), every
//! checkout through [`crate::Db`] sets `beam.org_id` to the running task's
//! org ([`with_org`]) for its transaction only (`set_config(..., true)`), so
//! no pooled connection keeps an org between checkouts (plan 025). Outside an
//! org scope the setting is empty, so RLS tables refuse the query (fail
//! closed). Work across orgs (the queue, reconcile, retention) needs a role
//! that bypasses RLS.

use std::future::Future;
use std::sync::atomic::{AtomicU8, Ordering};

use diesel::result::QueryResult;
use diesel::sql_query;
use diesel::sql_types::Text;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use uuid::Uuid;

tokio::task_local! {
    static ORG: Option<Uuid>;
}

/// 0 unknown (read the env), 1 off, 2 on.
static RLS: AtomicU8 = AtomicU8::new(0);

/// Whether stores set `beam.org_id` on their connections.
#[must_use]
pub fn rls_enabled() -> bool {
    match AtomicU8::load(&RLS, Ordering::Relaxed) {
        1 => false,
        2 => true,
        _ => {
            let on = beam_env::flag("BEAM_RLS_ENABLED", false);
            RLS.store(if on { 2 } else { 1 }, Ordering::Relaxed);
            on
        }
    }
}

/// Override `BEAM_RLS_ENABLED` for this process.
pub fn set_rls_enabled(enabled: bool) {
    RLS.store(if enabled { 2 } else { 1 }, Ordering::Relaxed);
}

/// Run `fut` with `org` (a UUID, as `coerce_org_id` reads it) as the task's org.
pub async fn with_org<F: Future>(org: Option<&str>, fut: F) -> F::Output {
    let org = org.and_then(|o| Uuid::parse_str(o.trim()).ok());
    ORG.scope(org, fut).await
}

/// `fut` with the running task's org, for `tokio::spawn` (a spawned task
/// does not inherit task-locals).
pub fn propagate<F: Future>(fut: F) -> impl Future<Output = F::Output> {
    ORG.scope(current_org(), fut)
}

/// The running task's org, if any.
#[must_use]
pub fn current_org() -> Option<Uuid> {
    ORG.try_with(|o| *o).ok().flatten()
}

/// Set `org` as `beam.org_id` for the current transaction (no-op unless RLS
/// is on). Outside a transaction it would last one statement: callers run it
/// first in a transaction ([`crate::Db`], [`crate::entity::write::begin`]).
pub(crate) async fn apply(conn: &mut AsyncPgConnection, org: Option<Uuid>) -> QueryResult<()> {
    if !rls_enabled() {
        return Ok(());
    }
    let org = org.map(|o| o.to_string()).unwrap_or_default();
    sql_query("SELECT set_config('beam.org_id', $1, true)")
        .bind::<Text, _>(org)
        .execute(conn)
        .await
        .map(|_| ())
}
