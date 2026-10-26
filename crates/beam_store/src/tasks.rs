//! The durable work queue, `ref_beam.agent_task`.
//!
//! A domain command inserts a row with [`enqueue`] on its own transaction and
//! emits a [`TASK_TOPIC`] hint. A worker claims the row with [`claim_by_id`]
//! (the hint) or [`claim_next_of`] (its polling tick): one compare-and-swap
//! statement with `FOR UPDATE SKIP LOCKED`, so a row runs on one worker at a
//! time. Both claim only the task names they are given (a [`Lane`]'s), so a
//! task this version does not know stays queued for a worker that does. The
//! worker renews its lease while it runs and records the outcome with
//! [`complete`] or [`fail`]; both check that it still owns the lease.
//! [`sweep_expired`] requeues a row whose worker died, or fails it.
//!
//! Every function takes the connection to run on, so a command can enqueue
//! inside its transaction and a worker can use any pooled connection. They
//! are reached through the repositories: `db.tasks()` for workers, and the
//! domain's `CommandExec::enqueue_task` for commands.

use std::time::Duration;

use diesel::sql_query;
use diesel::sql_types::{Array, Double, Jsonb, Text, Uuid as SqlUuid};
use diesel::{
    ExpressionMethods, OptionalExtension, QueryDsl, Queryable, QueryableByName, Selectable,
    SelectableHelper,
};
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::schema::SchemaName;
use crate::store::StoreResult;

/// Hint emitted after a task is enqueued: `{"task_id": "<uuid>", "task": "<name>"}`.
pub const TASK_TOPIC: &str = "beam.queue.task";
/// Emitted after a revision or canary change, so every worker reloads its
/// revision book.
pub const REVISION_TOPIC: &str = "beam.revision.changed";

/// The task that runs an agent (`invoke`, `stream`, `resume`).
pub const INVOKE_TASK: &str = WorkerTask::InvokeAgent.as_str();

/// Declares [`WorkerTask`] from one list of variants and stored names, so
/// every variant has a name and is in [`WorkerTask::ALL`], which `parse`
/// and the lanes read: no task can exist that a worker never claims.
macro_rules! worker_tasks {
    ($($(#[$doc:meta])* $variant:ident => $name:literal,)+) => {
        /// Every task a worker runs, by its stored name. Commands queue them
        /// with [`NewTask::of`]; a worker claims only these names.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum WorkerTask {
            $($(#[$doc])* $variant,)+
        }

        impl WorkerTask {
            /// Every task.
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];

            /// The stored task name.
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $name,)+
                }
            }
        }
    };
}

worker_tasks! {
    InvokeAgent => "invoke_agent",
    IngestKnowledgeDocument => "ingest_knowledge_document",
    DeleteKnowledgeDocument => "delete_knowledge_document",
    RememberMemory => "remember_memory",
    EditMemory => "edit_memory",
    ExpireMemory => "expire_memory",
    ForgetMemory => "forget_memory",
    CancelExecution => "cancel_execution",
    RetryExecution => "retry_execution",
    /// Publishes the terminal frame of a run that ended without its
    /// pipeline (a queued run cancelled before it ran).
    EndRun => "end_run",
    // Operations.
    ReconcileNow => "reconcile_now",
    ListDeadLetters => "list_dead_letters",
    RetryDeadLetter => "retry_dead_letter",
    DiscardDeadLetter => "discard_dead_letter",
    DeleteUserAgentData => "delete_user_agent_data",
    // Revisions and evals.
    MarkRevisionValidating => "mark_revision_validating",
    ActivateRevision => "activate_revision",
    RetireRevision => "retire_revision",
    ListRevisions => "list_revisions",
    SubmitAgentFeedback => "submit_agent_feedback",
    RunEvaluation => "run_evaluation",
}

impl WorkerTask {
    /// The task stored as `raw`, if this version runs it.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        Self::ALL.iter().copied().find(|task| task.as_str() == raw)
    }

    /// The lane whose permits run it.
    #[must_use]
    pub const fn lane(self) -> Lane {
        match self {
            Self::CancelExecution | Self::EndRun => Lane::Control,
            Self::InvokeAgent | Self::RetryExecution => Lane::Interactive,
            Self::IngestKnowledgeDocument
            | Self::DeleteKnowledgeDocument
            | Self::RememberMemory
            | Self::EditMemory
            | Self::ExpireMemory
            | Self::ForgetMemory
            | Self::ReconcileNow
            | Self::ListDeadLetters
            | Self::RetryDeadLetter
            | Self::DiscardDeadLetter
            | Self::DeleteUserAgentData
            | Self::MarkRevisionValidating
            | Self::ActivateRevision
            | Self::RetireRevision
            | Self::ListRevisions
            | Self::SubmitAgentFeedback
            | Self::RunEvaluation => Lane::Batch,
        }
    }
}

/// A group of tasks with permits of its own, so one kind of work never
/// waits behind another: control tasks act on the runs they would wait
/// behind, and a burst of batch work must not hold up user runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lane {
    /// Cancels and run endings.
    Control,
    /// Agent runs a user waits for.
    Interactive,
    /// Memory, knowledge, operations, revisions, feedback and evaluations.
    Batch,
}

impl Lane {
    /// Every lane, in the order a worker's tick claims from them.
    pub const ALL: [Self; 3] = [Self::Control, Self::Interactive, Self::Batch];

    /// The names of the tasks in this lane.
    #[must_use]
    pub fn names(self) -> Vec<&'static str> {
        WorkerTask::ALL
            .iter()
            .copied()
            .filter(|task| task.lane() == self)
            .map(WorkerTask::as_str)
            .collect()
    }
}

/// A task row's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl TaskStatus {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "QUEUED",
            Self::Running => "RUNNING",
            Self::Succeeded => "SUCCEEDED",
            Self::Failed => "FAILED",
            Self::Cancelled => "CANCELLED",
        }
    }

    /// The status stored as `raw`, if it is one.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "QUEUED" => Some(Self::Queued),
            "RUNNING" => Some(Self::Running),
            "SUCCEEDED" => Some(Self::Succeeded),
            "FAILED" => Some(Self::Failed),
            "CANCELLED" => Some(Self::Cancelled),
            _ => None,
        }
    }
}

/// A task to enqueue.
#[derive(Debug, Clone)]
pub struct NewTask {
    pub id: Uuid,
    /// The worker task name (a [`WorkerTask`]'s).
    pub task: String,
    pub payload: Value,
    /// Whether a task whose worker died may run again.
    pub retryable: bool,
    pub max_attempts: i32,
    pub user_id: Option<Uuid>,
    pub org_id: Option<Uuid>,
    pub execution_id: Option<Uuid>,
    /// The riverbase command that enqueued it.
    pub src_cmd: Option<Uuid>,
}

impl NewTask {
    /// Task `task` of the catalog, retryable with three attempts and no owner.
    #[must_use]
    pub fn of(task: WorkerTask, payload: Value) -> Self {
        Self::new(task.as_str(), payload)
    }

    /// A retryable task with three attempts and no owner, by any name: for
    /// tests and for rows of other versions. Commands use [`Self::of`].
    #[must_use]
    pub fn new(task: impl Into<String>, payload: Value) -> Self {
        Self {
            id: Uuid::new_v4(),
            task: task.into(),
            payload,
            retryable: true,
            max_attempts: 3,
            user_id: None,
            org_id: None,
            execution_id: None,
            src_cmd: None,
        }
    }
}

/// A task row: a typed select over the generated `agent_task` schema
/// and read by name from the claims' `RETURNING` (`COLUMNS`).
#[derive(Debug, Clone, Queryable, Selectable, QueryableByName)]
#[diesel(table_name = crate::entity::agent_task::agent_task)]
pub struct TaskRow {
    #[diesel(column_name = _id)]
    pub id: Uuid,
    pub task: String,
    pub payload: Value,
    pub status: String,
    pub retryable: bool,
    pub attempt_count: i32,
    pub max_attempts: i32,
    pub user_id: Option<Uuid>,
    pub org_id: Option<Uuid>,
    pub execution_id: Option<Uuid>,
    pub lease_owner: Option<String>,
    pub result: Option<Value>,
    pub error: Option<Value>,
}

impl TaskRow {
    /// The row's status, if it is a known one.
    #[must_use]
    pub fn status(&self) -> Option<TaskStatus> {
        TaskStatus::parse(&self.status)
    }
}

#[derive(QueryableByName)]
struct IdRow {
    #[diesel(sql_type = SqlUuid)]
    id: Uuid,
}

/// The columns [`TaskRow`] reads by name, for the SQL that stays (claims).
const COLUMNS: &str = "t._id, t.task, t.payload, t.status, t.retryable, \
    t.attempt_count, t.max_attempts, t.user_id, t.org_id, t.execution_id, t.lease_owner, \
    t.result, t.error";

fn db(e: impl std::fmt::Display) -> riverbase_core::RiverbaseError {
    tracing::error!(error = %e, "beam work queue failed");
    crate::BEM_507.raise()
}

fn secs(d: Duration) -> f64 {
    d.as_secs_f64().max(1.0)
}

/// Insert `task` as `QUEUED`. Like the other typed queries here,
/// it names the table without a schema: `conn` must have the schema's
/// `search_path` ([`SchemaName::pool_url`]); `_schema` is kept for callers.
///
/// # Errors
/// `BEM-507` when the insert fails.
pub(crate) async fn enqueue(
    conn: &mut AsyncPgConnection,
    _schema: &SchemaName,
    task: &NewTask,
) -> StoreResult<()> {
    // Through the task's entity (plan 014 D3), on `conn`'s transaction.
    let data = json!({
        "task": task.task,
        "payload": task.payload,
        "status": TaskStatus::Queued.as_str(),
        "retryable": task.retryable,
        "max_attempts": task.max_attempts.max(1),
        "user_id": task.user_id,
        "org_id": task.org_id,
        "execution_id": task.execution_id,
        "src_cmd": task.src_cmd,
        "_insert": true,
    });
    crate::entity::agent_task::upsert_agent_task(conn, task.id, &data)
        .await
        .map_err(db)
}

/// One task by id.
///
/// # Errors
/// `BEM-507` when the read fails.
pub(crate) async fn get(
    conn: &mut AsyncPgConnection,
    _schema: &SchemaName,
    id: Uuid,
) -> StoreResult<Option<TaskRow>> {
    use crate::entity::agent_task::agent_task as t;
    t::table
        .filter(t::_id.eq(id))
        .filter(t::_deleted.is_null())
        .select(TaskRow::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(db)
}

/// The claim statement: move one due `QUEUED` row named in `$3` (row `$4`
/// when `only`) to `RUNNING` under `owner`'s lease. The inner `SKIP LOCKED`
/// select and the outer update are one statement, so two workers never
/// claim the same row.
fn claim_sql(schema: &SchemaName, only: bool) -> String {
    let only = if only {
        " AND q.task = ANY($3) AND q._id = $4"
    } else {
        " AND q.task = ANY($3)"
    };
    format!(
        "UPDATE {schema}.agent_task t SET status = 'RUNNING', attempt_count = t.attempt_count + 1, \
         lease_owner = $1, lease_expires_at = now() + make_interval(secs => $2), \
         started_at = COALESCE(t.started_at, now()), _updated = now() \
         WHERE t._id = (SELECT q._id FROM {schema}.agent_task q \
           WHERE q.status = 'QUEUED' AND q._deleted IS NULL AND q.next_attempt_at <= now(){only} \
           ORDER BY q.next_attempt_at, q._created FOR UPDATE SKIP LOCKED LIMIT 1) \
         AND t.status = 'QUEUED' \
         RETURNING {COLUMNS}"
    )
}

fn owned(names: &[&str]) -> Vec<String> {
    names.iter().map(|n| (*n).to_owned()).collect()
}

/// Claim task `id` for `owner` if it is still queued, due, and named in
/// `names`.
///
/// # Errors
/// `BEM-507` when the update fails.
pub(crate) async fn claim_by_id(
    conn: &mut AsyncPgConnection,
    schema: &SchemaName,
    id: Uuid,
    owner: &str,
    lease: Duration,
    names: &[&str],
) -> StoreResult<Option<TaskRow>> {
    let rows: Vec<TaskRow> = sql_query(claim_sql(schema, true))
        .bind::<Text, _>(owner)
        .bind::<Double, _>(secs(lease))
        .bind::<Array<Text>, _>(owned(names))
        .bind::<SqlUuid, _>(id)
        .load(conn)
        .await
        .map_err(db)?;
    Ok(rows.into_iter().next())
}

/// Claim the oldest due task among `names` for `owner`.
///
/// # Errors
/// `BEM-507` when the update fails.
pub(crate) async fn claim_next_of(
    conn: &mut AsyncPgConnection,
    schema: &SchemaName,
    owner: &str,
    lease: Duration,
    names: &[&str],
) -> StoreResult<Option<TaskRow>> {
    let rows: Vec<TaskRow> = sql_query(claim_sql(schema, false))
        .bind::<Text, _>(owner)
        .bind::<Double, _>(secs(lease))
        .bind::<Array<Text>, _>(owned(names))
        .load(conn)
        .await
        .map_err(db)?;
    Ok(rows.into_iter().next())
}

/// Extend `owner`'s lease on a running task. `false` when the lease is no
/// longer `owner`'s (the sweep gave the task to another worker).
///
/// # Errors
/// `BEM-507` when the update fails.
pub(crate) async fn renew(
    conn: &mut AsyncPgConnection,
    schema: &SchemaName,
    id: Uuid,
    owner: &str,
    lease: Duration,
) -> StoreResult<bool> {
    let n = sql_query(format!(
        "UPDATE {schema}.agent_task SET lease_expires_at = now() + make_interval(secs => $3), _updated = now() \
         WHERE _id = $1 AND status = 'RUNNING' AND lease_owner = $2"
    ))
    .bind::<SqlUuid, _>(id)
    .bind::<Text, _>(owner)
    .bind::<Double, _>(secs(lease))
    .execute(conn)
    .await
    .map_err(db)?;
    Ok(n == 1)
}

/// Record success. `payload` replaces the stored payload (callers redact it).
/// `false` when `owner` no longer holds the lease, and nothing is written.
///
/// # Errors
/// `BEM-507` when the update fails.
pub(crate) async fn complete(
    conn: &mut AsyncPgConnection,
    schema: &SchemaName,
    id: Uuid,
    owner: &str,
    result: &Value,
    payload: &Value,
) -> StoreResult<bool> {
    let n = sql_query(format!(
        "UPDATE {schema}.agent_task SET status = 'SUCCEEDED', result = $3, payload = $4, error = NULL, \
         lease_owner = NULL, lease_expires_at = NULL, finished_at = now(), _updated = now() \
         WHERE _id = $1 AND status = 'RUNNING' AND lease_owner = $2"
    ))
    .bind::<SqlUuid, _>(id)
    .bind::<Text, _>(owner)
    .bind::<Jsonb, _>(result)
    .bind::<Jsonb, _>(payload)
    .execute(conn)
    .await
    .map_err(db)?;
    Ok(n == 1)
}

/// Record a failure. With `retry_in`, the task goes back to `QUEUED` and is
/// due after that delay; without it, it ends `FAILED`. `false` when `owner`
/// no longer holds the lease.
///
/// # Errors
/// `BEM-507` when the update fails.
pub(crate) async fn fail(
    conn: &mut AsyncPgConnection,
    schema: &SchemaName,
    id: Uuid,
    owner: &str,
    error: &Value,
    payload: &Value,
    retry_in: Option<Duration>,
) -> StoreResult<bool> {
    let n = match retry_in {
        Some(delay) => {
            sql_query(format!(
                "UPDATE {schema}.agent_task SET status = 'QUEUED', error = $3, \
             next_attempt_at = now() + make_interval(secs => $4), \
             lease_owner = NULL, lease_expires_at = NULL, _updated = now() \
             WHERE _id = $1 AND status = 'RUNNING' AND lease_owner = $2"
            ))
            .bind::<SqlUuid, _>(id)
            .bind::<Text, _>(owner)
            .bind::<Jsonb, _>(error)
            .bind::<Double, _>(secs(delay))
            .execute(conn)
            .await
        }
        None => {
            sql_query(format!(
                "UPDATE {schema}.agent_task SET status = 'FAILED', error = $3, payload = $4, \
             lease_owner = NULL, lease_expires_at = NULL, finished_at = now(), _updated = now() \
             WHERE _id = $1 AND status = 'RUNNING' AND lease_owner = $2"
            ))
            .bind::<SqlUuid, _>(id)
            .bind::<Text, _>(owner)
            .bind::<Jsonb, _>(error)
            .bind::<Jsonb, _>(payload)
            .execute(conn)
            .await
        }
    }
    .map_err(db)?;
    Ok(n == 1)
}

/// Put back a running task whose session is busy (another run holds it):
/// `QUEUED` again with `error` recorded, and the claim's attempt given back,
/// so waiting never uses up the task's attempts. It is due after a tenth of
/// the time it has waited so far, between one second and `max_delay` (at
/// once with a zero `max_delay`): a fresh wait polls often, a long one less.
/// The queue's TTL bounds the wait. `false` when `owner` no longer holds the
/// lease.
///
/// # Errors
/// `BEM-507` when the update fails.
pub(crate) async fn wait_busy(
    conn: &mut AsyncPgConnection,
    schema: &SchemaName,
    id: Uuid,
    owner: &str,
    error: &Value,
    max_delay: Duration,
) -> StoreResult<bool> {
    let n = sql_query(format!(
        "UPDATE {schema}.agent_task SET status = 'QUEUED', error = $3, \
         attempt_count = GREATEST(attempt_count - 1, 0), \
         next_attempt_at = now() + LEAST(make_interval(secs => $4), \
           GREATEST(interval '1 second', (now() - _created) / 10)), \
         lease_owner = NULL, lease_expires_at = NULL, _updated = now() \
         WHERE _id = $1 AND status = 'RUNNING' AND lease_owner = $2"
    ))
    .bind::<SqlUuid, _>(id)
    .bind::<Text, _>(owner)
    .bind::<Jsonb, _>(error)
    // Not `secs`: its one-second floor would make a zero `max_delay` wait.
    .bind::<Double, _>(max_delay.as_secs_f64())
    .execute(conn)
    .await
    .map_err(db)?;
    Ok(n == 1)
}

/// What [`sweep_expired`] did.
#[derive(Debug, Clone, Default)]
pub struct Swept {
    /// Tasks put back to `QUEUED`.
    pub requeued: Vec<Uuid>,
    /// Tasks that ended `FAILED`, returned so the worker can tell a failed
    /// run's clients.
    pub failed: Vec<TaskRow>,
}

/// Running tasks whose lease expired (their worker died or hung): a
/// retryable task with attempts left goes back to `QUEUED`; any other ends
/// `FAILED` with `{"code": "worker_lost"}`.
///
/// # Errors
/// `BEM-507` when the update fails.
pub(crate) async fn sweep_expired(
    conn: &mut AsyncPgConnection,
    schema: &SchemaName,
) -> StoreResult<Swept> {
    let requeued: Vec<IdRow> = sql_query(format!(
        "UPDATE {schema}.agent_task SET status = 'QUEUED', next_attempt_at = now(), \
         lease_owner = NULL, lease_expires_at = NULL, error = $1, _updated = now() \
         WHERE status = 'RUNNING' AND lease_expires_at < now() AND retryable \
         AND attempt_count < max_attempts RETURNING _id AS id"
    ))
    .bind::<Jsonb, _>(json!({"code": "worker_lost", "requeued": true}))
    .load(conn)
    .await
    .map_err(db)?;
    let failed: Vec<TaskRow> = sql_query(format!(
        "UPDATE {schema}.agent_task t SET status = 'FAILED', error = $1, finished_at = now(), \
         lease_owner = NULL, lease_expires_at = NULL, _updated = now() \
         WHERE t.status = 'RUNNING' AND t.lease_expires_at < now() \
         RETURNING {COLUMNS}"
    ))
    .bind::<Jsonb, _>(json!({"code": "worker_lost"}))
    .load(conn)
    .await
    .map_err(db)?;
    Ok(Swept {
        requeued: requeued.into_iter().map(|r| r.id).collect(),
        failed,
    })
}

/// Fail tasks still `QUEUED` after `ttl` (no worker ran them): they end
/// `FAILED` with `{"code": "queue_ttl"}`. Returns them, so the worker can
/// tell their clients.
///
/// # Errors
/// `BEM-507` when the update fails.
pub(crate) async fn expire_queued(
    conn: &mut AsyncPgConnection,
    schema: &SchemaName,
    ttl: Duration,
) -> StoreResult<Vec<TaskRow>> {
    sql_query(format!(
        "UPDATE {schema}.agent_task t SET status = 'FAILED', error = $1, finished_at = now(), _updated = now() \
         WHERE t.status = 'QUEUED' AND t._deleted IS NULL AND t._created < now() - make_interval(secs => $2) \
         RETURNING {COLUMNS}"
    ))
    .bind::<Jsonb, _>(json!({"code": "queue_ttl"}))
    .bind::<Double, _>(secs(ttl))
    .load(conn)
    .await
    .map_err(db)
}

/// Whether an invoke task for `execution_id` is queued or running.
///
/// # Errors
/// `BEM-507` when the read fails.
pub(crate) async fn has_active_invoke(
    conn: &mut AsyncPgConnection,
    _schema: &SchemaName,
    execution_id: Uuid,
) -> StoreResult<bool> {
    use crate::entity::agent_task::agent_task as t;
    diesel::select(diesel::dsl::exists(
        t::table
            .filter(t::execution_id.eq(execution_id))
            .filter(t::task.eq(INVOKE_TASK))
            .filter(t::status.eq_any([TaskStatus::Queued.as_str(), TaskStatus::Running.as_str()]))
            .filter(t::_deleted.is_null()),
    ))
    .get_result(conn)
    .await
    .map_err(db)
}

/// Whether `agent_id`'s execution in `session_id` holds the session's
/// concurrency slot while it waits for input: a new run there cannot start
/// until someone answers or cancels it, so waiting for it would not end.
///
/// # Errors
/// `BEM-507` when the read fails.
pub(crate) async fn slot_waits_for_input(
    conn: &mut AsyncPgConnection,
    schema: &SchemaName,
    session_id: Uuid,
    agent_id: Uuid,
) -> StoreResult<bool> {
    let rows: Vec<IdRow> = sql_query(format!(
        "SELECT e._id AS id FROM {schema}.execution_concurrency_slot s \
         JOIN {schema}.agent_execution e ON e._id = s.execution_id \
         WHERE e.session_id = $1 AND e.agent_id = $2 AND e.status = 'WAITING_FOR_INPUT' \
         AND s._deleted IS NULL AND e._deleted IS NULL LIMIT 1"
    ))
    .bind::<SqlUuid, _>(session_id)
    .bind::<SqlUuid, _>(agent_id)
    .load(conn)
    .await
    .map_err(db)?;
    Ok(!rows.is_empty())
}

/// Delete finished tasks older than `age` (retention).
///
/// # Errors
/// `BEM-507` when the delete fails.
pub(crate) async fn purge_finished(
    conn: &mut AsyncPgConnection,
    _schema: &SchemaName,
    age: Duration,
) -> StoreResult<usize> {
    use crate::entity::agent_task::agent_task as t;
    let age = chrono::Duration::from_std(age.max(Duration::from_secs(1)))
        .unwrap_or_else(|_| chrono::Duration::days(36_500));
    let before = chrono::Utc::now() - age;
    diesel::delete(
        t::table
            .filter(t::finished_at.is_not_null())
            .filter(t::finished_at.lt(before)),
    )
    .execute(conn)
    .await
    .map_err(db)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statuses_round_trip() {
        for status in [
            TaskStatus::Queued,
            TaskStatus::Running,
            TaskStatus::Succeeded,
            TaskStatus::Failed,
            TaskStatus::Cancelled,
        ] {
            assert_eq!(TaskStatus::parse(status.as_str()), Some(status));
        }
        assert_eq!(TaskStatus::parse("queued"), None);
    }

    #[test]
    fn the_claim_is_one_statement_that_skips_locked_rows() {
        let sql = claim_sql(&SchemaName::default(), true);
        assert!(sql.starts_with("UPDATE riverbeam.agent_task t SET status = 'RUNNING'"));
        assert!(sql.contains("FOR UPDATE SKIP LOCKED LIMIT 1"));
        assert!(sql.contains("AND q._id = $4"));
        let next = claim_sql(&SchemaName::default(), false);
        assert!(!next.contains("q._id ="));
        // Both claim only the names they are given.
        for sql in [sql, next] {
            assert!(sql.contains("AND q.task = ANY($3)"), "{sql}");
        }
    }

    #[test]
    fn every_catalog_task_round_trips_by_a_unique_name() {
        let mut names = std::collections::HashSet::new();
        for &task in WorkerTask::ALL {
            assert_eq!(WorkerTask::parse(task.as_str()), Some(task));
            assert!(names.insert(task.as_str()), "{} twice", task.as_str());
        }
        assert_eq!(WorkerTask::parse("nope"), None);
        assert_eq!(INVOKE_TASK, "invoke_agent");
    }

    /// Control holds cancels and run endings, interactive the agent runs,
    /// and batch everything else, memory included (plan 029 D1).
    #[test]
    fn lanes_hold_the_decided_tasks() {
        assert_eq!(Lane::Control.names(), ["cancel_execution", "end_run"]);
        assert_eq!(
            Lane::Interactive.names(),
            ["invoke_agent", "retry_execution"]
        );
        let batch = Lane::Batch.names();
        assert_eq!(batch.len(), WorkerTask::ALL.len() - 4, "{batch:?}");
        for name in [
            "remember_memory",
            "ingest_knowledge_document",
            "run_evaluation",
        ] {
            assert!(batch.contains(&name), "{name} not in {batch:?}");
        }
    }

    #[test]
    fn topics_do_not_collide_with_the_old_rpc_subjects() {
        for topic in [TASK_TOPIC, REVISION_TOPIC] {
            assert!(!topic.starts_with("beam.task."), "{topic}");
        }
    }
}
