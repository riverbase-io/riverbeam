//! Durable execution identity (port of fluvius-beam `execution/stores/sql/*`).

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use uuid::Uuid;

use crate::idempotency::IdempotencyScope;
use crate::status::ExecutionStatus;

pub type ExecResult<T> = riverbase_core::RiverbaseResult<T>;

/// An `agent_execution` row.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionRow {
    pub id: Uuid,
    pub session_id: Uuid,
    pub agent_id: Uuid,
    pub agent_revision_id: Option<Uuid>,
    pub status: ExecutionStatus,
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
    pub metadata: Value,
    pub org_id: Option<Uuid>,
    /// The runtime versions that write this execution's checkpoints (its pin).
    pub runtime_adapter_version: Option<String>,
    pub runtime_library_version: Option<String>,
}

/// What [`ExecutionRecords::set_revision`] writes; `None` keeps the stored value.
#[derive(Debug, Clone, Default)]
pub struct RevisionPin<'a> {
    pub revision_id: Uuid,
    pub adapter_version: Option<&'a str>,
    pub library_version: Option<&'a str>,
    pub timeout_at: Option<DateTime<Utc>>,
    pub runtime_ref: Option<&'a Value>,
}

/// Fields for a new execution (Python `create_execution` / `start_or_get`).
#[derive(Debug, Clone)]
pub struct NewExecution {
    pub id: Uuid,
    pub session_id: Uuid,
    pub agent_id: Uuid,
    pub org_id: Option<Uuid>,
    pub runtime_name: Option<String>,
    pub runtime_ref: Option<Value>,
    pub concurrency_key: Option<String>,
    pub timeout_at: Option<DateTime<Utc>>,
    /// Stored as `metadata_json.request_id`.
    pub request_id: Option<Uuid>,
    /// Stored as `metadata_json.invoke_request`; used to redispatch on retry.
    pub request_snapshot: Option<Value>,
}

impl NewExecution {
    /// `metadata_json` as Python writes it.
    #[must_use]
    pub fn metadata(&self) -> Value {
        let mut map = serde_json::Map::new();
        if let Some(id) = self.request_id {
            map.insert("request_id".into(), Value::String(id.to_string()));
        }
        if let Some(snapshot) = &self.request_snapshot {
            map.insert("invoke_request".into(), snapshot.clone());
        }
        Value::Object(map)
    }
}

/// A claimed run lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lease {
    pub run_id: Uuid,
    pub lease_id: Uuid,
    pub fence: i32,
}

/// Persistence for executions, idempotency, concurrency slots and run leases.
#[async_trait]
pub trait ExecutionRecords: Send + Sync {
    /// `true` for Postgres (rows other stacks can see); `false` for the in-memory store.
    fn durable(&self) -> bool;

    async fn get(&self, execution_id: Uuid) -> ExecResult<Option<ExecutionRow>>;
    /// Pin an execution to an agent revision (with the runtime versions that
    /// write its checkpoints, for a new pin); a spec's timeout narrows
    /// `timeout_at`; a migrated runtime ref replaces the stored one.
    async fn set_revision(&self, execution_id: Uuid, pin: RevisionPin<'_>) -> ExecResult<()>;

    /// A session's executions, oldest first (the replay route's lookup).
    async fn list_for_session(&self, session_id: Uuid) -> ExecResult<Vec<ExecutionRow>>;
    async fn lookup_idempotency(&self, scope: &IdempotencyScope) -> ExecResult<Option<Uuid>>;
    /// The execution that owns a pending HITL action (`get_execution_by_action`).
    async fn execution_for_action(&self, action_id: Uuid) -> ExecResult<Option<Uuid>>;
    /// Remember that `action_id` belongs to `execution_id`. Postgres reads
    /// `agent_action.execution_id` instead, so the default does nothing.
    fn link_action(&self, _action_id: Uuid, _execution_id: Uuid) {}

    /// Insert an execution and take its concurrency slot (`create_execution`).
    async fn create(&self, new: &NewExecution) -> ExecResult<ExecutionRow>;
    /// Insert execution + idempotency row + slot in one transaction, or return the
    /// execution that already holds the key (`start_or_get`).
    async fn start_or_get(
        &self,
        new: &NewExecution,
        scope: &IdempotencyScope,
    ) -> ExecResult<ExecutionRow>;

    /// Count a new attempt; moves `created|queued|waiting_for_input|retrying` to `running`.
    /// `timeout_at`, when given, is this attempt's deadline (it replaces the previous one).
    async fn increment_attempt(
        &self,
        execution_id: Uuid,
        timeout_at: Option<DateTime<Utc>>,
    ) -> ExecResult<i32>;
    /// Legal status change with Python's timestamps; frees the concurrency slot on terminal statuses.
    async fn update_status(
        &self,
        execution_id: Uuid,
        target: ExecutionStatus,
        error_code: Option<&str>,
        error_message: Option<&str>,
    ) -> ExecResult<()>;
    /// `RUNNING` back to `WAITING_FOR_INPUT`, only while `attempt` is still the
    /// execution's current one (a refused resume that no later attempt has
    /// taken over); `true` when it moved.
    async fn return_to_waiting(&self, execution_id: Uuid, attempt: i32) -> ExecResult<bool>;
    /// `FAILED` or lease-expired `RUNNING` to `RETRYING` (`cas_mark_retrying`); `true` when it moved.
    async fn mark_retrying(&self, execution_id: Uuid, run_id: Option<Uuid>) -> ExecResult<bool>;
    /// Expire this execution's pending HITL actions; returns how many.
    async fn expire_pending_actions(&self, execution_id: Uuid) -> ExecResult<u64>;

    /// Claim the run's lease for `worker_id` until `ttl` from now; returns the new fence.
    async fn claim_lease(
        &self,
        run_id: Uuid,
        worker_id: &str,
        lease_id: Uuid,
        ttl: std::time::Duration,
    ) -> ExecResult<i32>;
    async fn heartbeat(
        &self,
        lease: &Lease,
        worker_id: &str,
        ttl: std::time::Duration,
    ) -> ExecResult<()>;
    async fn release_lease(&self, lease: &Lease) -> ExecResult<()>;
    /// Fail with `BEM-376` unless `lease` is still the run's current lease.
    async fn require_fence(&self, lease: &Lease) -> ExecResult<()>;
    /// Runs still `running` whose lease has expired: `(run_id, execution_id)`.
    async fn expired_leases(&self) -> ExecResult<Vec<(Uuid, Option<Uuid>)>>;
}
