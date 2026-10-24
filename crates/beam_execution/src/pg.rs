//! Postgres [`ExecutionRecords`]: an adapter on `db.executions()`, which
//! holds fluvius-beam's SQL execution store (statement order and lock keys
//! as Python's). The status rules stay here: each status change is decided
//! by [`transition`] and [`next_running`] on the locked row.

use std::time::Duration;

use async_trait::async_trait;
use beam_store::repo::executions::{
    Change, ExecutionRecord, IdempotencyRecord, LeaseClaim, NewExecutionRecord, RetryLabels,
    RetryStep,
};
use beam_store::repo::runtime_state::Fence;
use beam_store::{Db, DbEnum, DbError, PgBeamStore, RunStatus};
use chrono::{DateTime, Utc};
use riverbase_core::base::ErrorSpec;
use riverbase_core::RiverbaseError;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::concurrency::normalize_concurrency_scope;
use crate::idempotency::IdempotencyScope;
use crate::memory::next_running;
use crate::records::{ExecResult, ExecutionRecords, ExecutionRow, Lease, NewExecution};
use crate::status::{transition, ExecutionStatus};

/// Execution records on the beam schema of a [`PgBeamStore`]'s database.
#[derive(Clone)]
pub struct PgExecutionRecords {
    db: Db,
}

impl PgExecutionRecords {
    /// Share the store's database.
    #[must_use]
    pub fn for_store(store: &PgBeamStore) -> Self {
        Self {
            db: store.db().clone(),
        }
    }
}

fn err(spec: ErrorSpec) -> impl Fn(DbError) -> RiverbaseError {
    move |e| e.or(spec)
}

impl TryFrom<ExecutionRecord> for ExecutionRow {
    type Error = RiverbaseError;
    fn try_from(r: ExecutionRecord) -> ExecResult<Self> {
        Ok(Self {
            id: r.id,
            session_id: r.session_id,
            agent_id: r.agent_id,
            agent_revision_id: r.agent_revision_id,
            status: ExecutionStatus::from_db_label(&r.status).ok_or_else(|| {
                crate::BEM_380.with_data(format!("unknown executionstatus {:?}", r.status))
            })?,
            runtime_name: r.runtime_name,
            runtime_ref: r.runtime_ref,
            concurrency_key: r.concurrency_key,
            idempotency_key: r.idempotency_key,
            created_at: r.created_at,
            started_at: r.started_at,
            timeout_at: r.timeout_at,
            current_attempt: r.current_attempt,
            last_error_code: r.last_error_code,
            last_error_message: r.last_error_message,
            metadata: r.metadata_json,
            org_id: r.org_id,
            runtime_adapter_version: r.runtime_adapter_version,
            runtime_library_version: r.runtime_library_version,
        })
    }
}

/// `new` as the repository writes it: created, with its normalized slot.
fn record_of(new: &NewExecution) -> NewExecutionRecord {
    let org = new.org_id.map(|o| o.to_string());
    NewExecutionRecord {
        id: new.id,
        session_id: new.session_id,
        agent_id: new.agent_id,
        org_id: new.org_id,
        status: ExecutionStatus::Created.db_label().to_owned(),
        runtime_name: new.runtime_name.clone(),
        runtime_ref: new.runtime_ref.clone(),
        concurrency_key: new.concurrency_key.clone(),
        timeout_at: new.timeout_at,
        metadata: new.metadata(),
        slot_scope: normalize_concurrency_scope(new.concurrency_key.as_deref(), org.as_deref()),
    }
}

fn fence_of(lease: &Lease) -> Fence {
    Fence {
        run_id: lease.run_id,
        lease_id: lease.lease_id,
        fence: lease.fence,
    }
}

/// Name `column` in an entity patch only when `value` is `Some`: the
/// SQL's `COALESCE($n, column)`.
fn set_some<T: serde::Serialize>(data: &mut Value, column: &str, value: Option<T>) {
    if let Some(value) = value {
        data[column] = json!(value);
    }
}

fn status_of(label: &str) -> ExecResult<ExecutionStatus> {
    ExecutionStatus::from_db_label(label).ok_or_else(|| crate::BEM_381.with_data(label.to_owned()))
}

#[async_trait]
impl ExecutionRecords for PgExecutionRecords {
    fn durable(&self) -> bool {
        true
    }

    async fn get(&self, execution_id: Uuid) -> ExecResult<Option<ExecutionRow>> {
        self.db
            .executions()
            .get(execution_id)
            .await
            .map_err(err(crate::BEM_380))?
            .map(ExecutionRow::try_from)
            .transpose()
    }

    async fn set_revision(
        &self,
        execution_id: Uuid,
        pin: crate::records::RevisionPin<'_>,
    ) -> ExecResult<()> {
        let mut data = json!({"_update": true, "agent_revision_id": pin.revision_id});
        set_some(&mut data, "runtime_adapter_version", pin.adapter_version);
        set_some(&mut data, "runtime_library_version", pin.library_version);
        set_some(&mut data, "timeout_at", pin.timeout_at);
        set_some(&mut data, "runtime_ref", pin.runtime_ref);
        self.db
            .executions()
            .patch(execution_id, &data)
            .await
            .map_err(err(crate::BEM_380))
    }

    async fn list_for_session(&self, session_id: Uuid) -> ExecResult<Vec<ExecutionRow>> {
        self.db
            .executions()
            .list_for_session(session_id)
            .await
            .map_err(err(crate::BEM_380))?
            .into_iter()
            .map(ExecutionRow::try_from)
            .collect()
    }

    async fn lookup_idempotency(&self, scope: &IdempotencyScope) -> ExecResult<Option<Uuid>> {
        let user = scope
            .sql_user_id()
            .ok_or_else(|| crate::BEM_371.with_data("user_id is not a UUID"))?;
        self.db
            .executions()
            .lookup_idempotency(&scope.key, user, &scope.agent_name)
            .await
            .map_err(err(crate::BEM_380))
    }

    async fn execution_for_action(&self, action_id: Uuid) -> ExecResult<Option<Uuid>> {
        self.db
            .executions()
            .execution_for_action(action_id)
            .await
            .map_err(err(crate::BEM_380))
    }

    async fn create(&self, new: &NewExecution) -> ExecResult<ExecutionRow> {
        let row = self
            .db
            .executions()
            .create(&record_of(new))
            .await
            .map_err(err(crate::BEM_380))?
            .ok_or_else(|| crate::BEM_380.with_data("execution vanished after insert"))?;
        ExecutionRow::try_from(row)
    }

    async fn start_or_get(
        &self,
        new: &NewExecution,
        scope: &IdempotencyScope,
    ) -> ExecResult<ExecutionRow> {
        let user = scope
            .sql_user_id()
            .ok_or_else(|| crate::BEM_371.with_data("user_id is not a UUID"))?;
        let idempotency = IdempotencyRecord {
            key: scope.key.clone(),
            user_id: user,
            agent_name: scope.agent_name.clone(),
            lock: scope.lock_parts(),
        };
        match self
            .db
            .executions()
            .start_or_get(&record_of(new), &idempotency)
            .await
        {
            Ok(Some(row)) => ExecutionRow::try_from(row),
            Ok(None) => Err(crate::BEM_380.with_data("execution vanished after insert")),
            // Python: on a duplicate entry, return the winner of the race.
            Err(DbError::Conflict(_)) => {
                let winner = self.lookup_idempotency(scope).await?.ok_or_else(|| {
                    crate::BEM_371.with_data("idempotency conflict could not be resolved")
                })?;
                self.get(winner)
                    .await?
                    .ok_or_else(|| crate::BEM_371.with_data("idempotency winner is missing"))
            }
            Err(e) => Err(e.or(crate::BEM_380)),
        }
    }

    async fn increment_attempt(
        &self,
        execution_id: Uuid,
        timeout_at: Option<DateTime<Utc>>,
    ) -> ExecResult<i32> {
        self.db
            .executions()
            .update_locked(execution_id, move |row| {
                let next = next_running(status_of(&row.status)?)?;
                let attempt = row.current_attempt + 1;
                let mut data = json!({
                    "_update": true,
                    "current_attempt": attempt,
                    "status": next.db_label(),
                });
                if row.started_at.is_none() {
                    data["started_at"] = json!(Utc::now());
                }
                set_some(&mut data, "timeout_at", timeout_at);
                Ok(Change {
                    patch: Some(data),
                    release_slot: false,
                    output: attempt,
                })
            })
            .await
            .map_err(err(crate::BEM_381))?
            .ok_or_else(|| crate::BEM_377.with_data(execution_id.to_string()))
    }

    async fn update_status(
        &self,
        execution_id: Uuid,
        target: ExecutionStatus,
        error_code: Option<&str>,
        error_message: Option<&str>,
    ) -> ExecResult<()> {
        let (code, message) = (
            error_code.map(str::to_owned),
            error_message.map(str::to_owned),
        );
        self.db
            .executions()
            .update_locked(execution_id, move |row| {
                let current = status_of(&row.status)?;
                if current != target {
                    transition(current, target)?;
                }
                let stamp = match target {
                    ExecutionStatus::WaitingForInput => Some("interrupted_at"),
                    ExecutionStatus::Completed => Some("completed_at"),
                    ExecutionStatus::Cancelled => Some("cancelled_at"),
                    ExecutionStatus::Failed | ExecutionStatus::TimedOut => Some("failed_at"),
                    _ => None,
                };
                let mut data = json!({"_update": true, "status": target.db_label()});
                if let Some(column) = stamp {
                    data[column] = json!(Utc::now());
                }
                set_some(&mut data, "last_error_code", code);
                set_some(&mut data, "last_error_message", message);
                Ok(Change {
                    patch: Some(data),
                    release_slot: target.is_terminal(),
                    output: (),
                })
            })
            .await
            .map_err(err(crate::BEM_381))?;
        Ok(())
    }

    async fn return_to_waiting(&self, execution_id: Uuid, attempt: i32) -> ExecResult<bool> {
        self.db
            .executions()
            .return_to_waiting(
                execution_id,
                attempt,
                ExecutionStatus::WaitingForInput.db_label(),
                ExecutionStatus::Running.db_label(),
            )
            .await
            .map_err(err(crate::BEM_381))
    }

    async fn mark_retrying(&self, execution_id: Uuid, run_id: Option<Uuid>) -> ExecResult<bool> {
        let labels = RetryLabels {
            retrying: ExecutionStatus::Retrying.db_label(),
            interrupted_run: RunStatus::Interrupted.db_label(),
        };
        self.db
            .executions()
            .mark_retrying(execution_id, run_id, labels, |label| {
                let step = match status_of(label)? {
                    ExecutionStatus::Failed => RetryStep::Retry,
                    ExecutionStatus::Running => RetryStep::UnlessLeased,
                    _ => return Ok(RetryStep::Skip),
                };
                transition(status_of(label)?, ExecutionStatus::Retrying)?;
                Ok(step)
            })
            .await
            .map_err(err(crate::BEM_381))
    }

    async fn expire_pending_actions(&self, execution_id: Uuid) -> ExecResult<u64> {
        self.db
            .executions()
            .expire_pending_actions(execution_id)
            .await
            .map_err(err(crate::BEM_381))
    }

    async fn claim_lease(
        &self,
        run_id: Uuid,
        worker_id: &str,
        lease_id: Uuid,
        ttl: Duration,
    ) -> ExecResult<i32> {
        let claim = self
            .db
            .executions()
            .claim_run_lease(run_id, worker_id, lease_id, ttl)
            .await
            .map_err(err(crate::BEM_382))?;
        match claim {
            LeaseClaim::Claimed(fence) => Ok(fence),
            LeaseClaim::HeldByOther => Err(crate::BEM_375.with_data(run_id.to_string())),
            LeaseClaim::Missing => Err(crate::BEM_382.with_data(format!("run {run_id} not found"))),
        }
    }

    async fn heartbeat(&self, lease: &Lease, worker_id: &str, ttl: Duration) -> ExecResult<()> {
        let held = self
            .db
            .executions()
            .heartbeat(fence_of(lease), worker_id, ttl)
            .await
            .map_err(err(crate::BEM_382))?;
        if held {
            Ok(())
        } else {
            Err(crate::BEM_376.with_data(lease.run_id.to_string()))
        }
    }

    async fn release_lease(&self, lease: &Lease) -> ExecResult<()> {
        self.db
            .executions()
            .release_lease(fence_of(lease))
            .await
            .map_err(err(crate::BEM_382))
    }

    async fn require_fence(&self, lease: &Lease) -> ExecResult<()> {
        let holds = self
            .db
            .executions()
            .fence_holds(fence_of(lease))
            .await
            .map_err(err(crate::BEM_382))?;
        if holds {
            Ok(())
        } else {
            Err(crate::BEM_376.with_data(lease.run_id.to_string()))
        }
    }

    async fn expired_leases(&self) -> ExecResult<Vec<(Uuid, Option<Uuid>)>> {
        self.db
            .executions()
            .expired_leases()
            .await
            .map_err(err(crate::BEM_382))
    }
}
