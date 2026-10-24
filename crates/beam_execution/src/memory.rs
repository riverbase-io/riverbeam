//! In-process [`ExecutionRecords`] for unit tests and storeless setups.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::concurrency::normalize_concurrency_scope;
use crate::idempotency::IdempotencyScope;
use crate::records::{ExecResult, ExecutionRecords, ExecutionRow, Lease, NewExecution};
use crate::status::{transition, ExecutionStatus};

#[derive(Default)]
struct State {
    executions: HashMap<Uuid, ExecutionRow>,
    idempotency: HashMap<(String, Option<String>, String), Uuid>,
    slots: HashMap<String, Uuid>,
    /// `run_id` -> (worker, lease, fence, `expires_at`)
    leases: HashMap<Uuid, (String, Uuid, i32, chrono::DateTime<Utc>)>,
    actions: HashMap<Uuid, Uuid>,
}

/// Hermetic execution store. Not durable: other processes cannot see it.
#[derive(Default)]
pub struct InMemoryExecutionRecords {
    state: Mutex<State>,
}

impl InMemoryExecutionRecords {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> ExecResult<std::sync::MutexGuard<'_, State>> {
        self.state
            .lock()
            .map_err(|e| crate::BEM_383.with_data(e.to_string()))
    }
}

fn row_for(new: &NewExecution, idempotency_key: Option<String>) -> ExecutionRow {
    ExecutionRow {
        id: new.id,
        session_id: new.session_id,
        agent_id: new.agent_id,
        agent_revision_id: None,
        status: ExecutionStatus::Created,
        runtime_name: new.runtime_name.clone(),
        runtime_ref: new.runtime_ref.clone(),
        concurrency_key: new.concurrency_key.clone(),
        idempotency_key,
        created_at: Utc::now(),
        started_at: None,
        timeout_at: new.timeout_at,
        current_attempt: 0,
        last_error_code: None,
        last_error_message: None,
        metadata: new.metadata(),
        org_id: new.org_id,
        runtime_adapter_version: None,
        runtime_library_version: None,
    }
}

fn take_slot(s: &mut State, new: &NewExecution) -> ExecResult<()> {
    let org = new.org_id.map(|o| o.to_string());
    if let Some(scope) = normalize_concurrency_scope(new.concurrency_key.as_deref(), org.as_deref())
    {
        match s.slots.get(&scope) {
            Some(owner) if *owner != new.id => return Err(crate::BEM_372.with_data(scope)),
            _ => {
                s.slots.insert(scope, new.id);
            }
        }
    }
    Ok(())
}

fn ttl(d: Duration) -> chrono::Duration {
    chrono::Duration::from_std(d).unwrap_or(chrono::Duration::seconds(30))
}

#[async_trait]
impl ExecutionRecords for InMemoryExecutionRecords {
    fn durable(&self) -> bool {
        false
    }

    fn link_action(&self, action_id: Uuid, execution_id: Uuid) {
        if let Ok(mut s) = self.state.lock() {
            s.actions.insert(action_id, execution_id);
        }
    }

    async fn get(&self, execution_id: Uuid) -> ExecResult<Option<ExecutionRow>> {
        Ok(self.lock()?.executions.get(&execution_id).cloned())
    }

    async fn set_revision(
        &self,
        execution_id: Uuid,
        pin: crate::records::RevisionPin<'_>,
    ) -> ExecResult<()> {
        let mut t = self.lock()?;
        if let Some(row) = t.executions.get_mut(&execution_id) {
            row.agent_revision_id = Some(pin.revision_id);
            if let Some(v) = pin.adapter_version {
                row.runtime_adapter_version = Some(v.to_owned());
            }
            if let Some(v) = pin.library_version {
                row.runtime_library_version = Some(v.to_owned());
            }
            if pin.timeout_at.is_some() {
                row.timeout_at = pin.timeout_at;
            }
            if let Some(r) = pin.runtime_ref {
                row.runtime_ref = Some(r.clone());
            }
        }
        Ok(())
    }

    async fn list_for_session(&self, session_id: Uuid) -> ExecResult<Vec<ExecutionRow>> {
        let mut rows: Vec<ExecutionRow> = self
            .lock()?
            .executions
            .values()
            .filter(|e| e.session_id == session_id)
            .cloned()
            .collect();
        rows.sort_by_key(|e| (e.created_at, e.id));
        Ok(rows)
    }

    async fn lookup_idempotency(&self, scope: &IdempotencyScope) -> ExecResult<Option<Uuid>> {
        let key = (
            scope.key.clone(),
            scope.user_id.clone(),
            scope.agent_name.clone(),
        );
        Ok(self.lock()?.idempotency.get(&key).copied())
    }

    async fn execution_for_action(&self, action_id: Uuid) -> ExecResult<Option<Uuid>> {
        Ok(self.lock()?.actions.get(&action_id).copied())
    }

    async fn create(&self, new: &NewExecution) -> ExecResult<ExecutionRow> {
        let mut s = self.lock()?;
        take_slot(&mut s, new)?;
        let row = row_for(new, None);
        s.executions.insert(row.id, row.clone());
        Ok(row)
    }

    async fn start_or_get(
        &self,
        new: &NewExecution,
        scope: &IdempotencyScope,
    ) -> ExecResult<ExecutionRow> {
        let mut s = self.lock()?;
        let key = (
            scope.key.clone(),
            scope.user_id.clone(),
            scope.agent_name.clone(),
        );
        if let Some(existing) = s.idempotency.get(&key).and_then(|id| s.executions.get(id)) {
            return Ok(existing.clone());
        }
        take_slot(&mut s, new)?;
        let row = row_for(new, Some(scope.key.clone()));
        s.idempotency.insert(key, row.id);
        s.executions.insert(row.id, row.clone());
        Ok(row)
    }

    async fn increment_attempt(
        &self,
        execution_id: Uuid,
        timeout_at: Option<DateTime<Utc>>,
    ) -> ExecResult<i32> {
        let mut s = self.lock()?;
        let row = s
            .executions
            .get_mut(&execution_id)
            .ok_or_else(|| crate::BEM_377.with_data(execution_id.to_string()))?;
        row.status = next_running(row.status)?;
        row.current_attempt += 1;
        row.started_at.get_or_insert_with(Utc::now);
        if timeout_at.is_some() {
            row.timeout_at = timeout_at;
        }
        Ok(row.current_attempt)
    }

    async fn update_status(
        &self,
        execution_id: Uuid,
        target: ExecutionStatus,
        error_code: Option<&str>,
        error_message: Option<&str>,
    ) -> ExecResult<()> {
        let mut s = self.lock()?;
        let Some(row) = s.executions.get_mut(&execution_id) else {
            return Ok(());
        };
        if row.status != target {
            transition(row.status, target)?;
        }
        row.status = target;
        if let Some(code) = error_code {
            row.last_error_code = Some(code.to_owned());
        }
        if let Some(message) = error_message {
            row.last_error_message = Some(message.to_owned());
        }
        if target.is_terminal() {
            s.slots.retain(|_, owner| *owner != execution_id);
        }
        Ok(())
    }

    async fn return_to_waiting(&self, execution_id: Uuid, attempt: i32) -> ExecResult<bool> {
        let mut s = self.lock()?;
        match s.executions.get_mut(&execution_id) {
            Some(row)
                if row.status == ExecutionStatus::Running && row.current_attempt == attempt =>
            {
                row.status = ExecutionStatus::WaitingForInput;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn mark_retrying(&self, execution_id: Uuid, run_id: Option<Uuid>) -> ExecResult<bool> {
        let mut s = self.lock()?;
        let lease_live = run_id
            .and_then(|r| s.leases.get(&r))
            .is_some_and(|(_, _, _, expires)| *expires > Utc::now());
        let Some(row) = s.executions.get_mut(&execution_id) else {
            return Ok(false);
        };
        let movable = match row.status {
            ExecutionStatus::Failed => true,
            ExecutionStatus::Running => !lease_live,
            _ => false,
        };
        if !movable {
            return Ok(false);
        }
        row.status = transition(row.status, ExecutionStatus::Retrying)?;
        if let Some(r) = run_id {
            s.leases.remove(&r);
        }
        Ok(true)
    }

    async fn expire_pending_actions(&self, _execution_id: Uuid) -> ExecResult<u64> {
        // Action rows live in the beam store; the pipeline expires them there.
        Ok(0)
    }

    async fn claim_lease(
        &self,
        run_id: Uuid,
        worker_id: &str,
        lease_id: Uuid,
        lease_ttl: Duration,
    ) -> ExecResult<i32> {
        let mut s = self.lock()?;
        let now = Utc::now();
        let fence = match s.leases.get(&run_id) {
            Some((owner, _, _, expires)) if *expires > now && owner != worker_id => {
                return Err(crate::BEM_375.with_data(run_id.to_string()))
            }
            Some((_, _, fence, _)) => fence + 1,
            None => 1,
        };
        s.leases.insert(
            run_id,
            (worker_id.to_owned(), lease_id, fence, now + ttl(lease_ttl)),
        );
        Ok(fence)
    }

    async fn heartbeat(
        &self,
        lease: &Lease,
        worker_id: &str,
        lease_ttl: Duration,
    ) -> ExecResult<()> {
        let mut s = self.lock()?;
        match s.leases.get_mut(&lease.run_id) {
            Some((owner, id, fence, expires))
                if *id == lease.lease_id && *fence == lease.fence && owner == worker_id =>
            {
                *expires = Utc::now() + ttl(lease_ttl);
                Ok(())
            }
            _ => Err(crate::BEM_376.with_data(lease.run_id.to_string())),
        }
    }

    async fn release_lease(&self, lease: &Lease) -> ExecResult<()> {
        let mut s = self.lock()?;
        if s.leases
            .get(&lease.run_id)
            .is_some_and(|(_, id, fence, _)| *id == lease.lease_id && *fence == lease.fence)
        {
            s.leases.remove(&lease.run_id);
        }
        Ok(())
    }

    async fn require_fence(&self, lease: &Lease) -> ExecResult<()> {
        let s = self.lock()?;
        match s.leases.get(&lease.run_id) {
            Some((_, id, fence, _)) if *id == lease.lease_id && *fence == lease.fence => Ok(()),
            _ => Err(crate::BEM_376.with_data(lease.run_id.to_string())),
        }
    }

    async fn expired_leases(&self) -> ExecResult<Vec<(Uuid, Option<Uuid>)>> {
        let now = Utc::now();
        Ok(self
            .lock()?
            .leases
            .iter()
            .filter(|(_, l)| l.3 <= now)
            .map(|(r, _)| (*r, None))
            .collect())
    }
}

/// The status `increment_execution_attempt` moves to (terminal statuses cannot return).
pub(crate) fn next_running(current: ExecutionStatus) -> ExecResult<ExecutionStatus> {
    if current.is_terminal() {
        return Err(crate::BEM_370.with_data(format!(
            "terminal status {} cannot return to running",
            current.as_str()
        )));
    }
    match current {
        ExecutionStatus::Created
        | ExecutionStatus::WaitingForInput
        | ExecutionStatus::Queued
        | ExecutionStatus::Retrying => transition(current, ExecutionStatus::Running),
        other => Ok(other),
    }
}
