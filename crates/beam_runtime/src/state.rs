//! Saved runtime state (Python `RuntimeStateManager`). Rust keeps rig runs in
//! its own table, `runtime_state`, next to Python's schema; Python never reads it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use beam_execution::{ExecutionRecords, Lease};
use beam_store::repo::runtime_state::{Fence, RuntimeStateRow};
use beam_store::{Db, DbError, PgBeamStore};
use chrono::{DateTime, Utc};
use riverbase_core::{RiverbaseError, RiverbaseResult};
use serde_json::Value;
use uuid::Uuid;

use crate::contract::HealthStatus;

/// Statuses a saved run can have.
pub mod status {
    pub const RUNNING: &str = "running";
    pub const INTERRUPTED: &str = "interrupted";
    pub const COMPLETED: &str = "completed";
    pub const FAILED: &str = "failed";
    pub const CANCELLED: &str = "cancelled";

    /// Statuses retention may purge.
    pub const TERMINAL: &[&str] = &[COMPLETED, FAILED, CANCELLED];
}

/// One saved run.
#[derive(Debug, Clone, PartialEq)]
pub struct RuntimeStateRecord {
    pub state_id: Uuid,
    pub execution_id: Uuid,
    /// The run that wrote this version.
    pub run_id: Uuid,
    pub runtime_name: String,
    /// The serialized state's own format (rig's `RUN_FORMAT`).
    pub run_format: i32,
    pub adapter_version: String,
    pub library_version: String,
    pub status: String,
    /// Model calls made so far.
    pub turn: i32,
    pub state: Value,
    pub updated_at: DateTime<Utc>,
}

#[async_trait]
pub trait RuntimeStateStore: Send + Sync {
    /// Insert or replace a run's state. With a lease, the write happens only
    /// while that lease is still the run's current one.
    ///
    /// # Errors
    /// `BEM-397` when `lease` is stale; `BEM-396` for store failures.
    async fn save(&self, record: &RuntimeStateRecord, lease: Option<&Lease>)
        -> RiverbaseResult<()>;

    /// # Errors
    /// `BEM-396` for store failures.
    async fn load(&self, state_id: Uuid) -> RiverbaseResult<Option<RuntimeStateRecord>>;

    /// Atomically move a run from status `from` to `to` and record `run_id` as its
    /// writer. Returns `false`, changing nothing, when the run is not in `from`:
    /// of two resumes racing for one held run, exactly one wins.
    ///
    /// # Errors
    /// `BEM-396` for store failures.
    async fn claim(
        &self,
        state_id: Uuid,
        from: &str,
        to: &str,
        run_id: Uuid,
    ) -> RiverbaseResult<bool>;

    /// Change only the status. Returns whether the row exists.
    ///
    /// # Errors
    /// `BEM-396` for store failures.
    async fn set_status(&self, state_id: Uuid, status: &str) -> RiverbaseResult<bool>;

    /// Delete a run's state. Returns whether it existed.
    ///
    /// # Errors
    /// `BEM-396` for store failures.
    async fn purge(&self, state_id: Uuid) -> RiverbaseResult<bool>;

    /// Finished runs not updated for `older_than` (the retention task purges them).
    ///
    /// # Errors
    /// `BEM-396` for store failures.
    async fn retention_scan(&self, older_than: Duration) -> RiverbaseResult<Vec<Uuid>>;

    async fn health(&self) -> HealthStatus;

    /// Whether runs survive a restart (the production check refuses
    /// in-memory state for a durable runtime).
    fn durable(&self) -> bool {
        false
    }
}

fn cutoff(older_than: Duration) -> DateTime<Utc> {
    Utc::now() - chrono::Duration::from_std(older_than).unwrap_or(chrono::Duration::zero())
}

/// In-process state. With [`InMemoryRuntimeStateStore::fenced_by`], writes check
/// the lease against the execution records first.
#[derive(Clone, Default)]
pub struct InMemoryRuntimeStateStore {
    rows: Arc<Mutex<HashMap<Uuid, RuntimeStateRecord>>>,
    fence: Option<Arc<dyn ExecutionRecords>>,
}

impl InMemoryRuntimeStateStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Check leases against `records` before every write.
    #[must_use]
    pub fn fenced_by(mut self, records: Arc<dyn ExecutionRecords>) -> Self {
        self.fence = Some(records);
        self
    }

    fn lock(
        &self,
    ) -> RiverbaseResult<std::sync::MutexGuard<'_, HashMap<Uuid, RuntimeStateRecord>>> {
        self.rows
            .lock()
            .map_err(|_| crate::BEM_396.with_data("runtime state lock poisoned"))
    }
}

#[async_trait]
impl RuntimeStateStore for InMemoryRuntimeStateStore {
    async fn save(
        &self,
        record: &RuntimeStateRecord,
        lease: Option<&Lease>,
    ) -> RiverbaseResult<()> {
        if let (Some(records), Some(lease)) = (&self.fence, lease) {
            records
                .require_fence(lease)
                .await
                .map_err(|e| crate::BEM_397.with_data(e.errmesg))?;
        }
        let mut record = record.clone();
        record.updated_at = Utc::now();
        self.lock()?.insert(record.state_id, record);
        Ok(())
    }

    async fn load(&self, state_id: Uuid) -> RiverbaseResult<Option<RuntimeStateRecord>> {
        Ok(self.lock()?.get(&state_id).cloned())
    }

    async fn claim(
        &self,
        state_id: Uuid,
        from: &str,
        to: &str,
        run_id: Uuid,
    ) -> RiverbaseResult<bool> {
        Ok(self.lock()?.get_mut(&state_id).is_some_and(|row| {
            if row.status != from {
                return false;
            }
            to.clone_into(&mut row.status);
            row.run_id = run_id;
            row.updated_at = Utc::now();
            true
        }))
    }

    async fn set_status(&self, state_id: Uuid, status: &str) -> RiverbaseResult<bool> {
        Ok(self.lock()?.get_mut(&state_id).is_some_and(|row| {
            status.clone_into(&mut row.status);
            row.updated_at = Utc::now();
            true
        }))
    }

    async fn purge(&self, state_id: Uuid) -> RiverbaseResult<bool> {
        Ok(self.lock()?.remove(&state_id).is_some())
    }

    async fn retention_scan(&self, older_than: Duration) -> RiverbaseResult<Vec<Uuid>> {
        let before = cutoff(older_than);
        Ok(self
            .lock()?
            .values()
            .filter(|r| status::TERMINAL.contains(&r.status.as_str()) && r.updated_at <= before)
            .map(|r| r.state_id)
            .collect())
    }

    async fn health(&self) -> HealthStatus {
        HealthStatus {
            healthy: self.rows.lock().is_ok(),
            detail: "in-memory".into(),
        }
    }
}

/// State in `<schema>.runtime_state` (migration 0003, in `0012_baseline`):
/// an adapter on `db.runtime_state()`.
#[derive(Clone)]
pub struct PgRuntimeStateStore {
    db: Db,
}

impl PgRuntimeStateStore {
    /// Share the beam store's database.
    #[must_use]
    pub fn for_store(store: &PgBeamStore) -> Self {
        Self {
            db: store.db().clone(),
        }
    }
}

impl From<RuntimeStateRow> for RuntimeStateRecord {
    fn from(r: RuntimeStateRow) -> Self {
        Self {
            state_id: r.state_id,
            execution_id: r.execution_id,
            run_id: r.run_id,
            runtime_name: r.runtime_name,
            run_format: r.run_format,
            adapter_version: r.adapter_version,
            library_version: r.library_version,
            status: r.status,
            turn: r.turn,
            state: r.state,
            updated_at: r.updated_at,
        }
    }
}

fn row_of(r: &RuntimeStateRecord) -> RuntimeStateRow {
    RuntimeStateRow {
        state_id: r.state_id,
        execution_id: r.execution_id,
        run_id: r.run_id,
        runtime_name: r.runtime_name.clone(),
        run_format: r.run_format,
        adapter_version: r.adapter_version.clone(),
        library_version: r.library_version.clone(),
        status: r.status.clone(),
        turn: r.turn,
        state: r.state.clone(),
        updated_at: r.updated_at,
    }
}

fn store_err(e: DbError) -> RiverbaseError {
    e.or(crate::BEM_396)
}

#[async_trait]
impl RuntimeStateStore for PgRuntimeStateStore {
    fn durable(&self) -> bool {
        true
    }

    async fn save(
        &self,
        record: &RuntimeStateRecord,
        lease: Option<&Lease>,
    ) -> RiverbaseResult<()> {
        let fence = lease.map(|l| Fence {
            run_id: l.run_id,
            lease_id: l.lease_id,
            fence: l.fence,
        });
        let written = self
            .db
            .runtime_state()
            .save(&row_of(record), fence)
            .await
            .map_err(store_err)?;
        match (written, lease) {
            (false, Some(lease)) => Err(crate::BEM_397.with_data(lease.run_id.to_string())),
            _ => Ok(()),
        }
    }

    async fn load(&self, state_id: Uuid) -> RiverbaseResult<Option<RuntimeStateRecord>> {
        let row = self
            .db
            .runtime_state()
            .load(state_id)
            .await
            .map_err(store_err)?;
        Ok(row.map(Into::into))
    }

    async fn claim(
        &self,
        state_id: Uuid,
        from: &str,
        to: &str,
        run_id: Uuid,
    ) -> RiverbaseResult<bool> {
        self.db
            .runtime_state()
            .claim(state_id, from, to, run_id)
            .await
            .map_err(store_err)
    }

    async fn set_status(&self, state_id: Uuid, status: &str) -> RiverbaseResult<bool> {
        self.db
            .runtime_state()
            .set_status(state_id, status)
            .await
            .map_err(store_err)
    }

    async fn purge(&self, state_id: Uuid) -> RiverbaseResult<bool> {
        self.db
            .runtime_state()
            .purge(state_id)
            .await
            .map_err(store_err)
    }

    async fn retention_scan(&self, older_than: Duration) -> RiverbaseResult<Vec<Uuid>> {
        self.db
            .runtime_state()
            .retention_scan(status::TERMINAL, older_than)
            .await
            .map_err(store_err)
    }

    async fn health(&self) -> HealthStatus {
        match self.db.runtime_state().probe().await {
            Ok(()) => HealthStatus {
                healthy: true,
                detail: format!("postgres {}", self.db.schema()),
            },
            Err(e) => HealthStatus {
                healthy: false,
                detail: store_err(e).errmesg,
            },
        }
    }
}
