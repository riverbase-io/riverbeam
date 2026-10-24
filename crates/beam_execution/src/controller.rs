//! Execution controller: start-or-get, attempts, leases, heartbeat, cancel,
//! timeout, retry and admission (port of fluvius-beam `execution/controller.py`,
//! `orchestration/control.py`, `orchestration/admission.py` and the lease parts
//! of `orchestration/invocation/finalize.py`).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use serde_json::Value;
use tokio::sync::watch;
use uuid::Uuid;

use crate::idempotency::IdempotencyScope;
use crate::records::{ExecResult, ExecutionRecords, ExecutionRow, Lease, NewExecution};
use crate::retry::{classify_error, RetryClass};
use crate::status::ExecutionStatus;

/// Timing and identity for one Beam instance (`EXECUTION_TIMEOUT_SECONDS`,
/// `LEASE_TTL_SECONDS`, `LEASE_HEARTBEAT_SECONDS`).
#[derive(Debug, Clone)]
pub struct ExecutionConfig {
    pub timeout: Duration,
    pub lease_ttl: Duration,
    pub heartbeat: Duration,
    pub worker_id: String,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_mins(5),
            lease_ttl: Duration::from_secs(30),
            heartbeat: Duration::from_secs(10),
            worker_id: format!("beam-{}", Uuid::new_v4()),
        }
    }
}

impl ExecutionConfig {
    /// Python's env keys, with the defaults above when unset or invalid.
    #[must_use]
    pub fn from_env() -> Self {
        let secs = |key: &str, default: Duration| {
            beam_env::secs(key).map_or(default, Duration::from_secs_f64)
        };
        let base = Self::default();
        Self {
            timeout: secs("EXECUTION_TIMEOUT_SECONDS", base.timeout),
            lease_ttl: secs("LEASE_TTL_SECONDS", base.lease_ttl),
            heartbeat: secs("LEASE_HEARTBEAT_SECONDS", base.heartbeat),
            worker_id: beam_env::string("BEAM_WORKER_ID").unwrap_or(base.worker_id),
        }
    }
}

/// Why a running execution must stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    Cancelled,
    TimedOut,
    LeaseLost,
}

impl StopReason {
    /// The site error to raise for this stop.
    #[must_use]
    pub fn error(self, execution_id: Uuid) -> riverbase_core::RiverbaseError {
        match self {
            Self::Cancelled => crate::BEM_373.with_data(execution_id.to_string()),
            Self::TimedOut => crate::BEM_374.with_data(execution_id.to_string()),
            Self::LeaseLost => crate::BEM_376.with_data(execution_id.to_string()),
        }
    }
}

/// Result of [`ExecutionController::ensure`].
#[derive(Debug, Clone)]
pub struct Ensured {
    pub execution: ExecutionRow,
    /// Attempt number of this run (`agent_run.attempt_no`); 0 when replaying.
    pub attempt_no: i32,
    /// The execution is already terminal and this is not a resume: return without running.
    pub replay_terminal: bool,
}

/// What a caller needs to start or continue an execution.
#[derive(Debug, Clone)]
pub struct EnsureRequest {
    pub execution_id: Option<Uuid>,
    pub session_id: Uuid,
    pub agent_id: Uuid,
    pub agent_name: String,
    pub org_id: Option<Uuid>,
    pub idempotency: Option<IdempotencyScope>,
    pub concurrency_key: String,
    pub request_id: Option<Uuid>,
    pub request_snapshot: Option<Value>,
    pub runtime_name: Option<String>,
    pub runtime_ref: Option<Value>,
    /// A HITL resume: never replay, never take a new slot.
    pub resume: bool,
}

/// Stop senders of one execution's runs, by run id.
type RunStops = HashMap<Uuid, watch::Sender<Option<StopReason>>>;

/// One instance's execution control plane.
pub struct ExecutionController {
    records: Arc<dyn ExecutionRecords>,
    config: ExecutionConfig,
    /// execution -> run -> stop sender. Two invokes may share an execution
    /// (a duplicate retry or resume), so each run has its own sender.
    stops: Mutex<HashMap<Uuid, RunStops>>,
    accepting: AtomicBool,
    inflight: Arc<AtomicUsize>,
}

/// Counts one admitted invoke until dropped.
pub struct Admission {
    inflight: Arc<AtomicUsize>,
}

impl Drop for Admission {
    fn drop(&mut self) {
        self.inflight.fetch_sub(1, Ordering::SeqCst);
    }
}

impl ExecutionController {
    #[must_use]
    pub fn new(records: Arc<dyn ExecutionRecords>, config: ExecutionConfig) -> Self {
        Self {
            records,
            config,
            stops: Mutex::new(HashMap::new()),
            accepting: AtomicBool::new(true),
            inflight: Arc::new(AtomicUsize::new(0)),
        }
    }

    #[must_use]
    pub fn records(&self) -> &Arc<dyn ExecutionRecords> {
        &self.records
    }

    #[must_use]
    pub fn config(&self) -> &ExecutionConfig {
        &self.config
    }

    // --- admission (BeamLifecycle) -------------------------------------------

    /// Admit one invoke, or fail with `BEM-379` (public `BEM-427`) while draining.
    ///
    /// # Errors
    /// `BEM-379` when the instance is draining.
    pub fn admit(&self) -> ExecResult<Admission> {
        // Count first, then check: a drain that has just stopped admitting either
        // sees this invoke in flight or this invoke sees the drain.
        self.inflight.fetch_add(1, Ordering::SeqCst);
        let admission = Admission {
            inflight: self.inflight.clone(),
        };
        if !self.accepting.load(Ordering::SeqCst) {
            return Err(crate::BEM_379.raise());
        }
        Ok(admission)
    }

    /// Stop admitting work and wait up to `timeout` for in-flight invokes; then
    /// signal the remaining ones to stop.
    pub async fn drain(&self, timeout: Duration) {
        self.accepting.store(false, Ordering::SeqCst);
        let deadline = tokio::time::Instant::now() + timeout;
        while self.inflight.load(Ordering::SeqCst) > 0 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        if self.inflight.load(Ordering::SeqCst) > 0 {
            if let Ok(stops) = self.stops.lock() {
                for tx in stops.values().flat_map(HashMap::values) {
                    let _ = tx.send(Some(StopReason::Cancelled));
                }
            }
        }
    }

    /// `false` once [`Self::drain`] started (readiness reports not-ready).
    #[must_use]
    pub fn accepting(&self) -> bool {
        self.accepting.load(Ordering::SeqCst)
    }

    #[must_use]
    pub fn inflight(&self) -> usize {
        self.inflight.load(Ordering::SeqCst)
    }

    // --- identity --------------------------------------------------------------

    /// The execution an incoming request belongs to: its idempotency key's
    /// execution, else the execution owning a resumed action, else `explicit`.
    ///
    /// # Errors
    /// Store errors.
    pub async fn resolve(
        &self,
        explicit: Option<Uuid>,
        idempotency: Option<&IdempotencyScope>,
        resumed_actions: &[Uuid],
    ) -> ExecResult<Option<Uuid>> {
        if let Some(scope) = idempotency {
            if let Some(found) = self.records.lookup_idempotency(scope).await? {
                return Ok(Some(found));
            }
        }
        if explicit.is_none() {
            for action in resumed_actions {
                if let Some(found) = self.records.execution_for_action(*action).await? {
                    return Ok(Some(found));
                }
            }
        }
        Ok(explicit)
    }

    /// Create or continue the durable execution and count a new attempt.
    ///
    /// # Errors
    /// `BEM-372` when another execution holds the concurrency key; store errors.
    pub async fn ensure(&self, req: &EnsureRequest) -> ExecResult<Ensured> {
        let id = req.execution_id.unwrap_or_else(Uuid::new_v4);
        let existing = self.records.get(id).await?;
        let created = existing.is_none();
        let execution = if let Some(row) = existing {
            row
        } else {
            let new = NewExecution {
                id,
                session_id: req.session_id,
                agent_id: req.agent_id,
                org_id: req.org_id,
                runtime_name: req.runtime_name.clone(),
                runtime_ref: req.runtime_ref.clone(),
                concurrency_key: (!req.resume).then(|| req.concurrency_key.clone()),
                timeout_at: self.attempt_deadline(),
                request_id: req.request_id,
                request_snapshot: req.request_snapshot.clone(),
            };
            match &req.idempotency {
                Some(scope) => self.records.start_or_get(&new, scope).await?,
                None => self.records.create(&new).await?,
            }
        };
        if execution.status.is_terminal() && !req.resume {
            return Ok(Ensured {
                execution,
                attempt_no: 0,
                replay_terminal: true,
            });
        }
        let attempt_no = match self
            .records
            .increment_attempt(execution.id, self.attempt_deadline())
            .await
        {
            Ok(n) => n,
            Err(e) => {
                // A row this call created must not keep the concurrency slot. An
                // idempotency winner (another id) belongs to its own caller.
                if created && execution.id == id {
                    let _ = self
                        .records
                        .update_status(execution.id, ExecutionStatus::Cancelled, None, None)
                        .await;
                }
                return Err(e);
            }
        };
        let execution = self.records.get(execution.id).await?.unwrap_or(execution);
        Ok(Ensured {
            execution,
            attempt_no,
            replay_terminal: false,
        })
    }

    /// Deadline for an attempt starting now. Every attempt gets a fresh one, so a
    /// resumed or retried execution is not timed out by its first attempt's clock.
    fn attempt_deadline(&self) -> Option<chrono::DateTime<Utc>> {
        chrono::Duration::from_std(self.config.timeout)
            .ok()
            .map(|t| Utc::now() + t)
    }

    // --- leases ----------------------------------------------------------------

    /// Claim the run's lease for this worker.
    ///
    /// # Errors
    /// `BEM-375` when another worker holds a live lease.
    pub async fn claim(&self, run_id: Uuid) -> ExecResult<Lease> {
        let lease_id = Uuid::new_v4();
        let fence = self
            .records
            .claim_lease(
                run_id,
                &self.config.worker_id,
                lease_id,
                self.config.lease_ttl,
            )
            .await?;
        Ok(Lease {
            run_id,
            lease_id,
            fence,
        })
    }

    /// Fail unless this worker's lease is still current (`require_run_fence`).
    ///
    /// # Errors
    /// `BEM-376` for a stale worker.
    pub async fn assert_fence(&self, lease: &Lease) -> ExecResult<()> {
        self.records.require_fence(lease).await
    }

    /// Release the lease (no error if it was already replaced).
    ///
    /// # Errors
    /// Store errors.
    pub async fn release(&self, lease: &Lease) -> ExecResult<()> {
        self.records.release_lease(lease).await
    }

    /// Register run `run_id` of `execution_id`; the receiver fires when it must stop.
    #[must_use]
    pub fn watch(&self, execution_id: Uuid, run_id: Uuid) -> watch::Receiver<Option<StopReason>> {
        let (tx, rx) = watch::channel(None);
        if let Ok(mut stops) = self.stops.lock() {
            stops.entry(execution_id).or_default().insert(run_id, tx);
        }
        rx
    }

    /// Forget a finished run's stop signal.
    pub fn unwatch(&self, execution_id: Uuid, run_id: Uuid) {
        if let Ok(mut stops) = self.stops.lock() {
            if let Some(runs) = stops.get_mut(&execution_id) {
                runs.remove(&run_id);
                if runs.is_empty() {
                    stops.remove(&execution_id);
                }
            }
        }
    }

    /// Stop every run of `execution_id` in this instance.
    fn signal(&self, execution_id: Uuid, reason: StopReason) {
        if let Ok(stops) = self.stops.lock() {
            for tx in stops
                .get(&execution_id)
                .into_iter()
                .flat_map(HashMap::values)
            {
                let _ = tx.send(Some(reason));
            }
        }
    }

    /// Stop one run.
    fn signal_run(&self, execution_id: Uuid, run_id: Uuid, reason: StopReason) {
        if let Ok(stops) = self.stops.lock() {
            if let Some(tx) = stops.get(&execution_id).and_then(|runs| runs.get(&run_id)) {
                let _ = tx.send(Some(reason));
            }
        }
    }

    /// Why `execution_id` must stop now, if it must (`raise_if_stopped`): a
    /// cancelled or timed-out row, or a passed `timeout_at` (which this marks `TIMED_OUT`).
    ///
    /// # Errors
    /// Store errors.
    pub async fn stop_reason(&self, execution_id: Uuid) -> ExecResult<Option<StopReason>> {
        let Some(row) = self.records.get(execution_id).await? else {
            return Ok(None);
        };
        Ok(match row.status {
            ExecutionStatus::Cancelled => Some(StopReason::Cancelled),
            ExecutionStatus::TimedOut => Some(StopReason::TimedOut),
            _ if row.timeout_at.is_some_and(|t| t <= Utc::now()) && !row.status.is_terminal() => {
                self.timeout(execution_id).await?;
                Some(StopReason::TimedOut)
            }
            _ => None,
        })
    }

    /// Renew `lease` every heartbeat interval and watch for cancel / timeout until
    /// the returned sender is dropped or set. Stops are delivered on [`Self::watch`]:
    /// cancel and timeout to every run of the execution, lease loss to this run only.
    #[must_use]
    pub fn spawn_heartbeat(
        self: &Arc<Self>,
        execution_id: Uuid,
        lease: Lease,
    ) -> watch::Sender<bool> {
        let (done_tx, mut done_rx) = watch::channel(false);
        let this = Arc::clone(self);
        // The heartbeat's store calls keep the run's org (optional RLS).
        tokio::spawn(beam_store::org_scope::propagate(async move {
            loop {
                tokio::select! {
                    _ = done_rx.changed() => return,
                    () = tokio::time::sleep(this.config.heartbeat) => {}
                }
                match this.stop_reason(execution_id).await {
                    Ok(Some(reason)) => {
                        this.signal(execution_id, reason);
                        return;
                    }
                    Ok(None) => {}
                    Err(e) => {
                        tracing::warn!(%execution_id, error = %e, "execution status check failed");
                    }
                }
                match this
                    .records
                    .heartbeat(&lease, &this.config.worker_id, this.config.lease_ttl)
                    .await
                {
                    Ok(()) => {}
                    // Another worker holds the lease now: this run must stop.
                    Err(e) if e.errcode.as_str() == crate::BEM_376.errcode => {
                        tracing::warn!(%execution_id, error = %e, "lease lost; stopping run");
                        this.signal_run(execution_id, lease.run_id, StopReason::LeaseLost);
                        return;
                    }
                    // A transient store error: keep the run; the lease may still be renewed in time.
                    Err(e) => {
                        tracing::warn!(%execution_id, error = %e, "lease heartbeat failed; retrying");
                    }
                }
            }
        }));
        done_tx
    }

    // --- terminal and control operations ------------------------------------

    /// Record a successful run's outcome (`COMPLETED` or `WAITING_FOR_INPUT`) on the
    /// execution (Python `complete_execution`, which raises `BEM-425` on a terminal row).
    ///
    /// # Errors
    /// `BEM-370` when the row is already terminal (cancelled or timed out meanwhile)
    /// or the move is illegal; store errors.
    pub async fn finish(&self, execution_id: Uuid, status: ExecutionStatus) -> ExecResult<()> {
        self.records
            .update_status(execution_id, status, None, None)
            .await
    }

    /// A refused resume hands the execution back to waiting for input, unless a
    /// later attempt has taken it over; `true` when it moved.
    ///
    /// # Errors
    /// Store errors.
    pub async fn return_to_waiting(&self, execution_id: Uuid, attempt: i32) -> ExecResult<bool> {
        self.records.return_to_waiting(execution_id, attempt).await
    }

    /// Record a failed run. A row that is already terminal
    /// (cancelled or timed out meanwhile) keeps its status.
    ///
    /// # Errors
    /// Store errors.
    pub async fn fail(
        &self,
        execution_id: Uuid,
        error_code: Option<&str>,
        error_message: Option<&str>,
    ) -> ExecResult<()> {
        match self.records.get(execution_id).await? {
            Some(row) if row.status.is_terminal() => Ok(()),
            Some(_) => {
                self.records
                    .update_status(
                        execution_id,
                        ExecutionStatus::Failed,
                        error_code,
                        error_message,
                    )
                    .await
            }
            None => Ok(()),
        }
    }

    /// `cancel-execution`: `CANCELLED`, expire pending actions, stop the run.
    ///
    /// # Errors
    /// `BEM-377` when unknown; `BEM-370` when already terminal.
    pub async fn cancel(&self, execution_id: Uuid) -> ExecResult<ExecutionRow> {
        self.records
            .get(execution_id)
            .await?
            .ok_or_else(|| crate::BEM_377.with_data(execution_id.to_string()))?;
        self.records
            .update_status(execution_id, ExecutionStatus::Cancelled, None, None)
            .await?;
        self.records.expire_pending_actions(execution_id).await?;
        self.signal(execution_id, StopReason::Cancelled);
        self.records
            .get(execution_id)
            .await?
            .ok_or_else(|| crate::BEM_377.with_data(execution_id.to_string()))
    }

    /// Mark `TIMED_OUT` and stop the run.
    ///
    /// # Errors
    /// `BEM-370` when already terminal; store errors.
    pub async fn timeout(&self, execution_id: Uuid) -> ExecResult<()> {
        self.records
            .update_status(execution_id, ExecutionStatus::TimedOut, None, None)
            .await?;
        self.signal(execution_id, StopReason::TimedOut);
        Ok(())
    }

    /// `retry-execution`: move a failed or retrying execution back to `RETRYING`
    /// and return its stored invoke request for redispatch.
    ///
    /// # Errors
    /// `BEM-377` when unknown, `BEM-378` when it cannot be retried, `BEM-384`
    /// when no request snapshot is stored.
    pub async fn retry(&self, execution_id: Uuid) -> ExecResult<Value> {
        let row = self
            .records
            .get(execution_id)
            .await?
            .ok_or_else(|| crate::BEM_377.with_data(execution_id.to_string()))?;
        if classify_error(
            row.last_error_code.as_deref(),
            row.status == ExecutionStatus::WaitingForInput,
        ) == RetryClass::NotRetryable
        {
            return Err(crate::BEM_378.with_data(format!(
                "{} ({})",
                row.status.as_str(),
                row.last_error_code.as_deref().unwrap_or("no error")
            )));
        }
        match row.status {
            ExecutionStatus::Failed => {
                self.records
                    .update_status(execution_id, ExecutionStatus::Retrying, None, None)
                    .await?;
            }
            ExecutionStatus::Retrying => {}
            other => {
                return Err(
                    crate::BEM_378.with_data(format!("cannot retry from {}", other.as_str()))
                )
            }
        }
        row.metadata
            .get("invoke_request")
            .cloned()
            .ok_or_else(|| crate::BEM_384.with_data(execution_id.to_string()))
    }

    /// Lease-expired runs: mark their executions `RETRYING` (reconcile).
    /// Returns the executions that moved.
    ///
    /// # Errors
    /// Store errors.
    pub async fn retry_expired_leases(&self) -> ExecResult<Vec<Uuid>> {
        let mut moved = Vec::new();
        for (run_id, execution_id) in self.records.expired_leases().await? {
            let Some(execution_id) = execution_id else {
                continue;
            };
            // Python keeps going past a row that fails.
            match self.records.mark_retrying(execution_id, Some(run_id)).await {
                Ok(true) => moved.push(execution_id),
                Ok(false) => {}
                Err(e) => {
                    tracing::warn!(%execution_id, error = %e, "marking a lease-expired run retrying failed");
                }
            }
        }
        Ok(moved)
    }
}
