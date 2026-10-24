//! The worker side of the durable work queue.
//!
//! Domain commands insert `ref_beam.agent_task` rows and emit a
//! [`TASK_TOPIC`] hint. A [`QueueWorker`] runs them:
//!
//! - **Hints.** It subscribes to [`TASK_TOPIC`] on the riverbase bus and
//!   claims the named row at once.
//! - **Tick.** Every `BEAM_QUEUE_POLL_MS` it claims due rows, so a lost hint
//!   (or no bus at all) only delays a task.
//! - **Sweep.** Every lease period it requeues tasks whose worker died
//!   (`beam_store::tasks::sweep_expired`), fails tasks no worker ran within
//!   `BEAM_QUEUE_TTL_SECS`, deletes finished tasks past
//!   `BEAM_TASK_RETENTION_DAYS`, and queues the resumes that expire
//!   client-tool actions nobody answered.
//! - **Revisions.** It reloads its revision book on [`REVISION_TOPIC`] and
//!   every `BEAM_REVISION_RELOAD_SECS`, because a hint can be lost.
//!
//! A claim is one compare-and-swap statement, so a task runs on one worker
//! at a time. While a task runs its lease is renewed every third of the
//! lease; if the lease is lost (the worker stalled past it and the sweep
//! gave the task away), the task is stopped here, so it never runs twice
//! at once. The outcome is written only while the lease is still this
//! worker's.
//!
//! One semaphore (`BEAM_WORKER_CONCURRENCY`) bounds running tasks, and
//! nothing is claimed without a free permit. Control tasks
//! ([`CONTROL_TASKS`], such as `cancel_execution`) have a small pool of
//! their own, so a cancel never waits behind the runs it cancels.

use std::sync::Arc;
use std::time::{Duration, Instant};

use beam_store::tasks::{self, TaskRow, INVOKE_TASK, REVISION_TOPIC, TASK_TOPIC};
use beam_store::PgBeamStore;
use futures_util::StreamExt;
use riverbase_core::transport::StreamBus;
use riverbase_core::{RiverbaseError, RiverbaseResult};
use serde_json::{json, Map, Value};
use tokio::sync::{oneshot, watch, OwnedSemaphorePermit, Semaphore};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::worker::BeamWorker;

/// Tasks whose success changes every worker's revision book.
const REVISION_TASKS: [&str; 3] = [
    "mark_revision_validating",
    "activate_revision",
    "retire_revision",
];

/// Tasks that are not run again after an error (a failed run is the result).
const NO_RETRY_ON_ERROR: [&str; 2] = [INVOKE_TASK, "run_evaluation"];

/// Tasks claimed from their own pool, ahead of the runs they act on.
pub const CONTROL_TASKS: [&str; 1] = ["cancel_execution"];

/// Client-tool actions expired per sweep.
const CLIENT_TOOL_EXPIRY_BATCH: i64 = 100;

/// Permits for [`CONTROL_TASKS`].
const CONTROL_PERMITS: usize = 4;

/// Another run holds the session's concurrency slot: the run waits for it
/// ([`tasks::wait_busy`]) instead of failing, without using its attempts.
const SESSION_BUSY: &str = "BEM-372";

/// The longest pause between two looks at a busy session.
const BUSY_WAIT_MAX: Duration = Duration::from_secs(5);

/// Queue settings, from the environment.
#[derive(Debug, Clone)]
pub struct QueueConfig {
    /// `BEAM_WORKER_CONCURRENCY` (default 16): tasks running at once.
    pub concurrency: usize,
    /// `BEAM_QUEUE_POLL_MS` (default 1000): the polling tick.
    pub poll: Duration,
    /// `BEAM_TASK_LEASE_SECS` (default 30): a claim's lease, renewed while
    /// the task runs.
    pub lease: Duration,
    /// `BEAM_QUEUE_TTL_SECS` (default 3600): a task no worker ran by then fails.
    pub queue_ttl: Duration,
    /// `BEAM_TASK_RETENTION_DAYS` (default 7): finished tasks are deleted
    /// after this long.
    pub retention: Duration,
    /// `BEAM_REVISION_RELOAD_SECS` (default 30): periodic revision reload.
    pub revision_reload: Duration,
    /// This worker's lease owner id (`<HOSTNAME>-<pid>-<random>`).
    pub worker_id: String,
}

fn env_u64(key: &str, default: u64) -> u64 {
    beam_env::number(key).unwrap_or(default)
}

/// `HOSTNAME`, else `riverbeam`: the prefix of this process's worker ids.
pub(crate) fn hostname() -> String {
    beam_env::string("HOSTNAME").unwrap_or_else(|| "riverbeam".into())
}

fn worker_id(host: &str) -> String {
    format!("{host}-{}-{}", std::process::id(), Uuid::new_v4().simple())
}

/// Reads no environment: the host is `riverbeam` until
/// [`QueueConfig::from_env`] reads `HOSTNAME`.
impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            concurrency: 16,
            poll: Duration::from_secs(1),
            lease: Duration::from_secs(30),
            queue_ttl: Duration::from_hours(1),
            retention: Duration::from_hours(7 * 24),
            revision_reload: Duration::from_secs(30),
            worker_id: worker_id("riverbeam"),
        }
    }
}

impl QueueConfig {
    /// The defaults, with the `BEAM_*` keys above applied.
    #[must_use]
    pub fn from_env() -> Self {
        let concurrency = usize::try_from(env_u64("BEAM_WORKER_CONCURRENCY", 16)).unwrap_or(16);
        Self {
            concurrency: concurrency.max(1),
            poll: Duration::from_millis(env_u64("BEAM_QUEUE_POLL_MS", 1000).max(10)),
            lease: Duration::from_secs(env_u64("BEAM_TASK_LEASE_SECS", 30).max(3)),
            queue_ttl: Duration::from_secs(env_u64("BEAM_QUEUE_TTL_SECS", 3600).max(1)),
            retention: Duration::from_secs(env_u64("BEAM_TASK_RETENTION_DAYS", 7) * 24 * 3600),
            revision_reload: Duration::from_secs(env_u64("BEAM_REVISION_RELOAD_SECS", 30).max(1)),
            worker_id: worker_id(&hostname()),
        }
    }
}

/// Runs `agent_task` rows on a [`BeamWorker`].
pub struct QueueWorker {
    worker: Arc<BeamWorker>,
    store: PgBeamStore,
    bus: Option<Arc<dyn StreamBus>>,
    config: QueueConfig,
    permits: Arc<Semaphore>,
    control: Arc<Semaphore>,
}

/// What is kept of an invoke's result: the ids. The answer itself is in
/// the execution's frames and messages.
fn stored_result(task: &str, value: &Value) -> Value {
    if task != INVOKE_TASK {
        return value.clone();
    }
    let mut kept = Map::new();
    for key in [
        "execution_id",
        "session_id",
        "request_id",
        "agent_name",
        "interrupted",
    ] {
        if let Some(v) = value.get(key).filter(|v| !v.is_null()) {
            kept.insert(key.to_owned(), v.clone());
        }
    }
    Value::Object(kept)
}

/// The payload kept once a task is finished: message bodies are dropped
/// or redacted below `BEAM_TRACE_PRIVACY=full_debug`.
fn stored_payload(payload: &Value) -> Value {
    match payload {
        Value::Object(map) => Value::Object(beam_telemetry::apply_privacy(
            map,
            beam_telemetry::privacy_mode(),
        )),
        other => other.clone(),
    }
}

/// The error a task's owner may read: the public code and message, and
/// the site code. Never the error's data, which can hold driver text.
fn stored_error(err: &RiverbaseError) -> Value {
    let public = beam_core::public_errors::to_public(err.clone());
    json!({
        "errcode": public.errcode.as_str(),
        "errmesg": public.errmesg,
        "http_status": public.http_status,
        "site_code": err.errcode.as_str(),
    })
}

/// Exponential backoff for a retried task: 2, 4, 8 … seconds, at most 5 minutes.
fn backoff(attempt: i32) -> Duration {
    let exponent = u32::try_from(attempt.clamp(1, 8)).unwrap_or(1);
    Duration::from_secs(2_u64.pow(exponent).min(300))
}

/// Whether a failed task runs again (with [`backoff`]).
fn retries(row: &TaskRow, err: &RiverbaseError) -> bool {
    if row.attempt_count >= row.max_attempts {
        return false;
    }
    row.retryable && err.http_status >= 500 && !NO_RETRY_ON_ERROR.contains(&row.task.as_str())
}

/// Resolves when the lease is lost; never resolves when the renewal just
/// stops (its sender dropped).
async fn lease_lost(lost: oneshot::Receiver<()>) {
    if lost.await.is_err() {
        std::future::pending::<()>().await;
    }
}

impl QueueWorker {
    #[must_use]
    pub fn new(worker: Arc<BeamWorker>, store: PgBeamStore, config: QueueConfig) -> Self {
        let permits = Arc::new(Semaphore::new(config.concurrency.max(1)));
        Self {
            worker,
            store,
            bus: None,
            config,
            permits,
            control: Arc::new(Semaphore::new(CONTROL_PERMITS)),
        }
    }

    /// Subscribe to task and revision hints on `bus` (riverbase's bus, the
    /// same one the API's command outbox publishes to).
    #[must_use]
    pub fn with_bus(mut self, bus: Arc<dyn StreamBus>) -> Self {
        self.bus = Some(bus);
        self
    }

    #[must_use]
    pub fn config(&self) -> &QueueConfig {
        &self.config
    }

    #[must_use]
    pub fn worker(&self) -> &Arc<BeamWorker> {
        &self.worker
    }

    /// Claim and run task `id` (a hint naming `task`), or the oldest due
    /// task when `id` is `None`. Returns whether a task was claimed. The task
    /// runs on a spawned task; this returns once it is claimed.
    ///
    /// # Errors
    /// `BEM-507` when the claim fails.
    pub async fn claim(
        self: &Arc<Self>,
        id: Option<Uuid>,
        task: Option<&str>,
    ) -> RiverbaseResult<bool> {
        let control = task.is_some_and(|t| CONTROL_TASKS.contains(&t));
        let pool = if control {
            &self.control
        } else {
            &self.permits
        };
        let Ok(permit) = Arc::clone(pool).try_acquire_owned() else {
            return Ok(false);
        };
        let (owner, lease) = (self.config.worker_id.as_str(), self.config.lease);
        let queue = self.store.db().tasks();
        let row = match id {
            Some(id) => queue.claim_by_id(id, owner, lease).await?,
            None if control => queue.claim_next_of(owner, lease, &CONTROL_TASKS).await?,
            None => queue.claim_next(owner, lease).await?,
        };
        let Some(row) = row else { return Ok(false) };
        let this = Arc::clone(self);
        tokio::spawn(async move { this.execute(row, permit).await });
        Ok(true)
    }

    /// Claim and run the oldest due task here, waiting for it to finish
    /// (tests and one-shot tools). Returns the finished row, or `None`
    /// when nothing was due.
    ///
    /// # Errors
    /// `BEM-507` when the queue cannot be read or written.
    pub async fn run_next(&self) -> RiverbaseResult<Option<TaskRow>> {
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|e| beam_core::BEM_507.with_data(e.to_string()))?;
        let row = self
            .store
            .db()
            .tasks()
            .claim_next(&self.config.worker_id, self.config.lease)
            .await?;
        let Some(row) = row else { return Ok(None) };
        let id = row.id;
        self.execute(row, permit).await;
        self.store.db().tasks().get(id).await
    }

    /// Run a claimed task and record its outcome. A lost lease stops it.
    async fn execute(&self, row: TaskRow, permit: OwnedSemaphorePermit) {
        let _permit = permit;
        let (stop_renewal, lost, renewal) = self.spawn_renewal(row.id);
        let outcome = tokio::select! {
            outcome = self.worker.handle_task(&row.task, row.payload.clone()) => Some(outcome),
            () = lease_lost(lost) => None,
        };
        let _ = stop_renewal.send(true);
        let _ = renewal.await;
        let Some(outcome) = outcome else {
            warn!(task_id = %row.id, task = %row.task, "lost a task's lease: stopped it here; the sweep gives it to another worker");
            return;
        };
        if let Err(e) = self.record(&row, outcome).await {
            warn!(task_id = %row.id, task = %row.task, error = %e, "could not record a task's outcome");
        }
    }

    /// Renew the lease every third of it until told to stop. Signals `lost`
    /// when the lease is another worker's, or could not be renewed for a
    /// whole lease period.
    fn spawn_renewal(
        &self,
        id: Uuid,
    ) -> (
        watch::Sender<bool>,
        oneshot::Receiver<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let (stop_tx, mut stop_rx) = watch::channel(false);
        let (lost_tx, lost_rx) = oneshot::channel();
        let store = self.store.clone();
        let owner = self.config.worker_id.clone();
        let lease = self.config.lease;
        let handle = tokio::spawn(async move {
            let every = lease / 3;
            let mut last_ok = Instant::now();
            loop {
                tokio::select! {
                    _ = stop_rx.changed() => return,
                    () = tokio::time::sleep(every) => {}
                }
                let renewed = store.db().tasks().renew(id, &owner, lease).await;
                match renewed {
                    Ok(true) => last_ok = Instant::now(),
                    Ok(false) => {
                        let _ = lost_tx.send(());
                        return;
                    }
                    Err(e) => {
                        warn!(task_id = %id, error = %e, "could not renew a task's lease");
                        if last_ok.elapsed() >= lease {
                            let _ = lost_tx.send(());
                            return;
                        }
                    }
                }
            }
        });
        (stop_tx, lost_rx, handle)
    }

    async fn record(&self, row: &TaskRow, outcome: RiverbaseResult<Value>) -> RiverbaseResult<()> {
        let owner = &self.config.worker_id;
        let payload = stored_payload(&row.payload);
        let queue = self.store.db().tasks();
        // A session held by an execution waiting for input stays busy until
        // someone answers it: the run fails now (Python rc8 `BEM-372`).
        let held_for_input = match &outcome {
            Err(err) if err.errcode.as_str() == SESSION_BUSY => {
                self.slot_waits_for_input(&row.payload).await
            }
            _ => false,
        };
        match outcome {
            Ok(value) => {
                let written = queue
                    .complete(row.id, owner, &stored_result(&row.task, &value), &payload)
                    .await?;
                if written && REVISION_TASKS.contains(&row.task.as_str()) {
                    self.publish(REVISION_TOPIC, json!({})).await;
                }
            }
            Err(err) if err.errcode.as_str() == SESSION_BUSY && !held_for_input => {
                // Another run holds the session: wait for it, as long as the
                // queue's TTL allows, without using an attempt.
                let waiting = queue
                    .wait_busy(row.id, owner, &stored_error(&err), BUSY_WAIT_MAX)
                    .await?;
                debug!(task_id = %row.id, waiting, "the run's session is busy; it waits");
            }
            Err(err) => {
                let retry = retries(row, &err);
                warn!(task_id = %row.id, task = %row.task, errcode = %err.errcode.as_str(), retry, "a beam task failed");
                let delay = retry.then(|| backoff(row.attempt_count));
                let written = queue
                    .fail(row.id, owner, &stored_error(&err), &payload, delay)
                    .await?;
                // A run that failed before its pipeline published a frame:
                // tell its stream clients it ended.
                if written && !retry && row.task == INVOKE_TASK {
                    self.worker.publish_run_failure(&row.payload, &err).await;
                }
            }
        }
        Ok(())
    }

    /// Whether the busy session of an invoke `payload` is held by an execution
    /// waiting for input. Unknown (a bad payload, a failed read) counts as no:
    /// the run waits, as before.
    async fn slot_waits_for_input(&self, payload: &Value) -> bool {
        let session = payload
            .get("session_id")
            .and_then(Value::as_str)
            .and_then(|s| Uuid::parse_str(s).ok());
        let agent = payload.get("agent_name").and_then(Value::as_str);
        let (Some(session), Some(agent)) = (session, agent) else {
            return false;
        };
        match self
            .store
            .db()
            .tasks()
            .slot_waits_for_input(session, beam_store::agent_id_for(agent))
            .await
        {
            Ok(held) => held,
            Err(e) => {
                warn!(error = %e, "could not check what holds a busy session");
                false
            }
        }
    }

    async fn publish(&self, topic: &str, payload: Value) {
        if let Some(bus) = &self.bus {
            if let Err(e) = bus.publish(topic, payload).await {
                warn!(%topic, error = %e, "could not publish a queue hint");
            }
        }
    }

    /// Requeue tasks whose worker died, fail tasks no worker ran in time,
    /// and delete old finished tasks.
    ///
    /// # Errors
    /// `BEM-507` when the queue cannot be written.
    pub async fn sweep(&self) -> RiverbaseResult<Vec<Uuid>> {
        let queue = self.store.db().tasks();
        let requeued = queue.sweep_expired().await?;
        let expired = queue.expire_queued(self.config.queue_ttl).await?;
        let purged = queue.purge_finished(self.config.retention).await?;
        for row in expired.iter().filter(|r| r.task == INVOKE_TASK) {
            self.worker
                .publish_run_failure(&row.payload, &beam_core::BEM_508.raise())
                .await;
        }
        if !requeued.is_empty() || !expired.is_empty() || purged > 0 {
            info!(
                requeued = requeued.len(),
                expired = expired.len(),
                purged,
                "beam work queue swept"
            );
        }
        match self.expire_client_tools().await {
            Ok(0) => {}
            Ok(n) => info!(resumes = n, "expired client tool actions queued"),
            Err(e) => warn!(error = %e, "client tool expiry failed"),
        }
        Ok(requeued)
    }

    /// Queue a resume for each client-tool action no client answered in
    /// time. The resume runs as an ordinary invoke task, from a permit like
    /// any other run, so the sweep itself stays short.
    ///
    /// # Errors
    /// Store and queue errors.
    pub async fn expire_client_tools(&self) -> RiverbaseResult<usize> {
        let requests = self
            .worker
            .beam()
            .client_tool_expiries_due_now(CLIENT_TOOL_EXPIRY_BATCH)
            .await?;
        if requests.is_empty() {
            return Ok(0);
        }
        let queue = self.store.db().tasks();
        let mut queued = 0;
        for request in &requests {
            // A run of this execution is already on its way (a client's late
            // answer, say): it decides; this action is offered again later.
            if let Some(execution_id) = request.execution_id {
                if queue.has_active_invoke(execution_id).await? {
                    continue;
                }
            }
            let payload = serde_json::to_value(request)
                .map_err(|e| beam_core::BEM_001.with_data(e.to_string()))?;
            let id = |value: Option<&String>| value.and_then(|v| Uuid::parse_str(v).ok());
            let auth = request.auth_context.as_ref();
            let task = tasks::NewTask {
                execution_id: request.execution_id,
                user_id: id(auth.and_then(|a| a.user_id.as_ref())),
                org_id: id(auth.and_then(|a| a.org_id.as_ref())),
                ..tasks::NewTask::new(INVOKE_TASK, payload)
            };
            queue.enqueue(&task).await?;
            queued += 1;
        }
        Ok(queued)
    }

    /// Run every loop until `shutdown` turns true, then wait (up to the
    /// lease) for running tasks to finish.
    pub async fn run(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) {
        // The queue claims every org's tasks: under RLS that needs a role
        // that bypasses it, or the queue sees none (plan 025 D3).
        if beam_store::org_scope::rls_enabled() {
            match self.store.db().role_bypasses_rls().await {
                Ok(true) => {}
                Ok(false) => warn!(
                    "BEAM_RLS_ENABLED is on but the worker's database role does not bypass RLS: \
                     the queue, reconcile and retention will see no rows (grant BYPASSRLS)"
                ),
                Err(e) => {
                    warn!(error = %e, "could not check whether the worker's role bypasses RLS");
                }
            }
        }
        let mut loops = Vec::new();
        if let Some(bus) = self.bus.clone() {
            loops.push(tokio::spawn(Arc::clone(&self).hint_loop(bus)));
        }
        loops.push(tokio::spawn(Arc::clone(&self).revision_loop()));
        loops.push(tokio::spawn(Arc::clone(&self).tick_loop()));
        loops.push(tokio::spawn(Arc::clone(&self).sweep_loop()));
        info!(
            worker_id = %self.config.worker_id,
            concurrency = self.config.concurrency,
            hints = self.bus.is_some(),
            "beam work queue started"
        );
        while !*shutdown.borrow() {
            if shutdown.changed().await.is_err() {
                break;
            }
        }
        for task in &loops {
            task.abort();
        }
        let all = u32::try_from(self.config.concurrency).unwrap_or(u32::MAX);
        let drained = tokio::time::timeout(self.config.lease, self.permits.acquire_many(all)).await;
        if drained.is_err() {
            warn!("beam work queue stopped with tasks still running; their leases will expire");
        }
    }

    async fn hint_loop(self: Arc<Self>, bus: Arc<dyn StreamBus>) {
        loop {
            match bus.subscribe_values(TASK_TOPIC).await {
                Ok(mut hints) => {
                    while let Some(hint) = hints.next().await {
                        let id = hint
                            .get("task_id")
                            .and_then(Value::as_str)
                            .and_then(|s| Uuid::parse_str(s).ok());
                        let task = hint.get("task").and_then(Value::as_str);
                        if let Err(e) = self.claim(id, task).await {
                            warn!(error = %e, "could not claim a hinted task");
                        }
                    }
                }
                Err(e) => warn!(error = %e, "could not subscribe to task hints"),
            }
            // The subscription ended: the tick keeps the queue moving meanwhile.
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    async fn tick_loop(self: Arc<Self>) {
        let mut tick = tokio::time::interval(self.config.poll);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            // Control tasks first, from their own permits; then the rest,
            // until nothing is due or every permit is taken.
            for control in [true, false] {
                let kind = control.then_some(CONTROL_TASKS[0]);
                loop {
                    match self.claim(None, kind).await {
                        Ok(true) => {}
                        Ok(false) => break,
                        Err(e) => {
                            warn!(error = %e, "could not claim a due task");
                            break;
                        }
                    }
                }
            }
        }
    }

    async fn sweep_loop(self: Arc<Self>) {
        let mut tick = tokio::time::interval(self.config.lease);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            if let Err(e) = self.sweep().await {
                warn!(error = %e, "work queue sweep failed");
            }
        }
    }

    /// Reload revisions on each hint and on the timer, resubscribing when
    /// the hint stream ends.
    async fn revision_loop(self: Arc<Self>) {
        let mut hints = None;
        let mut tick = tokio::time::interval(self.config.revision_reload);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await;
        loop {
            if hints.is_none() {
                if let Some(bus) = &self.bus {
                    match bus.subscribe_values(REVISION_TOPIC).await {
                        Ok(stream) => hints = Some(stream),
                        Err(e) => warn!(error = %e, "could not subscribe to revision hints"),
                    }
                }
            }
            let ended = if let Some(stream) = hints.as_mut() {
                tokio::select! {
                    next = stream.next() => next.is_none(),
                    _ = tick.tick() => false,
                }
            } else {
                tick.tick().await;
                false
            };
            if ended {
                hints = None;
            }
            if let Err(e) = self.worker.beam().reconstruct_revisions().await {
                warn!(error = %e, "could not reload revisions");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Plan 021: `default()` reads no environment; `from_env()` reads
    /// `HOSTNAME`. The only test here that reads the environment.
    #[test]
    fn default_settings_read_no_environment() {
        let before = std::env::var("HOSTNAME").ok();
        std::env::set_var("HOSTNAME", "envhost");
        let (default, from_env) = (QueueConfig::default(), QueueConfig::from_env());
        match before {
            Some(host) => std::env::set_var("HOSTNAME", host),
            None => std::env::remove_var("HOSTNAME"),
        }
        assert!(
            default.worker_id.starts_with("riverbeam-"),
            "{}",
            default.worker_id
        );
        assert!(
            from_env.worker_id.starts_with("envhost-"),
            "{}",
            from_env.worker_id
        );
    }

    fn row(task: &str, attempts: i32, retryable: bool) -> TaskRow {
        TaskRow {
            id: Uuid::new_v4(),
            task: task.into(),
            payload: json!({}),
            status: "RUNNING".into(),
            retryable,
            attempt_count: attempts,
            max_attempts: 3,
            user_id: None,
            org_id: None,
            execution_id: None,
            lease_owner: None,
            result: None,
            error: None,
        }
    }

    #[test]
    fn invoke_results_keep_only_the_ids() {
        let full = json!({"execution_id": "e", "session_id": "s", "content": [{"text": "secret"}]});
        assert_eq!(
            stored_result(INVOKE_TASK, &full),
            json!({"execution_id": "e", "session_id": "s"})
        );
        assert_eq!(
            stored_result("remember_memory", &json!({"memory_id": "m"})),
            json!({"memory_id": "m"})
        );
    }

    #[test]
    fn finished_payloads_lose_their_bodies() {
        beam_telemetry::set_privacy_mode(Some(beam_telemetry::PrivacyMode::MetadataOnly));
        let kept = stored_payload(&json!({"user_id": "u", "content": "remember this"}));
        assert_eq!(kept["user_id"], "u");
        assert!(kept["content"].is_null(), "{kept}");
        beam_telemetry::set_privacy_mode(None);
    }

    #[test]
    fn stored_errors_carry_no_data() {
        let err = beam_core::BEM_113
            .with_data("connection to 10.0.0.5 refused: password authentication failed");
        let stored = stored_error(&err);
        assert_eq!(stored["site_code"], "BEM-113");
        assert!(!stored.to_string().contains("10.0.0.5"), "{stored}");
    }

    #[test]
    fn failed_runs_do_not_retry_and_store_errors_do() {
        // A busy session is a wait (`tasks::wait_busy`), never a retry.
        let busy = beam_core::BEM_372.raise();
        assert!(!retries(&row(INVOKE_TASK, 1, true), &busy));
        let runtime = beam_core::BEM_080.raise();
        assert!(!retries(&row(INVOKE_TASK, 1, true), &runtime));
        let store = beam_core::BEM_507.raise();
        assert!(retries(&row("remember_memory", 1, true), &store));
        assert!(!retries(&row("remember_memory", 1, false), &store));
    }

    #[test]
    fn backoff_grows_and_caps() {
        assert_eq!(backoff(1), Duration::from_secs(2));
        assert_eq!(backoff(3), Duration::from_secs(8));
        assert_eq!(backoff(50), Duration::from_secs(256));
    }
}
