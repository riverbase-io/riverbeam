//! NATS consumer that distributes worker tasks across processes.
//!
//! Subscribes to `{prefix}.task.*` (one token per task name), dispatches each
//! message to [`BeamWorker::handle_task`], and — for request/reply messages —
//! publishes the `{ "ok": .. } | { "error": .. }` envelope back to the caller's
//! reply subject. Every message is recorded in a `riverbase_task` [`TrackerStore`]
//! so jobs are observable (Postgres in production, in-memory in tests). Tasks
//! run concurrently up to a configurable bound.

use std::sync::Arc;

use async_nats::Client;
use chrono::Utc;
use futures_util::StreamExt;
use serde_json::{json, Value};
use tokio::sync::Semaphore;
use tracing::{info, warn};

use riverbase_core::base::TrackerId;
use riverbase_task::{JobStatus, NewWorkerJob, TrackerStore, WorkerJobUpdate};

use riverbase_core::RiverbaseResult;

use crate::worker::BeamWorker;

/// Default NATS subject prefix for beam worker tasks (matches the client side).
pub const DEFAULT_TASK_PREFIX: &str = "beam";

/// Default number of tasks processed concurrently.
pub const DEFAULT_CONCURRENCY: usize = 16;

/// NATS-driven worker server bridging the broker to [`BeamWorker`].
pub struct NatsWorkerServer {
    worker: Arc<BeamWorker>,
    client: Client,
    tracker: Arc<dyn TrackerStore>,
    prefix: String,
    concurrency: usize,
}

impl NatsWorkerServer {
    /// Connect to a NATS server and build a server with default settings.
    ///
    /// # Errors
    /// Returns a transport error if the NATS connection fails.
    pub async fn connect(
        server_addr: &str,
        worker: Arc<BeamWorker>,
        tracker: Arc<dyn TrackerStore>,
    ) -> RiverbaseResult<Self> {
        let client = async_nats::connect(server_addr)
            .await
            .map_err(|e| beam_core::BEM_093.with_data(e.to_string()))?;
        Ok(Self::new(client, worker, tracker))
    }

    /// Build a server over an existing NATS connection.
    #[must_use]
    pub fn new(client: Client, worker: Arc<BeamWorker>, tracker: Arc<dyn TrackerStore>) -> Self {
        Self {
            worker,
            client,
            tracker,
            prefix: DEFAULT_TASK_PREFIX.to_string(),
            concurrency: DEFAULT_CONCURRENCY,
        }
    }

    /// Override the subject prefix.
    #[must_use]
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// Override the maximum number of concurrently processed tasks.
    #[must_use]
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency.max(1);
        self
    }

    /// The wildcard subject this server subscribes to.
    #[must_use]
    pub fn subject(&self) -> String {
        format!("{}.task.*", self.prefix)
    }

    /// Subscribe and process tasks until the subscription closes.
    ///
    /// # Errors
    /// Returns a transport error if subscribing fails.
    pub async fn serve(&self) -> RiverbaseResult<()> {
        let subject = self.subject();
        let mut sub = self
            .client
            .subscribe(subject.clone())
            .await
            .map_err(|e| beam_core::BEM_094.with_data(e.to_string()))?;
        let limiter = Arc::new(Semaphore::new(self.concurrency));
        info!(%subject, concurrency = self.concurrency, "beam NATS worker subscribed");

        while let Some(msg) = sub.next().await {
            let Ok(permit) = limiter.clone().acquire_owned().await else {
                break;
            };
            let worker = self.worker.clone();
            let tracker = self.tracker.clone();
            let client = self.client.clone();
            tokio::spawn(async move {
                let _permit = permit;
                handle_message(&worker, &tracker, &client, msg).await;
            });
        }
        Ok(())
    }
}

/// Extract the task name (final subject token) from `{prefix}.task.{task}`.
fn task_name(subject: &str) -> &str {
    subject.rsplit('.').next().unwrap_or(subject)
}

async fn record_start(tracker: &Arc<dyn TrackerStore>, task: &str, queue: &str) -> Option<TrackerId> {
    let job = tracker
        .add_worker_job(NewWorkerJob {
            function: Some(task.to_string()),
            queue_name: Some(queue.to_string()),
            ..Default::default()
        })
        .await
        .map_err(|e| warn!(error = %e, "failed to record worker job"))
        .ok()?;
    let id = TrackerId(job.meta.id);
    let _ = tracker
        .update_worker_job(
            &id,
            WorkerJobUpdate {
                job_status: Some(JobStatus::Received),
                start_time: Some(Utc::now()),
                ..Default::default()
            },
            None,
        )
        .await
        .map_err(|e| warn!(error = %e, "failed to mark worker job received"));
    Some(id)
}

async fn record_finish(
    tracker: &Arc<dyn TrackerStore>,
    id: Option<TrackerId>,
    status: JobStatus,
    result: Option<Value>,
    err_message: Option<String>,
) {
    let Some(id) = id else { return };
    let _ = tracker
        .update_worker_job(
            &id,
            WorkerJobUpdate {
                job_status: Some(status),
                finish_time: Some(Utc::now()),
                job_progress: Some(100.0),
                result,
                err_message,
                ..Default::default()
            },
            None,
        )
        .await
        .map_err(|e| warn!(error = %e, "failed to finalize worker job"));
}

async fn handle_message(
    worker: &Arc<BeamWorker>,
    tracker: &Arc<dyn TrackerStore>,
    client: &Client,
    msg: async_nats::Message,
) {
    let task = task_name(msg.subject.as_ref()).to_string();
    let payload = serde_json::from_slice::<Value>(&msg.payload).unwrap_or(Value::Null);
    let job_id = record_start(tracker, &task, &task).await;

    match worker.handle_task(&task, payload).await {
        Ok(result) => {
            reply(client, msg.reply.as_ref(), json!({ "ok": result.clone() })).await;
            record_finish(tracker, job_id, JobStatus::Success, Some(result), None).await;
        }
        Err(err) => {
            warn!(%task, error = %err, "beam NATS worker task failed");
            reply(client, msg.reply.as_ref(), json!({ "error": err })).await;
            record_finish(
                tracker,
                job_id,
                JobStatus::Error,
                None,
                Some(err.errmesg.clone()),
            )
            .await;
        }
    }
}

async fn reply(client: &Client, reply_to: Option<&async_nats::Subject>, envelope: Value) {
    let Some(subject) = reply_to else { return };
    match serde_json::to_vec(&envelope) {
        Ok(bytes) => {
            if let Err(e) = client.publish(subject.clone(), bytes.into()).await {
                warn!(error = %e, "failed to publish worker reply");
            }
        }
        Err(e) => warn!(error = %e, "failed to serialize worker reply"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_name_from_subject() {
        assert_eq!(task_name("beam.task.invoke_agent"), "invoke_agent");
        assert_eq!(task_name("invoke_agent"), "invoke_agent");
    }
}
