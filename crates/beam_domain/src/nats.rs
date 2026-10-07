//! NATS-backed [`BeamWorkerClient`]: the API → worker RPC boundary.
//!
//! `request` (used by `invoke`) is a NATS request/reply round-trip; `send`
//! (used by `stream` / `resume`) is a fire-and-forget publish whose results
//! surface over the stream bridge. The worker side ([`beam_worker::NatsWorkerServer`])
//! consumes the same `{prefix}.task.{task}` subjects and replies with the
//! shared envelope below.

use async_nats::Client;
use async_trait::async_trait;
use riverbase_core::{RiverbaseError, RiverbaseErrorCode, RiverbaseResult};
use serde_json::{json, Value};

use crate::client::BeamWorkerClient;

/// Default NATS subject prefix for beam worker tasks.
pub const DEFAULT_TASK_PREFIX: &str = "beam";

/// Subject a given task is published to (`{prefix}.task.{task}`).
#[must_use]
pub fn task_subject(prefix: &str, task: &str) -> String {
    format!("{prefix}.task.{task}")
}

/// Rebuild a [`RiverbaseError`] from the worker's serialized error object.
fn riverbase_error_from_json(value: &Value) -> RiverbaseError {
    let Some(obj) = value.as_object() else {
        return beam_core::BEM_100.with_data(value.clone());
    };
    let Some(code) = obj.get("errcode").and_then(Value::as_str) else {
        return beam_core::BEM_100.with_data(value.clone());
    };
    let http_status = obj
        .get("http_status")
        .and_then(Value::as_u64)
        .unwrap_or(500) as u16;
    let http_title = obj
        .get("http_title")
        .and_then(Value::as_str)
        .unwrap_or("Internal Server Error");
    let errmesg = obj
        .get("errmesg")
        .and_then(Value::as_str)
        .unwrap_or("Worker failed.");
    let errdata = obj.get("errdata").cloned().unwrap_or(Value::Null);
    let mut err = RiverbaseError::new(
        http_status,
        http_title,
        RiverbaseErrorCode::new(code),
        errmesg,
        errdata,
    );
    if let Some(hint) = obj.get("errhint").and_then(Value::as_str) {
        err = err.with_errhint(hint);
    }
    err
}

/// Decode the worker's `{ "ok": .. } | { "error": .. }` reply envelope.
///
/// # Errors
/// Returns the reconstructed worker [`RiverbaseError`], or a beam catalogue
/// error when the envelope is malformed.
pub fn decode_reply(value: &Value) -> RiverbaseResult<Value> {
    if let Some(err) = value.get("error") {
        return Err(riverbase_error_from_json(err));
    }
    value
        .get("ok")
        .cloned()
        .ok_or_else(|| beam_core::BEM_099.with_data(value.clone()))
}

/// Encode a successful worker result into the reply envelope.
#[must_use]
pub fn encode_ok(result: &Value) -> Value {
    json!({ "ok": result })
}

/// Encode a worker error into the reply envelope.
#[must_use]
pub fn encode_err(err: &RiverbaseError) -> Value {
    json!({ "error": err })
}

/// Distributed [`BeamWorkerClient`] over NATS request/reply + publish.
#[derive(Clone)]
pub struct NatsWorkerClient {
    client: Client,
    prefix: String,
}

impl NatsWorkerClient {
    /// Connect to a NATS server with the default task prefix.
    ///
    /// # Errors
    /// Returns a transport error if the connection fails.
    pub async fn connect(server_addr: &str) -> RiverbaseResult<Self> {
        Self::connect_with_prefix(server_addr, DEFAULT_TASK_PREFIX).await
    }

    /// Connect to a NATS server with a custom task prefix.
    ///
    /// # Errors
    /// Returns a transport error if the connection fails.
    pub async fn connect_with_prefix(
        server_addr: &str,
        prefix: impl Into<String>,
    ) -> RiverbaseResult<Self> {
        let client = async_nats::connect(server_addr)
            .await
            .map_err(|e| beam_core::BEM_095.with_data(e.to_string()))?;
        Ok(Self::new(client, prefix))
    }

    /// Build a client over an existing NATS connection.
    #[must_use]
    pub fn new(client: Client, prefix: impl Into<String>) -> Self {
        Self {
            client,
            prefix: prefix.into(),
        }
    }

    fn subject(&self, task: &str) -> String {
        task_subject(&self.prefix, task)
    }

    fn encode(payload: &Value) -> RiverbaseResult<Vec<u8>> {
        serde_json::to_vec(payload).map_err(|e| beam_core::BEM_002.with_data(e.to_string()))
    }
}

#[async_trait]
impl BeamWorkerClient for NatsWorkerClient {
    async fn request(&self, task: &str, payload: Value) -> RiverbaseResult<Value> {
        let bytes = Self::encode(&payload)?;
        let response = self
            .client
            .request(self.subject(task), bytes.into())
            .await
            .map_err(|e| beam_core::BEM_096.with_data(e.to_string()))?;
        let value: Value = serde_json::from_slice(&response.payload)
            .map_err(|e| beam_core::BEM_098.with_data(e.to_string()))?;
        decode_reply(&value)
    }

    async fn send(&self, task: &str, payload: Value) -> RiverbaseResult<()> {
        let bytes = Self::encode(&payload)?;
        self.client
            .publish(self.subject(task), bytes.into())
            .await
            .map_err(|e| beam_core::BEM_097.with_data(e.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subject_format() {
        assert_eq!(task_subject("beam", "invoke_agent"), "beam.task.invoke_agent");
    }

    #[test]
    fn decode_ok_and_err() {
        let ok = decode_reply(&json!({"ok": {"agent_name": "demo"}})).unwrap();
        assert_eq!(ok["agent_name"], "demo");
        let err = decode_reply(&json!({
            "error": {
                "http_status": 422,
                "http_title": "Unprocessable Content",
                "errmesg": "Failed to serialize the beam invoke request.",
                "errcode": "BEM-001",
                "errdata": null
            }
        }))
        .unwrap_err();
        assert_eq!(err.errcode.as_str(), "BEM-001");
        assert_eq!(decode_reply(&json!({"error": "boom"})).unwrap_err().errcode.as_str(), "BEM-100");
        assert_eq!(decode_reply(&json!({"weird": 1})).unwrap_err().errcode.as_str(), "BEM-099");
    }

    /// Full request/reply round-trip over a local NATS server.
    ///
    /// Set `NATS_URL` (e.g. `nats://127.0.0.1:4222`) to run; skipped otherwise.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn nats_invoke_roundtrip() {
        use std::sync::Arc;
        use std::time::Duration;

        use std::collections::HashMap;
        use std::sync::Mutex;

        use beam_core::{BeamConfig, RiverBeam};
        use beam_worker::{BeamWorker, NatsWorkerServer};
        use riverbase_core::base::{RiverbaseResult, TrackerId};
        use riverbase_core::logstore::{ActivityLogRecord, LogStore};
        use riverbase_task::{
            JobRelation, JobRelationUpdate, NewJobRelation, NewWorker, NewWorkerJob, TrackerStore,
            Worker, WorkerJob, WorkerJobUpdate, WorkerUpdate,
        };
        use uuid::Uuid;

        #[derive(Default)]
        struct MemoryTrackerStore {
            jobs: Mutex<HashMap<Uuid, WorkerJob>>,
        }

        #[async_trait::async_trait]
        impl TrackerStore for MemoryTrackerStore {
            async fn add_worker(&self, row: NewWorker) -> RiverbaseResult<Worker> {
                Ok(row.into_worker())
            }
            async fn update_worker(
                &self,
                _id: &TrackerId,
                _patch: WorkerUpdate,
            ) -> RiverbaseResult<Worker> {
                unimplemented!("NATS roundtrip test does not update workers")
            }
            async fn fetch_worker(&self, _id: &TrackerId) -> RiverbaseResult<Worker> {
                unimplemented!("NATS roundtrip test does not fetch workers")
            }
            async fn add_worker_job(&self, row: NewWorkerJob) -> RiverbaseResult<WorkerJob> {
                let job = row.into_worker_job();
                self.jobs.lock().expect("tracker").insert(job.meta.id, job.clone());
                Ok(job)
            }
            async fn update_worker_job(
                &self,
                id: &TrackerId,
                patch: WorkerJobUpdate,
                _activities: Option<&dyn LogStore<ActivityLogRecord>>,
            ) -> RiverbaseResult<WorkerJob> {
                let mut jobs = self.jobs.lock().expect("tracker");
                let job = jobs.get_mut(&id.0).expect("recorded job");
                if let Some(v) = patch.job_status {
                    job.job_status = Some(v);
                }
                if let Some(v) = patch.start_time {
                    job.start_time = Some(v);
                }
                if let Some(v) = patch.finish_time {
                    job.finish_time = Some(v);
                }
                if let Some(v) = patch.job_progress {
                    job.job_progress = Some(v);
                }
                if let Some(v) = patch.result {
                    job.result = Some(v);
                }
                if let Some(v) = patch.err_message {
                    job.err_message = Some(v);
                }
                Ok(job.clone())
            }
            async fn fetch_worker_job(&self, id: &TrackerId) -> RiverbaseResult<WorkerJob> {
                Ok(self.jobs.lock().expect("tracker")[&id.0].clone())
            }
            async fn add_job_relation(&self, row: NewJobRelation) -> RiverbaseResult<JobRelation> {
                Ok(row.into_job_relation())
            }
            async fn update_job_relation(
                &self,
                _id: &TrackerId,
                _patch: JobRelationUpdate,
            ) -> RiverbaseResult<JobRelation> {
                unimplemented!("NATS roundtrip test does not update job relations")
            }
            async fn fetch_job_relation(&self, _id: &TrackerId) -> RiverbaseResult<JobRelation> {
                unimplemented!("NATS roundtrip test does not fetch job relations")
            }
        }

        let Ok(url) = std::env::var("NATS_URL") else {
            eprintln!("skipping: NATS_URL not set");
            return;
        };

        let beam = RiverBeam::new(BeamConfig::default());
        beam.register_echo_agent("demo");
        let worker = Arc::new(BeamWorker::new(Arc::new(beam)));
        let tracker = Arc::new(MemoryTrackerStore::default());
        // Isolate subjects so parallel/repeat runs don't cross-talk.
        let prefix = format!("beamtest-{}", Uuid::new_v4().simple());

        let server = NatsWorkerServer::connect(&url, worker, tracker)
            .await
            .expect("server connect")
            .with_prefix(prefix.clone());
        tokio::spawn(async move {
            let _ = server.serve().await;
        });
        // Give the subscription time to register before requesting.
        tokio::time::sleep(Duration::from_millis(300)).await;

        let client = NatsWorkerClient::connect_with_prefix(&url, prefix)
            .await
            .expect("client connect");
        let payload = json!({
            "agent_name": "demo",
            "content": [{"type": "text", "text": "hello"}],
            "session_id": Uuid::new_v4(),
            "request_id": Uuid::new_v4(),
            "stream": false,
        });
        let out = client
            .request("invoke_agent", payload)
            .await
            .expect("invoke roundtrip");
        assert_eq!(out["agent_name"], "demo");
    }
}
