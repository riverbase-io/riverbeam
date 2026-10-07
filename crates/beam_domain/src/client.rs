use std::sync::Arc;

use async_trait::async_trait;
use beam_worker::BeamWorker;
use riverbase_core::RiverbaseResult;
use serde_json::Value;

/// API → worker RPC boundary (parity with Python `service_proxy.beam_client`).
#[async_trait]
pub trait BeamWorkerClient: Send + Sync {
    /// Synchronous round-trip: enqueue `task` and await its result.
    async fn request(&self, task: &str, payload: Value) -> RiverbaseResult<Value>;

    /// Fire-and-forget: enqueue `task`; results surface over the stream bridge.
    async fn send(&self, task: &str, payload: Value) -> RiverbaseResult<()>;
}

/// In-process client driving a local [`BeamWorker`] (no broker).
///
/// `send` spawns the task so streaming chunks publish while the caller gets an
/// immediate ack, mirroring the queue-backed worker behavior.
pub struct LocalWorkerClient {
    worker: Arc<BeamWorker>,
}

impl LocalWorkerClient {
    #[must_use]
    pub fn new(worker: Arc<BeamWorker>) -> Self {
        Self { worker }
    }
}

#[async_trait]
impl BeamWorkerClient for LocalWorkerClient {
    async fn request(&self, task: &str, payload: Value) -> RiverbaseResult<Value> {
        self.worker.handle_task(task, payload).await
    }

    async fn send(&self, task: &str, payload: Value) -> RiverbaseResult<()> {
        let worker = self.worker.clone();
        let task = task.to_string();
        tokio::spawn(async move {
            let _ = worker.handle_task(&task, payload).await;
        });
        Ok(())
    }
}
