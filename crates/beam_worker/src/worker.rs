use std::str::FromStr;
use std::sync::Arc;

use beam_core::RiverBeam;
use beam_types::InvokeRequest;
use riverbase_core::{RiverbaseError, RiverbaseResult};
use serde_json::Value;

/// RPC task names exported by the worker (parity with Python `export_task`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerTask {
    InvokeAgent,
    IngestKnowledgeDocument,
    DeleteKnowledgeDocument,
    RememberMemory,
    ForgetMemory,
}

impl WorkerTask {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InvokeAgent => "invoke_agent",
            Self::IngestKnowledgeDocument => "ingest_knowledge_document",
            Self::DeleteKnowledgeDocument => "delete_knowledge_document",
            Self::RememberMemory => "remember_memory",
            Self::ForgetMemory => "forget_memory",
        }
    }
}

impl FromStr for WorkerTask {
    type Err = RiverbaseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "invoke_agent" => Ok(Self::InvokeAgent),
            "ingest_knowledge_document" => Ok(Self::IngestKnowledgeDocument),
            "delete_knowledge_document" => Ok(Self::DeleteKnowledgeDocument),
            "remember_memory" => Ok(Self::RememberMemory),
            "forget_memory" => Ok(Self::ForgetMemory),
            other => Err(beam_core::BEM_091.with_data(other.to_string())),
        }
    }
}

/// Owns a [`RiverBeam`] instance and dispatches worker RPC tasks.
pub struct BeamWorker {
    beam: Arc<RiverBeam>,
    queue_name: String,
}

impl BeamWorker {
    #[must_use]
    pub fn new(beam: Arc<RiverBeam>) -> Self {
        let queue_name = beam.config().worker_namespace.clone();
        Self { beam, queue_name }
    }

    #[must_use]
    pub fn beam(&self) -> &Arc<RiverBeam> {
        &self.beam
    }

    /// Redis/NATS queue namespace this worker consumes (`riverbeam-worker`).
    #[must_use]
    pub fn queue_name(&self) -> &str {
        &self.queue_name
    }

    /// Catalog check-in hook (registers agents/models in the store).
    pub async fn on_startup(&self) -> RiverbaseResult<()> {
        self.beam.checkin_catalog().await?;
        Ok(())
    }

    /// Catalog check-out hook (Phase 6+ wires usage/audit shutdown).
    pub async fn on_shutdown(&self) -> RiverbaseResult<()> {
        Ok(())
    }

    /// `invoke_agent` task — runs the full invoke pipeline.
    pub async fn invoke_agent(&self, request: InvokeRequest) -> RiverbaseResult<Value> {
        self.beam.invoke(request).await
    }

    /// Dispatch a task by name with a JSON payload (queue consumer entry point).
    pub async fn handle_task(&self, task: &str, payload: Value) -> RiverbaseResult<Value> {
        let task = WorkerTask::from_str(task)?;
        match task {
            WorkerTask::InvokeAgent => {
                let request: InvokeRequest = serde_json::from_value(payload)
                    .map_err(|e| beam_core::BEM_003.with_data(e.to_string()))?;
                self.invoke_agent(request).await
            }
            WorkerTask::IngestKnowledgeDocument => {
                self.beam.ingest_knowledge_document(payload).await
            }
            WorkerTask::DeleteKnowledgeDocument => {
                self.beam.delete_knowledge_document(payload).await
            }
            WorkerTask::RememberMemory | WorkerTask::ForgetMemory => {
                Err(beam_core::BEM_092.with_data(task.as_str().to_string()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use beam_core::BeamConfig;
    use beam_stream::{BeamStream, InMemoryTransport};
    use beam_types::{ContentPart, StreamChunkType};
    use uuid::Uuid;

    fn invoke_payload(session: Uuid, request: Uuid, stream: bool) -> Value {
        serde_json::json!({
            "agent_name": "demo",
            "content": [{"type": "text", "text": "hello"}],
            "session_id": session,
            "request_id": request,
            "stream": stream,
        })
    }

    #[tokio::test]
    async fn invoke_agent_task_returns_result() {
        let beam = Arc::new(build_beam(None));
        let worker = BeamWorker::new(beam);
        let session = Uuid::new_v4();
        let request = Uuid::new_v4();
        let out = worker
            .handle_task("invoke_agent", invoke_payload(session, request, false))
            .await
            .expect("invoke task succeeds");
        assert_eq!(out["agent_name"], "demo");
    }

    #[tokio::test]
    async fn streaming_invoke_publishes_terminal_done_frame() {
        let transport = Arc::new(InMemoryTransport::new());
        let stream = Arc::new(BeamStream::new(transport.clone()));
        let beam = Arc::new(build_beam(Some(stream)));
        let worker = BeamWorker::new(beam);

        let session = Uuid::new_v4();
        let request = Uuid::new_v4();
        worker
            .handle_task("invoke_agent", invoke_payload(session, request, true))
            .await
            .expect("streaming invoke succeeds");

        let frames = transport.frames().await;
        assert!(!frames.is_empty(), "expected published frames");
        let last = frames.last().unwrap();
        assert_eq!(last.frame.chunk_type, StreamChunkType::Done);
        assert_eq!(last.channel, format!("ws.channel.{session}:{request}"));
        assert!(last.frame.result.is_some());
    }

    #[tokio::test]
    async fn unknown_task_errors() {
        let beam = Arc::new(build_beam(None));
        let worker = BeamWorker::new(beam);
        let err = worker
            .handle_task("nope", Value::Null)
            .await
            .unwrap_err();
        assert_eq!(err.errcode.as_str(), "BEM-091");
    }

    #[tokio::test]
    async fn unimplemented_task_returns_bem_092() {
        let beam = Arc::new(build_beam(None));
        let worker = BeamWorker::new(beam);
        let err = worker
            .handle_task("remember_memory", serde_json::json!({}))
            .await
            .unwrap_err();
        assert_eq!(err.errcode.as_str(), "BEM-092");
    }

    fn build_beam(stream: Option<Arc<BeamStream>>) -> RiverBeam {
        let mut beam = RiverBeam::new(BeamConfig::default());
        if let Some(stream) = stream {
            beam.set_stream(stream);
        }
        beam.register_echo_agent("demo");
        let _ = ContentPart::text("warm");
        beam
    }
}
