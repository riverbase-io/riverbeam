use std::sync::Arc;

use beam_core::RiverBeam;
use beam_execution::StopReason;
use beam_store::tasks::WorkerTask;
use beam_types::InvokeRequest;
use riverbase_core::{RiverbaseError, RiverbaseResult};
use serde_json::Value;

/// `payload[key]` as a UUID.
fn uuid_field(payload: &Value, key: &str) -> RiverbaseResult<uuid::Uuid> {
    payload
        .get(key)
        .and_then(Value::as_str)
        .and_then(|s| uuid::Uuid::parse_str(s.trim()).ok())
        .ok_or_else(|| beam_core::BEM_003.with_data(format!("{key} is required")))
}

/// `payload[key]` as a non-empty string.
fn text_field(payload: &Value, key: &str) -> RiverbaseResult<String> {
    payload
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| beam_core::BEM_003.with_data(format!("{key} is required")))
}

fn to_json<T: serde::Serialize>(value: &T) -> RiverbaseResult<Value> {
    serde_json::to_value(value).map_err(|e| beam_core::BEM_002.with_data(e.to_string()))
}

/// `payload["execution_id"]` as a UUID (`CancelAgentExecutionPayload`, `RetryAgentExecutionPayload`).
fn execution_id(payload: &Value) -> RiverbaseResult<uuid::Uuid> {
    uuid_field(payload, "execution_id")
}

/// Owns a [`RiverBeam`] instance and dispatches worker RPC tasks.
pub struct BeamWorker {
    beam: Arc<RiverBeam>,
    queue_name: String,
    /// Stops the outbox relay.
    relay_stop: std::sync::Mutex<Option<tokio::sync::watch::Sender<bool>>>,
}

impl BeamWorker {
    #[must_use]
    pub fn new(beam: Arc<RiverBeam>) -> Self {
        let queue_name = beam.config().worker_namespace.clone();
        Self {
            beam,
            queue_name,
            relay_stop: std::sync::Mutex::new(None),
        }
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

    /// Catalog check-in hook (registers agents/models in the store), then
    /// the outbox relay when the store is a database.
    ///
    /// # Errors
    /// The store's errors from reconstructing revisions or the catalog check-in.
    pub async fn on_startup(&self) -> RiverbaseResult<()> {
        // Python reconstructs revisions at startup (binds keep their stored ids).
        self.beam.reconstruct_revisions().await?;
        self.beam.checkin_catalog().await?;
        if self.beam.store().durable() {
            self.start_outbox_relay();
        }
        Ok(())
    }

    /// Run the outbox relay in the background until shutdown. Idempotent.
    pub fn start_outbox_relay(&self) {
        let Ok(mut stop) = self.relay_stop.lock() else {
            return;
        };
        if stop.is_some() {
            return;
        }
        let (tx, rx) = tokio::sync::watch::channel(false);
        let beam = Arc::clone(&self.beam);
        let worker_id = format!("{}-{}", crate::queue::hostname(), std::process::id());
        tokio::spawn(async move { beam.run_outbox_relay(&worker_id, rx).await });
        *stop = Some(tx);
    }

    fn stop_outbox_relay(&self) {
        if let Some(tx) = self.relay_stop.lock().ok().and_then(|mut s| s.take()) {
            let _ = tx.send(true);
        }
    }

    /// Drain in-flight invokes before shutdown.
    ///
    /// # Errors
    /// None today; the `Result` keeps the lifecycle hook's shape.
    pub async fn on_shutdown(&self) -> RiverbaseResult<()> {
        self.stop_outbox_relay();
        self.beam
            .executions()
            .drain(std::time::Duration::from_secs(30))
            .await;
        Ok(())
    }

    /// `invoke_agent` task — runs the full invoke pipeline.
    ///
    /// # Errors
    /// The invoke pipeline's errors.
    pub async fn invoke_agent(&self, request: InvokeRequest) -> RiverbaseResult<Value> {
        self.beam.invoke(request).await
    }

    /// Publish the terminal `error` frame of a run whose pipeline did not
    /// end it: it failed before publishing one (concurrency conflict, drain,
    /// a bad payload), its worker died on its last attempt, no worker ran it
    /// in time, or it was cancelled while queued. The one place that ends a
    /// run's stream from outside its pipeline, so stream clients and the
    /// `/invoke` alias stop waiting. Does nothing without a stream or
    /// without the run's ids.
    pub async fn publish_run_end(&self, payload: &Value, err: &RiverbaseError) {
        let Some(stream) = self.beam.beam_stream() else {
            return;
        };
        let text = |k: &str| payload.get(k).and_then(Value::as_str).map(str::to_owned);
        let (Some(session), Some(request)) = (text("session_id"), text("request_id")) else {
            return;
        };
        let mut frame = beam_types::StreamChunk::error_frame(
            beam_core::public_errors::stream_error_text(err.clone()),
        );
        frame.session_id = Some(session.clone());
        frame.execution_id = text("execution_id");
        frame.agent_name = text("agent_name");
        if let Err(e) = stream.publish_event(&session, &request, frame).await {
            tracing::warn!(error = %e, "could not publish an ended run's error frame");
        }
    }

    /// `end_run` task: the terminal frame of a run that ended without its
    /// pipeline. Payload `{session_id, request_id, execution_id, agent_name,
    /// org_id, reason}`; the only reason is `cancelled`. Without the run's
    /// ids there is no stream to end, and nothing is published.
    ///
    /// # Errors
    /// `BEM-003` for another reason.
    async fn end_run(&self, payload: &Value) -> RiverbaseResult<Value> {
        let reason = text_field(payload, "reason")?;
        if reason != "cancelled" {
            return Err(beam_core::BEM_003.with_data(format!("unknown end_run reason {reason}")));
        }
        if let Ok(id) = execution_id(payload) {
            self.publish_run_end(payload, &StopReason::Cancelled.error(id))
                .await;
        }
        Ok(serde_json::json!({ "reason": reason }))
    }

    /// Dispatch a task by name with a JSON payload (queue consumer entry point).
    /// A payload's `org_id` scopes the task's store connections (optional RLS);
    /// an invoke scopes itself by its caller's org.
    ///
    /// # Errors
    /// `BEM-091` for an unknown task, and the task's own errors.
    pub async fn handle_task(&self, task: &str, payload: Value) -> RiverbaseResult<Value> {
        let org = payload
            .get("org_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        beam_store::org_scope::with_org(org.as_deref(), Box::pin(self.dispatch(task, payload)))
            .await
    }

    // One arm per worker task.
    #[allow(clippy::too_many_lines)]
    async fn dispatch(&self, task: &str, payload: Value) -> RiverbaseResult<Value> {
        let task =
            WorkerTask::parse(task).ok_or_else(|| beam_core::BEM_091.with_data(task.to_owned()))?;
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
            WorkerTask::CancelExecution => {
                self.beam.cancel_execution(execution_id(&payload)?).await
            }
            WorkerTask::EndRun => self.end_run(&payload).await,
            WorkerTask::RetryExecution => {
                // Python returns the new attempt; the redispatched invoke result is included.
                let id = execution_id(&payload)?;
                let result = self.beam.retry_execution(id).await?;
                Ok(serde_json::json!({
                    "execution_id": id.to_string(),
                    "status": "retrying",
                    "result": result,
                }))
            }
            // Long-term memory.
            WorkerTask::RememberMemory => self.beam.remember_memory(payload).await,
            WorkerTask::EditMemory => self.beam.edit_memory(payload).await,
            WorkerTask::ExpireMemory => self.beam.expire_memory(payload).await,
            WorkerTask::ForgetMemory => self.beam.forget_memory(payload).await,
            WorkerTask::ReconcileNow => self.beam.reconcile_now().await,
            WorkerTask::ListDeadLetters => self.beam.list_dead_letters().await,
            WorkerTask::RetryDeadLetter => {
                self.beam
                    .retry_dead_letter(uuid_field(&payload, "letter_id")?)
                    .await
            }
            WorkerTask::DiscardDeadLetter => {
                self.beam
                    .discard_dead_letter(uuid_field(&payload, "letter_id")?)
                    .await
            }
            WorkerTask::DeleteUserAgentData => {
                let user = payload
                    .get("user_id")
                    .and_then(Value::as_str)
                    .filter(|u| !u.trim().is_empty())
                    .ok_or_else(|| beam_core::BEM_003.with_data("user_id is required"))?;
                self.beam.delete_user_agent_data(user.trim()).await
            }
            WorkerTask::MarkRevisionValidating => to_json(
                &self
                    .beam
                    .mark_revision_validating(uuid_field(&payload, "revision_id")?)
                    .await?,
            ),
            WorkerTask::ActivateRevision => {
                // As primary: Python's restart makes each deploy's bind the
                // primary, while here the stored traffic wins, so this is
                // what moves new executions to the revision.
                let row = self
                    .beam
                    .activate_revision(uuid_field(&payload, "revision_id")?, true, None)
                    .await?;
                to_json(&row)
            }
            WorkerTask::RetireRevision => to_json(
                &self
                    .beam
                    .retire_revision(uuid_field(&payload, "revision_id")?)
                    .await?,
            ),
            WorkerTask::ListRevisions => {
                let rows = self
                    .beam
                    .list_revisions(&text_field(&payload, "agent_name")?)
                    .await?;
                Ok(serde_json::json!({ "revisions": rows }))
            }
            WorkerTask::SubmitAgentFeedback => {
                let text = |k: &str| payload.get(k).and_then(Value::as_str).map(str::to_owned);
                let rating = payload
                    .get("rating")
                    .and_then(Value::as_i64)
                    .and_then(|r| i32::try_from(r).ok());
                let row = self
                    .beam
                    .submit_feedback(
                        uuid_field(&payload, "execution_id")?,
                        text("thumb"),
                        rating,
                        text("comment"),
                    )
                    .await?;
                to_json(&row)
            }
            WorkerTask::RunEvaluation => {
                let run = self
                    .beam
                    .run_evaluation(
                        uuid_field(&payload, "dataset_id")?,
                        uuid_field(&payload, "agent_revision_id")?,
                    )
                    .await?;
                to_json(&run)
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
        let err = worker.handle_task("nope", Value::Null).await.unwrap_err();
        assert_eq!(err.errcode.as_str(), "BEM-091");
    }

    #[tokio::test]
    async fn memory_tasks_need_knowledge_then_round_trip() {
        let worker = BeamWorker::new(Arc::new(build_beam(None)));
        let err = worker
            .handle_task("remember_memory", serde_json::json!({}))
            .await
            .unwrap_err();
        assert_eq!(
            err.errcode.as_str(),
            "BEM-045",
            "Python: KnowledgeManager is not configured"
        );

        let beam = beam_on(
            RiverBeam::builder(BeamConfig::default()).knowledge(Arc::new(
                beam_knowledge::KnowledgeManager::new(
                    Arc::new(beam_knowledge::MockEmbedder::new(32)),
                    Arc::new(beam_knowledge::InMemoryVectorStore::new()),
                ),
            )),
        );
        let worker = BeamWorker::new(Arc::new(beam));
        let user = Uuid::new_v4().to_string();
        let remembered = worker
            .handle_task(
                "remember_memory",
                serde_json::json!({"user_id": user, "content": "likes tea"}),
            )
            .await
            .unwrap();
        let memory_id = remembered["memory_id"].as_str().unwrap().to_owned();
        let edited = worker
            .handle_task(
                "edit_memory",
                serde_json::json!({"memory_id": memory_id, "content": "likes coffee"}),
            )
            .await
            .unwrap();
        assert_eq!(edited["memory_id"], memory_id.as_str());
        worker
            .handle_task(
                "expire_memory",
                serde_json::json!({"memory_id": memory_id, "expires_at": "2020-01-01T00:00:00+00:00"}),
            )
            .await
            .unwrap();
        let forgotten = worker
            .handle_task("forget_memory", serde_json::json!({"memory_id": memory_id}))
            .await
            .unwrap();
        assert_eq!(forgotten["deleted"], 1);
    }

    fn build_beam(stream: Option<Arc<BeamStream>>) -> RiverBeam {
        let builder = RiverBeam::builder(BeamConfig::default());
        beam_on(match stream {
            Some(stream) => builder.stream(stream),
            None => builder,
        })
    }

    /// `builder`'s beam, with the `demo` echo agent.
    fn beam_on(builder: beam_core::RiverBeamBuilder) -> RiverBeam {
        let beam = builder.build().unwrap();
        beam.register_echo_agent("demo");
        let _ = ContentPart::text("warm");
        beam
    }

    /// `mark-revision-validating`'s task: activation then needs a passing eval.
    #[tokio::test]
    async fn a_validating_revision_needs_a_passing_eval_to_activate() {
        let beam = Arc::new(build_beam(None));
        let worker = BeamWorker::new(Arc::clone(&beam));
        worker
            .handle_task(
                "invoke_agent",
                invoke_payload(Uuid::new_v4(), Uuid::new_v4(), false),
            )
            .await
            .expect("the first run pins revision 1");
        let spec = beam_types::AgentSpec {
            version: "2.0.0".into(),
            ..beam_types::AgentSpec::new("demo", "echo")
        };
        let snapshot = beam.compile_spec(&spec, None, false).expect("spec");
        let draft = beam.revisions().record_bind(&snapshot).expect("draft");
        let revision = serde_json::json!({ "revision_id": draft.id });

        let marked = worker
            .handle_task("mark_revision_validating", revision.clone())
            .await
            .expect("mark validating");
        assert_eq!(marked["status"], "validating");
        let refused = worker
            .handle_task("activate_revision", revision)
            .await
            .expect_err("no eval run yet");
        assert_eq!(refused.errcode.as_str(), "BEM-492");
    }

    #[tokio::test]
    async fn execution_control_tasks_are_dispatched() {
        let worker = BeamWorker::new(Arc::new(RiverBeam::new(BeamConfig::default())));
        let missing = serde_json::json!({"execution_id": Uuid::new_v4().to_string()});
        let err = worker
            .handle_task("cancel_execution", missing.clone())
            .await
            .unwrap_err();
        assert_eq!(err.errcode.as_str(), "BEM-377");
        let err = worker
            .handle_task("retry_execution", missing)
            .await
            .unwrap_err();
        assert_eq!(err.errcode.as_str(), "BEM-377");
        let err = worker
            .handle_task("cancel_execution", serde_json::json!({}))
            .await
            .unwrap_err();
        assert_eq!(err.errcode.as_str(), "BEM-003");
    }
}
