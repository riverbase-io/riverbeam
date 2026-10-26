//! The ops tests.

use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use beam_stream::{BeamStream, StreamTransport};
use beam_types::{ContentPart, InvokeAuthContext, InvokeRequest};

use std::sync::{Arc, Mutex};

use beam_store::LegalHoldStore;
use beam_types::{StreamChunk, StreamChunkType};
use riverbase_core::RiverbaseResult;
use serde_json::json;
use uuid::Uuid;

use crate::river_beam::tests::{bind_turn, ControlledAgent};
use crate::river_beam::RiverBeam;
use crate::BeamConfig;

/// A transport that can drop `done` frames (the worker died between
/// completion and publish) and records what it sent.
#[derive(Default)]
struct FlakyTransport {
    drop_done: AtomicBool,
    frames: Mutex<Vec<StreamChunk>>,
}

#[async_trait]
impl StreamTransport for FlakyTransport {
    async fn publish(&self, _channel: &str, frame: &StreamChunk) -> RiverbaseResult<String> {
        if self.drop_done.load(Ordering::SeqCst) && frame.chunk_type == StreamChunkType::Done {
            return Err(crate::BEM_101.with_data("nats down"));
        }
        self.frames.lock().unwrap().push(frame.clone());
        Ok("ok".into())
    }
}

impl FlakyTransport {
    fn sequences(&self) -> Vec<i64> {
        self.frames
            .lock()
            .unwrap()
            .iter()
            .filter_map(|f| f.sequence)
            .collect()
    }
}

fn request(agent: &str, user: &str) -> InvokeRequest {
    InvokeRequest {
        agent_name: agent.into(),
        content: vec![ContentPart::text("hello")],
        session_id: Uuid::new_v4(),
        model_override: None,
        metadata: json!({"api_key": "sk-live-secret"}),
        interaction_id: None,
        request_id: Some(Uuid::new_v4()),
        stream: true,
        actions: None,
        auth_context: Some(InvokeAuthContext {
            profile_id: None,
            user_id: Some(user.into()),
            org_id: None,
            mcp_token: None,
        }),
        context: None,
        execution_id: None,
        idempotency_key: None,
        client: None,
    }
}

fn beam(transport: &Arc<FlakyTransport>) -> RiverBeam {
    let transport: Arc<dyn StreamTransport> = transport.clone();
    let beam = RiverBeam::builder(BeamConfig::default())
        .stream(Arc::new(BeamStream::new(transport)))
        .build()
        .unwrap();
    beam.register_echo_agent("demo");
    beam
}

/// `started` and `done` are durable with sequences 1 and 2;
/// `done` goes out after the result is persisted; a reconnect after 1
/// replays only 2, then `done`.
#[tokio::test]
async fn durable_frames_replay_after_the_cursor() {
    let transport = Arc::new(FlakyTransport::default());
    let beam = beam(&transport);
    let req = request("demo", "u1");
    let (session, request_id) = (req.session_id, req.request_id.unwrap());
    let result = beam.invoke_result(req).await.unwrap();
    let execution = result.execution_id.unwrap();
    let frames = transport.frames.lock().unwrap().clone();
    let done = frames
        .iter()
        .find(|f| f.chunk_type == StreamChunkType::Done)
        .expect("done published");
    assert_eq!(done.sequence, Some(2), "started is 1, done is 2");
    assert_eq!(
        frames.last().unwrap().chunk_type,
        StreamChunkType::Done,
        "done is the last frame"
    );

    let events = beam.ops().events_after(execution, 0, 10).await.unwrap();
    assert_eq!(
        events
            .iter()
            .map(|e| e.event_type.as_str())
            .collect::<Vec<_>>(),
        ["started", "done"]
    );
    assert!(
        beam.ops().claim_batch("w", 10).await.unwrap().is_empty(),
        "both were marked sent"
    );

    let sse = beam
        .replay_stream(
            session,
            &request_id.to_string(),
            Some("1"),
            Some("u1"),
            None,
        )
        .await
        .unwrap();
    assert!(sse.contains("\"sequence\": 2"));
    assert!(
        !sse.contains("\"type\": \"started\""),
        "frames at or before the cursor are skipped"
    );
    assert!(
        sse.ends_with("event: done\ndata: {\"type\": \"done\"}\n\n"),
        "a completed execution closes"
    );
    let other = beam
        .replay_stream(session, &request_id.to_string(), None, Some("u2"), None)
        .await
        .unwrap_err();
    assert_eq!(other.errcode.as_str(), "BEM-069");

    // A request with no run yet (still queued): the plain replay falls
    // back to the session's last run, the exact one does not.
    let queued = Uuid::new_v4().to_string();
    let fallback = beam
        .replay_stream(session, &queued, None, Some("u1"), None)
        .await
        .unwrap();
    assert!(fallback.contains("event: done"), "{fallback}");
    let exact = beam
        .replay_request(session, &queued, None, Some("u1"), None)
        .await
        .unwrap();
    assert!(!exact.contains("event: done"), "{exact}");
    assert!(!exact.contains(&execution.to_string()), "{exact}");
    assert!(exact.contains("\"type\": \"ready\""), "{exact}");
}

/// A `done` frame whose publish was lost stays in the
/// outbox; the relay delivers it once.
#[tokio::test]
async fn the_relay_delivers_what_the_invoke_could_not() {
    let transport = Arc::new(FlakyTransport::default());
    transport.drop_done.store(true, Ordering::SeqCst);
    let beam = beam(&transport);
    beam.invoke_result(request("demo", "u1")).await.unwrap();
    assert!(transport.sequences().is_empty(), "done never went out");

    transport.drop_done.store(false, Ordering::SeqCst);
    let stats = beam.relay_once("w1").await.unwrap();
    assert_eq!(
        (stats.claimed, stats.sent, stats.failed),
        (1, 1, 0),
        "started was already marked sent"
    );
    assert_eq!(transport.sequences(), [2], "done goes out once");
    assert_eq!(
        beam.relay_once("w1").await.unwrap().claimed,
        0,
        "exactly once after it is sent"
    );
}

/// Only the session's owner may subscribe to its stream channel.
#[tokio::test]
async fn only_the_session_owner_subscribes() {
    let transport = Arc::new(FlakyTransport::default());
    let beam = beam(&transport);
    let req = request("demo", "u1");
    let channel = format!("ws.channel.{}:{}", req.session_id, req.request_id.unwrap());
    beam.invoke_result(req).await.unwrap();
    assert!(beam
        .authorize_stream_channel(&channel, Some("u1"), None)
        .await
        .unwrap());
    assert!(!beam
        .authorize_stream_channel(&channel, Some("u2"), None)
        .await
        .unwrap());
    assert!(!beam
        .authorize_stream_channel("ws.channel.nope", Some("u1"), None)
        .await
        .unwrap());
}

/// A failed invoke is a dead letter that holds only its ids, none of
/// the request; retry runs it again, discard removes it.
#[tokio::test]
async fn failed_invokes_become_dead_letters() {
    let transport = Arc::new(FlakyTransport::default());
    let beam = beam(&transport);
    // Fails its first call, then echoes.
    bind_turn(&beam, ControlledAgent::new("flaky", 0, true));
    let err = beam
        .invoke_result(request("flaky", "u1"))
        .await
        .unwrap_err();
    assert_eq!(err.errcode.as_str(), "BEM-082");
    let frames = transport.frames.lock().unwrap().clone();
    assert_eq!(
        frames.last().unwrap().chunk_type,
        StreamChunkType::Error,
        "the error frame went out"
    );

    let listed = beam.list_dead_letters().await.unwrap();
    let letter = &listed["letters"][0];
    assert_eq!(letter["task_name"], "invoke_agent");
    let mut keys: Vec<&str> = letter["payload"]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(keys, ["execution_id", "session_id"]);
    let id = Uuid::parse_str(letter["id"].as_str().unwrap()).unwrap();

    let retried = beam.retry_dead_letter(id).await.unwrap();
    assert_eq!(retried["attempts"], 1);
    let execution = Uuid::parse_str(letter["payload"]["execution_id"].as_str().unwrap()).unwrap();
    let row = beam
        .executions()
        .records()
        .get(execution)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        row.status.as_str(),
        "completed",
        "the retry ran the execution again"
    );
    beam.discard_dead_letter(id).await.unwrap();
    assert_eq!(
        beam.list_dead_letters().await.unwrap()["letters"],
        json!([])
    );
}

/// A held user is refused with the public code clients see.
#[tokio::test]
async fn legal_holds_block_deletion() {
    let transport = Arc::new(FlakyTransport::default());
    let beam = beam(&transport);
    let req = request("demo", "u1");
    let session = req.session_id;
    beam.invoke_result(req).await.unwrap();
    beam.holds().place("u1", None, None, None).await.unwrap();
    let err = beam.delete_user_agent_data("u1").await.unwrap_err();
    assert_eq!(
        crate::public_errors::to_public(err).errcode.as_str(),
        "BEM-433"
    );
    beam.holds().release("u1").await.unwrap();
    let deleted = beam.delete_user_agent_data("u1").await.unwrap();
    assert_eq!(deleted["sessions"], 1);
    assert!(beam.store().get_session(session).await.unwrap().is_none());
}

/// Holds that cannot be read.
struct UnreadableHolds;

#[async_trait]
impl LegalHoldStore for UnreadableHolds {
    async fn place(
        &self,
        _user_id: &str,
        _org_id: Option<Uuid>,
        _reason: Option<&str>,
        _placed_by: Option<&str>,
    ) -> beam_store::StoreResult<()> {
        Ok(())
    }

    async fn release(&self, _user_id: &str) -> beam_store::StoreResult<bool> {
        Ok(false)
    }

    async fn release_placed_by(
        &self,
        _user_id: &str,
        _placed_by: &str,
    ) -> beam_store::StoreResult<bool> {
        Ok(false)
    }

    async fn is_held(&self, _user_id: &str) -> beam_store::StoreResult<bool> {
        Err(crate::BEM_139.with_data("holds unavailable"))
    }
}

/// Plan 019: deletion fails closed when the holds cannot be read.
#[tokio::test]
async fn deletion_fails_closed_when_holds_cannot_be_read() {
    let transport: Arc<dyn StreamTransport> = Arc::new(FlakyTransport::default());
    let beam = RiverBeam::builder(BeamConfig::default())
        .stream(Arc::new(BeamStream::new(transport)))
        .holds(Arc::new(UnreadableHolds))
        .build()
        .unwrap();
    beam.register_echo_agent("demo");
    let req = request("demo", "u1");
    let session = req.session_id;
    beam.invoke_result(req).await.unwrap();
    let err = beam.delete_user_agent_data("u1").await.unwrap_err();
    assert_eq!(err.errcode.as_str(), "BEM-139");
    assert!(beam.store().get_session(session).await.unwrap().is_some());
}

#[tokio::test]
async fn reconcile_runs_once_at_a_time_and_readiness_tracks_drain() {
    let transport = Arc::new(FlakyTransport::default());
    let beam = beam(&transport);
    let held = beam.ops().try_reconcile_lock().await.unwrap().unwrap();
    assert_eq!(beam.reconcile_now().await.unwrap()["skipped"], true);
    held.release().await;
    let report = beam.reconcile_now().await.unwrap();
    assert_eq!(report["leases_expired"], 0);
    assert_eq!(beam.readiness().await["status"], "ok");
    beam.executions()
        .drain(std::time::Duration::from_millis(1))
        .await;
    assert_eq!(beam.readiness().await["status"], "unavailable");
    assert_eq!(RiverBeam::liveness()["status"], "ok");
}

/// The text of every frame the transport sent.
fn sent_text(transport: &FlakyTransport) -> String {
    serde_json::to_string(&*transport.frames.lock().unwrap()).unwrap()
}

/// Plan 030 acceptance 4: a secret the model streams never reaches a stream
/// client; the run ends with an error frame. The denial leaves no dead
/// letter, so the secret is not stored there either.
#[tokio::test]
async fn a_streamed_secret_ends_the_run_with_an_error_frame() {
    let transport = Arc::new(FlakyTransport::default());
    let beam = beam(&transport);
    let mut req = request("demo", "u1");
    req.content = vec![ContentPart::text("Your key is AKIAIOSFODNN7EXAMPLE")];
    let err = beam.invoke_result(req).await.unwrap_err();
    assert_eq!(err.errcode.as_str(), "BEM-061");
    let frames = transport.frames.lock().unwrap().clone();
    assert!(
        frames
            .iter()
            .any(|f| f.chunk_type == StreamChunkType::Error),
        "an error frame ends the stream"
    );
    assert!(!frames.iter().any(|f| f.chunk_type == StreamChunkType::Done));
    assert!(!sent_text(&transport).contains("AKIA"));
    assert_eq!(
        beam.list_dead_letters().await.unwrap()["letters"],
        json!([])
    );
}

/// Plan 030 acceptance 4: streamed PII reaches the client masked, and so
/// does the result and the stored answer.
#[tokio::test]
async fn streamed_pii_reaches_the_client_and_the_transcript_masked() {
    let transport = Arc::new(FlakyTransport::default());
    let beam = beam(&transport);
    let mut req = request("demo", "u1");
    req.content = vec![ContentPart::text("write to jane.doe@example.com")];
    let result = beam.invoke_result(req).await.unwrap();
    assert_eq!(
        result.content[0].text.as_deref(),
        Some("Echo: write to [redacted]")
    );
    let sent = sent_text(&transport);
    assert!(sent.contains("[redacted]"), "{sent}");
    assert!(!sent.contains("jane.doe"), "{sent}");
    let stored = beam
        .store()
        .list_messages(result.session_id.unwrap())
        .await
        .unwrap();
    let answer = stored
        .iter()
        .find(|m| m.content.to_string().contains("Echo:"))
        .expect("the stored answer");
    assert!(answer.content.to_string().contains("[redacted]"));
    assert!(!answer.content.to_string().contains("jane.doe"));
}
