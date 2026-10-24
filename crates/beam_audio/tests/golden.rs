//! The audio protocol replayed against Python's `handle_audio_websocket`
//! (`tests/audio_compat/fixtures.json`, `scripts/gen_audio_fixtures.py`):
//! the same inbound frames give the same outbound frames.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use beam_audio::{
    can_transition, AudioConfig, AudioDriver, AudioInvoker, AudioSession, AudioSink, Inbound,
    MockStt, ServerMessage, SessionState, TtsBackend, HEARTBEAT_INTERVAL_SEC,
    HEARTBEAT_TIMEOUT_SEC, MAX_AUDIO_BUFFER_BYTES, MAX_AUDIO_CHUNK_BYTES, MAX_MSG_PER_SEC,
    MIN_UTTERANCE_BYTES,
};
use riverbase_core::RiverbaseResult;
use serde_json::{json, Value};

fn fixture() -> Value {
    serde_json::from_str(include_str!("../../../tests/audio_compat/fixtures.json"))
        .expect("fixture parses")
}

#[derive(Default)]
struct Recorder(Mutex<Vec<Value>>);

#[async_trait]
impl AudioSink for Recorder {
    async fn send_text(&self, message: ServerMessage) {
        self.0
            .lock()
            .unwrap()
            .push(serde_json::to_value(message).unwrap());
    }
    async fn send_bytes(&self, payload: Vec<u8>) {
        self.0
            .lock()
            .unwrap()
            .push(json!({"$bytes": payload.len()}));
    }
    async fn close(&self, code: u16, reason: &str) {
        self.0
            .lock()
            .unwrap()
            .push(json!({"$close": code, "reason": reason}));
    }
}

/// Python's fake agent: `Echo: <transcript>`, or never answers.
struct Agent {
    hang: bool,
}

#[async_trait]
impl AudioInvoker for Agent {
    async fn invoke(
        &self,
        _session_id: &str,
        transcript: &str,
        _inputs: &beam_audio::TurnInputs,
    ) -> RiverbaseResult<String> {
        if self.hang {
            std::future::pending::<()>().await;
        }
        Ok(format!("Echo: {transcript}"))
    }
}

struct Wav(usize);

#[async_trait]
impl TtsBackend for Wav {
    async fn synthesize(&self, _text: &str) -> RiverbaseResult<Vec<u8>> {
        Ok(vec![0; self.0])
    }
}

fn inbound(frame: &Value) -> Inbound {
    match frame.get("bytes").and_then(Value::as_u64) {
        Some(n) => Inbound::Binary(vec![0; usize::try_from(n).unwrap()]),
        None => Inbound::Text(frame["text"].as_str().unwrap().to_owned()),
    }
}

async fn replay(name: &str) {
    let fixture = fixture();
    let scenario = &fixture["scenarios"][name];
    let config = AudioConfig {
        ai_timeout: Duration::from_secs_f64(scenario["timeout"].as_f64().unwrap_or(30.0)),
        ..AudioConfig::default()
    };
    let recorder = Arc::new(Recorder::default());
    let driver = AudioDriver::new(
        config,
        AudioSession::new("voice", "6f1c2b9a-0d3e-4b5a-9c8d-7e6f5a4b3c2d"),
        Arc::new(MockStt::new(fixture["transcript"].as_str().unwrap())),
        Arc::new(Wav(
            usize::try_from(fixture["wav_bytes"].as_u64().unwrap()).unwrap()
        )),
        Arc::new(Agent {
            hang: scenario["hang"].as_bool().unwrap(),
        }),
        recorder.clone(),
    );
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let frames =
        futures_util::stream::unfold(rx, |mut rx| async { rx.recv().await.map(|f| (f, rx)) });
    let served = tokio::spawn(driver.run(Box::pin(frames)));
    // The generator delivers each frame after a 50 ms pause, bursts at once.
    for frame in scenario["inbound"].as_array().unwrap() {
        if frame.get("burst").is_none() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let _ = tx.send(inbound(frame));
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(tx);
    served.await.unwrap();
    let sent = recorder.0.lock().unwrap().clone();
    assert_eq!(Value::Array(sent), scenario["outbound"], "scenario {name}");
}

#[tokio::test(start_paused = true)]
async fn turn() {
    replay("turn").await;
}

#[tokio::test(start_paused = true)]
async fn controls() {
    replay("controls").await;
}

#[tokio::test(start_paused = true)]
async fn base64_turn() {
    replay("base64_turn").await;
}

#[tokio::test(start_paused = true)]
async fn interrupt_mid_turn() {
    replay("interrupt_mid_turn").await;
}

#[tokio::test(start_paused = true)]
async fn stop_mid_turn() {
    replay("stop_mid_turn").await;
}

#[tokio::test(start_paused = true)]
async fn timeout() {
    replay("timeout").await;
}

#[tokio::test(start_paused = true)]
async fn rate_limit() {
    replay("rate_limit").await;
}

#[tokio::test(start_paused = true)]
async fn overflow() {
    replay("overflow").await;
}

#[test]
fn constants_and_transitions_match_python() {
    let fixture = fixture();
    let c = &fixture["constants"];
    assert_eq!(c["max_audio_chunk_bytes"], MAX_AUDIO_CHUNK_BYTES);
    assert_eq!(c["max_audio_buffer_bytes"], MAX_AUDIO_BUFFER_BYTES);
    assert_eq!(c["min_utterance_bytes"], MIN_UTTERANCE_BYTES);
    assert_eq!(c["heartbeat_interval_sec"], HEARTBEAT_INTERVAL_SEC);
    assert_eq!(c["heartbeat_timeout_sec"], HEARTBEAT_TIMEOUT_SEC);
    assert_eq!(c["max_msg_per_sec"], MAX_MSG_PER_SEC);
    let state = |s: &Value| serde_json::from_value::<SessionState>(s.clone()).unwrap();
    for row in fixture["transitions"].as_array().unwrap() {
        assert_eq!(
            can_transition(state(&row["from"]), state(&row["to"])),
            row["allowed"].as_bool().unwrap(),
            "{row}"
        );
    }
}

/// No `pong` for longer than the timeout: pings at 15, 30 and 45 s, then the
/// heartbeat closes with 4408 at 60 s.
#[tokio::test(start_paused = true)]
async fn heartbeat_closes_a_silent_client() {
    let recorder = Arc::new(Recorder::default());
    let driver = AudioDriver::new(
        AudioConfig::default(),
        AudioSession::new("voice", "s"),
        Arc::new(MockStt::new("x")),
        Arc::new(Wav(1)),
        Arc::new(Agent { hang: false }),
        recorder.clone(),
    );
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Inbound>();
    let frames =
        futures_util::stream::unfold(rx, |mut rx| async { rx.recv().await.map(|f| (f, rx)) });
    let served = tokio::spawn(driver.run(Box::pin(frames)));
    tokio::time::sleep(Duration::from_secs(61)).await;
    drop(tx);
    served.await.unwrap();
    let sent = recorder.0.lock().unwrap().clone();
    assert_eq!(
        sent,
        vec![
            json!({"type": "state", "state": "LISTENING"}),
            json!({"type": "ping"}),
            json!({"type": "ping"}),
            json!({"type": "ping"}),
            json!({"$close": 4408, "reason": "Heartbeat timeout"}),
        ]
    );
}

/// An agent that records the inputs of each turn.
#[derive(Default)]
struct Inputs(Mutex<Vec<beam_audio::TurnInputs>>);

#[async_trait]
impl AudioInvoker for Inputs {
    async fn invoke(
        &self,
        _session_id: &str,
        transcript: &str,
        inputs: &beam_audio::TurnInputs,
    ) -> RiverbaseResult<String> {
        self.0.lock().unwrap().push(inputs.clone());
        Ok(format!("Echo: {transcript}"))
    }
}

/// Run `frames` through a driver with an [`Inputs`] agent; returns the
/// agent's turns and the frames sent.
async fn run_frames(frames: Vec<Inbound>) -> (Vec<beam_audio::TurnInputs>, Vec<Value>) {
    let recorder = Arc::new(Recorder::default());
    let agent = Arc::new(Inputs::default());
    let driver = AudioDriver::new(
        AudioConfig::default(),
        AudioSession::new("voice", "6f1c2b9a-0d3e-4b5a-9c8d-7e6f5a4b3c2d"),
        Arc::new(MockStt::new("hello")),
        Arc::new(Wav(64)),
        agent.clone(),
        recorder.clone(),
    );
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let stream =
        futures_util::stream::unfold(rx, |mut rx| async { rx.recv().await.map(|f| (f, rx)) });
    let served = tokio::spawn(driver.run(Box::pin(stream)));
    for frame in frames {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = tx.send(frame);
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(tx);
    served.await.unwrap();
    let turns = agent.0.lock().unwrap().clone();
    let sent = recorder.0.lock().unwrap().clone();
    (turns, sent)
}

/// Plan 020: a `context` message's facts and client capabilities ride on
/// every later turn.
#[tokio::test(start_paused = true)]
async fn a_context_message_reaches_the_next_turn() {
    let context = json!({"facts": []});
    let client = json!({"client_id": "voice-1", "kind": "web", "tools": ["show_form"]});
    let message = json!({"type": "context", "context": context, "client": client});
    let (turns, _) = run_frames(vec![
        Inbound::Text(message.to_string()),
        Inbound::Binary(vec![0; MIN_UTTERANCE_BYTES * 2]),
        Inbound::Text(json!({"type": "end_of_speech"}).to_string()),
    ])
    .await;
    assert_eq!(turns.len(), 1, "{turns:?}");
    assert_eq!(turns[0].client.as_ref(), Some(&client));
    assert!(turns[0].context.is_some());
}

/// Plan 020: a `context` message whose client capabilities do not parse is
/// refused, and nothing is kept.
#[tokio::test(start_paused = true)]
async fn an_invalid_context_message_is_refused() {
    let message = json!({"type": "context", "client": {"tools": "not-a-list"}});
    let (turns, sent) = run_frames(vec![
        Inbound::Text(message.to_string()),
        Inbound::Binary(vec![0; MIN_UTTERANCE_BYTES * 2]),
        Inbound::Text(json!({"type": "end_of_speech"}).to_string()),
    ])
    .await;
    assert!(
        sent.iter()
            .any(|m| m.to_string().contains("invalid_turn_context")),
        "{sent:?}"
    );
    assert_eq!(turns.len(), 1);
    assert_eq!(turns[0], beam_audio::TurnInputs::default());
}
