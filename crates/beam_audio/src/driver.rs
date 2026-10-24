//! The realtime audio protocol (Python `handle_audio_websocket`), apart from
//! the socket: the host feeds inbound frames and gives an [`AudioSink`] for
//! the outbound ones (JSON text, WAV bytes, close codes).
//!
//! As in Python: the state starts at `LISTENING`; `end_of_speech` runs the turn
//! as a task bounded by `AUDIO_AI_TIMEOUT_SEC`; `interrupt` and `stop` cancel
//! it, and a cancelled turn still ends in `LISTENING`; more than 100 frames in
//! a second closes with 4429; no `pong` for 45 s closes with 4408. Audio comes
//! as binary frames or as base64 `audio_chunk` JSON.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine as _;
use futures_util::{Stream, StreamExt};
use serde_json::Value;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::backends::{AudioInvoker, SttBackend, TtsBackend};
use crate::messages::{ControlMessage, ControlType, ServerMessage};
use crate::pipeline::{run_audio_turn, AudioSink};
use crate::session::{
    can_transition, AppendOutcome, AudioSession, SessionState, TurnInputs, HEARTBEAT_INTERVAL_SEC,
    HEARTBEAT_TIMEOUT_SEC, MIN_UTTERANCE_BYTES,
};

/// Python `AUDIO_AI_TIMEOUT_SEC` default.
pub const DEFAULT_AI_TIMEOUT_SEC: f64 = 120.0;
/// Python `AUDIO_DEFAULT_AGENT_NAME` default.
pub const DEFAULT_AGENT_NAME: &str = "agent-calculator";

/// Timing and defaults of one audio connection.
#[derive(Debug, Clone)]
pub struct AudioConfig {
    pub ai_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub heartbeat_timeout: Duration,
    /// The agent when the client names none (`?agent=`).
    pub default_agent: String,
}

impl Default for AudioConfig {
    fn default() -> Self {
        Self {
            ai_timeout: Duration::from_secs_f64(DEFAULT_AI_TIMEOUT_SEC),
            heartbeat_interval: Duration::from_secs_f64(HEARTBEAT_INTERVAL_SEC),
            heartbeat_timeout: Duration::from_secs_f64(HEARTBEAT_TIMEOUT_SEC),
            default_agent: DEFAULT_AGENT_NAME.into(),
        }
    }
}

impl AudioConfig {
    /// `AUDIO_AI_TIMEOUT_SEC` and `AUDIO_DEFAULT_AGENT_NAME`.
    #[must_use]
    pub fn from_env() -> Self {
        let mut config = Self::default();
        if let Some(secs) = beam_env::secs("AUDIO_AI_TIMEOUT_SEC") {
            config.ai_timeout = Duration::from_secs_f64(secs);
        }
        if let Some(agent) = beam_env::string("AUDIO_DEFAULT_AGENT_NAME") {
            config.default_agent = agent;
        }
        config
    }
}

/// One frame from the client.
#[derive(Debug, Clone)]
pub enum Inbound {
    Binary(Vec<u8>),
    Text(String),
}

struct Shared {
    config: AudioConfig,
    stt: Arc<dyn SttBackend>,
    tts: Arc<dyn TtsBackend>,
    invoker: Arc<dyn AudioInvoker>,
    sink: Arc<dyn AudioSink>,
    session: Mutex<AudioSession>,
    last_pong: Mutex<Instant>,
}

fn state_message(state: SessionState) -> ServerMessage {
    ServerMessage::State {
        state: state.as_str().into(),
    }
}

impl Shared {
    fn session(&self) -> MutexGuard<'_, AudioSession> {
        self.session.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A no-op for the same state or an illegal move.
    async fn set_state(&self, next: SessionState) {
        let changed = {
            let mut session = self.session();
            if session.state != next && !can_transition(session.state, next) {
                tracing::warn!(
                    from = session.state.as_str(),
                    to = next.as_str(),
                    "audio-ws invalid transition"
                );
            }
            session.set_state(next)
        };
        if changed {
            self.sink.send_text(state_message(next)).await;
        }
    }

    async fn error(&self, code: &str) {
        self.sink.send_text(ServerMessage::error(code)).await;
    }

    /// Python `append_audio_bytes`.
    async fn append(&self, chunk: &[u8]) {
        let outcome = self.session().append_audio(chunk);
        match outcome {
            AppendOutcome::Appended => {}
            AppendOutcome::ChunkTooLarge => self.error("audio_chunk_too_large").await,
            AppendOutcome::BufferOverflow => {
                self.error("audio_buffer_overflow").await;
                self.set_state(SessionState::Listening).await;
            }
        }
    }
}

/// A turn's output: frames go to the connection, and `before_play` moves the
/// session to `SPEAKING`.
struct TurnSink(Arc<Shared>);

#[async_trait]
impl AudioSink for TurnSink {
    async fn send_text(&self, message: ServerMessage) {
        self.0.sink.send_text(message).await;
    }

    async fn send_bytes(&self, payload: Vec<u8>) {
        self.0.sink.send_bytes(payload).await;
    }

    async fn before_play(&self) {
        self.0.set_state(SessionState::Speaking).await;
    }
}

/// Run the agent on the turn's transcript and speak the answer.
async fn run_ai_response(shared: Arc<Shared>, seq: u64) {
    shared.set_state(SessionState::Processing).await;
    let (raw, session_id, inputs) = {
        let mut session = shared.session();
        (
            session.take_buffer(),
            session.session_id.clone(),
            session.inputs.clone(),
        )
    };
    let sink = TurnSink(shared.clone());
    let turn = run_audio_turn(
        &raw,
        seq,
        &session_id,
        &inputs,
        shared.stt.as_ref(),
        shared.tts.as_ref(),
        shared.invoker.as_ref(),
        &sink,
    );
    match tokio::time::timeout(shared.config.ai_timeout, turn).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            tracing::warn!(seq, error = %e, "audio-ws pipeline failed");
            shared.error("audio_pipeline_failed").await;
        }
        Err(_) => {
            tracing::warn!(seq, "audio-ws pipeline timeout");
            shared.error("audio_pipeline_timeout").await;
        }
    }
    shared.set_state(SessionState::Listening).await;
}

/// Python `_cancel_task`. A turn cut short still ends in `LISTENING`, which
/// Python's `finally` does.
async fn cancel(shared: &Shared, task: &mut Option<JoinHandle<()>>) {
    let Some(handle) = task.take() else {
        return;
    };
    if handle.is_finished() {
        return;
    }
    handle.abort();
    let _ = handle.await;
    shared.set_state(SessionState::Listening).await;
}

/// Ping the client on an interval; one silent past the timeout is closed (4408).
async fn heartbeat(shared: Arc<Shared>) {
    loop {
        tokio::time::sleep(shared.config.heartbeat_interval).await;
        let idle = shared
            .last_pong
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .elapsed();
        if idle > shared.config.heartbeat_timeout {
            tracing::warn!("audio-ws heartbeat timeout");
            shared.sink.close(4408, "Heartbeat timeout").await;
            return;
        }
        shared.sink.send_text(ServerMessage::Ping).await;
    }
}

/// The audio of an `audio_chunk` message, or `None` when it is not valid.
/// Python's `base64.b64decode` drops characters outside the alphabet.
fn decode_chunk(data: Option<&Value>) -> Option<Vec<u8>> {
    let encoded = data?.as_str().filter(|s| !s.is_empty())?;
    let kept: String = encoded
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '/' | '='))
        .collect();
    base64::engine::general_purpose::STANDARD.decode(kept).ok()
}

/// The connection's tasks, aborted if `run` is dropped before it ends.
struct Tasks {
    beat: JoinHandle<()>,
    turn: Option<JoinHandle<()>>,
}

impl Drop for Tasks {
    fn drop(&mut self) {
        self.beat.abort();
        if let Some(turn) = &self.turn {
            turn.abort();
        }
    }
}

/// One audio connection.
pub struct AudioDriver {
    shared: Arc<Shared>,
}

impl AudioDriver {
    #[must_use]
    pub fn new(
        config: AudioConfig,
        session: AudioSession,
        stt: Arc<dyn SttBackend>,
        tts: Arc<dyn TtsBackend>,
        invoker: Arc<dyn AudioInvoker>,
        sink: Arc<dyn AudioSink>,
    ) -> Self {
        Self {
            shared: Arc::new(Shared {
                config,
                stt,
                tts,
                invoker,
                sink,
                session: Mutex::new(session),
                last_pong: Mutex::new(Instant::now()),
            }),
        }
    }

    /// Serve the connection until `inbound` ends or the driver closes it.
    #[allow(clippy::too_many_lines)]
    pub async fn run<S>(self, mut inbound: S)
    where
        S: Stream<Item = Inbound> + Unpin + Send,
    {
        let shared = self.shared;
        let mut tasks = Tasks {
            beat: tokio::spawn(heartbeat(shared.clone())),
            turn: None,
        };
        let started = Instant::now();
        shared.set_state(SessionState::Listening).await;
        while let Some(frame) = inbound.next().await {
            if shared
                .session()
                .record_message(started.elapsed().as_secs_f64())
            {
                shared.error("rate_limit_exceeded").await;
                shared.sink.close(4429, "Too many messages").await;
                break;
            }
            let text = match frame {
                Inbound::Binary(bytes) if !bytes.is_empty() => {
                    shared.append(&bytes).await;
                    continue;
                }
                Inbound::Text(text) if !text.is_empty() => text,
                _ => continue,
            };
            let Ok(parsed) = serde_json::from_str::<Value>(&text) else {
                shared.error("invalid_control_message").await;
                continue;
            };
            // Plan 020 D8: the turn's request facts and client capabilities,
            // kept for every later turn.
            if parsed.get("type").and_then(Value::as_str) == Some("context") {
                match TurnInputs::parse(&parsed) {
                    Some(inputs) => shared.session().inputs = inputs,
                    None => shared.error("invalid_turn_context").await,
                }
                continue;
            }
            if parsed.get("type").and_then(Value::as_str) == Some("audio_chunk") {
                match decode_chunk(parsed.get("data")) {
                    None => shared.error("invalid_audio_chunk").await,
                    Some(audio) if audio.is_empty() => {}
                    Some(audio) => shared.append(&audio).await,
                }
                continue;
            }
            let control = parsed
                .is_object()
                .then(|| serde_json::from_value::<ControlMessage>(parsed).ok())
                .flatten();
            let Some(control) = control else {
                shared.error("invalid_control_message").await;
                continue;
            };
            match control.control_type {
                ControlType::Pong => {
                    *shared
                        .last_pong
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner) = Instant::now();
                }
                ControlType::Start => {
                    let state = shared.session().state;
                    if state == SessionState::Idle {
                        shared.set_state(SessionState::Listening).await;
                    } else {
                        shared.sink.send_text(state_message(state)).await;
                    }
                }
                ControlType::EndOfSpeech => {
                    let (state, buffered) = {
                        let session = shared.session();
                        (session.state, session.audio_buffer.len())
                    };
                    if state != SessionState::Listening {
                        shared.error("invalid_state_for_end_of_speech").await;
                        continue;
                    }
                    if buffered < MIN_UTTERANCE_BYTES {
                        shared.error("audio_too_short").await;
                        shared.session().audio_buffer.clear();
                        continue;
                    }
                    cancel(&shared, &mut tasks.turn).await;
                    let seq = {
                        let mut session = shared.session();
                        session.tts_seq += 1;
                        session.tts_seq
                    };
                    tasks.turn = Some(tokio::spawn(run_ai_response(shared.clone(), seq)));
                }
                ControlType::Interrupt => {
                    cancel(&shared, &mut tasks.turn).await;
                    let seq = {
                        let mut session = shared.session();
                        session.tts_seq += 1;
                        session.tts_seq
                    };
                    shared.sink.send_text(ServerMessage::AudioEnd { seq }).await;
                    shared.set_state(SessionState::Listening).await;
                }
                ControlType::Stop => {
                    cancel(&shared, &mut tasks.turn).await;
                    let was = {
                        let mut session = shared.session();
                        session.audio_buffer.clear();
                        std::mem::replace(&mut session.state, SessionState::Idle)
                    };
                    if was != SessionState::Idle {
                        shared
                            .sink
                            .send_text(state_message(SessionState::Idle))
                            .await;
                    }
                }
            }
        }
        cancel(&shared, &mut tasks.turn).await;
        drop(tasks);
    }
}
