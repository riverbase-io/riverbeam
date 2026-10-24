//! Realtime audio WebSocket pipeline.
//!
//! The [`SessionState`] machine with validated transitions and
//! buffering limits, the control/server messages, pure-Rust WAV encode,
//! `SttBackend`/`TtsBackend` with Python's mocks and the D8 HTTP sidecars
//! (`audio-http`), [`run_audio_turn`] (STT → agent → TTS) and the connection
//! protocol in [`AudioDriver`] (Python `handle_audio_websocket`). Native `WebM`
//! decode and the socket bind at the host layer (see `audio-decode` feature).

#[allow(clippy::wildcard_imports)] // The error catalog (`beam_errors`).
pub(crate) use beam_errors::*;

mod backends;
mod driver;
mod format;
mod messages;
mod pipeline;
mod session;

pub use backends::{
    backends_from_env, build_stt_backend, build_tts_backend, AudioInvoker, MockStt, MockTts,
    SttBackend, TtsBackend,
};
#[cfg(feature = "audio-http")]
pub use backends::{HttpStt, HttpTts};
pub use driver::{AudioConfig, AudioDriver, Inbound, DEFAULT_AGENT_NAME, DEFAULT_AI_TIMEOUT_SEC};
pub use format::{decode_to_pcm_16k, parse_wav_pcm_f32, pcm_f32_to_wav_bytes, resample_pcm};
pub use messages::{ControlMessage, ControlType, ServerMessage};
pub use pipeline::{run_audio_turn, AudioSink};
pub use session::{
    can_transition, is_probable_webm, AppendOutcome, AudioSession, SessionState, TurnInputs,
    HEARTBEAT_INTERVAL_SEC, HEARTBEAT_TIMEOUT_SEC, MAX_AUDIO_BUFFER_BYTES, MAX_AUDIO_CHUNK_BYTES,
    MAX_MSG_PER_SEC, MIN_UTTERANCE_BYTES,
};
