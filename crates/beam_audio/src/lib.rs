//! Realtime audio WebSocket pipeline.
//!
//! The [`SessionState`] machine with validated transitions and
//! buffering limits, the control/server [`messages`], pure-Rust WAV encode in
//! [`format`], `SttBackend`/`TtsBackend` traits with always-available mocks,
//! and [`run_audio_turn`] (STT → agent → TTS). Native WebM decode and the WS
//! transport bind at the host layer (see `audio-decode` feature).

mod errors;
#[allow(unused_imports)]
pub(crate) use errors::*;

mod backends;
mod format;
mod messages;
mod pipeline;
mod session;

pub use backends::{AudioInvoker, MockStt, MockTts, SttBackend, TtsBackend};
#[cfg(feature = "audio-http-stt")]
pub use backends::HttpStt;
pub use format::{decode_to_pcm_16k, parse_wav_pcm_f32, pcm_f32_to_wav_bytes, resample_pcm};
pub use messages::{ControlMessage, ControlType, ServerMessage};
pub use pipeline::{run_audio_turn, AudioSink};
pub use session::{
    can_transition, is_probable_webm, AppendOutcome, AudioSession, SessionState,
    HEARTBEAT_INTERVAL_SEC, HEARTBEAT_TIMEOUT_SEC, MAX_AUDIO_BUFFER_BYTES, MAX_AUDIO_CHUNK_BYTES,
    MAX_MSG_PER_SEC, MIN_UTTERANCE_BYTES,
};
