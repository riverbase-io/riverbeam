use serde::{Deserialize, Serialize};

pub const MAX_AUDIO_CHUNK_BYTES: usize = 256 * 1024;
pub const MAX_AUDIO_BUFFER_BYTES: usize = 8 * 1024 * 1024;
pub const MIN_UTTERANCE_BYTES: usize = 4 * 1024;
pub const HEARTBEAT_INTERVAL_SEC: f64 = 15.0;
pub const HEARTBEAT_TIMEOUT_SEC: f64 = 45.0;
pub const MAX_MSG_PER_SEC: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionState {
    #[serde(rename = "IDLE")]
    Idle,
    #[serde(rename = "LISTENING")]
    Listening,
    #[serde(rename = "PROCESSING")]
    Processing,
    #[serde(rename = "SPEAKING")]
    Speaking,
}

impl SessionState {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "IDLE",
            Self::Listening => "LISTENING",
            Self::Processing => "PROCESSING",
            Self::Speaking => "SPEAKING",
        }
    }
}

/// Whether `cur -> nxt` is a legal session transition (ports `_can_transition`).
#[must_use]
pub fn can_transition(cur: SessionState, nxt: SessionState) -> bool {
    use SessionState::{Idle, Listening, Processing, Speaking};
    matches!(
        (cur, nxt),
        (Idle | Processing | Speaking, Listening)
            | (Listening, Processing | Idle)
            | (Processing, Speaking | Idle)
            | (Speaking, Idle)
    )
}

/// Outcome of appending an audio chunk to the session buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendOutcome {
    Appended,
    ChunkTooLarge,
    BufferOverflow,
}

/// `WebM` EBML magic check (ports `_is_probable_webm`).
#[must_use]
pub fn is_probable_webm(chunk: &[u8]) -> bool {
    chunk.len() >= 4 && chunk[..4] == [0x1a, 0x45, 0xdf, 0xa3]
}

/// Per-connection audio session state.
#[derive(Debug)]
pub struct AudioSession {
    pub state: SessionState,
    pub audio_buffer: Vec<u8>,
    pub tts_seq: u64,
    pub agent_name: String,
    pub session_id: String,
    /// What the client last sent with `{"type": "context"}`; every later
    /// turn carries it.
    pub inputs: TurnInputs,
    msg_timestamps: std::collections::VecDeque<f64>,
}

/// A voice turn's request facts and client capabilities, as the
/// text chat sends them: `context` is a `beam_types::RequestContext`,
/// `client` a `beam_types::ClientCapabilities`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TurnInputs {
    pub context: Option<serde_json::Value>,
    pub client: Option<serde_json::Value>,
}

impl TurnInputs {
    /// The inputs of a `{"type": "context", "context": …, "client": …}`
    /// message, each checked against its wire type. `None` when either does
    /// not parse.
    #[must_use]
    pub fn parse(message: &serde_json::Value) -> Option<Self> {
        let part = |key: &str| message.get(key).filter(|v| !v.is_null()).cloned();
        let (context, client) = (part("context"), part("client"));
        if let Some(context) = &context {
            serde_json::from_value::<beam_types::RequestContext>(context.clone()).ok()?;
        }
        if let Some(client) = &client {
            serde_json::from_value::<beam_types::ClientCapabilities>(client.clone()).ok()?;
        }
        Some(Self { context, client })
    }
}

impl AudioSession {
    #[must_use]
    pub fn new(agent_name: impl Into<String>, session_id: impl Into<String>) -> Self {
        Self {
            state: SessionState::Idle,
            audio_buffer: Vec::new(),
            tts_seq: 0,
            agent_name: agent_name.into(),
            session_id: session_id.into(),
            inputs: TurnInputs::default(),
            msg_timestamps: std::collections::VecDeque::new(),
        }
    }

    /// Attempt the state transition; returns whether it was applied.
    pub fn set_state(&mut self, next: SessionState) -> bool {
        if self.state == next {
            return false;
        }
        if !can_transition(self.state, next) {
            return false;
        }
        self.state = next;
        true
    }

    /// Append one utterance chunk, enforcing per-chunk and buffer limits.
    pub fn append_audio(&mut self, chunk: &[u8]) -> AppendOutcome {
        if chunk.len() > MAX_AUDIO_CHUNK_BYTES {
            return AppendOutcome::ChunkTooLarge;
        }
        if self.audio_buffer.len() + chunk.len() > MAX_AUDIO_BUFFER_BYTES {
            self.audio_buffer.clear();
            return AppendOutcome::BufferOverflow;
        }
        self.audio_buffer.extend_from_slice(chunk);
        AppendOutcome::Appended
    }

    /// Record a message timestamp and report whether the 1s rate limit is exceeded.
    pub fn record_message(&mut self, now: f64) -> bool {
        self.msg_timestamps.push_back(now);
        while let Some(&front) = self.msg_timestamps.front() {
            if now - front > 1.0 {
                self.msg_timestamps.pop_front();
            } else {
                break;
            }
        }
        self.msg_timestamps.len() > MAX_MSG_PER_SEC
    }

    pub fn take_buffer(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.audio_buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transitions_match_reference_table() {
        assert!(can_transition(SessionState::Idle, SessionState::Listening));
        assert!(!can_transition(SessionState::Idle, SessionState::Speaking));
        assert!(can_transition(
            SessionState::Processing,
            SessionState::Speaking
        ));
        assert!(!can_transition(
            SessionState::Listening,
            SessionState::Speaking
        ));
    }

    #[test]
    fn append_enforces_limits() {
        let mut s = AudioSession::new("demo", "sid");
        assert_eq!(s.append_audio(&[0u8; 10]), AppendOutcome::Appended);
        assert_eq!(
            s.append_audio(&vec![0u8; MAX_AUDIO_CHUNK_BYTES + 1]),
            AppendOutcome::ChunkTooLarge
        );
    }

    #[test]
    fn rate_limit_trips_above_threshold() {
        let mut s = AudioSession::new("demo", "sid");
        let mut tripped = false;
        for _ in 0..(MAX_MSG_PER_SEC + 5) {
            tripped = s.record_message(0.5);
        }
        assert!(tripped);
    }
}
