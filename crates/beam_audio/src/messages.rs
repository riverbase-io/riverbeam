use serde::{Deserialize, Serialize};

/// Client → server control message (ports `ControlMessage`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlType {
    Start,
    EndOfSpeech,
    Interrupt,
    Stop,
    Pong,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlMessage {
    #[serde(rename = "type")]
    pub control_type: ControlType,
}

/// Server → client JSON messages (tagged on `type`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    State {
        state: String,
    },
    Ping,
    Text {
        text: String,
        seq: u64,
        #[serde(rename = "final")]
        final_: bool,
        phase: String,
    },
    AudioStart {
        seq: u64,
        format: String,
    },
    AudioEnd {
        seq: u64,
    },
    Error {
        error: String,
    },
}

impl ServerMessage {
    #[must_use]
    pub fn text(text: impl Into<String>, seq: u64, final_: bool, phase: &str) -> Self {
        Self::Text {
            text: text.into(),
            seq,
            final_,
            phase: phase.to_string(),
        }
    }

    #[must_use]
    pub fn error(error: impl Into<String>) -> Self {
        Self::Error {
            error: error.into(),
        }
    }
}
