use serde::{Deserialize, Serialize};

use crate::message::AgentMessage;
use crate::result::AgentResult;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StreamChunkType {
    Chunk,
    Done,
    Error,
}

impl StreamChunk {
    /// A terminal `error` frame carrying `error` text and no ids.
    #[must_use]
    pub fn error_frame(error: impl Into<String>) -> Self {
        Self {
            chunk_type: StreamChunkType::Error,
            data: None,
            result: None,
            error: Some(error.into()),
            id: None,
            agent_id: None,
            agent_name: None,
            session_id: None,
            interaction_id: None,
            execution_id: None,
            run_id: None,
            step_id: None,
            message_id: None,
            timestamp: None,
            sequence: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamChunk {
    #[serde(rename = "type")]
    pub chunk_type: StreamChunkType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<AgentMessage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<AgentResult>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interaction_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
    /// Monotonic durable sequence per execution (replay cursor).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence: Option<i64>,
}
