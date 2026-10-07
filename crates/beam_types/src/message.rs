use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::content::ContentPart;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AgentMessageRole {
    System,
    Human,
    Ai,
    Tool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentMessageType {
    Message,
    ToolResult,
    Action,
    ActionResult,
    Step,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AgentMessagePhase {
    #[default]
    Final,
    Thinking,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentMessage {
    #[serde(default = "default_role")]
    pub role: AgentMessageRole,
    #[serde(default = "default_msg_type")]
    #[serde(rename = "type")]
    pub msg_type: AgentMessageType,
    #[serde(default)]
    pub content: Vec<ContentPart>,
    #[serde(default = "new_uuid")]
    pub id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default = "utc_now")]
    pub timestamp: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interaction_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_args: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_result: Option<Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<Value>,
    #[serde(default)]
    pub is_delta: bool,
    #[serde(default)]
    pub phase: AgentMessagePhase,
    #[serde(default = "empty_metadata")]
    pub metadata: Value,
}

fn default_role() -> AgentMessageRole {
    AgentMessageRole::Ai
}

fn default_msg_type() -> AgentMessageType {
    AgentMessageType::Message
}

fn new_uuid() -> Uuid {
    Uuid::new_v4()
}

fn utc_now() -> DateTime<Utc> {
    Utc::now()
}

fn empty_metadata() -> Value {
    Value::Object(Default::default())
}

impl Default for AgentMessage {
    fn default() -> Self {
        Self {
            role: default_role(),
            msg_type: default_msg_type(),
            content: Vec::new(),
            id: new_uuid(),
            name: None,
            timestamp: utc_now(),
            run_id: None,
            request_id: None,
            interaction_id: None,
            step_id: None,
            tool_call_id: None,
            tool_name: None,
            tool_args: None,
            tool_result: None,
            tool_calls: Vec::new(),
            is_delta: false,
            phase: AgentMessagePhase::default(),
            metadata: empty_metadata(),
        }
    }
}
