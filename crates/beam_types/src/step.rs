use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::content::empty_object;

/// One human action required during an invocation. Mirrors Python `AgentAction`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentAction {
    pub action_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_id: Option<String>,
    #[serde(rename = "type", default = "default_action_type")]
    pub action_type: String,
    #[serde(default = "default_action_kind")]
    pub kind: String,
    pub request_id: String,
    pub title: String,
    pub description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submit_label: Option<String>,
    pub payload: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_schema: Option<Value>,
    #[serde(default = "empty_object")]
    pub metadata: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lc_step_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_name: Option<String>,
}

fn default_action_type() -> String {
    "confirmation".into()
}

fn default_action_kind() -> String {
    "beam.confirmation".into()
}

/// One runtime step recorded during an invocation. Mirrors Python `AgentStep`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentStep {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_id: Option<String>,
    pub lc_step_id: String,
    pub step_name: String,
    #[serde(default = "default_step_status")]
    pub status: String,
    #[serde(default = "empty_object")]
    pub input: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
}

fn default_step_status() -> String {
    "pending".into()
}
