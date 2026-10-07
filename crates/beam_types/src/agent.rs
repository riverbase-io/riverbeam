use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::action::ActionResponseEnvelope;
use crate::content::{ContentPart, Modality};
use crate::invoke::InvokeAuthContext;
use crate::message::AgentMessage;
use crate::model::ModelConfig;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentMetadata {
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    #[serde(default = "default_text_modalities")]
    pub supported_modalities: Vec<Modality>,
    #[serde(default = "default_text_modalities")]
    pub output_modalities: Vec<Modality>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default = "default_version")]
    pub version: String,
}

fn default_text_modalities() -> Vec<Modality> {
    vec![Modality::Text]
}

fn default_version() -> String {
    "1.0.0".into()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentInput {
    pub content: Vec<ContentPart>,
    pub session_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interaction_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<Uuid>,
    pub run_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_override: Option<String>,
    #[serde(default, skip_serializing_if = "is_empty_object")]
    pub metadata: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_context: Option<InvokeAuthContext>,
    #[serde(default)]
    pub stream: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actions: Option<Vec<ActionResponseEnvelope>>,
}

fn is_empty_object(v: &Value) -> bool {
    v.is_null() || (v.is_object() && v.as_object().is_some_and(|m| m.is_empty()))
}

impl AgentInput {
    pub fn modalities(&self) -> HashSet<Modality> {
        self.content
            .iter()
            .map(ContentPart::detected_modality)
            .collect()
    }

    pub fn user_id(&self) -> Option<String> {
        self.auth_context.as_ref().and_then(|a| a.user_id.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentContext {
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    #[serde(default)]
    pub conversation_history: Vec<AgentMessage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_config_resolved: Option<ModelConfig>,
    #[serde(default)]
    pub available_tools: Vec<String>,
    #[serde(default, skip_serializing_if = "is_empty_object")]
    pub metadata: Value,
}
