use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::content::Modality;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ModelProvider {
    Openai,
    Anthropic,
    Custom,
    Ollama,
    Groq,
    Nvidia,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelConfig {
    pub name: String,
    pub provider: ModelProvider,
    #[serde(default, skip_serializing_if = "is_empty_object")]
    pub provider_config: Value,
    #[serde(default = "default_modalities")]
    pub modalities: Vec<Modality>,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub cost_per_1k_tokens: f64,
    #[serde(default = "default_max_tokens")]
    pub max_tokens: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
    #[serde(default, skip_serializing_if = "is_empty_object")]
    pub extra: Value,
}

fn is_empty_object(v: &Value) -> bool {
    v.is_object() && v.as_object().is_some_and(|m| m.is_empty())
}

fn default_modalities() -> Vec<Modality> {
    vec![Modality::Text]
}

fn default_max_tokens() -> u32 {
    4096
}
