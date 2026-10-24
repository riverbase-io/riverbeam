use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use riverbase_core::RiverbaseResult;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    #[serde(default)]
    pub confirm_required: bool,
    #[serde(default)]
    pub parameters: Value,
}

#[async_trait]
pub trait Tool: Send + Sync {
    fn spec(&self) -> ToolSpec;
    async fn invoke(&self, args: Value, input: &beam_types::AgentInput) -> RiverbaseResult<Value>;
}

#[must_use]
pub fn json_params(properties: Value, required: &[&str]) -> Value {
    let mut schema = serde_json::Map::new();
    schema.insert("type".into(), "object".into());
    schema.insert("properties".into(), properties);
    schema.insert("required".into(), serde_json::json!(required));
    Value::Object(schema)
}
