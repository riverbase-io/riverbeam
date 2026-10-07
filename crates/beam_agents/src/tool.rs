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
    async fn invoke(
        &self,
        args: Value,
        input: &beam_types::AgentInput,
    ) -> RiverbaseResult<Value>;
}

pub fn json_params(properties: Value, required: &[&str]) -> Value {
    serde_json::json!({
        "type": "object",
        "properties": properties,
        "required": required,
    })
}
