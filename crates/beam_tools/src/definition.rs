//! Tool contracts (Python `tools/contract.py`).

use std::sync::Arc;

use async_trait::async_trait;
use beam_types::{AgentInput, InvokeAuthContext};
use riverbase_core::RiverbaseResult;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ToolRisk {
    Low,
    #[default]
    Medium,
    High,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ToolSource {
    #[default]
    Native,
    Mcp,
    Http,
    /// Run by the calling client (a browser tab); the run holds until it answers.
    Client,
}

/// A client tool's default timeout: how long its action waits for an answer
/// (Python rc8 `CLIENT_TOOL_TIMEOUT_SECONDS`).
pub const CLIENT_TOOL_TIMEOUT_SECONDS: f64 = 600.0;

/// What a tool call runs with besides its arguments.
#[derive(Debug, Clone, Default)]
pub struct ToolCallContext {
    pub execution_id: Uuid,
    pub call_id: String,
    pub auth: Option<InvokeAuthContext>,
    /// The invoke (native tools read its metadata).
    pub input: Option<Arc<AgentInput>>,
}

/// The code behind a tool.
#[async_trait]
pub trait ToolHandler: Send + Sync {
    /// # Errors
    /// Whatever the tool fails with; the runtime retries up to `max_retries`.
    async fn call(&self, args: Value, ctx: &ToolCallContext) -> RiverbaseResult<Value>;
}

fn timeout_default() -> f64 {
    30.0
}

fn yes() -> bool {
    true
}

/// A tool, with Python's field names and defaults.
#[derive(Clone, Serialize, Deserialize)]
#[serde(from = "WireToolDefinition")]
pub struct ToolDefinition {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub input_schema: Value,
    #[serde(default)]
    pub output_schema: Option<Value>,
    #[serde(default)]
    pub risk: ToolRisk,
    #[serde(default = "yes")]
    pub side_effect: bool,
    #[serde(default)]
    pub idempotent: bool,
    #[serde(default = "timeout_default")]
    pub timeout_seconds: f64,
    #[serde(default)]
    pub max_retries: u32,
    #[serde(default)]
    pub require_approval: bool,
    #[serde(default)]
    pub required_scopes: Vec<String>,
    #[serde(default)]
    pub source: ToolSource,
    #[serde(default)]
    pub mcp_server: Option<String>,
    /// Not serialized, as in Python (`exclude=True`). A definition without one
    /// echoes its arguments.
    #[serde(skip)]
    pub callable: Option<Arc<dyn ToolHandler>>,
}

/// What a definition reads from JSON: a client tool without a timeout gets
/// [`CLIENT_TOOL_TIMEOUT_SECONDS`], as in Python rc8 (`model_fields_set`).
#[derive(Deserialize)]
struct WireToolDefinition {
    name: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    input_schema: Value,
    #[serde(default)]
    output_schema: Option<Value>,
    #[serde(default)]
    risk: ToolRisk,
    #[serde(default = "yes")]
    side_effect: bool,
    #[serde(default)]
    idempotent: bool,
    #[serde(default)]
    timeout_seconds: Option<f64>,
    #[serde(default)]
    max_retries: u32,
    #[serde(default)]
    require_approval: bool,
    #[serde(default)]
    required_scopes: Vec<String>,
    #[serde(default)]
    source: ToolSource,
    #[serde(default)]
    mcp_server: Option<String>,
}

impl From<WireToolDefinition> for ToolDefinition {
    fn from(w: WireToolDefinition) -> Self {
        let timeout_seconds = w
            .timeout_seconds
            .unwrap_or(if w.source == ToolSource::Client {
                CLIENT_TOOL_TIMEOUT_SECONDS
            } else {
                timeout_default()
            });
        Self {
            name: w.name,
            description: w.description,
            input_schema: w.input_schema,
            output_schema: w.output_schema,
            risk: w.risk,
            side_effect: w.side_effect,
            idempotent: w.idempotent,
            timeout_seconds,
            max_retries: w.max_retries,
            require_approval: w.require_approval,
            required_scopes: w.required_scopes,
            source: w.source,
            mcp_server: w.mcp_server,
            callable: None,
        }
    }
}

impl std::fmt::Debug for ToolDefinition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToolDefinition")
            .field("name", &self.name)
            .field("source", &self.source)
            .field("side_effect", &self.side_effect)
            .field("require_approval", &self.require_approval)
            .finish_non_exhaustive()
    }
}

impl ToolDefinition {
    /// A definition with Python's defaults.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: String::new(),
            input_schema: Value::Object(serde_json::Map::new()),
            output_schema: None,
            risk: ToolRisk::Medium,
            side_effect: true,
            idempotent: false,
            timeout_seconds: timeout_default(),
            max_retries: 0,
            require_approval: false,
            required_scopes: Vec::new(),
            source: ToolSource::Native,
            mcp_server: None,
            callable: None,
        }
    }

    /// A tool the calling client runs: no side effect on the server and
    /// [`CLIENT_TOOL_TIMEOUT_SECONDS`] to answer.
    #[must_use]
    pub fn client(name: impl Into<String>) -> Self {
        Self {
            risk: ToolRisk::Low,
            side_effect: false,
            timeout_seconds: CLIENT_TOOL_TIMEOUT_SECONDS,
            source: ToolSource::Client,
            ..Self::new(name)
        }
    }

    #[must_use]
    pub fn is_client(&self) -> bool {
        self.source == ToolSource::Client
    }

    /// Whether a call must be approved by a human first: `require_approval`,
    /// or a high-risk tool (Python rc8 `default_mode`).
    #[must_use]
    pub fn needs_approval(&self) -> bool {
        self.require_approval || self.risk == ToolRisk::High
    }

    #[must_use]
    pub fn with_handler(mut self, handler: Arc<dyn ToolHandler>) -> Self {
        self.callable = Some(handler);
        self
    }
}
