//! The runtime contract (Python `runtime/contract.py`, `capabilities.py`,
//! `kernel/contracts/execution_ref.py`).

use std::sync::Arc;

use async_trait::async_trait;
use beam_execution::Lease;
use beam_types::{ActionResponseEnvelope, AgentInput, AgentMessage, InvokeAuthContext};
use futures_util::stream::BoxStream;
use riverbase_core::RiverbaseResult;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::events::RuntimeEvent;
use crate::seams::{ModelCaller, NoModel, NoTools, ToolExecutor};

/// Advertised runtime features. Field names match Python's `RuntimeCapabilities`.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RuntimeCapabilities {
    pub streaming: bool,
    pub durable_execution: bool,
    pub resume: bool,
    pub cancellation: bool,
    pub state_inspection: bool,
    pub checkpointing: bool,
    pub time_travel: bool,
    pub subgraphs: bool,
    pub structured_output: bool,
    pub native_tools: bool,
}

impl RuntimeCapabilities {
    /// Whether the capability called `name` (Python's field name) is advertised.
    #[must_use]
    pub fn has(&self, name: &str) -> bool {
        match name {
            "streaming" => self.streaming,
            "durable_execution" => self.durable_execution,
            "resume" => self.resume,
            "cancellation" => self.cancellation,
            "state_inspection" => self.state_inspection,
            "checkpointing" => self.checkpointing,
            "time_travel" => self.time_travel,
            "subgraphs" => self.subgraphs,
            "structured_output" => self.structured_output,
            "native_tools" => self.native_tools,
            _ => false,
        }
    }
}

/// Fail with `BEM-391` (public `BEM-424`) unless `runtime` advertises `name`.
///
/// # Errors
/// `BEM-391` when the capability is missing.
pub fn ensure_capability(runtime: &dyn AgentRuntime, name: &str) -> RiverbaseResult<()> {
    if runtime.capabilities().has(name) {
        Ok(())
    } else {
        Err(crate::BEM_391.with_data(format!(
            "Runtime '{}' does not support {name}",
            runtime.runtime_name()
        )))
    }
}

/// Opaque runtime identity. Beam stores it and never reads into `ref`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeExecutionRef {
    pub runtime: String,
    #[serde(default)]
    pub runtime_version: Option<String>,
    #[serde(default, rename = "ref")]
    pub reference: Map<String, Value>,
}

impl RuntimeExecutionRef {
    /// Read a ref from its JSON dump (`agent_execution.runtime_ref`).
    ///
    /// # Errors
    /// `BEM-392` when the value is not a runtime ref.
    pub fn from_value(value: &Value) -> RiverbaseResult<Self> {
        serde_json::from_value(value.clone()).map_err(|e| crate::BEM_392.with_data(e.to_string()))
    }

    #[must_use]
    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }
}

/// Start a new run of an execution.
#[derive(Debug, Clone)]
pub struct RuntimeStartRequest {
    pub execution_id: Uuid,
    pub run_id: Uuid,
    pub input: AgentInput,
    pub runtime_ref: Option<RuntimeExecutionRef>,
}

/// Continue a run that stopped for human input.
///
/// Rust adds `input` (Python reads the invoke from the runtime context), so the
/// result can carry the invoke's identity.
#[derive(Debug, Clone)]
pub struct RuntimeResumeRequest {
    pub execution_id: Uuid,
    pub run_id: Uuid,
    pub runtime_ref: RuntimeExecutionRef,
    pub actions: Vec<ActionResponseEnvelope>,
    pub input: AgentInput,
}

/// What Beam hands a runtime for one start or resume.
///
/// The model gateway and the tool runtime arrive as the
/// [`ModelCaller`] and [`ToolExecutor`] seams; the defaults refuse model calls
/// and offer no tools.
#[derive(Clone)]
pub struct RuntimeContext {
    pub auth: Option<InvokeAuthContext>,
    pub history: Vec<AgentMessage>,
    pub knowledge_context: Vec<Value>,
    pub long_term_memory: Vec<Value>,
    pub metadata: Map<String, Value>,
    pub model_gateway: Arc<dyn ModelCaller>,
    pub tool_runtime: Arc<dyn ToolExecutor>,
    pub available_tools: Vec<String>,
    /// `available_tools` was computed from the tool catalog and is the whole
    /// allowlist, even when empty. Otherwise an empty list means "no catalog".
    pub restrict_tools: bool,
    /// The run's lease. Runtime state writes are fenced by it, so a
    /// worker that lost the run cannot overwrite the new owner's state.
    pub lease: Option<Lease>,
    /// Fits each model call of the run to its budget and meters it (plan 023
    /// A2); without one, calls go as the run built them.
    pub fitter: Option<Arc<dyn crate::seams::ContextFitter>>,
}

impl Default for RuntimeContext {
    fn default() -> Self {
        Self {
            auth: None,
            history: Vec::new(),
            knowledge_context: Vec::new(),
            long_term_memory: Vec::new(),
            metadata: Map::new(),
            model_gateway: Arc::new(NoModel),
            tool_runtime: Arc::new(NoTools),
            available_tools: Vec::new(),
            restrict_tools: false,
            lease: None,
            fitter: None,
        }
    }
}

/// A runtime's view of a stored run (`inspect`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeState {
    pub runtime_ref: RuntimeExecutionRef,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub metadata: Map<String, Value>,
}

/// Health of a runtime's state store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthStatus {
    pub healthy: bool,
    #[serde(default)]
    pub detail: String,
}

/// A runtime's event stream. It ends after an `execution_completed`,
/// `execution_interrupted` or `execution_failed` event, or with an error item.
pub type EventStream = BoxStream<'static, RiverbaseResult<RuntimeEvent>>;

/// An agent runtime adapter.
#[async_trait]
pub trait AgentRuntime: Send + Sync {
    fn runtime_name(&self) -> &str;
    fn capabilities(&self) -> RuntimeCapabilities;
    /// Version of this adapter's state format; stored with each checkpoint.
    fn adapter_version(&self) -> &str;
    /// Version of the library under the adapter; stored with each checkpoint.
    fn library_version(&self) -> &str;

    /// The ref a new execution gets.
    /// The agent layer of the tool policy: rules this runtime's
    /// agent sets on its tools. The default sets none.
    fn tool_rules(&self) -> Vec<beam_types::tool_policy::ToolRule> {
        Vec::new()
    }

    fn allocate_ref(&self, execution_id: Uuid) -> RuntimeExecutionRef;

    /// Whether this runtime can read a checkpoint written by these versions.
    fn can_read_checkpoint(
        &self,
        adapter_version: Option<&str>,
        library_version: Option<&str>,
    ) -> bool {
        crate::compat::same_checkpoint_versions(
            adapter_version,
            Some(self.adapter_version()),
            library_version,
            Some(self.library_version()),
        )
    }

    fn start(&self, request: RuntimeStartRequest, context: RuntimeContext) -> EventStream;
    fn resume(&self, request: RuntimeResumeRequest, context: RuntimeContext) -> EventStream;

    /// Check that `request` can resume, changing nothing. Beam calls this before
    /// it resolves the request's actions, so a refused resume leaves them pending.
    ///
    /// # Errors
    /// The error [`Self::resume`] would refuse with.
    async fn check_resume(
        &self,
        _request: &RuntimeResumeRequest,
        _context: &RuntimeContext,
    ) -> RiverbaseResult<()> {
        Ok(())
    }

    /// Stop a run and record it as cancelled.
    ///
    /// # Errors
    /// `BEM-391` when the runtime cannot cancel; store errors.
    async fn cancel(
        &self,
        runtime_ref: &RuntimeExecutionRef,
        context: &RuntimeContext,
    ) -> RiverbaseResult<()>;

    /// The stored state of a run, if this runtime keeps any.
    ///
    /// # Errors
    /// Store errors.
    async fn inspect(
        &self,
        runtime_ref: &RuntimeExecutionRef,
        context: &RuntimeContext,
    ) -> RiverbaseResult<Option<RuntimeState>>;
}
