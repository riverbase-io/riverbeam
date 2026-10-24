//! Agent runtime contract for riverbeam.
//!
//! A port of fluvius-beam's `runtime/` package: the [`AgentRuntime`] contract,
//! runtime events and their wire mapping, the runtime registry, HITL action
//! contracts and checkpoint compatibility. [`RigRuntime`] replaces `LangGraph`:
//! Beam drives rig's sans-I/O `AgentRun`, calling the model and tools through
//! the [`ModelCaller`] and [`ToolExecutor`] seams and saving the run after
//! every step in a [`RuntimeStateStore`].

#[allow(clippy::wildcard_imports)] // The error catalog (`beam_errors`).
pub(crate) use beam_errors::*;

pub mod actions;
pub mod compat;
mod content;
mod contract;
mod direct;
mod echo;
mod events;
mod nested;
mod registry;
mod rig;
#[cfg(any(test, feature = "testing"))]
mod scripted;
mod seams;
pub mod state;

pub use contract::{
    ensure_capability, AgentRuntime, EventStream, HealthStatus, RuntimeCapabilities,
    RuntimeContext, RuntimeExecutionRef, RuntimeResumeRequest, RuntimeStartRequest, RuntimeState,
};
pub use direct::{DirectModelRuntime, DIRECT_ADAPTER_VERSION, DIRECT_LIBRARY_VERSION};
pub use echo::{EchoRuntime, ECHO_RUNTIME_NAME};
pub use events::{
    collect_events, runtime_event_to_stream_chunk, terminal_result, RuntimeEvent, RuntimeEventType,
};
pub use nested::run_nested;
pub use registry::RuntimeRegistry;
pub use rig::{
    RigAgentDef, RigRuntime, RIG_ADAPTER_VERSION, RIG_LIBRARY_VERSION, RIG_RUNTIME_NAME,
};
#[cfg(any(test, feature = "testing"))]
pub use scripted::{ScriptedModel, ScriptedTools, ScriptedTurn};
pub use seams::{
    ClientAnswer, ContextFitter, Fitted, ModelCall, ModelCaller, ModelReply, ModelStream,
    ModelStreamItem, NoModel, NoTools, ToolExecutor, ToolInfo, ToolInvocation,
};
pub use state::{
    InMemoryRuntimeStateStore, PgRuntimeStateStore, RuntimeStateRecord, RuntimeStateStore,
};

/// rig's run envelope format this build reads and writes.
pub const RUN_FORMAT: u32 = rig_agent::run::RUN_FORMAT;
