//! Wire types for invoke requests, agent results, and stream chunks.

mod action;
mod agent;
mod content;
mod context;
mod invoke;
mod message;
mod model;
mod output;
pub mod pyjson;
mod result;
pub mod spec;
mod step;
mod stream;
mod time;
pub mod tool_policy;

pub use action::{ActionRequestEnvelope, ActionResponseEnvelope, ActionResponseStatus, ActionType};
pub use agent::{AgentContext, AgentInput, AgentMetadata};
pub use content::{ContentPart, ContentPartType, Modality};
pub use context::{
    ContextUsage, KnowledgeMode, MemoryMode, RequestContext, RequestFact, ResourceRef, Sensitivity,
};
pub use invoke::{ChatRequest, ClientCapabilities, InvokeAuthContext, InvokeRequest};
pub use message::{AgentMessage, AgentMessagePhase, AgentMessageRole, AgentMessageType};
pub use model::{CredentialRef, ModelConfig, ModelProvider};
pub use output::{AgentOutputContract, Artifact};
pub use result::{AgentResult, TokenUsage, ToolCallRecord};
pub use spec::{AgentSpec, CompatibilityPolicy, RevisionStatus};
pub use step::{AgentAction, AgentStep};
pub use stream::{StreamChunk, StreamChunkType};

#[cfg(test)]
mod tests;
