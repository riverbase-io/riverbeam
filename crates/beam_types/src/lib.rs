//! Wire types for invoke requests, agent results, and stream chunks.

mod action;
mod agent;
mod content;
mod invoke;
mod message;
mod model;
mod result;
mod stream;

pub use action::{ActionResponseEnvelope, ActionResponseStatus};
pub use agent::{AgentContext, AgentInput, AgentMetadata};
pub use content::{ContentPart, ContentPartType, Modality};
pub use invoke::{ChatRequest, InvokeAuthContext, InvokeRequest};
pub use message::{AgentMessage, AgentMessagePhase, AgentMessageRole, AgentMessageType};
pub use model::{ModelConfig, ModelProvider};
pub use result::{AgentResult, TokenUsage, ToolCallRecord};
pub use stream::{StreamChunk, StreamChunkType};

#[cfg(test)]
mod tests;
