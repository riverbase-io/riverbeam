use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use beam_types::{
    AgentContext, AgentInput, AgentMetadata, AgentResult, Modality, StreamChunk, StreamChunkType,
};

use riverbase_core::RiverbaseResult;

#[async_trait]
pub trait Agent: Send + Sync {
    fn metadata(&self) -> AgentMetadata;
    async fn stream(
        &self,
        input: &AgentInput,
        context: &AgentContext,
    ) -> RiverbaseResult<Vec<StreamChunk>>;
    async fn health_check(&self) -> bool {
        true
    }
}

pub struct AgentRegistry {
    agents: RwLock<HashMap<String, Arc<dyn Agent>>>,
}

impl Default for AgentRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl AgentRegistry {
    pub fn new() -> Self {
        Self {
            agents: RwLock::new(HashMap::new()),
        }
    }

    pub fn register(&self, agent: Arc<dyn Agent>) {
        let name = agent.metadata().name.clone();
        let mut guard = self.agents.write().expect("agent registry lock");
        guard.insert(name, agent);
    }

    pub fn get_metadata(&self, name: &str) -> RiverbaseResult<AgentMetadata> {
        let guard = self.agents.read().expect("agent registry lock");
        guard
            .get(name)
            .map(|a| a.metadata())
            .ok_or_else(|| crate::BEM_020.with_data(name.to_string()))
    }

    /// Run an agent and return its raw stream frames (modality-validated).
    pub async fn dispatch_frames(
        &self,
        agent_name: &str,
        input: &AgentInput,
        context: &AgentContext,
    ) -> RiverbaseResult<Vec<StreamChunk>> {
        let agent = {
            let guard = self.agents.read().expect("agent registry lock");
            let meta = guard
                .get(agent_name)
                .map(|a| a.metadata())
                .ok_or_else(|| crate::BEM_021.with_data(agent_name.to_string()))?;
            validate_modalities(&meta, input)?;
            guard.get(agent_name).cloned()
        };
        let agent = agent.expect("agent exists after lookup");
        agent.stream(input, context).await
    }

    /// Run an agent and reduce its stream to the terminal [`AgentResult`].
    pub async fn dispatch_stream(
        &self,
        agent_name: &str,
        input: &AgentInput,
        context: &AgentContext,
    ) -> RiverbaseResult<AgentResult> {
        let chunks = self.dispatch_frames(agent_name, input, context).await?;
        terminal_result(chunks)
    }

    pub fn list_agents(&self) -> Vec<AgentMetadata> {
        let guard = self.agents.read().expect("agent registry lock");
        guard.values().map(|a| a.metadata()).collect()
    }
}

fn validate_modalities(metadata: &AgentMetadata, input: &AgentInput) -> RiverbaseResult<()> {
    let required: HashSet<Modality> = input.modalities();
    let supported: HashSet<Modality> = metadata.supported_modalities.iter().copied().collect();
    let unsupported: Vec<_> = required.difference(&supported).copied().collect();
    if unsupported.is_empty() {
        Ok(())
    } else {
        Err(crate::BEM_040.with_data(format!(
            "Agent '{}' does not support modalities: {unsupported:?}. Supported: {supported:?}",
            metadata.name
        )))
    }
}

/// Reduce a finished agent stream to its terminal [`AgentResult`].
pub fn terminal_result(chunks: Vec<StreamChunk>) -> RiverbaseResult<AgentResult> {
    let mut final_result = None;
    for chunk in chunks {
        match chunk.chunk_type {
            StreamChunkType::Done => {
                final_result = chunk.result;
                break;
            }
            StreamChunkType::Error => {
                return Err(crate::BEM_087.with_data(chunk.error.unwrap_or_else(|| "stream error".into())));
            }
            StreamChunkType::Chunk
            | StreamChunkType::Placeholder
            | StreamChunkType::Thinking
            | StreamChunkType::Tool => {}
        }
    }
    final_result.ok_or_else(|| {
        crate::BEM_086.raise()
    })
}

pub fn human_message_from_input(input: &AgentInput) -> beam_types::AgentMessage {
    use beam_types::{AgentMessageRole, AgentMessageType};
    beam_types::AgentMessage {
        role: AgentMessageRole::Human,
        msg_type: AgentMessageType::Message,
        content: input.content.clone(),
        ..Default::default()
    }
}
