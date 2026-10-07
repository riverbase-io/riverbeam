use async_trait::async_trait;
use beam_types::{
    AgentContext, AgentInput, AgentMessage, AgentMessageRole, AgentMessageType, AgentMetadata,
    AgentResult, ContentPart, Modality, StreamChunk, StreamChunkType, TokenUsage,
};

use riverbase_core::RiverbaseResult;

use crate::registry::Agent;

/// Local test agent — echoes user text (Phase 1 stand-in for rig-core runner).
pub struct EchoAgent {
    metadata: AgentMetadata,
}

impl EchoAgent {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            metadata: AgentMetadata {
                name: name.into(),
                description: "Echo agent for tests and local dev".into(),
                supported_modalities: vec![Modality::Text],
                output_modalities: vec![Modality::Text],
                tags: vec![],
                version: "1.0.0".into(),
            },
        }
    }
}

#[async_trait]
impl Agent for EchoAgent {
    fn metadata(&self) -> AgentMetadata {
        self.metadata.clone()
    }

    async fn stream(
        &self,
        input: &AgentInput,
        _context: &AgentContext,
    ) -> RiverbaseResult<Vec<StreamChunk>> {
        let user_text: String = input
            .content
            .iter()
            .filter_map(|p| p.text.as_deref())
            .collect::<Vec<_>>()
            .join(" ");
        let reply = if user_text.is_empty() {
            "(empty)".to_string()
        } else {
            format!("Echo: {user_text}")
        };

        let chunk = StreamChunk {
            chunk_type: StreamChunkType::Chunk,
            data: Some(AgentMessage {
                role: AgentMessageRole::Ai,
                msg_type: AgentMessageType::Message,
                content: vec![ContentPart::text(&reply)],
                ..Default::default()
            }),
            result: None,
            error: None,
            id: None,
            agent_id: None,
            agent_name: Some(self.metadata.name.clone()),
            session_id: Some(input.session_id.to_string()),
            interaction_id: input.interaction_id.map(|u| u.to_string()),
            run_id: Some(input.run_id.to_string()),
            step_id: None,
            message_id: None,
            timestamp: None,
        };

        let done = StreamChunk {
            chunk_type: StreamChunkType::Done,
            data: None,
            result: Some(AgentResult {
                content: vec![ContentPart::text(reply)],
                agent_name: self.metadata.name.clone(),
                model_used: context_model(_context),
                session_id: Some(input.session_id),
                request_id: input.request_id,
                interaction_id: input.interaction_id,
                token_usage: Some(TokenUsage {
                    prompt_tokens: user_text.len() as u32 / 4,
                    completion_tokens: 4,
                    total_tokens: user_text.len() as u32 / 4 + 4,
                }),
                tool_calls: vec![],
                metadata: serde_json::json!({}),
                latency_ms: Some(0.0),
                interrupted: false,
                action_required: None,
            }),
            error: None,
            id: None,
            agent_id: None,
            agent_name: Some(self.metadata.name.clone()),
            session_id: Some(input.session_id.to_string()),
            interaction_id: input.interaction_id.map(|u| u.to_string()),
            run_id: Some(input.run_id.to_string()),
            step_id: None,
            message_id: None,
            timestamp: None,
        };

        Ok(vec![chunk, done])
    }
}

fn context_model(context: &AgentContext) -> Option<String> {
    context
        .model_config_resolved
        .as_ref()
        .map(|m| m.name.clone())
}
