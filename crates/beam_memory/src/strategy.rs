use async_trait::async_trait;
use beam_types::AgentMessage;

pub type StrategyResult<T> = riverbase_core::RiverbaseResult<T>;

#[async_trait]
pub trait ContextStrategy: Send + Sync {
    async fn apply(&self, messages: Vec<AgentMessage>, max_tokens: u32) -> StrategyResult<Vec<AgentMessage>>;
}

/// Rough token estimate: ~4 characters per token (mirrors simple Python strategies).
fn estimate_tokens(messages: &[AgentMessage]) -> u32 {
    messages
        .iter()
        .map(|m| m.text_content().chars().count() as u32)
        .map(|c| c.div_ceil(4))
        .sum()
}

trait TextContent {
    fn text_content(&self) -> String;
}

impl TextContent for AgentMessage {
    fn text_content(&self) -> String {
        self.content
            .iter()
            .filter_map(|p| p.text.as_deref())
            .collect::<Vec<_>>()
            .join(" ")
    }
}

pub struct TokenLimitStrategy;

#[async_trait]
impl ContextStrategy for TokenLimitStrategy {
    async fn apply(&self, messages: Vec<AgentMessage>, max_tokens: u32) -> StrategyResult<Vec<AgentMessage>> {
        if max_tokens == 0 || estimate_tokens(&messages) <= max_tokens {
            return Ok(messages);
        }
        let mut kept = Vec::new();
        let mut used = 0u32;
        for msg in messages.into_iter().rev() {
            let cost = msg.text_content().chars().count() as u32 / 4 + 1;
            if used + cost > max_tokens {
                break;
            }
            used += cost;
            kept.push(msg);
        }
        kept.reverse();
        Ok(kept)
    }
}
