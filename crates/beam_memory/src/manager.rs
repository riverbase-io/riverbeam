use std::sync::Arc;

use beam_types::{AgentMessage, AgentMessageRole, AgentMessageType, ContentPart};

use crate::backend::{MemoryBackend, MemoryItem};
use crate::scope::MemoryScope;
use crate::strategy::ContextStrategy;

pub struct MemoryManager {
    backend: Arc<dyn MemoryBackend>,
    strategy: Arc<dyn ContextStrategy>,
}

impl MemoryManager {
    pub fn new(backend: Arc<dyn MemoryBackend>, strategy: Arc<dyn ContextStrategy>) -> Self {
        Self { backend, strategy }
    }

    pub fn backend(&self) -> &Arc<dyn MemoryBackend> {
        &self.backend
    }

    /// Load layers in order (system → … → session) and trim to `max_tokens`.
    pub async fn load_context(
        &self,
        scopes: &[MemoryScope],
        max_tokens: u32,
    ) -> riverbase_core::RiverbaseResult<Vec<AgentMessage>> {
        let mut raw = Vec::new();
        for scope in scopes {
            for item in self.backend.load_layer(scope).await? {
                if item.messages.is_empty() && !item.text.is_empty() {
                    raw.push(AgentMessage {
                        role: AgentMessageRole::System,
                        msg_type: AgentMessageType::Message,
                        content: vec![ContentPart::text(format!(
                            "[{} {}] {}",
                            item.kind,
                            scope.key(),
                            item.text
                        ))],
                        ..Default::default()
                    });
                } else {
                    raw.extend(item.messages);
                }
            }
        }
        self.strategy.apply(raw, max_tokens).await
    }

    pub async fn save_turn(
        &self,
        session: &MemoryScope,
        messages: &[AgentMessage],
        _user_id: Option<&str>,
    ) -> riverbase_core::RiverbaseResult<()> {
        self.backend
            .append_layer(session, &[MemoryItem::turn(session.clone(), messages.to_vec())])
            .await
    }

    pub async fn remember(
        &self,
        scope: &MemoryScope,
        kind: &str,
        text: &str,
    ) -> riverbase_core::RiverbaseResult<String> {
        let item = MemoryItem::fact(scope.clone(), kind, text);
        let id = item.id.clone();
        self.backend.append_layer(scope, &[item]).await?;
        Ok(id)
    }

    pub async fn forget(&self, id: &str) -> riverbase_core::RiverbaseResult<bool> {
        self.backend.forget(id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{InMemoryBackend, TokenLimitStrategy};

    #[tokio::test]
    async fn layers_concatenate_in_order() {
        let mgr = MemoryManager::new(
            Arc::new(InMemoryBackend::new()),
            Arc::new(TokenLimitStrategy),
        );
        let sys = MemoryScope::system();
        let doc = MemoryScope::new("docset", "d1");
        let sess = MemoryScope::session("s1");
        mgr.remember(&sys, "fact", "system rule").await.unwrap();
        mgr.remember(&doc, "style", "short sentences").await.unwrap();
        mgr.save_turn(
            &sess,
            &[AgentMessage {
                role: AgentMessageRole::Human,
                content: vec![ContentPart::text("hi")],
                ..Default::default()
            }],
            None,
        )
        .await
        .unwrap();
        let ctx = mgr
            .load_context(&[sys, doc, sess], 4096)
            .await
            .unwrap();
        assert_eq!(ctx.len(), 3);
        assert!(ctx[0].content[0].text.as_deref().unwrap().contains("system rule"));
        assert!(ctx[1].content[0].text.as_deref().unwrap().contains("short sentences"));
        assert_eq!(ctx[2].role, AgentMessageRole::Human);
    }
}
