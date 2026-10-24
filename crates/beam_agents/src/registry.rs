use std::collections::{HashMap, HashSet};
use std::sync::{PoisonError, RwLock};

use beam_types::{AgentInput, AgentMetadata, Modality};

use riverbase_core::RiverbaseResult;

/// Registered agents' metadata by name.
#[derive(Default)]
pub struct AgentRegistry {
    agents: RwLock<HashMap<String, AgentMetadata>>,
}

impl AgentRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `metadata` under its name, replacing any agent of that name.
    pub fn register(&self, metadata: AgentMetadata) {
        // A writer that panicked left whole entries: the map is still usable.
        let mut agents = self.agents.write().unwrap_or_else(PoisonError::into_inner);
        agents.insert(metadata.name.clone(), metadata);
    }

    fn find(&self, name: &str) -> Option<AgentMetadata> {
        let agents = self.agents.read().unwrap_or_else(PoisonError::into_inner);
        agents.get(name).cloned()
    }

    /// The metadata of agent `name`.
    ///
    /// # Errors
    /// `BEM-020` for an unknown agent.
    pub fn get_metadata(&self, name: &str) -> RiverbaseResult<AgentMetadata> {
        self.find(name)
            .ok_or_else(|| crate::BEM_020.with_data(name.to_string()))
    }

    /// Fail unless `agent_name` is registered and supports `input`'s modalities.
    ///
    /// # Errors
    /// `BEM-021` for an unknown agent; `BEM-040` for an unsupported modality.
    pub fn validate_input(&self, agent_name: &str, input: &AgentInput) -> RiverbaseResult<()> {
        let metadata = self
            .find(agent_name)
            .ok_or_else(|| crate::BEM_021.with_data(agent_name.to_string()))?;
        validate_modalities(&metadata, input)
    }

    /// Every registered agent's metadata.
    #[must_use]
    pub fn list_agents(&self) -> Vec<AgentMetadata> {
        let agents = self.agents.read().unwrap_or_else(PoisonError::into_inner);
        agents.values().cloned().collect()
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

#[must_use]
pub fn human_message_from_input(input: &AgentInput) -> beam_types::AgentMessage {
    use beam_types::{AgentMessageRole, AgentMessageType};
    beam_types::AgentMessage {
        role: AgentMessageRole::Human,
        msg_type: AgentMessageType::Message,
        content: input.content.clone(),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use beam_types::{AgentInput, AgentMetadata, ContentPart, ContentPartType, Modality};

    use super::AgentRegistry;

    fn text_agent(name: &str) -> AgentMetadata {
        AgentMetadata {
            name: name.into(),
            description: String::new(),
            supported_modalities: vec![Modality::Text],
            output_modalities: vec![Modality::Text],
            tags: vec![],
            version: "1.0.0".into(),
            final_nodes: Vec::new(),
            output_contract: None,
            tools: Vec::new(),
        }
    }

    fn image_input() -> AgentInput {
        let mut input = AgentInput::text("look");
        input.content.push(ContentPart {
            part_type: ContentPartType::ImageUrl,
            text: None,
            url: Some("https://example.com/a.png".into()),
            ..ContentPart::text("")
        });
        input
    }

    #[test]
    fn registered_agents_are_listed_and_replaced_by_name() {
        let registry = AgentRegistry::new();
        registry.register(text_agent("a"));
        registry.register(text_agent("b"));
        let mut replaced = text_agent("a");
        replaced.version = "2.0.0".into();
        registry.register(replaced);
        let mut names: Vec<String> = registry.list_agents().into_iter().map(|m| m.name).collect();
        names.sort();
        assert_eq!(names, ["a", "b"]);
        assert_eq!(registry.get_metadata("a").expect("a").version, "2.0.0");
        let err = registry.get_metadata("missing").unwrap_err();
        assert_eq!(err.errcode.as_str(), "BEM-020");
    }

    #[test]
    fn unknown_agents_and_unsupported_modalities_are_refused() {
        let registry = AgentRegistry::new();
        registry.register(text_agent("echo"));
        let text = AgentInput::text("hi");
        let err = registry.validate_input("missing", &text).unwrap_err();
        assert_eq!(err.errcode.as_str(), "BEM-021");
        // The agent takes text only.
        let err = registry.validate_input("echo", &image_input()).unwrap_err();
        assert_eq!(err.errcode.as_str(), "BEM-040");
        assert!(registry.validate_input("echo", &text).is_ok());
    }
}
