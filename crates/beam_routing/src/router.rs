use crate::aliases::ModelAliasMap;
use crate::strategies::{
    CapabilityStrategy, CostAwareStrategy, FallbackStrategy, ModalityStrategy, RoutingStrategy,
};
use beam_types::{AgentInput, AgentMetadata, ModelConfig};
use riverbase_core::RiverbaseResult;

pub struct ModelRouter {
    models: Vec<ModelConfig>,
    strategies: Vec<Box<dyn RoutingStrategy>>,
    default_model: Option<String>,
    aliases: ModelAliasMap,
}

impl ModelRouter {
    /// # Errors
    /// `BEM-042` for duplicate model names.
    pub fn new(models: Vec<ModelConfig>, default_model: Option<String>) -> RiverbaseResult<Self> {
        validate_models(&models)?;
        let fallback_source = models.clone();
        Ok(Self {
            models,
            default_model,
            aliases: ModelAliasMap::default(),
            strategies: vec![
                Box::new(ModalityStrategy),
                Box::new(CapabilityStrategy),
                Box::new(CostAwareStrategy),
                Box::new(FallbackStrategy::new(fallback_source)),
            ],
        })
    }

    /// # Errors
    /// `BEM-042` for duplicate model names.
    pub fn with_strategies(
        models: Vec<ModelConfig>,
        strategies: Vec<Box<dyn RoutingStrategy>>,
        default_model: Option<String>,
    ) -> RiverbaseResult<Self> {
        validate_models(&models)?;
        Ok(Self {
            models,
            strategies,
            default_model,
            aliases: ModelAliasMap::default(),
        })
    }

    /// Resolve `model_override` and the default model through `aliases`.
    #[must_use]
    pub fn with_aliases(mut self, aliases: ModelAliasMap) -> Self {
        self.aliases = aliases;
        self
    }

    /// The model for this request (the head of [`Self::resolve_chain`]).
    ///
    /// # Errors
    /// `BEM-041` (public `BEM-422`) when no model fits.
    pub fn resolve(
        &self,
        input: &AgentInput,
        agent_metadata: Option<&AgentMetadata>,
    ) -> RiverbaseResult<ModelConfig> {
        let mut chain = self.resolve_chain(input, agent_metadata)?;
        Ok(chain.swap_remove(0))
    }

    /// The selected model, then the other catalog models that passed routing,
    /// in order: the gateway's failover chain.
    /// `model_override` (a name or an alias) bypasses the strategies.
    ///
    /// # Errors
    /// `BEM-041` (public `BEM-422`) for an unknown override or when no model fits.
    pub fn resolve_chain(
        &self,
        input: &AgentInput,
        agent_metadata: Option<&AgentMetadata>,
    ) -> RiverbaseResult<Vec<ModelConfig>> {
        if let Some(override_name) = &input.model_override {
            let Some(selected) = self.resolve_name(override_name)? else {
                return Err(crate::BEM_041
                    .with_data(format!("No model available for override={override_name}")));
            };
            let rest: Vec<ModelConfig> = self
                .filtered(input, agent_metadata)
                .into_iter()
                .filter(|m| m.name != selected.name)
                .collect();
            let mut chain = vec![selected];
            chain.extend(rest);
            return Ok(chain);
        }
        let candidates = self.filtered(input, agent_metadata);
        if candidates.is_empty() {
            if let Some(name) = &self.default_model {
                if let Some(model) = self.resolve_name(name)? {
                    return Ok(vec![model]);
                }
            }
            return Err(crate::BEM_041.with_data(format!("modalities={:?}", input.modalities())));
        }
        Ok(candidates)
    }

    /// A catalog model by name, else by alias.
    ///
    /// # Errors
    /// `BEM-041` for an unknown dotted alias.
    pub fn resolve_name(&self, name: &str) -> RiverbaseResult<Option<ModelConfig>> {
        if let Some(model) = self.find_by_name(name) {
            return Ok(Some(model));
        }
        let target = self.aliases.resolve(name)?;
        if target == name {
            return Ok(None);
        }
        Ok(self.find_by_name(target))
    }

    fn filtered(
        &self,
        input: &AgentInput,
        agent_metadata: Option<&AgentMetadata>,
    ) -> Vec<ModelConfig> {
        let mut candidates = self.models.clone();
        for strategy in &self.strategies {
            candidates = strategy.filter(&candidates, input, agent_metadata);
            if candidates.is_empty() {
                break;
            }
        }
        candidates
    }

    #[must_use]
    pub fn list_models(&self) -> &[ModelConfig] {
        &self.models
    }

    fn find_by_name(&self, name: &str) -> Option<ModelConfig> {
        self.models.iter().find(|m| m.name == name).cloned()
    }
}

fn validate_models(models: &[ModelConfig]) -> RiverbaseResult<()> {
    let mut seen = std::collections::HashSet::new();
    for model in models {
        if !seen.insert(&model.name) {
            return Err(crate::BEM_042.with_data(format!("duplicate model name={}", model.name)));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use beam_types::{ContentPart, ModelProvider};

    fn model(name: &str, cost: f64) -> ModelConfig {
        serde_json::from_value(serde_json::json!({
            "name": name, "provider": "openai", "cost_per_1k_tokens": cost,
        }))
        .unwrap()
    }

    fn input(model_override: Option<&str>) -> AgentInput {
        AgentInput {
            content: vec![ContentPart::text("hi")],
            session_id: uuid::Uuid::new_v4(),
            interaction_id: None,
            execution_id: None,
            request_id: None,
            run_id: uuid::Uuid::new_v4(),
            runtime_ref: None,
            agent_name: Some("a".into()),
            model_override: model_override.map(str::to_owned),
            metadata: serde_json::Value::Null,
            auth_context: None,
            stream: false,
            actions: None,
            context: None,
            client: None,
        }
    }

    fn router() -> ModelRouter {
        ModelRouter::new(vec![model("cheap", 0.1), model("big", 1.0)], None)
            .unwrap()
            .with_aliases(ModelAliasMap::new(
                [("chat.default".to_owned(), "big".to_owned())].into(),
            ))
    }

    #[test]
    fn chain_is_the_routed_order() {
        let chain = router().resolve_chain(&input(None), None).unwrap();
        let names: Vec<_> = chain.iter().map(|m| m.name.as_str()).collect();
        assert_eq!(names, ["cheap", "big"]);
        assert_eq!(chain[0].provider, ModelProvider::Openai);
    }

    #[test]
    fn an_alias_override_bypasses_the_strategies() {
        let chain = router()
            .resolve_chain(&input(Some("chat.default")), None)
            .unwrap();
        let names: Vec<_> = chain.iter().map(|m| m.name.as_str()).collect();
        assert_eq!(names, ["big", "cheap"]);
    }

    #[test]
    fn an_unknown_override_is_refused() {
        let err = router()
            .resolve_chain(&input(Some("nope")), None)
            .unwrap_err();
        assert_eq!(err.errcode.as_str(), "BEM-041");
        let err = router()
            .resolve_chain(&input(Some("chat.nope")), None)
            .unwrap_err();
        assert_eq!(err.errcode.as_str(), "BEM-041");
    }
}
