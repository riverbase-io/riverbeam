use beam_types::{AgentInput, AgentMetadata, ModelConfig};
use riverbase_core::RiverbaseResult;
use crate::strategies::{
    CapabilityStrategy, CostAwareStrategy, FallbackStrategy, ModalityStrategy, RoutingStrategy,
};

pub struct ModelRouter {
    models: Vec<ModelConfig>,
    strategies: Vec<Box<dyn RoutingStrategy>>,
    default_model: Option<String>,
}

impl ModelRouter {
    pub fn new(models: Vec<ModelConfig>, default_model: Option<String>) -> RiverbaseResult<Self> {
        validate_models(&models)?;
        let fallback_source = models.clone();
        Ok(Self {
            models,
            default_model,
            strategies: vec![
                Box::new(ModalityStrategy),
                Box::new(CapabilityStrategy),
                Box::new(CostAwareStrategy),
                Box::new(FallbackStrategy::new(fallback_source)),
            ],
        })
    }

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
        })
    }

    pub fn resolve(
        &self,
        input: &AgentInput,
        agent_metadata: Option<&AgentMetadata>,
    ) -> RiverbaseResult<ModelConfig> {
        if let Some(override_name) = &input.model_override {
            if let Some(model) = self.find_by_name(override_name) {
                return Ok(model);
            }
        }

        let mut candidates = self.models.clone();
        for strategy in &self.strategies {
            candidates = strategy.filter(&candidates, input, agent_metadata);
            if candidates.is_empty() {
                break;
            }
        }

        if candidates.is_empty() {
            if let Some(name) = &self.default_model {
                if let Some(model) = self.find_by_name(name) {
                    return Ok(model);
                }
            }
            return Err(crate::BEM_041.with_data(format!(
                "modalities={:?}",
                input.modalities()
            )));
        }

        Ok(candidates[0].clone())
    }

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
            return Err(crate::BEM_042.with_data(format!(
                "duplicate model name={}",
                model.name
            )));
        }
    }
    Ok(())
}
