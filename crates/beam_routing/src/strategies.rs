use std::collections::HashSet;

use beam_types::{AgentInput, AgentMetadata, Modality, ModelConfig};
use serde_json::Value;

pub trait RoutingStrategy: Send + Sync {
    fn filter(
        &self,
        candidates: &[ModelConfig],
        input: &AgentInput,
        agent_metadata: Option<&AgentMetadata>,
    ) -> Vec<ModelConfig>;
}

pub struct ModalityStrategy;

impl RoutingStrategy for ModalityStrategy {
    fn filter(
        &self,
        candidates: &[ModelConfig],
        input: &AgentInput,
        _agent_metadata: Option<&AgentMetadata>,
    ) -> Vec<ModelConfig> {
        let required: HashSet<Modality> = input.modalities();
        candidates
            .iter()
            .filter(|m| {
                let supported: HashSet<Modality> = m.modalities.iter().copied().collect();
                required.is_subset(&supported)
            })
            .cloned()
            .collect()
    }
}

pub struct CapabilityStrategy;

impl RoutingStrategy for CapabilityStrategy {
    fn filter(
        &self,
        candidates: &[ModelConfig],
        input: &AgentInput,
        agent_metadata: Option<&AgentMetadata>,
    ) -> Vec<ModelConfig> {
        let mut required_caps = capability_set(&input.metadata);
        if required_caps.is_empty() {
            if let Some(meta) = agent_metadata {
                let known: HashSet<String> = candidates
                    .iter()
                    .flat_map(|m| m.capabilities.clone())
                    .collect();
                required_caps = meta
                    .tags
                    .iter()
                    .filter(|t| known.contains(*t))
                    .cloned()
                    .collect();
            }
        }
        if required_caps.is_empty() {
            return candidates.to_vec();
        }
        candidates
            .iter()
            .filter(|m| {
                let supported: HashSet<String> = m.capabilities.iter().cloned().collect();
                required_caps.is_subset(&supported)
            })
            .cloned()
            .collect()
    }
}

fn capability_set(metadata: &Value) -> HashSet<String> {
    metadata
        .get("capabilities")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|x| x.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

pub struct CostAwareStrategy;

impl RoutingStrategy for CostAwareStrategy {
    fn filter(
        &self,
        candidates: &[ModelConfig],
        _input: &AgentInput,
        _agent_metadata: Option<&AgentMetadata>,
    ) -> Vec<ModelConfig> {
        let mut sorted = candidates.to_vec();
        sorted.sort_by(|a, b| {
            a.cost_per_1k_tokens
                .partial_cmp(&b.cost_per_1k_tokens)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        sorted
    }
}

pub struct FallbackStrategy {
    all_models: Vec<ModelConfig>,
}

impl FallbackStrategy {
    pub fn new(all_models: Vec<ModelConfig>) -> Self {
        Self {
            all_models,
        }
    }
}

impl RoutingStrategy for FallbackStrategy {
    fn filter(
        &self,
        candidates: &[ModelConfig],
        _input: &AgentInput,
        _agent_metadata: Option<&AgentMetadata>,
    ) -> Vec<ModelConfig> {
        if candidates.is_empty() {
            self.all_models.clone()
        } else {
            candidates.to_vec()
        }
    }
}
