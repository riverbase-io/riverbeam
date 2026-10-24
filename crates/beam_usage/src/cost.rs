//! Cost ledger (Python `usage/cost.py`).

use std::sync::Mutex;

use async_trait::async_trait;
use beam_types::TokenUsage;
use riverbase_core::RiverbaseResult;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CostEvent {
    #[serde(default)]
    pub execution_id: Option<Uuid>,
    #[serde(default)]
    pub agent_name: Option<String>,
    pub model: String,
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub org_id: Option<String>,
    #[serde(default)]
    pub prompt_tokens: u32,
    #[serde(default)]
    pub completion_tokens: u32,
    #[serde(default)]
    pub usd: f64,
}

/// `total_tokens / 1000 * cost_per_1k`, as a float.
#[must_use]
pub fn cost_usd(usage: &TokenUsage, cost_per_1k: f64) -> f64 {
    (f64::from(usage.total_tokens) / 1000.0) * cost_per_1k
}

/// Dimensions of a cost event besides the model and usage.
#[derive(Debug, Clone, Default)]
pub struct CostDims {
    pub execution_id: Option<Uuid>,
    pub agent_name: Option<String>,
    pub provider: Option<String>,
    pub org_id: Option<String>,
}

/// The cost event of one invoke.
#[must_use]
pub fn cost_event(model: &str, usage: &TokenUsage, cost_per_1k: f64, dims: CostDims) -> CostEvent {
    CostEvent {
        execution_id: dims.execution_id,
        agent_name: dims.agent_name,
        model: model.to_owned(),
        provider: dims.provider,
        org_id: dims.org_id,
        prompt_tokens: usage.prompt_tokens,
        completion_tokens: usage.completion_tokens,
        usd: cost_usd(usage, cost_per_1k),
    }
}

#[async_trait]
pub trait CostStore: Send + Sync {
    /// # Errors
    /// `BEM-361` for store failures.
    async fn save(&self, event: &CostEvent) -> RiverbaseResult<()>;
}

/// The most recent cost events (Python keeps every one; Rust keeps the last
/// [`InMemoryCostStore::CAPACITY`] so a long-running process does not grow).
#[derive(Default)]
pub struct InMemoryCostStore {
    events: Mutex<std::collections::VecDeque<CostEvent>>,
}

impl InMemoryCostStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub const CAPACITY: usize = 1000;

    #[must_use]
    pub fn events(&self) -> Vec<CostEvent> {
        self.events
            .lock()
            .map(|e| e.iter().cloned().collect())
            .unwrap_or_default()
    }
}

#[async_trait]
impl CostStore for InMemoryCostStore {
    async fn save(&self, event: &CostEvent) -> RiverbaseResult<()> {
        let mut events = self
            .events
            .lock()
            .map_err(|_| crate::BEM_361.with_data("in-memory cost store lock poisoned"))?;
        if events.len() == Self::CAPACITY {
            events.pop_front();
        }
        events.push_back(event.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Expected values are Python's `cost_usd` for the same inputs.
    #[test]
    fn usd_matches_python() {
        let usage = TokenUsage {
            prompt_tokens: 1200,
            completion_tokens: 345,
            total_tokens: 1545,
            ..TokenUsage::default()
        };
        // Python: (1545 / 1000.0) * 0.002 == 0.00309 exactly (same IEEE operations).
        assert_eq!(cost_usd(&usage, 0.002).to_bits(), 0.00309_f64.to_bits());
        assert_eq!(cost_usd(&usage, 0.0).to_bits(), 0.0_f64.to_bits());
        let event = cost_event("gpt", &usage, 0.5, CostDims::default());
        assert_eq!((event.prompt_tokens, event.completion_tokens), (1200, 345));
        assert_eq!(event.usd.to_bits(), 0.7725_f64.to_bits());
    }
}
