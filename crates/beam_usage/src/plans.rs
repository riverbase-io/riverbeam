use beam_types::Modality;
use serde::{Deserialize, Serialize};

use crate::guard::OveragePolicy;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct UsageLimits {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_requests_per_day: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens_per_day: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens_per_request: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed_agents: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed_modalities: Option<Vec<Modality>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed_models: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct UsagePlan {
    pub name: String,
    #[serde(default)]
    pub limits: UsageLimits,
    #[serde(default)]
    pub overage_policy: OveragePolicy,
}

impl UsagePlan {
    /// The `agent_usage_plan` row for this plan.
    #[must_use]
    pub fn to_row(&self, status: &str) -> beam_store::PlanRow {
        let int = |v: Option<u64>| v.map(|n| i32::try_from(n).unwrap_or(i32::MAX));
        let policy = serde_json::to_value(self.overage_policy)
            .ok()
            .and_then(|v| v.as_str().map(str::to_owned))
            .unwrap_or_else(|| "hard_block".into());
        beam_store::PlanRow {
            id: beam_store::agent_id_for(&self.name),
            name: self.name.clone(),
            overage_policy: policy,
            status: status.to_owned(),
            max_requests_per_day: int(self.limits.max_requests_per_day),
            max_tokens_per_day: int(self.limits.max_tokens_per_day),
            max_tokens_per_request: int(self.limits.max_tokens_per_request),
            allowed_agents: self.limits.allowed_agents.clone(),
            allowed_modalities: self.limits.allowed_modalities.as_ref().map(|m| {
                m.iter()
                    .filter_map(|v| serde_json::to_value(v).ok()?.as_str().map(str::to_owned))
                    .collect()
            }),
            allowed_models: self.limits.allowed_models.clone(),
        }
    }

    #[must_use]
    pub fn unlimited(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            limits: UsageLimits::default(),
            overage_policy: OveragePolicy::default(),
        }
    }
}
