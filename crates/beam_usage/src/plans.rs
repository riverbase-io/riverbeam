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
    #[must_use]
    pub fn unlimited(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            limits: UsageLimits::default(),
            overage_policy: OveragePolicy::default(),
        }
    }
}
