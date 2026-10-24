//! The showcases' subscription plans (the same as Python's `examples/beam_app/plans.py`).

use beam_types::Modality;
use beam_usage::{OveragePolicy, UsageLimits, UsagePlan};

/// The plan a user without a profile gets.
pub const DEFAULT_PLAN: &str = "free";

/// The free plan.
#[must_use]
pub fn free_plan() -> UsagePlan {
    UsagePlan {
        name: "free".into(),
        limits: UsageLimits {
            max_requests_per_day: Some(50),
            max_tokens_per_day: Some(100_000),
            max_tokens_per_request: Some(32_768),
            allowed_modalities: Some(vec![Modality::Text, Modality::Image]),
            ..UsageLimits::default()
        },
        overage_policy: OveragePolicy::HardBlock,
    }
}

/// The pro plan.
#[must_use]
pub fn pro_plan() -> UsagePlan {
    UsagePlan {
        name: "pro".into(),
        limits: UsageLimits {
            max_requests_per_day: Some(1000),
            max_tokens_per_day: Some(2_000_000),
            max_tokens_per_request: Some(8192),
            allowed_modalities: Some(vec![Modality::Text, Modality::Image]),
            ..UsageLimits::default()
        },
        overage_policy: OveragePolicy::SoftWarn,
    }
}

/// No limits.
#[must_use]
pub fn enterprise_plan() -> UsagePlan {
    UsagePlan {
        name: "enterprise".into(),
        limits: UsageLimits::default(),
        overage_policy: OveragePolicy::SoftWarn,
    }
}

/// Every plan the showcases check in.
#[must_use]
pub fn all_plans() -> Vec<UsagePlan> {
    vec![free_plan(), pro_plan(), enterprise_plan()]
}
