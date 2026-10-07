use std::collections::HashSet;
use std::sync::Arc;

use beam_types::{AgentInput, Modality};
use serde::{Deserialize, Serialize};

use crate::plans::UsagePlan;
use crate::tracker::UsageTracker;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OveragePolicy {
    HardBlock,
    SoftWarn,
    Throttle,
}

impl Default for OveragePolicy {
    fn default() -> Self {
        Self::HardBlock
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GuardResult {
    pub allowed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub overage_policy: OveragePolicy,
}

impl GuardResult {
    fn allow(overage_policy: OveragePolicy) -> Self {
        Self {
            allowed: true,
            reason: None,
            overage_policy,
        }
    }

    fn deny(reason: String, overage_policy: OveragePolicy) -> Self {
        Self {
            allowed: false,
            reason: Some(reason),
            overage_policy,
        }
    }
}

/// Pre-dispatch quota enforcement (parity with Python `UsageGuard`).
pub struct UsageGuard {
    tracker: Arc<UsageTracker>,
}

impl UsageGuard {
    #[must_use]
    pub fn new(tracker: Arc<UsageTracker>) -> Self {
        Self { tracker }
    }

    pub async fn check(
        &self,
        user_id: Option<&str>,
        plan: &UsagePlan,
        request: &AgentInput,
    ) -> GuardResult {
        let Some(user_id) = user_id else {
            return GuardResult::allow(plan.overage_policy);
        };
        let usage = self.tracker.get_usage(user_id).await;
        let limits = &plan.limits;

        if let Some(max) = limits.max_requests_per_day {
            if usage.total_requests >= max {
                return GuardResult::deny(
                    format!("Daily request limit reached ({max})"),
                    plan.overage_policy,
                );
            }
        }
        if let Some(max) = limits.max_tokens_per_day {
            if usage.total_tokens >= max {
                return GuardResult::deny(
                    format!("Daily token limit reached ({max})"),
                    plan.overage_policy,
                );
            }
        }
        if let (Some(allowed), Some(agent)) = (&limits.allowed_agents, &request.agent_name) {
            if !allowed.contains(agent) {
                return GuardResult::deny(
                    format!("Agent '{agent}' not allowed on plan '{}'", plan.name),
                    plan.overage_policy,
                );
            }
        }
        if let Some(allowed) = &limits.allowed_modalities {
            let allowed_set: HashSet<Modality> = allowed.iter().copied().collect();
            let disallowed: Vec<Modality> = request
                .modalities()
                .into_iter()
                .filter(|m| !allowed_set.contains(m))
                .collect();
            if !disallowed.is_empty() {
                return GuardResult::deny(
                    format!("Modalities {disallowed:?} not allowed on plan '{}'", plan.name),
                    plan.overage_policy,
                );
            }
        }
        if let (Some(allowed), Some(model)) = (&limits.allowed_models, &request.model_override) {
            if !allowed.contains(model) {
                return GuardResult::deny(
                    format!("Model '{model}' not allowed on plan '{}'", plan.name),
                    plan.overage_policy,
                );
            }
        }
        GuardResult::allow(plan.overage_policy)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plans::UsageLimits;
    use crate::store::InMemoryUsageStore;
    use crate::tracker::{UsageEvent, UsageTracker};
    use beam_types::ContentPart;
    use uuid::Uuid;

    fn request() -> AgentInput {
        AgentInput {
            content: vec![ContentPart::text("hi")],
            session_id: Uuid::new_v4(),
            interaction_id: None,
            request_id: None,
            run_id: Uuid::new_v4(),
            agent_name: Some("demo".into()),
            model_override: None,
            metadata: serde_json::Value::Null,
            auth_context: None,
            stream: false,
            actions: None,
        }
    }

    #[tokio::test]
    async fn blocks_when_request_limit_reached() {
        let store = Arc::new(InMemoryUsageStore::new());
        let tracker = Arc::new(UsageTracker::new(store));
        tracker.record("u1", &UsageEvent { tokens: 0, requests: 5, ..Default::default() }).await;
        let guard = UsageGuard::new(tracker);
        let plan = UsagePlan {
            name: "basic".into(),
            limits: UsageLimits {
                max_requests_per_day: Some(5),
                ..Default::default()
            },
            overage_policy: OveragePolicy::HardBlock,
        };
        let result = guard.check(Some("u1"), &plan, &request()).await;
        assert!(!result.allowed);
    }

    #[tokio::test]
    async fn allows_without_user() {
        let store = Arc::new(InMemoryUsageStore::new());
        let tracker = Arc::new(UsageTracker::new(store));
        let guard = UsageGuard::new(tracker);
        let result = guard.check(None, &UsagePlan::unlimited("free"), &request()).await;
        assert!(result.allowed);
    }
}
