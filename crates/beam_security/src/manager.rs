use std::sync::Arc;

use async_trait::async_trait;

use crate::decision::SecurityDecision;
use crate::detect::Detector;
use crate::enums::SecurityStage;
use crate::policy::SecurityPolicy;
use crate::request::SecurityRequest;

/// Stages that pass through without rule evaluation. Python's
/// `PASSTHROUGH_STAGES` is empty: every stage has a route (`stages.ROUTES`).
pub const PASSTHROUGH_STAGES: &[SecurityStage] = &[];

/// Executor-facing security façade (ports `SecurityManager` /
/// `DisabledSecurityManager`).
///
/// `check` is async: a manager may store each decision. Audit fan-out stays
/// with the caller.
#[async_trait]
pub trait SecurityManagement: Send + Sync {
    async fn check(&self, request: &SecurityRequest) -> SecurityDecision;

    /// When `false`, deny verdicts are advisory (callers log but proceed).
    fn enforce(&self) -> bool {
        true
    }

    /// The policy's step limit (Python `policy.max_steps`, default 50).
    fn max_steps(&self) -> u64 {
        SecurityPolicy::default().max_steps
    }

    /// The detectors that scan model output: the stream guard runs them as
    /// text streams, and a `Mask` verdict masks what they flag. None by
    /// default.
    fn output_detectors(&self) -> Vec<Arc<dyn Detector>> {
        Vec::new()
    }
}

/// No-op manager — always allows (default when security is disabled).
#[derive(Debug, Default, Clone, Copy)]
pub struct DisabledSecurityManager;

#[async_trait]
impl SecurityManagement for DisabledSecurityManager {
    async fn check(&self, _request: &SecurityRequest) -> SecurityDecision {
        SecurityDecision::allow()
    }

    fn enforce(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enums::SecurityAction;
    use serde_json::json;

    #[tokio::test]
    async fn disabled_manager_allows_everything() {
        let payload = json!({"content": "AKIAIOSFODNN7EXAMPLE"})
            .as_object()
            .cloned()
            .unwrap_or_default();
        let d = DisabledSecurityManager
            .check(&SecurityRequest::new(SecurityStage::AfterLlm).with_payload(payload))
            .await;
        assert_eq!(d.action, SecurityAction::Allow);
        assert_eq!(DisabledSecurityManager.max_steps(), 50);
        assert!(DisabledSecurityManager.output_detectors().is_empty());
    }
}
