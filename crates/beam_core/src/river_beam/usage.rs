//! Usage plans, token reservations and cost events.

use beam_audit::{AuditAction, AuditEvent};
use beam_types::{AgentInput, AgentResult, ModelConfig};
use beam_usage::{cost_event, CostDims, OveragePolicy, UsagePlan};

use riverbase_core::RiverbaseResult;

use super::{HeldReservation, RiverBeam};

impl RiverBeam {
    /// The user's plan: their profile's, else the default (and a profile for it).
    /// A plan's limits are checked here; `hard_block` refuses with `BEM-060`.
    pub(super) async fn resolve_usage_plan(
        &self,
        input: &AgentInput,
    ) -> RiverbaseResult<Option<UsagePlan>> {
        let Some(usage) = self.usage.as_ref().filter(|u| !u.plans.is_empty()) else {
            return Ok(None);
        };
        let (guard, tracker) = (&usage.guard, &usage.tracker);
        let user_id = input.user_id();
        let profile = match &user_id {
            Some(user) => tracker.get_usage_profile(user).await?,
            None => None,
        };
        let plan = if let Some(key) = profile {
            usage.plans.get(&key).cloned()
        } else {
            let plan = usage
                .default_plan
                .as_ref()
                .and_then(|d| usage.plans.get(d))
                .cloned();
            if let (Some(plan), Some(user)) = (&plan, &user_id) {
                tracker.set_usage_profile(user, &plan.name).await?;
            }
            plan
        };
        let Some(plan) = plan else {
            if user_id.is_some() {
                tracing::warn!(
                    user_id = ?user_id,
                    agent = ?input.agent_name,
                    "quota_enforcement_skipped: no plan found"
                );
            }
            return Ok(None);
        };
        let result = guard.check(user_id.as_deref(), &plan, input).await?;
        if !result.allowed {
            let reason = result.reason.unwrap_or_else(|| "quota exceeded".into());
            self.audit
                .log(
                    AuditEvent::new(AuditAction::QuotaExceeded)
                        .agent_name(input.agent_name.clone())
                        .user_id(input.user_id())
                        .session_id(Some(input.session_id))
                        .status("blocked")
                        .error(Some(reason.clone())),
                )
                .await;
            if result.overage_policy == OveragePolicy::HardBlock {
                return Err(crate::BEM_060.with_data(reason));
            }
        }
        Ok(Some(plan))
    }

    /// Reserve the routed model's input budget (window minus output tokens).
    pub(super) async fn reserve_usage(
        &self,
        input: &AgentInput,
        plan: Option<&UsagePlan>,
        model: Option<&ModelConfig>,
        reserved_tokens: u64,
    ) -> RiverbaseResult<Option<HeldReservation>> {
        let (Some(plan), Some(model)) = (plan, model) else {
            return Ok(None);
        };
        // Python `_enforce_resolved_model_allowlist` (needs only the plan).
        if let Some(allowed) = plan
            .limits
            .allowed_models
            .as_ref()
            .filter(|a| !a.is_empty())
        {
            if !allowed.contains(&model.name) {
                return Err(crate::BEM_060.with_data(format!(
                    "Model '{}' not allowed on plan '{}'",
                    model.name, plan.name
                )));
            }
        }
        let (Some(guard), Some(user)) = (self.usage.as_ref().map(|u| &u.guard), input.user_id())
        else {
            return Ok(None);
        };
        // Python: `state.reserved_tokens or (window - output)`.
        let estimate = if reserved_tokens > 0 {
            reserved_tokens
        } else {
            beam_gateway::window_tokens(model).saturating_sub(beam_gateway::output_tokens(model))
        };
        let org = input.auth_context.as_ref().and_then(|a| a.org_id.clone());
        let reservation = guard
            .reserve(&user, plan, estimate, input.execution_id, org)
            .await?;
        Ok(Some(HeldReservation {
            usage: guard.clone(),
            id: Some(reservation.id),
            reserved: estimate,
        }))
    }

    /// One cost event per successful invoke.
    pub(super) async fn record_cost(
        &self,
        input: &AgentInput,
        result: &AgentResult,
        model: Option<&ModelConfig>,
    ) -> RiverbaseResult<()> {
        let Some(usage) = &result.token_usage else {
            return Ok(());
        };
        let name = result
            .model_used
            .clone()
            .or_else(|| model.map(|m| m.name.clone()))
            .unwrap_or_default();
        let provider = model.and_then(|m| {
            serde_json::to_value(m.provider)
                .ok()
                .and_then(|v| v.as_str().map(str::to_owned))
        });
        let event = cost_event(
            &name,
            usage,
            model.map_or(0.0, |m| m.cost_per_1k_tokens),
            CostDims {
                execution_id: input.execution_id,
                agent_name: input.agent_name.clone(),
                provider,
                org_id: input.auth_context.as_ref().and_then(|a| a.org_id.clone()),
            },
        );
        self.cost_store.save(&event).await
    }
}
