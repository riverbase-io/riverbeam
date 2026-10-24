use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use beam_types::{AgentInput, Modality};
use serde::{Deserialize, Serialize};

use riverbase_core::RiverbaseResult;
use uuid::Uuid;

use crate::plans::UsagePlan;
use crate::reservation::{Reservation, ReservationStore, ReserveRequest};
use crate::tracker::UsageTracker;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OveragePolicy {
    #[default]
    HardBlock,
    SoftWarn,
    Throttle,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GuardResult {
    pub allowed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Requests and tokens left today, for the limits the plan sets.
    #[serde(default)]
    pub remaining: BTreeMap<String, i64>,
    pub overage_policy: OveragePolicy,
}

impl GuardResult {
    fn allow(overage_policy: OveragePolicy, remaining: BTreeMap<String, i64>) -> Self {
        Self {
            allowed: true,
            reason: None,
            remaining,
            overage_policy,
        }
    }

    fn deny(
        reason: String,
        overage_policy: OveragePolicy,
        remaining: BTreeMap<String, i64>,
    ) -> Self {
        Self {
            allowed: false,
            reason: Some(reason),
            remaining,
            overage_policy,
        }
    }
}

fn left(limit: u64, used: u64) -> i64 {
    i64::try_from(limit).unwrap_or(i64::MAX) - i64::try_from(used).unwrap_or(i64::MAX)
}

/// In-process reservations, used when no [`ReservationStore`] is set.
#[derive(Default)]
struct Held {
    reserved: u64,
    rows: HashMap<Uuid, Reservation>,
}

/// Pre-dispatch quota enforcement (parity with Python `UsageGuard`).
pub struct UsageGuard {
    tracker: Arc<UsageTracker>,
    store: Option<Arc<dyn ReservationStore>>,
    held: Mutex<Held>,
}

impl UsageGuard {
    #[must_use]
    pub fn new(tracker: Arc<UsageTracker>) -> Self {
        Self {
            tracker,
            store: None,
            held: Mutex::new(Held::default()),
        }
    }

    /// Keep reservations in a shared store (every worker sees them).
    #[must_use]
    pub fn with_reservations(mut self, store: Arc<dyn ReservationStore>) -> Self {
        self.store = Some(store);
        self
    }

    /// Expire reservations past their TTL (reconcile). `0` without a
    /// reservation store.
    ///
    /// # Errors
    /// Store errors.
    pub async fn expire_abandoned(&self) -> RiverbaseResult<u64> {
        match &self.store {
            Some(store) => store.expire_abandoned().await,
            None => Ok(0),
        }
    }

    #[must_use]
    pub fn tracker(&self) -> &Arc<UsageTracker> {
        &self.tracker
    }

    fn held(&self) -> RiverbaseResult<std::sync::MutexGuard<'_, Held>> {
        self.held
            .lock()
            .map_err(|_| crate::BEM_361.with_data("reservation lock poisoned"))
    }

    /// Hold `estimate` tokens for this request.
    ///
    /// # Errors
    /// `BEM-363` when the estimate passes `max_tokens_per_request`; `BEM-362`
    /// when the daily limit would be passed; store errors.
    pub async fn reserve(
        &self,
        user_id: &str,
        plan: &UsagePlan,
        estimate: u64,
        execution_id: Option<Uuid>,
        org_id: Option<String>,
    ) -> RiverbaseResult<Reservation> {
        if let Some(max) = plan.limits.max_tokens_per_request {
            if estimate > max {
                return Err(crate::BEM_363.with_data(format!(
                    "Request estimate {estimate} exceeds max_tokens_per_request"
                )));
            }
        }
        let usage = self.tracker.get_usage(user_id).await?;
        if let Some(store) = &self.store {
            let id = store
                .reserve(
                    user_id,
                    &ReserveRequest {
                        tokens: estimate,
                        daily_limit: plan.limits.max_tokens_per_day,
                        usage_tokens: usage.total_tokens,
                        execution_id,
                        org_id,
                        owner: None,
                    },
                )
                .await?;
            return Ok(Reservation {
                id,
                user_id: user_id.to_owned(),
                tokens: estimate,
            });
        }
        let mut held = self.held()?;
        if let Some(limit) = plan.limits.max_tokens_per_day {
            if usage.total_tokens + held.reserved + estimate > limit {
                return Err(crate::BEM_362.with_data("daily token reservation denied"));
            }
            held.reserved += estimate;
        }
        let reservation = Reservation {
            id: Uuid::new_v4(),
            user_id: user_id.to_owned(),
            tokens: estimate,
        };
        held.rows.insert(reservation.id, reservation.clone());
        Ok(reservation)
    }

    /// Close a reservation with the tokens actually used.
    ///
    /// # Errors
    /// Store errors.
    pub async fn settle(&self, reservation_id: Uuid, actual_tokens: u64) -> RiverbaseResult<()> {
        if let Some(store) = &self.store {
            return store.settle(reservation_id, Some(actual_tokens)).await;
        }
        self.drop_held(reservation_id)
    }

    /// Close a reservation unused (the invoke failed).
    ///
    /// # Errors
    /// Store errors.
    pub async fn release(&self, reservation_id: Uuid) -> RiverbaseResult<()> {
        if let Some(store) = &self.store {
            return store.release(reservation_id).await;
        }
        self.drop_held(reservation_id)
    }

    fn drop_held(&self, reservation_id: Uuid) -> RiverbaseResult<()> {
        let mut held = self.held()?;
        if let Some(row) = held.rows.remove(&reservation_id) {
            held.reserved = held.reserved.saturating_sub(row.tokens);
        }
        Ok(())
    }

    /// Tokens held by in-process reservations.
    #[must_use]
    pub fn reserved(&self) -> u64 {
        self.held.lock().map_or(0, |h| h.reserved)
    }

    /// Check today's usage against the plan.
    ///
    /// # Errors
    /// Store errors.
    pub async fn check(
        &self,
        user_id: Option<&str>,
        plan: &UsagePlan,
        request: &AgentInput,
    ) -> RiverbaseResult<GuardResult> {
        let Some(user_id) = user_id else {
            return Ok(GuardResult::allow(plan.overage_policy, BTreeMap::new()));
        };
        let usage = self.tracker.get_usage(user_id).await?;
        let limits = &plan.limits;
        let mut remaining = BTreeMap::new();
        if let Some(max) = limits.max_requests_per_day {
            remaining.insert("requests".to_owned(), left(max, usage.total_requests));
        }
        if let Some(max) = limits.max_tokens_per_day {
            remaining.insert("tokens".to_owned(), left(max, usage.total_tokens));
        }
        let deny = |reason: String| {
            Ok(GuardResult::deny(
                reason,
                plan.overage_policy,
                remaining.clone(),
            ))
        };

        if let Some(max) = limits.max_requests_per_day {
            if usage.total_requests >= max {
                return deny(format!("Daily request limit reached ({max})"));
            }
        }
        if let Some(max) = limits.max_tokens_per_day {
            if usage.total_tokens >= max {
                return deny(format!("Daily token limit reached ({max})"));
            }
        }
        if let (Some(allowed), Some(agent)) = (&limits.allowed_agents, &request.agent_name) {
            if !allowed.contains(agent) {
                return deny(format!(
                    "Agent '{agent}' not allowed on plan '{}'",
                    plan.name
                ));
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
                return deny(format!(
                    "Modalities {disallowed:?} not allowed on plan '{}'",
                    plan.name
                ));
            }
        }
        if let (Some(allowed), Some(model)) = (&limits.allowed_models, &request.model_override) {
            if !allowed.contains(model) {
                return deny(format!(
                    "Model '{model}' not allowed on plan '{}'",
                    plan.name
                ));
            }
        }
        Ok(GuardResult::allow(plan.overage_policy, remaining))
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
            context: None,
            execution_id: None,
            runtime_ref: None,
            client: None,
        }
    }

    #[tokio::test]
    async fn blocks_when_request_limit_reached() {
        let store = Arc::new(InMemoryUsageStore::new());
        let tracker = Arc::new(UsageTracker::new(store));
        tracker
            .record(
                "u1",
                &UsageEvent {
                    tokens: 0,
                    requests: 5,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let guard = UsageGuard::new(tracker);
        let plan = UsagePlan {
            name: "basic".into(),
            limits: UsageLimits {
                max_requests_per_day: Some(5),
                ..Default::default()
            },
            overage_policy: OveragePolicy::HardBlock,
        };
        let result = guard.check(Some("u1"), &plan, &request()).await.unwrap();
        assert!(!result.allowed);
        assert_eq!(result.remaining.get("requests"), Some(&0));
    }

    #[tokio::test]
    async fn allows_without_user() {
        let store = Arc::new(InMemoryUsageStore::new());
        let tracker = Arc::new(UsageTracker::new(store));
        let guard = UsageGuard::new(tracker);
        let result = guard
            .check(None, &UsagePlan::unlimited("free"), &request())
            .await
            .unwrap();
        assert!(result.allowed);
    }

    fn daily(limit: u64) -> UsagePlan {
        UsagePlan {
            name: "daily".into(),
            limits: UsageLimits {
                max_tokens_per_day: Some(limit),
                max_tokens_per_request: Some(600),
                ..Default::default()
            },
            overage_policy: OveragePolicy::HardBlock,
        }
    }

    #[tokio::test]
    async fn reservations_hold_tokens_until_settled_or_released() {
        let tracker = Arc::new(UsageTracker::new(Arc::new(InMemoryUsageStore::new())));
        tracker
            .record(
                "u1",
                &UsageEvent {
                    tokens: 100,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let guard = UsageGuard::new(tracker);
        let plan = daily(1000);
        let first = guard.reserve("u1", &plan, 500, None, None).await.unwrap();
        assert_eq!(guard.reserved(), 500);
        // 100 used + 500 held + 500 asked > 1000.
        let denied = guard
            .reserve("u1", &plan, 500, None, None)
            .await
            .unwrap_err();
        assert_eq!(denied.errcode.as_str(), "BEM-362");
        let too_big = guard
            .reserve("u1", &plan, 601, None, None)
            .await
            .unwrap_err();
        assert_eq!(too_big.errcode.as_str(), "BEM-363");
        guard.settle(first.id, 42).await.unwrap();
        assert_eq!(guard.reserved(), 0);
        let second = guard.reserve("u1", &plan, 500, None, None).await.unwrap();
        guard.release(second.id).await.unwrap();
        guard.release(second.id).await.unwrap();
        assert_eq!(guard.reserved(), 0);
    }

    fn limited(limits: UsageLimits) -> UsagePlan {
        UsagePlan {
            name: "limited".into(),
            limits,
            overage_policy: OveragePolicy::HardBlock,
        }
    }

    /// Plan 019: the per-day token limit denies once it is used up.
    #[tokio::test]
    async fn blocks_when_the_daily_token_limit_is_reached() {
        let tracker = Arc::new(UsageTracker::new(Arc::new(InMemoryUsageStore::new())));
        tracker
            .record(
                "u1",
                &UsageEvent {
                    tokens: 1000,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let guard = UsageGuard::new(tracker);
        let plan = limited(UsageLimits {
            max_tokens_per_day: Some(1000),
            ..Default::default()
        });
        let result = guard.check(Some("u1"), &plan, &request()).await.unwrap();
        assert!(!result.allowed);
        assert_eq!(result.remaining.get("tokens"), Some(&0));
    }

    /// Plan 019: a plan's allowed agents, modalities and models each deny
    /// what they do not list, and allow what they do.
    #[tokio::test]
    async fn allow_lists_deny_unlisted_agents_modalities_and_models() {
        let guard = UsageGuard::new(Arc::new(UsageTracker::new(Arc::new(
            InMemoryUsageStore::new(),
        ))));
        let check = |plan: UsagePlan, request: AgentInput| {
            let guard = &guard;
            async move {
                guard
                    .check(Some("u1"), &plan, &request)
                    .await
                    .unwrap()
                    .allowed
            }
        };

        let agents = |names: &[&str]| {
            limited(UsageLimits {
                allowed_agents: Some(names.iter().map(|n| (*n).to_owned()).collect()),
                ..Default::default()
            })
        };
        assert!(check(agents(&["demo"]), request()).await);
        assert!(!check(agents(&["other"]), request()).await);

        let modalities = |allowed: Vec<Modality>| {
            limited(UsageLimits {
                allowed_modalities: Some(allowed),
                ..Default::default()
            })
        };
        let mut with_image = request();
        with_image.content.push(ContentPart {
            part_type: beam_types::ContentPartType::ImageUrl,
            text: None,
            url: Some("https://example.com/a.png".into()),
            ..ContentPart::text("")
        });
        assert!(check(modalities(vec![Modality::Text]), request()).await);
        assert!(!check(modalities(vec![Modality::Text]), with_image.clone()).await);
        assert!(
            check(
                modalities(vec![Modality::Text, Modality::Image]),
                with_image
            )
            .await
        );

        let models = |names: &[&str]| {
            limited(UsageLimits {
                allowed_models: Some(names.iter().map(|n| (*n).to_owned()).collect()),
                ..Default::default()
            })
        };
        let mut overridden = request();
        overridden.model_override = Some("gpt-big".into());
        assert!(
            check(models(&["gpt-small"]), request()).await,
            "no override"
        );
        assert!(!check(models(&["gpt-small"]), overridden.clone()).await);
        assert!(check(models(&["gpt-big"]), overridden).await);
    }
}
