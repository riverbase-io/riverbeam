use std::sync::Arc;

use beam_types::Modality;
use serde::{Deserialize, Serialize};

use riverbase_core::RiverbaseResult;

use crate::store::UsageStore;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct UsageEvent {
    #[serde(default)]
    pub tokens: u64,
    #[serde(default = "one")]
    pub requests: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_used: Option<String>,
    #[serde(default)]
    pub modalities: Vec<Modality>,
}

fn one() -> u64 {
    1
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct UsageSummary {
    pub total_requests: u64,
    pub total_tokens: u64,
    pub period: String,
}

/// Accumulates per-user usage counters via a [`UsageStore`].
pub struct UsageTracker {
    store: Arc<dyn UsageStore>,
}

impl UsageTracker {
    #[must_use]
    pub fn new(store: Arc<dyn UsageStore>) -> Self {
        Self { store }
    }

    /// Save the event and add it to the period's counters.
    ///
    /// # Errors
    /// Store errors.
    pub async fn record(&self, user_id: &str, event: &UsageEvent) -> RiverbaseResult<()> {
        self.store.save_event(user_id, event).await?;
        let period = self.store.current_period();
        self.store
            .increment(user_id, &period, "requests", event.requests)
            .await?;
        if event.tokens > 0 {
            self.store
                .increment(user_id, &period, "tokens", event.tokens)
                .await?;
        }
        Ok(())
    }

    /// # Errors
    /// Store errors.
    pub async fn get_usage(&self, user_id: &str) -> RiverbaseResult<UsageSummary> {
        let period = self.store.current_period();
        let counters = self.store.get_all(user_id, &period).await?;
        let get = |name: &str| {
            counters
                .iter()
                .find(|(k, _)| k == name)
                .map_or(0, |(_, v)| *v)
        };
        Ok(UsageSummary {
            total_requests: get("requests"),
            total_tokens: get("tokens"),
            period,
        })
    }

    /// # Errors
    /// Store errors.
    pub async fn get_usage_profile(&self, user_id: &str) -> RiverbaseResult<Option<String>> {
        self.store.get_usage_profile(user_id).await
    }

    /// # Errors
    /// Store errors.
    pub async fn set_usage_profile(&self, user_id: &str, plan_key: &str) -> RiverbaseResult<()> {
        self.store.set_usage_profile(user_id, plan_key).await
    }
}
