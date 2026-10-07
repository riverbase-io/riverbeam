use std::sync::Arc;

use beam_types::Modality;
use serde::{Deserialize, Serialize};

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

    pub async fn record(&self, user_id: &str, event: &UsageEvent) {
        let period = self.store.current_period();
        self.store.increment(user_id, &period, "requests", event.requests).await;
        if event.tokens > 0 {
            self.store.increment(user_id, &period, "tokens", event.tokens).await;
        }
    }

    pub async fn get_usage(&self, user_id: &str) -> UsageSummary {
        let period = self.store.current_period();
        let counters = self.store.get_all(user_id, &period).await;
        UsageSummary {
            total_requests: counters.iter().find(|(k, _)| k == "requests").map_or(0, |(_, v)| *v),
            total_tokens: counters.iter().find(|(k, _)| k == "tokens").map_or(0, |(_, v)| *v),
            period,
        }
    }
}
