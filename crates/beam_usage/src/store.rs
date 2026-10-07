use std::collections::HashMap;
use std::sync::RwLock;

use async_trait::async_trait;
use chrono::Utc;

/// Per-user usage counter store keyed by (user, period, field).
#[async_trait]
pub trait UsageStore: Send + Sync {
    /// Current accounting period (default: UTC day `YYYY-MM-DD`).
    fn current_period(&self) -> String {
        Utc::now().format("%Y-%m-%d").to_string()
    }
    async fn increment(&self, user_id: &str, period: &str, field: &str, amount: u64);
    async fn get_all(&self, user_id: &str, period: &str) -> Vec<(String, u64)>;
    async fn reset(&self, user_id: &str, period: &str);
}

#[derive(Default)]
pub struct InMemoryUsageStore {
    counters: RwLock<HashMap<(String, String, String), u64>>,
}

impl InMemoryUsageStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl UsageStore for InMemoryUsageStore {
    async fn increment(&self, user_id: &str, period: &str, field: &str, amount: u64) {
        let mut guard = self.counters.write().expect("usage store lock");
        *guard
            .entry((user_id.to_string(), period.to_string(), field.to_string()))
            .or_insert(0) += amount;
    }

    async fn get_all(&self, user_id: &str, period: &str) -> Vec<(String, u64)> {
        let guard = self.counters.read().expect("usage store lock");
        guard
            .iter()
            .filter(|((u, p, _), _)| u == user_id && p == period)
            .map(|((_, _, field), v)| (field.clone(), *v))
            .collect()
    }

    async fn reset(&self, user_id: &str, period: &str) {
        let mut guard = self.counters.write().expect("usage store lock");
        guard.retain(|(u, p, _), _| !(u == user_id && p == period));
    }
}
