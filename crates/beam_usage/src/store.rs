use std::collections::HashMap;
use std::sync::RwLock;

use async_trait::async_trait;
use chrono::Utc;
use riverbase_core::RiverbaseResult;

use crate::tracker::UsageEvent;

/// Per-user usage counters keyed by (user, period, field), usage events and
/// plan profiles.
#[async_trait]
pub trait UsageStore: Send + Sync {
    /// Current accounting period (UTC day `YYYY-MM-DD`).
    fn current_period(&self) -> String {
        Utc::now().format("%Y-%m-%d").to_string()
    }

    /// Add `amount` and return the new value.
    ///
    /// # Errors
    /// `BEM-361` for store failures; `BEM-364` for a non-UUID user on SQL.
    async fn increment(
        &self,
        user_id: &str,
        period: &str,
        field: &str,
        amount: u64,
    ) -> RiverbaseResult<u64>;

    /// # Errors
    /// As [`Self::increment`].
    async fn get_all(&self, user_id: &str, period: &str) -> RiverbaseResult<Vec<(String, u64)>>;

    /// # Errors
    /// As [`Self::increment`].
    async fn reset(&self, user_id: &str, period: &str) -> RiverbaseResult<()>;

    /// # Errors
    /// As [`Self::increment`].
    async fn save_event(&self, user_id: &str, event: &UsageEvent) -> RiverbaseResult<()>;

    /// The plan key of the user's active profile.
    ///
    /// # Errors
    /// As [`Self::increment`].
    async fn get_usage_profile(&self, user_id: &str) -> RiverbaseResult<Option<String>>;

    /// # Errors
    /// As [`Self::increment`].
    async fn set_usage_profile(&self, user_id: &str, plan_key: &str) -> RiverbaseResult<()>;
}

/// Counters only, as Python's in-memory store: events and profiles are not
/// kept (an in-process deployment would grow them without bound).
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

fn poisoned<T>(_: T) -> riverbase_core::RiverbaseError {
    crate::BEM_361.with_data("in-memory usage store lock poisoned")
}

#[async_trait]
impl UsageStore for InMemoryUsageStore {
    async fn increment(
        &self,
        user_id: &str,
        period: &str,
        field: &str,
        amount: u64,
    ) -> RiverbaseResult<u64> {
        let mut guard = self.counters.write().map_err(poisoned)?;
        let value = guard
            .entry((user_id.to_owned(), period.to_owned(), field.to_owned()))
            .or_insert(0);
        *value += amount;
        Ok(*value)
    }

    async fn get_all(&self, user_id: &str, period: &str) -> RiverbaseResult<Vec<(String, u64)>> {
        let guard = self.counters.read().map_err(poisoned)?;
        Ok(guard
            .iter()
            .filter(|((u, p, _), _)| u == user_id && p == period)
            .map(|((_, _, field), v)| (field.clone(), *v))
            .collect())
    }

    async fn reset(&self, user_id: &str, period: &str) -> RiverbaseResult<()> {
        let mut guard = self.counters.write().map_err(poisoned)?;
        guard.retain(|(u, p, _), _| !(u == user_id && p == period));
        Ok(())
    }

    async fn save_event(&self, _user_id: &str, _event: &UsageEvent) -> RiverbaseResult<()> {
        Ok(())
    }

    async fn get_usage_profile(&self, _user_id: &str) -> RiverbaseResult<Option<String>> {
        Ok(None)
    }

    async fn set_usage_profile(&self, _user_id: &str, _plan_key: &str) -> RiverbaseResult<()> {
        Ok(())
    }
}
