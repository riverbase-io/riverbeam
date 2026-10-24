//! Durable tool-call idempotency keyed by execution, call and tool revision
//! (Python `ToolIdempotencyStore`, `tools/stores/*`).

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use beam_store::repo::tool_idempotency::ToolCallKey;
use beam_store::{Db, PgBeamStore};
use chrono::{DateTime, Utc};
use riverbase_core::RiverbaseResult;

use crate::runtime::ToolRuntimeResult;

/// Python's default TTL for a remembered result: one day.
pub const DEFAULT_TTL: Duration = Duration::from_hours(24);

/// `(execution_id, call_id, tool_revision)`.
pub type IdempotencyKey = (String, String, String);

#[async_trait]
pub trait ToolIdempotencyStore: Send + Sync {
    /// The remembered result, unless it expired.
    ///
    /// # Errors
    /// `BEM-474` for store failures.
    async fn get(&self, key: &IdempotencyKey) -> RiverbaseResult<Option<ToolRuntimeResult>>;

    /// Remember a result (replacing an older one) for `ttl`.
    ///
    /// # Errors
    /// `BEM-474` for store failures.
    async fn put(
        &self,
        key: &IdempotencyKey,
        result: &ToolRuntimeResult,
        ttl: Duration,
    ) -> RiverbaseResult<()>;
}

#[derive(Default)]
pub struct InMemoryToolIdempotencyStore {
    rows: Mutex<HashMap<IdempotencyKey, (ToolRuntimeResult, DateTime<Utc>)>>,
}

impl InMemoryToolIdempotencyStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl ToolIdempotencyStore for InMemoryToolIdempotencyStore {
    async fn get(&self, key: &IdempotencyKey) -> RiverbaseResult<Option<ToolRuntimeResult>> {
        let mut rows = self
            .rows
            .lock()
            .map_err(|_| crate::BEM_474.with_data("idempotency lock poisoned"))?;
        // Expired rows are dropped on read, as in Python, and old ones are swept.
        let now = Utc::now();
        rows.retain(|_, (_, expires)| *expires > now);
        Ok(rows.get(key).map(|(result, _)| result.clone()))
    }

    async fn put(
        &self,
        key: &IdempotencyKey,
        result: &ToolRuntimeResult,
        ttl: Duration,
    ) -> RiverbaseResult<()> {
        let expires =
            Utc::now() + chrono::Duration::from_std(ttl).unwrap_or(chrono::Duration::zero());
        self.rows
            .lock()
            .map_err(|_| crate::BEM_474.with_data("idempotency lock poisoned"))?
            .insert(key.clone(), (result.clone(), expires));
        Ok(())
    }
}

/// `tool_call_idempotency`, so a remembered
/// side effect survives a worker restart and is shared with Python.
#[derive(Clone)]
pub struct PgToolIdempotencyStore {
    db: Db,
}

impl PgToolIdempotencyStore {
    #[must_use]
    pub fn for_store(store: &PgBeamStore) -> Self {
        Self {
            db: store.db().clone(),
        }
    }
}

fn key_of(key: &IdempotencyKey) -> ToolCallKey<'_> {
    ToolCallKey {
        execution_id: &key.0,
        call_id: &key.1,
        tool_revision: &key.2,
    }
}

#[async_trait]
impl ToolIdempotencyStore for PgToolIdempotencyStore {
    async fn get(&self, key: &IdempotencyKey) -> RiverbaseResult<Option<ToolRuntimeResult>> {
        let stored = self
            .db
            .tool_idempotency()
            .get(key_of(key))
            .await
            .map_err(|e| e.or(crate::BEM_474))?;
        stored
            .map(|value| {
                serde_json::from_value(value).map_err(|e| crate::BEM_474.with_data(e.to_string()))
            })
            .transpose()
    }

    async fn put(
        &self,
        key: &IdempotencyKey,
        result: &ToolRuntimeResult,
        ttl: Duration,
    ) -> RiverbaseResult<()> {
        let payload =
            serde_json::to_value(result).map_err(|e| crate::BEM_474.with_data(e.to_string()))?;
        self.db
            .tool_idempotency()
            .put(key_of(key), &payload, ttl)
            .await
            .map_err(|e| e.or(crate::BEM_474))
    }
}
