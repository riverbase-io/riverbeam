//! Usage counters, events, profiles, reservations and the cost ledger on
//! fluvius-beam's tables (Python `usage/stores/sql.py`): adapters on
//! `db.usage()`.

use std::time::Duration;

use async_trait::async_trait;
use beam_store::repo::usage::NewReservation;
use beam_store::{agent_id_for, Db, DbError, PgBeamStore};
use chrono::Utc;
use riverbase_core::{RiverbaseError, RiverbaseResult};
use serde_json::json;
use uuid::Uuid;

use crate::cost::{CostEvent, CostStore};
use crate::reservation::{ReservationStore, ReserveRequest};
use crate::store::UsageStore;
use crate::tracker::UsageEvent;

/// How long an unsettled reservation holds tokens (Python's 15 minutes).
pub const RESERVATION_TTL: Duration = Duration::from_mins(15);

fn db(e: DbError) -> RiverbaseError {
    e.or(crate::BEM_361)
}

/// SQL usage rows key users by UUID, as Python's schema does.
fn user_uuid(user_id: &str) -> RiverbaseResult<Uuid> {
    Uuid::parse_str(user_id.trim()).map_err(|_| crate::BEM_364.with_data(user_id.to_owned()))
}

/// A UUID, or nothing.
fn org_uuid(org_id: Option<&str>) -> Option<Uuid> {
    org_id.and_then(|o| Uuid::parse_str(o.trim()).ok())
}

fn int(value: u64) -> i32 {
    i32::try_from(value).unwrap_or(i32::MAX)
}

fn uint(value: i32) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

/// Counters, events and plan profiles in `agent_usage_counter`,
/// `agent_usage_event` and `agent_usage_profile` (`USAGE_BACKEND=sql`).
#[derive(Clone)]
pub struct PgUsageStore {
    db: Db,
}

impl PgUsageStore {
    #[must_use]
    pub fn for_store(store: &PgBeamStore) -> Self {
        Self {
            db: store.db().clone(),
        }
    }
}

#[async_trait]
impl UsageStore for PgUsageStore {
    async fn increment(
        &self,
        user_id: &str,
        period: &str,
        field: &str,
        amount: u64,
    ) -> RiverbaseResult<u64> {
        let user = user_uuid(user_id)?;
        let value = self
            .db
            .usage()
            .increment(user, period, field, int(amount))
            .await
            .map_err(db)?;
        Ok(value.map_or(amount, uint))
    }

    async fn get_all(&self, user_id: &str, period: &str) -> RiverbaseResult<Vec<(String, u64)>> {
        let user = user_uuid(user_id)?;
        let rows = self.db.usage().counters(user, period).await.map_err(db)?;
        Ok(rows.into_iter().map(|(f, v)| (f, uint(v))).collect())
    }

    async fn reset(&self, user_id: &str, period: &str) -> RiverbaseResult<()> {
        let user = user_uuid(user_id)?;
        self.db.usage().reset(user, period).await.map_err(db)
    }

    async fn save_event(&self, user_id: &str, event: &UsageEvent) -> RiverbaseResult<()> {
        let user = user_uuid(user_id)?;
        let modalities = serde_json::to_value(&event.modalities)
            .map_err(|e| crate::BEM_361.with_data(e.to_string()))?;
        let data = json!({
            "_insert": true,
            "user_id": user,
            "tokens": int(event.tokens),
            "requests": int(event.requests),
            "model_used": event.model_used,
            "modalities": modalities,
        });
        self.db.usage().append_usage_event(&data).await.map_err(db)
    }

    async fn get_usage_profile(&self, user_id: &str) -> RiverbaseResult<Option<String>> {
        let user = user_uuid(user_id)?;
        self.db.usage().active_plan(user).await.map_err(db)
    }

    async fn set_usage_profile(&self, user_id: &str, plan_key: &str) -> RiverbaseResult<()> {
        let user = user_uuid(user_id)?;
        // Python: `UUID_GENF(plan_key)`.
        self.db
            .usage()
            .ensure_profile(user, plan_key, agent_id_for(plan_key))
            .await
            .map_err(db)
    }
}

/// Reservations in `agent_usage_reservation`, serialized per user with
/// Python's advisory lock key, so both stacks see each other's holds.
#[derive(Clone)]
pub struct PgReservationStore {
    db: Db,
}

impl PgReservationStore {
    #[must_use]
    pub fn for_store(store: &PgBeamStore) -> Self {
        Self {
            db: store.db().clone(),
        }
    }
}

#[async_trait]
impl ReservationStore for PgReservationStore {
    async fn reserve(&self, user_id: &str, request: &ReserveRequest) -> RiverbaseResult<Uuid> {
        let user = user_uuid(user_id)?;
        let ttl = chrono::Duration::from_std(RESERVATION_TTL).unwrap_or(chrono::Duration::zero());
        let reservation = NewReservation {
            id: Uuid::new_v4(),
            user_id: user,
            lock_key: user_id.to_owned(),
            org_id: org_uuid(request.org_id.as_deref()),
            tokens: int(request.tokens),
            execution_id: request.execution_id,
            ttl_seconds: ttl.as_seconds_f64(),
            owner: request.owner.clone(),
            daily_limit: request.daily_limit,
            usage_tokens: request.usage_tokens,
        };
        self.db.usage().reserve(reservation).await.map_err(db)
    }

    async fn settle(
        &self,
        reservation_id: Uuid,
        actual_tokens: Option<u64>,
    ) -> RiverbaseResult<()> {
        self.db
            .usage()
            .finish_reservation(reservation_id, "settled", actual_tokens.map(int))
            .await
            .map_err(db)
    }

    async fn release(&self, reservation_id: Uuid) -> RiverbaseResult<()> {
        self.db
            .usage()
            .finish_reservation(reservation_id, "released", None)
            .await
            .map_err(db)
    }

    async fn expire_abandoned(&self) -> RiverbaseResult<u64> {
        self.db.usage().expire_abandoned().await.map_err(db)
    }
}

/// The cost ledger, `agent_cost_event`.
#[derive(Clone)]
pub struct PgCostStore {
    db: Db,
}

impl PgCostStore {
    #[must_use]
    pub fn for_store(store: &PgBeamStore) -> Self {
        Self {
            db: store.db().clone(),
        }
    }
}

#[async_trait]
impl CostStore for PgCostStore {
    async fn save(&self, event: &CostEvent) -> RiverbaseResult<()> {
        let data = json!({
            "_insert": true,
            "execution_id": event.execution_id,
            "org_id": org_uuid(event.org_id.as_deref()),
            "agent_name": event.agent_name,
            "model": event.model,
            "provider": event.provider,
            "prompt_tokens": i32::try_from(event.prompt_tokens).unwrap_or(i32::MAX),
            "completion_tokens": i32::try_from(event.completion_tokens).unwrap_or(i32::MAX),
            "usd": event.usd,
            // The app clock (plan 015 D4).
            "recorded_at": Utc::now(),
        });
        self.db.usage().append_cost_event(&data).await.map_err(db)
    }
}
