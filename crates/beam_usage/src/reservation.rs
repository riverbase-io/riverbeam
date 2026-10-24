//! Token reservations held during an invoke (Python `Reservation`, `SqlReservationStore`).

use async_trait::async_trait;
use riverbase_core::RiverbaseResult;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reservation {
    pub id: Uuid,
    pub user_id: String,
    pub tokens: u64,
}

/// What a reservation is for.
#[derive(Debug, Clone, Default)]
pub struct ReserveRequest {
    pub tokens: u64,
    /// The plan's daily token limit, if any.
    pub daily_limit: Option<u64>,
    /// Tokens already counted for the user today.
    pub usage_tokens: u64,
    pub execution_id: Option<Uuid>,
    pub org_id: Option<String>,
    pub owner: Option<String>,
}

/// Durable reservations shared by every worker.
#[async_trait]
pub trait ReservationStore: Send + Sync {
    /// Hold `request.tokens` for `user_id`, unless today's usage plus every
    /// live reservation plus this one would pass the daily limit.
    ///
    /// # Errors
    /// `BEM-362` when denied; `BEM-361` for store failures.
    async fn reserve(&self, user_id: &str, request: &ReserveRequest) -> RiverbaseResult<Uuid>;

    /// Close a reservation as used, recording the actual tokens.
    ///
    /// # Errors
    /// `BEM-361` for store failures.
    async fn settle(&self, reservation_id: Uuid, actual_tokens: Option<u64>)
        -> RiverbaseResult<()>;

    /// Close a reservation unused.
    ///
    /// # Errors
    /// `BEM-361` for store failures.
    async fn release(&self, reservation_id: Uuid) -> RiverbaseResult<()>;

    /// Expire reservations past their deadline; returns how many.
    ///
    /// # Errors
    /// `BEM-361` for store failures.
    async fn expire_abandoned(&self) -> RiverbaseResult<u64>;
}
