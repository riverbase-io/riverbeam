use async_trait::async_trait;
use uuid::Uuid;

use crate::rows::{
    ActionRow, AgentRow, InteractionRow, InteractionStatus, MessageRow, ModelRow, RunRow,
    RunStatus, SessionRow,
};

pub type StoreResult<T> = riverbase_core::RiverbaseResult<T>;

/// Stable namespace for deterministic agent/model ids (parity with Python `UUID_GENF`).
const BEAM_NS: Uuid = Uuid::from_u128(0x6e02_b3c1_4f7a_4d2e_9a3b_1c5d6e7f8091);

/// Deterministic agent id from its registered name.
#[must_use]
pub fn agent_id_for(name: &str) -> Uuid {
    Uuid::new_v5(&BEAM_NS, name.as_bytes())
}

/// Persistence façade for the execution hierarchy + catalog check-in.
#[async_trait]
pub trait BeamStore: Send + Sync {
    async fn checkin_agents(&self, agents: &[AgentRow]) -> StoreResult<()>;
    async fn checkin_models(&self, models: &[ModelRow]) -> StoreResult<()>;

    async fn ensure_session(
        &self,
        session_id: Uuid,
        agent_id: Uuid,
        user_id: Option<&str>,
        profile_id: Option<&str>,
        name: Option<&str>,
    ) -> StoreResult<()>;
    async fn update_session_activity(&self, session_id: Uuid) -> StoreResult<()>;

    async fn create_interaction(
        &self,
        interaction_id: Uuid,
        session_id: Uuid,
        agent_id: Uuid,
        agent_name: &str,
    ) -> StoreResult<()>;
    async fn complete_interaction(
        &self,
        interaction_id: Uuid,
        status: InteractionStatus,
    ) -> StoreResult<()>;

    async fn create_run(
        &self,
        run_id: Uuid,
        session_id: Uuid,
        interaction_id: Uuid,
        agent_id: Uuid,
        agent_name: &str,
    ) -> StoreResult<()>;
    async fn complete_run(&self, run_id: Uuid, status: RunStatus) -> StoreResult<()>;

    async fn save_message(&self, message: MessageRow) -> StoreResult<()>;
    async fn next_sequence(&self, session_id: Uuid) -> StoreResult<i32>;

    async fn create_action(&self, action: ActionRow) -> StoreResult<()>;
    async fn get_action(&self, action_id: Uuid) -> StoreResult<ActionRow>;
    async fn resolve_action(
        &self,
        action_id: Uuid,
        result: serde_json::Value,
    ) -> StoreResult<()>;

    // Read helpers (catalog queries + tests).
    async fn get_session(&self, session_id: Uuid) -> StoreResult<Option<SessionRow>>;
    async fn get_interaction(&self, interaction_id: Uuid) -> StoreResult<Option<InteractionRow>>;
    async fn get_run(&self, run_id: Uuid) -> StoreResult<Option<RunRow>>;
    async fn list_messages(&self, session_id: Uuid) -> StoreResult<Vec<MessageRow>>;
}
