use async_trait::async_trait;
use uuid::Uuid;

use crate::rows::{
    ActionRow, AgentRow, InteractionRow, InteractionStatus, ManifestRow, MessageRow, ModelRow,
    PlanRow, RunRow, RunStatus, SessionRow, StepRow,
};

pub type StoreResult<T> = riverbase_core::RiverbaseResult<T>;

/// Stable-sequence cursors for [`ExecutionStore::list_messages_page`]
/// filters first, then with a limit the
/// latest `limit` rows, always in ascending sequence order.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MessagePage {
    pub limit: Option<usize>,
    pub before: Option<i64>,
    pub after: Option<i64>,
    pub through: Option<i64>,
}

impl MessagePage {
    /// Whether `sequence` passes the cursors.
    #[must_use]
    pub fn admits(&self, sequence: i64) -> bool {
        self.after.is_none_or(|a| sequence > a)
            && self.before.is_none_or(|b| sequence < b)
            && self.through.is_none_or(|t| sequence <= t)
    }

    /// Apply the cursors and limit to rows already in sequence order.
    #[must_use]
    pub fn apply<T>(&self, rows: Vec<T>, sequence: impl Fn(&T) -> i64) -> Vec<T> {
        let mut rows: Vec<T> = rows
            .into_iter()
            .filter(|r| self.admits(sequence(r)))
            .collect();
        if let Some(limit) = self.limit.filter(|l| *l > 0) {
            if rows.len() > limit {
                rows.drain(..rows.len() - limit);
            }
        }
        rows
    }
}

/// Fluvius `UUID5_NAMESPACE` (`fluvius.data`), so `agent_id_for` equals Python's
/// `UUID_GENF(agent_name)` and both stacks key the same agent row (plan 005, D5).
const BEAM_NS: Uuid = Uuid::from_u128(0xf853_f38d_83f8_4ef4_9e1b_12b9_e27b_c1ad);

/// Deterministic catalog id from a registered name (agents, models, plans).
#[must_use]
pub fn agent_id_for(name: &str) -> Uuid {
    Uuid::new_v5(&BEAM_NS, name.as_bytes())
}

/// Catalog facet: agents and models checked in at worker start.
#[async_trait]
pub trait CatalogStore: Send + Sync {
    async fn checkin_agents(&self, agents: &[AgentRow]) -> StoreResult<()>;
    async fn checkin_models(&self, models: &[ModelRow]) -> StoreResult<()>;
    /// Upsert usage plans.
    async fn checkin_plans(&self, plans: &[PlanRow]) -> StoreResult<()>;
}

/// Execution facet: session → interaction → run → step,
/// message and action rows.
#[async_trait]
pub trait ExecutionStore: Send + Sync {
    /// Whether rows outlive the process (a database). Execution records must be
    /// durable too when this is, since the store's rows reference them.
    fn durable(&self) -> bool {
        false
    }

    /// Insert the session if it does not exist yet; an existing row is left as is.
    async fn ensure_session(&self, session: &SessionRow) -> StoreResult<()>;
    async fn update_session_activity(&self, session_id: Uuid) -> StoreResult<()>;

    async fn create_interaction(&self, interaction: &InteractionRow) -> StoreResult<()>;
    async fn complete_interaction(
        &self,
        interaction_id: Uuid,
        status: InteractionStatus,
    ) -> StoreResult<()>;

    async fn create_run(&self, run: &RunRow) -> StoreResult<()>;
    /// Record the runtime ref the run executed under (`agent_run.runtime_ref_snapshot`).
    async fn set_run_runtime_ref(
        &self,
        _run_id: Uuid,
        _runtime_ref: &serde_json::Value,
    ) -> StoreResult<()> {
        Ok(())
    }
    async fn complete_run(&self, run_id: Uuid, status: RunStatus) -> StoreResult<()>;
    /// The execution's runs, oldest attempt first.
    async fn list_execution_runs(&self, execution_id: Uuid) -> StoreResult<Vec<RunRow>>;
    /// Record the W3C trace id the run executed under (`agent_run.trace_id`).
    async fn set_run_trace_id(&self, _run_id: Uuid, _trace_id: &str) -> StoreResult<()> {
        Ok(())
    }
    /// The run's trace id, when the store keeps it.
    async fn run_trace_id(&self, _run_id: Uuid) -> StoreResult<Option<String>> {
        Ok(None)
    }

    /// Insert or update one runtime step.
    async fn save_step(&self, step: &StepRow) -> StoreResult<()>;

    async fn save_message(&self, message: MessageRow) -> StoreResult<()>;
    async fn next_sequence(&self, session_id: Uuid) -> StoreResult<i32>;

    /// Append `rows` to the session as one turn: sequences are assigned from
    /// the session's next one, densely, with no other writer interleaving.
    async fn append_messages(&self, session_id: Uuid, rows: Vec<MessageRow>) -> StoreResult<()>;

    async fn create_action(&self, action: ActionRow) -> StoreResult<()>;
    async fn get_action(&self, action_id: Uuid) -> StoreResult<ActionRow>;
    /// Mark this execution's pending actions expired (on cancel); returns how many.
    async fn expire_pending_actions(&self, execution_id: Uuid) -> StoreResult<u64>;
    async fn resolve_action(&self, action_id: Uuid, result: serde_json::Value) -> StoreResult<()>;
    /// Claim up to `limit` pending `CLIENT_TOOL` actions whose `expires_at`
    /// has passed and whose `sweep_after` is unset or passed, oldest first
    /// (Python rc8 `find_due_client_tools`): each is returned once, with its
    /// `sweep_after` set to `until` in the same statement, so two sweeps never
    /// claim the same action before `until`.
    async fn claim_due_client_tool_actions(
        &self,
        now: chrono::DateTime<chrono::Utc>,
        until: chrono::DateTime<chrono::Utc>,
        limit: i64,
    ) -> StoreResult<Vec<ActionRow>>;
    /// Hold these actions back from the sweep until `until` (`sweep_after`).
    async fn defer_client_tool_actions(
        &self,
        action_ids: &[Uuid],
        until: chrono::DateTime<chrono::Utc>,
    ) -> StoreResult<()>;
    /// The interaction's pending actions.
    async fn list_pending_actions(&self, interaction_id: Uuid) -> StoreResult<Vec<ActionRow>>;

    async fn get_session(&self, session_id: Uuid) -> StoreResult<Option<SessionRow>>;
    async fn get_interaction(&self, interaction_id: Uuid) -> StoreResult<Option<InteractionRow>>;
    async fn get_run(&self, run_id: Uuid) -> StoreResult<Option<RunRow>>;
    async fn list_steps(&self, run_id: Uuid) -> StoreResult<Vec<StepRow>>;
    async fn list_messages(&self, session_id: Uuid) -> StoreResult<Vec<MessageRow>>;

    /// One page of a session's messages, ascending by sequence.
    async fn list_messages_page(
        &self,
        session_id: Uuid,
        page: MessagePage,
    ) -> StoreResult<Vec<MessageRow>> {
        let rows = self.list_messages(session_id).await?;
        Ok(page.apply(rows, |m| i64::from(m.sequence)))
    }

    /// The session's message count.
    async fn count_messages(&self, session_id: Uuid) -> StoreResult<usize> {
        Ok(self.list_messages(session_id).await?.len())
    }

    /// Hard-delete the session's messages.
    async fn delete_messages(&self, session_id: Uuid) -> StoreResult<u64>;

    /// The user's session ids, most recently active first.
    async fn list_user_sessions(&self, user_id: &str) -> StoreResult<Vec<Uuid>>;

    /// Insert or replace the context manifest for `row.operation_id`.
    async fn put_manifest(&self, row: &ManifestRow) -> StoreResult<()>;
    async fn get_manifest(&self, operation_id: &str) -> StoreResult<Option<ManifestRow>>;
    async fn list_manifests(&self, execution_id: Uuid) -> StoreResult<Vec<ManifestRow>>;
}

/// The catalog, execution and retention facets the invoke pipeline uses.
/// Implemented for any type that implements all three. Knowledge, legal
/// holds, the outbox and security decisions have their own stores.
pub trait BeamStore: CatalogStore + ExecutionStore + crate::ops::RetentionStore {}

impl<T: CatalogStore + ExecutionStore + crate::ops::RetentionStore> BeamStore for T {}

#[cfg(test)]
mod tests {
    use super::agent_id_for;

    /// Python: `fluvius.data.UUID_GENF("agent-calculator")`.
    #[test]
    fn agent_id_matches_python_uuid_genf() {
        assert_eq!(
            agent_id_for("agent-calculator").to_string(),
            "4b9616c7-b32f-5dfc-b5dd-54021301f2d7"
        );
    }
}
