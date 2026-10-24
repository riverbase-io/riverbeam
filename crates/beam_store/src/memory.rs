use std::collections::HashMap;
use std::sync::RwLock;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::rows::{
    ActionRow, ActionStatus, ActionType, AgentRow, InteractionRow, InteractionStatus, ManifestRow,
    MessageRow, ModelRow, PlanRow, RunRow, RunStatus, SessionRow, StepRow,
};
use crate::store::{CatalogStore, ExecutionStore, StoreResult};

#[derive(Default)]
struct Tables {
    agents: HashMap<Uuid, AgentRow>,
    models: HashMap<Uuid, ModelRow>,
    plans: HashMap<Uuid, PlanRow>,
    sessions: HashMap<Uuid, SessionRow>,
    interactions: HashMap<Uuid, InteractionRow>,
    runs: HashMap<Uuid, RunRow>,
    steps: HashMap<Uuid, StepRow>,
    messages: Vec<MessageRow>,
    actions: HashMap<Uuid, ActionRow>,
    manifests: Vec<ManifestRow>,
    traces: HashMap<Uuid, String>,
}

/// Hermetic, in-process store used in unit tests of the invoke pipeline.
#[derive(Default)]
pub struct InMemoryStore {
    tables: RwLock<Tables>,
}

impl InMemoryStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> StoreResult<std::sync::RwLockWriteGuard<'_, Tables>> {
        self.tables
            .write()
            .map_err(|e| crate::BEM_131.with_data(e.to_string()))
    }

    fn read(&self) -> StoreResult<std::sync::RwLockReadGuard<'_, Tables>> {
        self.tables
            .read()
            .map_err(|e| crate::BEM_132.with_data(e.to_string()))
    }
}

#[async_trait]
impl CatalogStore for InMemoryStore {
    async fn checkin_agents(&self, agents: &[AgentRow]) -> StoreResult<()> {
        let mut t = self.lock()?;
        for agent in agents {
            t.agents.insert(agent.id, agent.clone());
        }
        Ok(())
    }

    async fn checkin_models(&self, models: &[ModelRow]) -> StoreResult<()> {
        let mut t = self.lock()?;
        for model in models {
            t.models.insert(model.id, model.clone());
        }
        Ok(())
    }

    async fn checkin_plans(&self, plans: &[PlanRow]) -> StoreResult<()> {
        let mut t = self.lock()?;
        for plan in plans {
            t.plans.insert(plan.id, plan.clone());
        }
        Ok(())
    }
}

#[async_trait]
impl ExecutionStore for InMemoryStore {
    async fn ensure_session(&self, session: &SessionRow) -> StoreResult<()> {
        let mut t = self.lock()?;
        t.sessions
            .entry(session.id)
            .or_insert_with(|| session.clone());
        Ok(())
    }

    async fn update_session_activity(&self, session_id: Uuid) -> StoreResult<()> {
        let mut t = self.lock()?;
        if let Some(session) = t.sessions.get_mut(&session_id) {
            session.last_active_at = Some(Utc::now());
        }
        Ok(())
    }

    async fn create_interaction(&self, interaction: &InteractionRow) -> StoreResult<()> {
        let mut t = self.lock()?;
        t.interactions
            .entry(interaction.id)
            .or_insert_with(|| interaction.clone());
        Ok(())
    }

    async fn complete_interaction(
        &self,
        interaction_id: Uuid,
        status: InteractionStatus,
    ) -> StoreResult<()> {
        let mut t = self.lock()?;
        if let Some(row) = t.interactions.get_mut(&interaction_id) {
            row.status = status;
            row.ended_at = Some(Utc::now());
        }
        Ok(())
    }

    async fn create_run(&self, run: &RunRow) -> StoreResult<()> {
        let mut t = self.lock()?;
        t.runs.entry(run.id).or_insert_with(|| run.clone());
        Ok(())
    }

    async fn complete_run(&self, run_id: Uuid, status: RunStatus) -> StoreResult<()> {
        let mut t = self.lock()?;
        if let Some(row) = t.runs.get_mut(&run_id) {
            row.status = status;
            row.ended_at = Some(Utc::now());
        }
        Ok(())
    }

    async fn list_execution_runs(&self, execution_id: Uuid) -> StoreResult<Vec<RunRow>> {
        let mut runs: Vec<RunRow> = self
            .read()?
            .runs
            .values()
            .filter(|r| r.execution_id == Some(execution_id))
            .cloned()
            .collect();
        runs.sort_by_key(|r| (r.attempt_no, r.started_at));
        Ok(runs)
    }

    async fn save_step(&self, step: &StepRow) -> StoreResult<()> {
        let mut t = self.lock()?;
        t.steps.insert(step.id, step.clone());
        Ok(())
    }

    async fn save_message(&self, message: MessageRow) -> StoreResult<()> {
        let mut t = self.lock()?;
        t.messages.push(message);
        Ok(())
    }

    async fn append_messages(&self, session_id: Uuid, rows: Vec<MessageRow>) -> StoreResult<()> {
        let mut t = self.lock()?;
        let next = t
            .messages
            .iter()
            .filter(|m| m.session_id == session_id)
            .map(|m| m.sequence.saturating_add(1))
            .max()
            .unwrap_or(0);
        for (offset, mut row) in (0_i32..).zip(rows) {
            row.sequence = next.saturating_add(offset);
            t.messages.push(row);
        }
        Ok(())
    }

    async fn next_sequence(&self, session_id: Uuid) -> StoreResult<i32> {
        // MAX(sequence) + 1, as in Postgres and Python's `_next_sequence`.
        let t = self.read()?;
        Ok(t.messages
            .iter()
            .filter(|m| m.session_id == session_id)
            .map(|m| m.sequence.saturating_add(1))
            .max()
            .unwrap_or(0))
    }

    async fn create_action(&self, action: ActionRow) -> StoreResult<()> {
        let mut t = self.lock()?;
        t.actions.insert(action.id, action);
        Ok(())
    }

    async fn get_action(&self, action_id: Uuid) -> StoreResult<ActionRow> {
        let t = self.read()?;
        t.actions
            .get(&action_id)
            .cloned()
            .ok_or_else(|| crate::BEM_027.with_data(action_id.to_string()))
    }

    async fn expire_pending_actions(&self, execution_id: Uuid) -> StoreResult<u64> {
        let mut t = self.lock()?;
        let mut n = 0;
        for action in t.actions.values_mut() {
            if action.execution_id == Some(execution_id) && action.status == ActionStatus::Pending {
                action.status = ActionStatus::Expired;
                n += 1;
            }
        }
        Ok(n)
    }

    async fn resolve_action(&self, action_id: Uuid, result: serde_json::Value) -> StoreResult<()> {
        let mut t = self.lock()?;
        let row = t
            .actions
            .get_mut(&action_id)
            .ok_or_else(|| crate::BEM_028.with_data(action_id.to_string()))?;
        row.status = ActionStatus::Resolved;
        row.result = Some(result);
        row.resolved_at = Some(Utc::now());
        Ok(())
    }

    async fn claim_due_client_tool_actions(
        &self,
        now: DateTime<Utc>,
        until: DateTime<Utc>,
        limit: i64,
    ) -> StoreResult<Vec<ActionRow>> {
        let mut t = self.lock()?;
        let mut due: Vec<&mut ActionRow> = t
            .actions
            .values_mut()
            .filter(|a| {
                a.action_type == ActionType::ClientTool
                    && a.status == ActionStatus::Pending
                    && a.expires_at.is_some_and(|e| e <= now)
                    && a.sweep_after.is_none_or(|s| s <= now)
            })
            .collect();
        due.sort_by_key(|a| a.expires_at);
        due.truncate(usize::try_from(limit).unwrap_or(0));
        Ok(due
            .into_iter()
            .map(|a| {
                a.sweep_after = Some(until);
                a.clone()
            })
            .collect())
    }

    async fn defer_client_tool_actions(
        &self,
        action_ids: &[Uuid],
        until: DateTime<Utc>,
    ) -> StoreResult<()> {
        let mut t = self.lock()?;
        for id in action_ids {
            if let Some(action) = t.actions.get_mut(id) {
                action.sweep_after = Some(until);
            }
        }
        Ok(())
    }

    async fn list_pending_actions(&self, interaction_id: Uuid) -> StoreResult<Vec<ActionRow>> {
        Ok(self
            .read()?
            .actions
            .values()
            .filter(|a| a.interaction_id == interaction_id && a.status == ActionStatus::Pending)
            .cloned()
            .collect())
    }

    async fn get_session(&self, session_id: Uuid) -> StoreResult<Option<SessionRow>> {
        Ok(self.read()?.sessions.get(&session_id).cloned())
    }

    async fn get_interaction(&self, interaction_id: Uuid) -> StoreResult<Option<InteractionRow>> {
        Ok(self.read()?.interactions.get(&interaction_id).cloned())
    }

    async fn get_run(&self, run_id: Uuid) -> StoreResult<Option<RunRow>> {
        Ok(self.read()?.runs.get(&run_id).cloned())
    }

    async fn list_steps(&self, run_id: Uuid) -> StoreResult<Vec<StepRow>> {
        let t = self.read()?;
        Ok(t.steps
            .values()
            .filter(|s| s.run_id == run_id)
            .cloned()
            .collect())
    }

    async fn list_messages(&self, session_id: Uuid) -> StoreResult<Vec<MessageRow>> {
        let t = self.read()?;
        let mut rows: Vec<MessageRow> = t
            .messages
            .iter()
            .filter(|m| m.session_id == session_id)
            .cloned()
            .collect();
        rows.sort_by_key(|m| m.sequence);
        Ok(rows)
    }

    async fn delete_messages(&self, session_id: Uuid) -> StoreResult<u64> {
        let mut t = self.lock()?;
        let before = t.messages.len();
        t.messages.retain(|m| m.session_id != session_id);
        Ok((before - t.messages.len()) as u64)
    }

    async fn set_run_trace_id(&self, run_id: Uuid, trace_id: &str) -> StoreResult<()> {
        self.lock()?.traces.insert(run_id, trace_id.to_owned());
        Ok(())
    }

    async fn run_trace_id(&self, run_id: Uuid) -> StoreResult<Option<String>> {
        Ok(self.read()?.traces.get(&run_id).cloned())
    }

    async fn put_manifest(&self, row: &ManifestRow) -> StoreResult<()> {
        let mut t = self.lock()?;
        match t
            .manifests
            .iter_mut()
            .find(|m| m.operation_id == row.operation_id)
        {
            Some(existing) => *existing = row.clone(),
            None => t.manifests.push(row.clone()),
        }
        Ok(())
    }

    async fn get_manifest(&self, operation_id: &str) -> StoreResult<Option<ManifestRow>> {
        let t = self.read()?;
        Ok(t.manifests
            .iter()
            .find(|m| m.operation_id == operation_id)
            .cloned())
    }

    async fn list_manifests(&self, execution_id: Uuid) -> StoreResult<Vec<ManifestRow>> {
        let t = self.read()?;
        Ok(t.manifests
            .iter()
            .filter(|m| m.execution_id == Some(execution_id))
            .cloned()
            .collect())
    }

    async fn list_user_sessions(&self, user_id: &str) -> StoreResult<Vec<Uuid>> {
        let t = self.read()?;
        let mut sessions: Vec<&SessionRow> = t
            .sessions
            .values()
            .filter(|s| s.user_id.as_deref() == Some(user_id))
            .collect();
        // `last_active_at DESC NULLS LAST`.
        sessions.sort_by_key(|s| std::cmp::Reverse(s.last_active_at));
        Ok(sessions.into_iter().map(|s| s.id).collect())
    }
}

#[async_trait]
impl crate::ops::RetentionStore for InMemoryStore {
    async fn delete_user_data(&self, user_id: &str) -> StoreResult<crate::ops::DeletionCounts> {
        let mut t = self.lock()?;
        let sessions: std::collections::HashSet<Uuid> = t
            .sessions
            .values()
            .filter(|s| s.user_id.as_deref() == Some(user_id))
            .map(|s| s.id)
            .collect();
        let mut counts = crate::ops::DeletionCounts::new();
        let mut count = |table: &str, n: usize| {
            counts.insert(table.to_owned(), n as u64);
        };
        let before = t.actions.len();
        t.actions.retain(|_, a| !sessions.contains(&a.session_id));
        count("agent_action", before - t.actions.len());
        let before = t.messages.len();
        t.messages.retain(|m| !sessions.contains(&m.session_id));
        count("agent_message", before - t.messages.len());
        let before = t.steps.len();
        t.steps.retain(|_, s| !sessions.contains(&s.session_id));
        count("agent_step", before - t.steps.len());
        let before = t.runs.len();
        t.runs.retain(|_, r| !sessions.contains(&r.session_id));
        count("agent_run", before - t.runs.len());
        let before = t.interactions.len();
        t.interactions
            .retain(|_, i| !sessions.contains(&i.session_id));
        count("agent_interaction", before - t.interactions.len());
        let before = t.sessions.len();
        t.sessions.retain(|id, _| !sessions.contains(id));
        count("agent_session", before - t.sessions.len());
        Ok(counts)
    }

    async fn export_session(&self, session_id: Uuid) -> StoreResult<serde_json::Value> {
        let t = self.read()?;
        let mut messages: Vec<&MessageRow> = t
            .messages
            .iter()
            .filter(|m| m.session_id == session_id)
            .collect();
        messages.sort_by_key(|m| m.sequence);
        Ok(serde_json::json!({
            "session_id": session_id.to_string(),
            "session": t.sessions.get(&session_id).map_or(serde_json::Value::Null, |s| serde_json::to_value(s).unwrap_or_default()),
            "messages": messages.iter().map(|m| serde_json::to_value(m).unwrap_or_default()).collect::<Vec<_>>(),
            "executions": Vec::<String>::new(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures;

    #[tokio::test]
    async fn due_client_tool_actions_follow_expiry_and_sweep_after() {
        fixtures::check_due_client_tools(&InMemoryStore::new()).await;
    }

    #[tokio::test]
    async fn session_interaction_run_lifecycle() {
        let store = InMemoryStore::new();
        let ids = fixtures::Ids::new();
        store
            .ensure_session(&fixtures::session(&ids))
            .await
            .unwrap();
        store
            .create_interaction(&fixtures::interaction(&ids))
            .await
            .unwrap();
        store.create_run(&fixtures::run(&ids)).await.unwrap();
        store.save_step(&fixtures::step(&ids)).await.unwrap();
        store
            .complete_run(ids.run, RunStatus::Completed)
            .await
            .unwrap();
        store
            .complete_interaction(ids.interaction, InteractionStatus::Completed)
            .await
            .unwrap();

        assert_eq!(
            store.get_run(ids.run).await.unwrap().unwrap().status,
            RunStatus::Completed
        );
        assert_eq!(
            store
                .get_interaction(ids.interaction)
                .await
                .unwrap()
                .unwrap()
                .status,
            InteractionStatus::Completed
        );
        assert_eq!(store.list_steps(ids.run).await.unwrap().len(), 1);
    }
}
