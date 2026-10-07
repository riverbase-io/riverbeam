use std::collections::HashMap;
use std::sync::RwLock;

use async_trait::async_trait;
use chrono::Utc;
use uuid::Uuid;

use crate::rows::{
    ActionRow, ActionStatus, AgentRow, InteractionRow, InteractionStatus, MessageRow, ModelRow,
    RunRow, RunStatus, SessionRow,
};
use crate::store::{BeamStore, StoreResult};

#[derive(Default)]
struct Tables {
    agents: HashMap<Uuid, AgentRow>,
    models: HashMap<Uuid, ModelRow>,
    sessions: HashMap<Uuid, SessionRow>,
    interactions: HashMap<Uuid, InteractionRow>,
    runs: HashMap<Uuid, RunRow>,
    messages: Vec<MessageRow>,
    actions: HashMap<Uuid, ActionRow>,
}

/// Hermetic, in-process [`BeamStore`] used by default and in tests.
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
impl BeamStore for InMemoryStore {
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

    async fn ensure_session(
        &self,
        session_id: Uuid,
        agent_id: Uuid,
        user_id: Option<&str>,
        profile_id: Option<&str>,
        name: Option<&str>,
    ) -> StoreResult<()> {
        let mut t = self.lock()?;
        t.sessions.entry(session_id).or_insert_with(|| SessionRow {
            id: session_id,
            agent_id,
            user_id: user_id.map(str::to_string),
            profile_id: profile_id.map(str::to_string),
            name: name.map(str::to_string),
            last_active_at: None,
        });
        Ok(())
    }

    async fn update_session_activity(&self, session_id: Uuid) -> StoreResult<()> {
        let mut t = self.lock()?;
        if let Some(session) = t.sessions.get_mut(&session_id) {
            session.last_active_at = Some(Utc::now());
        }
        Ok(())
    }

    async fn create_interaction(
        &self,
        interaction_id: Uuid,
        session_id: Uuid,
        agent_id: Uuid,
        agent_name: &str,
    ) -> StoreResult<()> {
        let mut t = self.lock()?;
        t.interactions.insert(
            interaction_id,
            InteractionRow {
                id: interaction_id,
                session_id,
                agent_id,
                agent_name: agent_name.to_string(),
                status: InteractionStatus::InProgress,
                started_at: Utc::now(),
                ended_at: None,
            },
        );
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

    async fn create_run(
        &self,
        run_id: Uuid,
        session_id: Uuid,
        interaction_id: Uuid,
        agent_id: Uuid,
        agent_name: &str,
    ) -> StoreResult<()> {
        let mut t = self.lock()?;
        t.runs.insert(
            run_id,
            RunRow {
                id: run_id,
                session_id,
                interaction_id,
                agent_id,
                agent_name: agent_name.to_string(),
                status: RunStatus::Running,
                started_at: Utc::now(),
                ended_at: None,
            },
        );
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

    async fn save_message(&self, message: MessageRow) -> StoreResult<()> {
        let mut t = self.lock()?;
        t.messages.push(message);
        Ok(())
    }

    async fn next_sequence(&self, session_id: Uuid) -> StoreResult<i32> {
        let t = self.read()?;
        let count = t
            .messages
            .iter()
            .filter(|m| m.session_id == session_id)
            .count();
        Ok(i32::try_from(count).unwrap_or(i32::MAX))
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

    async fn resolve_action(
        &self,
        action_id: Uuid,
        result: serde_json::Value,
    ) -> StoreResult<()> {
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

    async fn get_session(&self, session_id: Uuid) -> StoreResult<Option<SessionRow>> {
        Ok(self.read()?.sessions.get(&session_id).cloned())
    }

    async fn get_interaction(&self, interaction_id: Uuid) -> StoreResult<Option<InteractionRow>> {
        Ok(self.read()?.interactions.get(&interaction_id).cloned())
    }

    async fn get_run(&self, run_id: Uuid) -> StoreResult<Option<RunRow>> {
        Ok(self.read()?.runs.get(&run_id).cloned())
    }

    async fn list_messages(&self, session_id: Uuid) -> StoreResult<Vec<MessageRow>> {
        let t = self.read()?;
        Ok(t.messages
            .iter()
            .filter(|m| m.session_id == session_id)
            .cloned()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn session_interaction_run_lifecycle() {
        let store = InMemoryStore::new();
        let session = Uuid::new_v4();
        let interaction = Uuid::new_v4();
        let run = Uuid::new_v4();
        let agent = Uuid::new_v4();

        store
            .ensure_session(session, agent, Some("u1"), None, None)
            .await
            .unwrap();
        store
            .create_interaction(interaction, session, agent, "demo")
            .await
            .unwrap();
        store
            .create_run(run, session, interaction, agent, "demo")
            .await
            .unwrap();
        store.complete_run(run, RunStatus::Completed).await.unwrap();
        store
            .complete_interaction(interaction, InteractionStatus::Completed)
            .await
            .unwrap();

        assert_eq!(
            store.get_run(run).await.unwrap().unwrap().status,
            RunStatus::Completed
        );
        assert_eq!(
            store
                .get_interaction(interaction)
                .await
                .unwrap()
                .unwrap()
                .status,
            InteractionStatus::Completed
        );
    }
}
