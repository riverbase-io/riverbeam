use serde::{Deserialize, Serialize};
use uuid::Uuid;

use riverbase_core::RiverbaseResult;

/// Catalog row for a registered agent (`agent` query resource).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AgentInfo {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub supported_modalities: Vec<String>,
    #[serde(default)]
    pub version: String,
}

/// Catalog row for an available model (`model` query resource).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ModelInfo {
    pub name: String,
    pub provider: String,
    #[serde(default)]
    pub modalities: Vec<String>,
}

/// Catalog row for a session (`agent_session` query resource).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SessionInfo {
    pub session_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

/// In-memory catalog query façade (Phase 4 swaps in SQL-backed queries).
#[derive(Debug, Default)]
pub struct Catalog {
    agents: Vec<AgentInfo>,
    models: Vec<ModelInfo>,
    sessions: Vec<SessionInfo>,
}

impl Catalog {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_agents(&mut self, agents: Vec<AgentInfo>) {
        self.agents = agents;
    }

    pub fn set_models(&mut self, models: Vec<ModelInfo>) {
        self.models = models;
    }

    pub fn upsert_session(&mut self, session: SessionInfo) {
        if let Some(existing) = self
            .sessions
            .iter_mut()
            .find(|s| s.session_id == session.session_id)
        {
            *existing = session;
        } else {
            self.sessions.push(session);
        }
    }

    #[must_use]
    pub fn list_agents(&self) -> &[AgentInfo] {
        &self.agents
    }

    #[must_use]
    pub fn list_models(&self) -> &[ModelInfo] {
        &self.models
    }

    #[must_use]
    pub fn list_sessions(&self) -> &[SessionInfo] {
        &self.sessions
    }

    pub fn get_agent(&self, name: &str) -> RiverbaseResult<&AgentInfo> {
        self.agents
            .iter()
            .find(|a| a.name == name)
            .ok_or_else(|| beam_core::BEM_023.with_data(name.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_lists_and_upserts() {
        let mut catalog = Catalog::new();
        catalog.set_agents(vec![AgentInfo {
            name: "demo".into(),
            description: "d".into(),
            supported_modalities: vec!["text".into()],
            version: "1.0.0".into(),
        }]);
        assert_eq!(catalog.list_agents().len(), 1);
        assert!(catalog.get_agent("demo").is_ok());
        assert!(catalog.get_agent("missing").is_err());

        let sid = Uuid::new_v4();
        catalog.upsert_session(SessionInfo { session_id: sid, name: None });
        catalog.upsert_session(SessionInfo {
            session_id: sid,
            name: Some("renamed".into()),
        });
        assert_eq!(catalog.list_sessions().len(), 1);
        assert_eq!(catalog.list_sessions()[0].name.as_deref(), Some("renamed"));
    }
}
