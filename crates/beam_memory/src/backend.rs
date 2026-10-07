use std::collections::HashMap;
use std::sync::RwLock;

use async_trait::async_trait;
use beam_types::AgentMessage;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::scope::MemoryScope;

pub type BeamMemoryResult<T> = riverbase_core::RiverbaseResult<T>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryItem {
    pub id: String,
    pub scope: MemoryScope,
    pub kind: String,
    pub text: String,
    #[serde(default)]
    pub messages: Vec<AgentMessage>,
}

impl MemoryItem {
    #[must_use]
    pub fn turn(scope: MemoryScope, messages: Vec<AgentMessage>) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            scope,
            kind: "turn".into(),
            text: String::new(),
            messages,
        }
    }

    #[must_use]
    pub fn fact(scope: MemoryScope, kind: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            scope,
            kind: kind.into(),
            text: text.into(),
            messages: Vec::new(),
        }
    }
}

#[async_trait]
pub trait MemoryBackend: Send + Sync {
    async fn load_layer(&self, scope: &MemoryScope) -> BeamMemoryResult<Vec<MemoryItem>>;
    async fn append_layer(
        &self,
        scope: &MemoryScope,
        items: &[MemoryItem],
    ) -> BeamMemoryResult<()>;
    async fn forget(&self, id: &str) -> BeamMemoryResult<bool>;
}

pub struct InMemoryBackend {
    layers: RwLock<HashMap<String, Vec<MemoryItem>>>,
}

impl Default for InMemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryBackend {
    #[must_use]
    pub fn new() -> Self {
        Self {
            layers: RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl MemoryBackend for InMemoryBackend {
    async fn load_layer(&self, scope: &MemoryScope) -> BeamMemoryResult<Vec<MemoryItem>> {
        let guard = self
            .layers
            .read()
            .map_err(|e| crate::BEM_140.with_data(e.to_string()))?;
        Ok(guard.get(&scope.key()).cloned().unwrap_or_default())
    }

    async fn append_layer(
        &self,
        scope: &MemoryScope,
        items: &[MemoryItem],
    ) -> BeamMemoryResult<()> {
        let mut guard = self
            .layers
            .write()
            .map_err(|e| crate::BEM_141.with_data(e.to_string()))?;
        guard
            .entry(scope.key())
            .or_default()
            .extend(items.iter().cloned());
        Ok(())
    }

    async fn forget(&self, id: &str) -> BeamMemoryResult<bool> {
        let mut guard = self
            .layers
            .write()
            .map_err(|e| crate::BEM_142.with_data(e.to_string()))?;
        let mut removed = false;
        for items in guard.values_mut() {
            let before = items.len();
            items.retain(|i| i.id != id);
            if items.len() != before {
                removed = true;
            }
        }
        Ok(removed)
    }
}
