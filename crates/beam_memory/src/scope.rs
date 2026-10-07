use serde::{Deserialize, Serialize};

/// Named memory layer. Hosts bind names such as `system`, `organization`, `docset`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub struct MemoryScope {
    pub name: String,
    #[serde(default)]
    pub id: String,
}

impl MemoryScope {
    #[must_use]
    pub fn new(name: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            id: id.into(),
        }
    }

    #[must_use]
    pub fn system() -> Self {
        Self::new("system", "")
    }

    #[must_use]
    pub fn session(id: impl Into<String>) -> Self {
        Self::new("session", id)
    }

    #[must_use]
    pub fn key(&self) -> String {
        if self.id.is_empty() {
            self.name.clone()
        } else {
            format!("{}:{}", self.name, self.id)
        }
    }
}
