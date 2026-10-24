//! Feedback on an execution (Python `evals/feedback.py`).

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Carries the execution's pinned revision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentFeedback {
    pub id: Uuid,
    pub execution_id: Uuid,
    #[serde(default)]
    pub agent_revision_id: Option<Uuid>,
    #[serde(default)]
    pub rating: Option<i32>,
    #[serde(default)]
    pub thumb: Option<String>,
    #[serde(default)]
    pub comment: Option<String>,
}
