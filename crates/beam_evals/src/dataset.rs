//! Evaluation cases and datasets (Python `evals/dataset.py`).

use beam_types::AgentInput;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

/// An input and what the answer should show.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvaluationCase {
    pub id: Uuid,
    pub dataset_id: Uuid,
    pub input: AgentInput,
    /// `text`, `json`, `sources`, `tools` (see the evaluators).
    #[serde(default)]
    pub expected: Option<Value>,
}

/// Named cases an agent revision is evaluated on.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvaluationDataset {
    pub id: Uuid,
    pub name: String,
    #[serde(default)]
    pub cases: Vec<EvaluationCase>,
}

impl EvaluationDataset {
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            id: Uuid::new_v4(),
            name: name.into(),
            cases: Vec::new(),
        }
    }

    /// Add a case for `input` expecting `expected`.
    pub fn push(&mut self, input: AgentInput, expected: Option<Value>) -> Uuid {
        let id = Uuid::new_v4();
        self.cases.push(EvaluationCase {
            id,
            dataset_id: self.id,
            input,
            expected,
        });
        id
    }
}

/// Two exact-match cases expecting `ok`.
#[must_use]
pub fn fixture_dataset(name: &str) -> EvaluationDataset {
    let mut dataset = EvaluationDataset::new(name);
    dataset.push(AgentInput::text("hello"), Some(json!({"text": "ok"})));
    dataset.push(AgentInput::text("again"), Some(json!({"text": "ok"})));
    dataset
}
