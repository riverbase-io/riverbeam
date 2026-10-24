//! Run a dataset against a revision and compare runs (Python `evals/runner.py`).

use std::sync::Arc;

use beam_types::{AgentInput, AgentResult};
use futures_util::future::BoxFuture;
use riverbase_core::RiverbaseResult;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::dataset::EvaluationDataset;
use crate::evaluators::{EvaluationScore, Evaluator, ExactMatchEvaluator};
use crate::store::EvaluationStore;

/// One case's score in a run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvaluationResult {
    pub case_id: Uuid,
    pub score: EvaluationScore,
}

/// A dataset run against one agent revision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvaluationRun {
    pub id: Uuid,
    pub dataset_id: Uuid,
    pub agent_revision_id: Uuid,
    #[serde(default)]
    pub results: Vec<EvaluationResult>,
}

impl EvaluationRun {
    /// At least one result, and all passed.
    #[must_use]
    pub fn passed(&self) -> bool {
        !self.results.is_empty() && self.results.iter().all(|r| r.score.passed)
    }

    #[allow(clippy::cast_precision_loss)] // Case counts, far below 2^52.
    fn pass_rate(&self) -> f64 {
        if self.results.is_empty() {
            return 0.0;
        }
        let passed = self.results.iter().filter(|r| r.score.passed).count();
        passed as f64 / self.results.len() as f64
    }
}

/// Pass-rate change from `run_a` to `run_b`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ComparisonReport {
    pub delta: f64,
    pub run_a: Uuid,
    pub run_b: Uuid,
}

/// The invoke a run drives (the beam's `invoke_result`).
pub type InvokeFn =
    Arc<dyn Fn(AgentInput) -> BoxFuture<'static, RiverbaseResult<AgentResult>> + Send + Sync>;

/// Every case runs with tools in dry run.
pub struct EvalRunner {
    invoke: InvokeFn,
    store: Option<Arc<dyn EvaluationStore>>,
    evaluator: Box<dyn Evaluator>,
}

impl EvalRunner {
    /// A runner scoring with exact match (Python's default).
    #[must_use]
    pub fn new(invoke: InvokeFn, store: Option<Arc<dyn EvaluationStore>>) -> Self {
        Self {
            invoke,
            store,
            evaluator: Box::new(ExactMatchEvaluator),
        }
    }

    #[must_use]
    pub fn with_evaluator(mut self, evaluator: Box<dyn Evaluator>) -> Self {
        self.evaluator = evaluator;
        self
    }

    /// Invoke each case (dry run), score it, save the run.
    ///
    /// # Errors
    /// The invoke's error; store errors.
    pub async fn run(
        &self,
        dataset: &EvaluationDataset,
        agent_revision_id: Uuid,
    ) -> RiverbaseResult<EvaluationRun> {
        let mut run = EvaluationRun {
            id: Uuid::new_v4(),
            dataset_id: dataset.id,
            agent_revision_id,
            results: Vec::new(),
        };
        for case in &dataset.cases {
            let mut input = case.input.clone();
            // Python `invocation_dry_run(True)`.
            if !input.metadata.is_object() {
                input.metadata = Value::Object(serde_json::Map::new());
            }
            if let Value::Object(map) = &mut input.metadata {
                map.insert("dry_run".into(), Value::Bool(true));
            }
            let result = (self.invoke)(input).await?;
            let score = self.evaluator.evaluate(case, &result).await;
            run.results.push(EvaluationResult {
                case_id: case.id,
                score,
            });
        }
        if let Some(store) = &self.store {
            store.save_run(&run).await?;
        }
        Ok(run)
    }

    /// Python `run_dataset`.
    ///
    /// # Errors
    /// `BEM-494` without a store or for an unknown dataset; [`Self::run`]'s.
    pub async fn run_dataset(
        &self,
        dataset_id: Uuid,
        agent_revision_id: Uuid,
    ) -> RiverbaseResult<EvaluationRun> {
        let store = self.store.as_ref().ok_or_else(|| {
            crate::BEM_494.with_data("EvalRunner store is required for run_dataset")
        })?;
        let dataset = store
            .get_dataset(dataset_id)
            .await?
            .ok_or_else(|| crate::BEM_494.with_data(format!("unknown dataset {dataset_id}")))?;
        self.run(&dataset, agent_revision_id).await
    }

    /// Python `compare`.
    #[must_use]
    pub fn compare(run_a: &EvaluationRun, run_b: &EvaluationRun) -> ComparisonReport {
        ComparisonReport {
            delta: run_b.pass_rate() - run_a.pass_rate(),
            run_a: run_a.id,
            run_b: run_b.id,
        }
    }
}
