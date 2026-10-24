//! The eval store (Python `evals/stores/inmemory.py`): datasets, runs and
//! feedback. Python keeps all three in process; [`PgEvaluationStore`] keeps
//! them in Postgres: datasets and cases in
//! `agent_evaluation_dataset`/`_case`, runs and results in
//! `agent_evaluation_run`/`_result`, feedback in `agent_feedback`. So any
//! worker can run a stored dataset, and any worker can activate a revision
//! whose eval passed.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use beam_store::repo::evals::{CaseRecord, DatasetRecord, FeedbackRecord, ResultRecord, RunRecord};
use beam_store::{Db, PgBeamStore};
use riverbase_core::RiverbaseResult;
use uuid::Uuid;

use crate::dataset::{EvaluationCase, EvaluationDataset};
use crate::evaluators::EvaluationScore;
use crate::feedback::AgentFeedback;
use crate::runner::{EvaluationResult, EvaluationRun};

/// Datasets, runs and feedback.
#[async_trait]
pub trait EvaluationStore: Send + Sync {
    async fn save_dataset(&self, dataset: &EvaluationDataset) -> RiverbaseResult<()>;
    async fn get_dataset(&self, id: Uuid) -> RiverbaseResult<Option<EvaluationDataset>>;
    async fn save_run(&self, run: &EvaluationRun) -> RiverbaseResult<()>;
    async fn get_run(&self, id: Uuid) -> RiverbaseResult<Option<EvaluationRun>>;
    /// The latest run of a revision.
    async fn last_run_for_revision(
        &self,
        revision_id: Uuid,
    ) -> RiverbaseResult<Option<EvaluationRun>>;
    async fn save_feedback(&self, feedback: &AgentFeedback) -> RiverbaseResult<()>;
    async fn list_feedback(&self, execution_id: Uuid) -> RiverbaseResult<Vec<AgentFeedback>>;
}

#[derive(Default)]
struct Tables {
    datasets: HashMap<Uuid, EvaluationDataset>,
    runs: Vec<EvaluationRun>,
    feedback: HashMap<Uuid, Vec<AgentFeedback>>,
}

/// Datasets, runs and feedback in process memory.
#[derive(Default)]
pub struct InMemoryEvaluationStore {
    tables: Mutex<Tables>,
}

impl InMemoryEvaluationStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn tables(&self) -> RiverbaseResult<std::sync::MutexGuard<'_, Tables>> {
        self.tables
            .lock()
            .map_err(|e| crate::BEM_139.with_data(e.to_string()))
    }
}

#[async_trait]
impl EvaluationStore for InMemoryEvaluationStore {
    async fn save_dataset(&self, dataset: &EvaluationDataset) -> RiverbaseResult<()> {
        self.tables()?.datasets.insert(dataset.id, dataset.clone());
        Ok(())
    }

    async fn get_dataset(&self, id: Uuid) -> RiverbaseResult<Option<EvaluationDataset>> {
        Ok(self.tables()?.datasets.get(&id).cloned())
    }

    async fn save_run(&self, run: &EvaluationRun) -> RiverbaseResult<()> {
        let mut t = self.tables()?;
        t.runs.retain(|r| r.id != run.id);
        t.runs.push(run.clone());
        Ok(())
    }

    async fn get_run(&self, id: Uuid) -> RiverbaseResult<Option<EvaluationRun>> {
        Ok(self.tables()?.runs.iter().find(|r| r.id == id).cloned())
    }

    async fn last_run_for_revision(
        &self,
        revision_id: Uuid,
    ) -> RiverbaseResult<Option<EvaluationRun>> {
        Ok(self
            .tables()?
            .runs
            .iter()
            .rev()
            .find(|r| r.agent_revision_id == revision_id)
            .cloned())
    }

    async fn save_feedback(&self, feedback: &AgentFeedback) -> RiverbaseResult<()> {
        self.tables()?
            .feedback
            .entry(feedback.execution_id)
            .or_default()
            .push(feedback.clone());
        Ok(())
    }

    async fn list_feedback(&self, execution_id: Uuid) -> RiverbaseResult<Vec<AgentFeedback>> {
        Ok(self
            .tables()?
            .feedback
            .get(&execution_id)
            .cloned()
            .unwrap_or_default())
    }
}

/// Datasets, runs and feedback in Postgres (fluvius-beam keeps datasets
/// and runs in process): an adapter on `db.evals()`.
pub struct PgEvaluationStore {
    db: Db,
}

impl PgEvaluationStore {
    /// The store on `store`'s database.
    #[must_use]
    pub fn for_store(store: &PgBeamStore) -> Self {
        Self {
            db: store.db().clone(),
        }
    }
}

fn db(e: impl std::fmt::Display) -> riverbase_core::RiverbaseError {
    crate::BEM_139.with_data(e.to_string())
}

fn run_of(record: RunRecord) -> EvaluationRun {
    EvaluationRun {
        id: record.id,
        dataset_id: record.dataset_id,
        agent_revision_id: record.agent_revision_id,
        results: record
            .results
            .into_iter()
            .map(|r| EvaluationResult {
                case_id: r.case_id,
                score: EvaluationScore {
                    passed: r.passed,
                    score: r.score,
                    details: r.details,
                },
            })
            .collect(),
    }
}

#[async_trait]
impl EvaluationStore for PgEvaluationStore {
    async fn save_dataset(&self, dataset: &EvaluationDataset) -> RiverbaseResult<()> {
        let mut cases = Vec::with_capacity(dataset.cases.len());
        for case in &dataset.cases {
            cases.push(CaseRecord {
                id: case.id,
                input: serde_json::to_value(&case.input).map_err(db)?,
                expected: case.expected.clone(),
            });
        }
        self.db
            .evals()
            .save_dataset(DatasetRecord {
                id: dataset.id,
                name: dataset.name.clone(),
                cases,
            })
            .await
            .map_err(|e| e.or(crate::BEM_139))
    }

    async fn get_dataset(&self, id: Uuid) -> RiverbaseResult<Option<EvaluationDataset>> {
        let Some(record) = self
            .db
            .evals()
            .get_dataset(id)
            .await
            .map_err(|e| e.or(crate::BEM_139))?
        else {
            return Ok(None);
        };
        let mut cases = Vec::with_capacity(record.cases.len());
        for case in record.cases {
            cases.push(EvaluationCase {
                id: case.id,
                dataset_id: id,
                input: serde_json::from_value(case.input).map_err(db)?,
                expected: case.expected,
            });
        }
        Ok(Some(EvaluationDataset {
            id,
            name: record.name,
            cases,
        }))
    }

    async fn save_run(&self, run: &EvaluationRun) -> RiverbaseResult<()> {
        let record = RunRecord {
            id: run.id,
            dataset_id: run.dataset_id,
            agent_revision_id: run.agent_revision_id,
            passed: run.passed(),
            results: run
                .results
                .iter()
                .map(|r| ResultRecord {
                    case_id: r.case_id,
                    passed: r.score.passed,
                    score: r.score.score,
                    details: r.score.details.clone(),
                })
                .collect(),
        };
        self.db
            .evals()
            .save_run(record)
            .await
            .map_err(|e| e.or(crate::BEM_139))
    }

    async fn get_run(&self, id: Uuid) -> RiverbaseResult<Option<EvaluationRun>> {
        let record = self
            .db
            .evals()
            .get_run(id)
            .await
            .map_err(|e| e.or(crate::BEM_139))?;
        Ok(record.map(run_of))
    }

    async fn last_run_for_revision(
        &self,
        revision_id: Uuid,
    ) -> RiverbaseResult<Option<EvaluationRun>> {
        let record = self
            .db
            .evals()
            .last_run_for_revision(revision_id)
            .await
            .map_err(|e| e.or(crate::BEM_139))?;
        Ok(record.map(run_of))
    }

    async fn save_feedback(&self, f: &AgentFeedback) -> RiverbaseResult<()> {
        let record = FeedbackRecord {
            id: f.id,
            execution_id: f.execution_id,
            agent_revision_id: f.agent_revision_id,
            rating: f.rating,
            thumb: f.thumb.clone(),
            comment: f.comment.clone(),
        };
        // Any failure is BEM-139, the unit of work's included (as before).
        self.db.evals().save_feedback(&record).await.map_err(db)
    }

    async fn list_feedback(&self, execution_id: Uuid) -> RiverbaseResult<Vec<AgentFeedback>> {
        let rows = self
            .db
            .evals()
            .list_feedback(execution_id)
            .await
            .map_err(|e| e.or(crate::BEM_139))?;
        Ok(rows
            .into_iter()
            .map(|r| AgentFeedback {
                id: r.id,
                execution_id: r.execution_id,
                agent_revision_id: r.agent_revision_id,
                rating: r.rating,
                thumb: r.thumb,
                comment: r.comment,
            })
            .collect())
    }
}
