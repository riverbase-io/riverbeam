//! The eval store, feedback and evaluation runs.

use super::spec::request_from;
use crate::river_beam::RiverBeam;
use beam_evals::{AgentFeedback, EvalRunner, EvaluationRun, EvaluationStore};
use beam_types::{AgentInput, AgentResult};
use riverbase_core::RiverbaseResult;
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

impl RiverBeam {
    /// The eval store (datasets, runs, feedback).
    #[must_use]
    pub fn eval_store(&self) -> &Arc<dyn EvaluationStore> {
        &self.eval_store
    }

    /// `submit_feedback`: the execution must exist; the feedback carries its
    /// pinned revision.
    ///
    /// # Errors
    /// `BEM-496` (public `BEM-404`) for an unknown execution.
    pub async fn submit_feedback(
        &self,
        execution_id: Uuid,
        thumb: Option<String>,
        rating: Option<i32>,
        comment: Option<String>,
    ) -> RiverbaseResult<AgentFeedback> {
        let execution = self.executions.records().get(execution_id).await?;
        let pinned = self.revisions.snapshot_for_execution(execution_id);
        if execution.is_none() && pinned.is_none() {
            return Err(crate::BEM_496.with_data(json!({"execution_id": execution_id.to_string()})));
        }
        let feedback = AgentFeedback {
            id: Uuid::new_v4(),
            execution_id,
            agent_revision_id: execution
                .and_then(|e| e.agent_revision_id)
                .or_else(|| pinned.map(|p| p.id)),
            rating,
            thumb,
            comment,
        };
        self.eval_store.save_feedback(&feedback).await?;
        Ok(feedback)
    }

    /// `run_evaluation`: run a stored dataset through this beam (tools in
    /// dry run) and record it against `agent_revision_id`. A case without an
    /// agent runs on the revision's agent, and a new execution of that agent
    /// runs on `agent_revision_id` whatever the traffic (a draft is
    /// evaluated without a canary). A case naming another agent, or resuming
    /// an execution, follows that agent's traffic or the execution's pin.
    ///
    /// # Errors
    /// `BEM-494` for an unknown dataset; `BEM-490`; invoke errors.
    pub async fn run_evaluation(
        self: &Arc<Self>,
        dataset_id: Uuid,
        agent_revision_id: Uuid,
    ) -> RiverbaseResult<EvaluationRun> {
        let agent = self
            .revisions
            .get_revision(agent_revision_id)
            .map(|r| r.snapshot.agent_name)
            .ok_or_else(|| {
                crate::BEM_490.with_data(format!("unknown revision {agent_revision_id}"))
            })?;
        let beam = Arc::clone(self);
        let invoke: beam_evals::runner::InvokeFn = Arc::new(move |input: AgentInput| {
            let beam = Arc::clone(&beam);
            let request = request_from(input, &agent);
            let revision = (request.agent_name == agent).then_some(agent_revision_id);
            Box::pin(async move { beam.invoke_on(request, revision).await })
                as futures_util::future::BoxFuture<'static, RiverbaseResult<AgentResult>>
        });
        EvalRunner::new(invoke, Some(Arc::clone(&self.eval_store)))
            .run_dataset(dataset_id, agent_revision_id)
            .await
    }
}
