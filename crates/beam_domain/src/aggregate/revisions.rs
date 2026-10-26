//! Revisions, evals and feedback: `submit-agent-feedback`,
//! `run-evaluation`, `mark-revision-validating`, `activate-revision`,
//! `retire-revision`.
//! Each queues a worker task: revisions live in every worker's revision
//! book, which a worker reloads after one changes.

use beam_store::tasks::{NewTask, WorkerTask};
use riverbase_core::RiverbaseResult;
use serde_json::{json, Value};

use super::{queued, BeamAggregate};
use crate::commands::{
    ActivateRevisionPayload, MarkRevisionValidatingPayload, RetireRevisionPayload,
    RunEvaluationPayload, SubmitAgentFeedbackPayload,
};
use crate::local::BeamSql;

impl BeamAggregate {
    /// `submit-agent-feedback`: the caller's feedback on their own run.
    ///
    /// # Errors
    /// `BEM-404` unknown execution; `BEM-411` / `BEM-412` not the caller's.
    pub async fn submit_agent_feedback(
        &mut self,
        p: SubmitAgentFeedbackPayload,
    ) -> RiverbaseResult<Value> {
        let caller = self.caller();
        let execution = p.execution_id.to_string();
        self.db().writable_execution(&execution, &caller).await?;
        let task = NewTask {
            execution_id: Some(p.execution_id),
            ..NewTask::of(
                WorkerTask::SubmitAgentFeedback,
                json!({
                    "execution_id": execution,
                    "thumb": p.thumb,
                    "rating": p.rating,
                    "comment": p.comment,
                    "org_id": caller.org_id,
                }),
            )
        };
        let task_id = self.enqueue(task).await?;
        Ok(queued(task_id, "submit_agent_feedback"))
    }

    /// `run-evaluation`: run a dataset against a revision. Not retried: a
    /// failed evaluation is its result.
    ///
    /// # Errors
    /// Store errors.
    pub async fn run_evaluation(&mut self, p: RunEvaluationPayload) -> RiverbaseResult<Value> {
        let caller = self.caller();
        let task = NewTask {
            retryable: false,
            max_attempts: 1,
            ..NewTask::of(
                WorkerTask::RunEvaluation,
                json!({ "dataset_id": p.dataset_id, "agent_revision_id": p.agent_revision_id, "org_id": caller.org_id }),
            )
        };
        let task_id = self.enqueue(task).await?;
        Ok(queued(task_id, "run_evaluation"))
    }

    /// `mark-revision-validating`: gate the revision's activation on its
    /// last eval run passing.
    ///
    /// # Errors
    /// Store errors.
    pub async fn mark_revision_validating(
        &mut self,
        p: MarkRevisionValidatingPayload,
    ) -> RiverbaseResult<Value> {
        let caller = self.caller();
        let task = NewTask::of(
            WorkerTask::MarkRevisionValidating,
            json!({ "revision_id": p.revision_id, "org_id": caller.org_id }),
        );
        let task_id = self.enqueue(task).await?;
        Ok(queued(task_id, "mark_revision_validating"))
    }

    /// `activate-revision`.
    ///
    /// # Errors
    /// Store errors.
    pub async fn activate_revision(
        &mut self,
        p: ActivateRevisionPayload,
    ) -> RiverbaseResult<Value> {
        let caller = self.caller();
        let task = NewTask::of(
            WorkerTask::ActivateRevision,
            json!({ "revision_id": p.revision_id, "org_id": caller.org_id }),
        );
        let task_id = self.enqueue(task).await?;
        Ok(queued(task_id, "activate_revision"))
    }

    /// `retire-revision`.
    ///
    /// # Errors
    /// Store errors.
    pub async fn retire_revision(&mut self, p: RetireRevisionPayload) -> RiverbaseResult<Value> {
        let caller = self.caller();
        let task = NewTask::of(
            WorkerTask::RetireRevision,
            json!({ "revision_id": p.revision_id, "org_id": caller.org_id }),
        );
        let task_id = self.enqueue(task).await?;
        Ok(queued(task_id, "retire_revision"))
    }
}
