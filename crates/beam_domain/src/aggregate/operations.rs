//! Operations: `reconcile-now`, `retry-dead-letter`, `discard-dead-letter`,
//! `delete-user-agent-data`, `delete-my-agent-data`, `place-legal-hold`,
//! `release-legal-hold`. The first five queue a worker task; the holds are
//! written on the command's transaction. The Casbin policy keeps them to
//! `beam-admin`, except `delete-my-agent-data` (a `beam-user`, on their own
//! data; rc8, plan 023 D9).

use beam_store::tasks::NewTask;
use riverbase_core::RiverbaseResult;
use serde_json::{json, Value};

use super::{queued, BeamAggregate};
use crate::commands::{
    DeleteUserAgentDataPayload, DiscardDeadLetterPayload, PlaceLegalHoldPayload,
    ReleaseLegalHoldPayload, RetryDeadLetterPayload,
};
use beam_store::repo::domain::DomainSql;

use crate::local::sql;

impl BeamAggregate {
    /// `reconcile-now`: reconcile executions and the outbox.
    ///
    /// # Errors
    /// Store errors.
    pub async fn reconcile_now(&mut self) -> RiverbaseResult<Value> {
        let caller = self.caller();
        let task_id = self
            .enqueue(NewTask::new(
                "reconcile_now",
                json!({ "org_id": caller.org_id }),
            ))
            .await?;
        Ok(queued(task_id, "reconcile_now"))
    }

    /// `retry-dead-letter`: run a dead letter's task again. Not retried on
    /// its own: it may run an agent.
    ///
    /// # Errors
    /// Store errors.
    pub async fn retry_dead_letter(&mut self, p: RetryDeadLetterPayload) -> RiverbaseResult<Value> {
        let caller = self.caller();
        let task = NewTask {
            retryable: false,
            max_attempts: 1,
            ..NewTask::new(
                "retry_dead_letter",
                json!({ "letter_id": p.letter_id, "org_id": caller.org_id }),
            )
        };
        let task_id = self.enqueue(task).await?;
        Ok(queued(task_id, "retry_dead_letter"))
    }

    /// `discard-dead-letter`.
    ///
    /// # Errors
    /// Store errors.
    pub async fn discard_dead_letter(
        &mut self,
        p: DiscardDeadLetterPayload,
    ) -> RiverbaseResult<Value> {
        let caller = self.caller();
        let task_id = self
            .enqueue(NewTask::new(
                "discard_dead_letter",
                json!({ "letter_id": p.letter_id, "org_id": caller.org_id }),
            ))
            .await?;
        Ok(queued(task_id, "discard_dead_letter"))
    }

    /// `delete-user-agent-data`: a held user is refused before anything
    /// goes; otherwise the user's queued work goes at once (it
    /// holds their requests) and a worker task deletes the rest. The worker
    /// checks the hold again.
    ///
    /// # Errors
    /// `WEB-001` for a blank user; `BEM-489` (public `BEM-433`) for a held
    /// user; store errors.
    pub async fn delete_user_agent_data(
        &mut self,
        p: DeleteUserAgentDataPayload,
    ) -> RiverbaseResult<Value> {
        let user = p.user_id.trim().to_owned();
        if user.is_empty() {
            return Err(beam_core::BEM_003.with_data("user_id is required"));
        }
        self.delete_agent_data(user).await
    }

    /// `delete-my-agent-data`: [`Self::delete_user_agent_data`] for the
    /// caller (rc8). The user always comes from the caller's identity, and a
    /// held caller is refused the same way.
    ///
    /// # Errors
    /// `WEB-001` without a user identity; `BEM-489` (public `BEM-433`) for a
    /// held caller; store errors.
    pub async fn delete_my_agent_data(&mut self) -> RiverbaseResult<Value> {
        let Some(user) = self.caller().user_id.filter(|u| !u.trim().is_empty()) else {
            return Err(beam_core::BEM_003.with_data("the caller has no user id"));
        };
        self.delete_agent_data(user).await
    }

    /// Refuse a held `user`, purge their queued work, and queue the worker's
    /// deletion of the rest.
    async fn delete_agent_data(&mut self, user: String) -> RiverbaseResult<Value> {
        // The hold lock comes before the purge, so a hold placed by another
        // command either committed already or waits for this one.
        if self.db().lock_hold(&user).await.map_err(sql)? {
            return Err(beam_core::BEM_489.with_data(json!({ "user_id": user })));
        }
        let purged = self.db().purge_user_tasks(&user).await.map_err(sql)?;
        let org = self.caller().org_id;
        let task_id = self
            .enqueue(NewTask::new(
                "delete_user_agent_data",
                json!({ "user_id": user, "org_id": org }),
            ))
            .await?;
        let mut out = queued(task_id, "delete_user_agent_data");
        out["purged_tasks"] = json!(purged);
        Ok(out)
    }

    /// `place-legal-hold`: hold `user_id`, so no process deletes their data
    /// until the hold is released. Placing it again replaces the
    /// reason.
    ///
    /// # Errors
    /// `WEB-001` for a blank user; store errors.
    pub async fn place_legal_hold(&mut self, p: PlaceLegalHoldPayload) -> RiverbaseResult<Value> {
        let user = p.user_id.trim().to_owned();
        if user.is_empty() {
            return Err(beam_core::BEM_003.with_data("user_id is required"));
        }
        let caller = self.caller();
        let org = caller
            .org_id
            .as_deref()
            .and_then(|o| uuid::Uuid::parse_str(o.trim()).ok());
        let reason = p.reason.as_deref().map(str::trim).filter(|r| !r.is_empty());
        self.db()
            .place_hold(&user, org, reason, caller.user_id.as_deref())
            .await
            .map_err(sql)?;
        Ok(json!({ "user_id": user, "held": true }))
    }

    /// `release-legal-hold`: release `user_id`'s hold. Releasing a user who
    /// is not held succeeds with `released: false`.
    ///
    /// # Errors
    /// `WEB-001` for a blank user; store errors.
    pub async fn release_legal_hold(
        &mut self,
        p: ReleaseLegalHoldPayload,
    ) -> RiverbaseResult<Value> {
        let user = p.user_id.trim().to_owned();
        if user.is_empty() {
            return Err(beam_core::BEM_003.with_data("user_id is required"));
        }
        let released = self.db().release_hold(&user).await.map_err(sql)?;
        Ok(json!({ "user_id": user, "held": false, "released": released }))
    }
}
