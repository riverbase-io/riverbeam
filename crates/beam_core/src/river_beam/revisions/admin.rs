//! Listing, activating, validating, canarying and retiring revisions.

use super::spec::assert_public_schemas_compatible;
use crate::river_beam::RiverBeam;
use beam_evals::EvaluationRun;
use beam_execution::revisions::{AgentRevision, TrafficPolicy};
use riverbase_core::RiverbaseResult;
use uuid::Uuid;

impl RiverBeam {
    /// `list_revisions` (reloaded from the store first, as Python does).
    ///
    /// # Errors
    /// Store errors.
    pub async fn list_revisions(&self, agent_name: &str) -> RiverbaseResult<Vec<AgentRevision>> {
        self.reconstruct_revisions().await?;
        Ok(self.revisions.list_revisions(agent_name))
    }

    /// `activate_revision`: a revision checked `against` another keeps its
    /// public schemas, and a `validating` revision needs its last eval run to
    /// have passed. (rc8 dropped the qualification gate.)
    ///
    /// # Errors
    /// `BEM-494`, `BEM-490`, `BEM-492`.
    pub async fn activate_revision(
        &self,
        revision_id: Uuid,
        as_primary: bool,
        against: Option<Uuid>,
    ) -> RiverbaseResult<AgentRevision> {
        // Another worker may have changed the revision: act on the stored state.
        self.reconstruct_revisions().await?;
        if let Some(against) = against {
            let old = self.revisions.get_revision(against).and_then(|r| r.spec());
            let new = self
                .revisions
                .get_revision(revision_id)
                .and_then(|r| r.spec());
            if let (Some(old), Some(new)) = (old, new) {
                assert_public_schemas_compatible(&old, &new)?;
            }
        }
        let last = self.eval_store.last_run_for_revision(revision_id).await?;
        let row = self.revisions.activate(
            revision_id,
            as_primary,
            last.as_ref().map(EvaluationRun::passed),
        )?;
        self.persist_revisions().await?;
        Ok(row)
    }

    /// `mark_validating` (before an eval-gated activation).
    ///
    /// # Errors
    /// `BEM-490`, `BEM-492`.
    pub async fn mark_revision_validating(
        &self,
        revision_id: Uuid,
    ) -> RiverbaseResult<AgentRevision> {
        // Another worker may have changed the revision: act on the stored state.
        self.reconstruct_revisions().await?;
        let row = self.revisions.mark_validating(revision_id)?;
        self.persist_revisions().await?;
        Ok(row)
    }

    /// `retire_revision`.
    ///
    /// # Errors
    /// `BEM-490`, `BEM-492`.
    pub async fn retire_revision(&self, revision_id: Uuid) -> RiverbaseResult<AgentRevision> {
        // Another worker may have changed the revision: act on the stored state.
        self.reconstruct_revisions().await?;
        let row = self.revisions.retire(revision_id)?;
        self.persist_revisions().await?;
        Ok(row)
    }

    /// `set_canary`.
    ///
    /// # Errors
    /// `BEM-490`, `BEM-491`.
    pub async fn set_canary(
        &self,
        agent_name: &str,
        revision_id: Uuid,
        percent: i32,
    ) -> RiverbaseResult<TrafficPolicy> {
        // Another worker may have changed the revision: act on the stored state.
        self.reconstruct_revisions().await?;
        let policy = self
            .revisions
            .set_canary(agent_name, revision_id, percent)?;
        self.persist_revisions().await?;
        Ok(policy)
    }
}
