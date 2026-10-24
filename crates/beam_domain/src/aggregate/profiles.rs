//! Agent profiles: `create-agent-profile`, `update-agent-profile`,
//! `remove-agent-profile`. SQL on the command's transaction; a profile is
//! the caller's own.

use riverbase_core::RiverbaseResult;
use serde_json::{json, Value};

use super::BeamAggregate;
use crate::commands::{CreateAgentProfilePayload, UpdateAgentProfilePayload};
use crate::local::BeamSql;

impl BeamAggregate {
    /// `create-agent-profile`: the caller's profile (the payload may repeat
    /// the caller's ids, never name another's).
    ///
    /// # Errors
    /// `BEM-412` another user's or profile's ids; `BEM-400` a duplicate or a
    /// `profile_id` that is not a UUID; `BEM-410` no identity.
    pub async fn create_agent_profile(
        &mut self,
        p: CreateAgentProfilePayload,
    ) -> RiverbaseResult<Value> {
        let caller = self.caller();
        let own_profile = self.context().actor.profile_id.map(|id| id.to_string());
        let fields = json!({
            "user_id": p.user_id,
            "profile_id": p.profile_id,
            "role": p.role,
            "rules": p.rules,
            "metadata_json": p.metadata_json,
        });
        self.db()
            .create_profile(&fields, &caller, own_profile.as_deref())
            .await
    }

    /// `update-agent-profile`: the given fields of the profile in the URL.
    ///
    /// # Errors
    /// `BEM-400` nothing to update; `BEM-404` unknown; `BEM-412` not the caller's.
    pub async fn update_agent_profile(
        &mut self,
        p: UpdateAgentProfilePayload,
    ) -> RiverbaseResult<Value> {
        let caller = self.caller();
        let profile = self.identifier();
        let fields = json!({ "role": p.role, "rules": p.rules, "metadata_json": p.metadata_json });
        self.db().update_profile(&profile, &fields, &caller).await
    }

    /// `remove-agent-profile`: soft-delete the profile in the URL.
    ///
    /// # Errors
    /// `BEM-404` unknown; `BEM-412` not the caller's.
    pub async fn remove_agent_profile(&mut self) -> RiverbaseResult<Value> {
        let caller = self.caller();
        let profile = self.identifier();
        self.db().remove_profile(&profile, &caller).await
    }
}
