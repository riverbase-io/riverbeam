//! Sessions: `update-session`, `remove-session`, `remove-message`. SQL on
//! the command's transaction, for the session's owner.

use riverbase_core::RiverbaseResult;
use serde_json::{json, Value};

use super::BeamAggregate;
use crate::commands::{RemoveMessagePayload, UpdateSessionPayload};
use crate::local::BeamSql;

impl BeamAggregate {
    /// `update-session`: rename the session in the URL.
    ///
    /// # Errors
    /// `BEM-404` unknown; `BEM-411` / `BEM-412` not the caller's.
    pub async fn update_session(&mut self, p: UpdateSessionPayload) -> RiverbaseResult<Value> {
        let caller = self.caller();
        let session = self.identifier();
        self.db()
            .update_session(&session, &json!({ "name": p.name }), &caller)
            .await
    }

    /// `remove-session`: soft-delete the session in the URL and purge its
    /// runtime state.
    ///
    /// # Errors
    /// `BEM-404` unknown; `BEM-411` / `BEM-412` not the caller's.
    pub async fn remove_session(&mut self) -> RiverbaseResult<Value> {
        let caller = self.caller();
        let session = self.identifier();
        self.db().remove_session(&session, &caller).await
    }

    /// `remove-message`: soft-delete one message of the session in the URL.
    ///
    /// # Errors
    /// `BEM-404` unknown session or message; `BEM-411` / `BEM-412`.
    pub async fn remove_message(&mut self, p: RemoveMessagePayload) -> RiverbaseResult<Value> {
        let caller = self.caller();
        let session = self.identifier();
        self.db()
            .remove_message(&session, &p.message_id, &caller)
            .await
    }
}
