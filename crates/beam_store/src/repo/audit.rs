//! Audit events, `db.audit()`: one `agent_event` row per event, written
//! through its entity on a unit of work of its own.

use serde_json::Value;
use uuid::Uuid;

use crate::db::{Db, DbResult};
use crate::entity::write;

/// The audit events on a [`Db`].
pub struct Audit<'d> {
    db: &'d Db,
}

impl Db {
    /// Audit events.
    #[must_use]
    pub fn audit(&self) -> Audit<'_> {
        Audit { db: self }
    }
}

impl Audit<'_> {
    /// Insert one event; `data` is keyed by physical column (see the
    /// generated `upsert_agent_event`).
    ///
    /// # Errors
    /// A failed unit of work or write (rolled back).
    pub async fn append_event(&self, data: Value) -> DbResult<()> {
        let uow = self.db.unit_of_work().await?;
        let written = write::put(
            self.db.datastore(),
            &uow,
            "agent_event",
            Uuid::new_v4(),
            data,
        )
        .await;
        write::finish(&uow, written).await?;
        Ok(())
    }
}
