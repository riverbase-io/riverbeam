//! Riverbase's root lookup for beam object commands (plans 006 and 013).
//!
//! Before an object command's handler runs, riverbase loads its root
//! (`/agent/my-agent:exec/...`) through the entity's `fetch`. Beam answers
//! with the identifier alone: agents are addressed by name as well as id,
//! and with the optional RLS the command's connection has no org yet at that
//! point. The handler loads (and ownership-checks) the real row itself.
//! Everything else (queries, writes) is the table's own entity.

use std::sync::Arc;

use async_trait::async_trait;
use diesel_async::AsyncPgConnection;
use riverbase_core::datastore::dsl::DataQuery;
use riverbase_core::datastore::{DataResult, ErasedEntity};
use serde_json::{json, Value};
use uuid::Uuid;

/// The resources beam object commands target.
pub const ROOT_RESOURCES: [&str; 5] = [
    "agent",
    "agent_session",
    "agent_memory",
    "agent_knowledge_document",
    "agent_profile",
];

/// A root resource's entity: `fetch` returns `{"_id": id}`, the rest is the
/// table's entity.
pub struct RootById(Arc<dyn ErasedEntity>);

/// `entity`, wrapped when it is a root resource's.
pub(super) fn wrap(entity: Arc<dyn ErasedEntity>) -> Arc<dyn ErasedEntity> {
    if ROOT_RESOURCES.contains(&entity.source()) {
        Arc::new(RootById(entity))
    } else {
        entity
    }
}

#[async_trait]
impl ErasedEntity for RootById {
    fn resources(&self) -> &[&'static str] {
        self.0.resources()
    }

    fn source(&self) -> &'static str {
        self.0.source()
    }

    fn enforces_policy_filter(&self) -> bool {
        self.0.enforces_policy_filter()
    }

    fn debug_orderable_columns(&self) -> &'static [&'static str] {
        self.0.debug_orderable_columns()
    }

    async fn query_list(
        &self,
        conn: &mut AsyncPgConnection,
        query: &DataQuery,
    ) -> DataResult<(Vec<Value>, i64)> {
        self.0.query_list(conn, query).await
    }

    async fn fetch(&self, _conn: &mut AsyncPgConnection, id: &str) -> DataResult<Option<Value>> {
        Ok(Some(json!({ "_id": id })))
    }

    async fn lock_version(
        &self,
        conn: &mut AsyncPgConnection,
        id: &str,
    ) -> DataResult<Option<Uuid>> {
        self.0.lock_version(conn, id).await
    }

    async fn upsert(&self, conn: &mut AsyncPgConnection, id: &str, data: Value) -> DataResult<()> {
        self.0.upsert(conn, id, data).await
    }

    async fn remove(&self, conn: &mut AsyncPgConnection, id: &str) -> DataResult<()> {
        self.0.remove(conn, id).await
    }

    async fn invalidate(&self, conn: &mut AsyncPgConnection, id: &str) -> DataResult<()> {
        self.0.invalidate(conn, id).await
    }
}
