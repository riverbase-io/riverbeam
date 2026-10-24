//! Persistence façade for the beam catalog and execution hierarchy.
//!
//! The schema is fluvius-beam's (`tests/schema/python_ref_beam.sql`, plan 005
//! D5), created by the explicit migrations in [`migrate`]. The store is split
//! into facets like Python's `BeamStoreManager`: [`CatalogStore`] and
//! [`ExecutionStore`]; [`BeamStore`] is every facet the invoke pipeline uses.
//!
//! [`PgBeamStore`] is the production backend over `riverbase_core`'s async
//! Diesel pool. [`InMemoryStore`] is a hermetic fixture for unit tests.

#[allow(clippy::wildcard_imports)] // The error catalog (`beam_errors`).
pub(crate) use beam_errors::*;

mod db;
pub mod entity;
#[cfg(test)]
mod fixtures;
mod holds;
mod memory;
pub mod migrate;
pub mod ops;
pub mod org_scope;
mod pg;
mod pg_ops;
pub mod repo;
mod rows;
mod schema;
pub mod session_summary;
mod store;
pub mod tasks;
#[cfg(any(test, feature = "testing"))]
pub mod testdb;
pub mod tool_policy;

pub use db::{Checkout, Db, DbError, DbResult, Site};
pub use memory::InMemoryStore;
pub use ops::{InMemoryOps, LegalHoldStore, LegalHolds, OpsStore, PgLegalHolds, RetentionStore};
pub use pg::PgBeamStore;
pub use pg_ops::PgOpsStore;
pub use repo::tasks::Tasks;
pub use rows::{
    ActionRow, ActionStatus, ActionType, AgentRow, AgentStatus, DbEnum, InteractionRow,
    InteractionStatus, ManifestRow, MessagePhase, MessageRole, MessageRow, MessageType, ModelRow,
    ModelStatus, PlanRow, RunRow, RunStatus, SessionRow, StepRow, StepStatus,
};
pub use schema::{SchemaName, DEFAULT_SCHEMA, SCHEMA_ENV};
pub use store::{agent_id_for, BeamStore, CatalogStore, ExecutionStore, MessagePage, StoreResult};
pub use tool_policy::{PgToolPolicy, ToolGrantStore, ToolGrants, ToolRuleStore, ToolRules};
