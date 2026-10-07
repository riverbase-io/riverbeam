//! Persistence façade for the execution hierarchy and catalog.
//!
//! `ensure_session`, `create_interaction`/`complete_interaction`,
//! `create_run`/`complete_run`, message persistence, HITL `agent_action` rows,
//! and catalog check-in. [`BeamStore`] is the trait the invoke pipeline calls.
//!
//! [`PgBeamStore`] is the production backend: an async-Diesel store over
//! `riverbase_core`'s connection pool, targeting the `ref_beam` schema in
//! `migrations/0001_beam_schema.sql`.
//! [`InMemoryStore`] is retained only as a hermetic fixture for unit-testing the
//! invoke pipeline without a database.

mod errors;
#[allow(unused_imports)]
pub(crate) use errors::*;

mod memory;
mod pg;
mod rows;
mod store;

pub use memory::InMemoryStore;
pub use pg::PgBeamStore;
pub use rows::{
    ActionRow, ActionStatus, AgentRow, InteractionRow, InteractionStatus, MessageRow, ModelRow,
    RunRow, RunStatus, SessionRow,
};
pub use store::{agent_id_for, BeamStore, StoreResult};
