//! Typed repositories: every statement beam runs, grouped by area and
//! reached from the gateway ([`crate::Db`]) as `db.<area>()` (plan 025).
//! Callers outside `beam_store` use these methods; they never see a
//! connection, a transaction or SQL.

pub mod audit;
pub mod domain;
pub mod evals;
pub mod executions;
pub mod knowledge;
pub mod revisions;
pub mod rows;
pub mod runtime_state;
pub mod security;
pub mod tasks;
pub mod tool_idempotency;
pub mod usage;
pub mod vector_json;
