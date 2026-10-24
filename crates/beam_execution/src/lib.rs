//! Execution control plane for riverbeam.
//!
//! A port of fluvius-beam's `execution/` package: durable execution identity
//! (`agent_execution`), idempotent start-or-get, one active start per
//! concurrency key, run attempts with a lease, heartbeat and fence, cancel,
//! timeout, retry, and instance admission and drain. [`PgExecutionRecords`]
//! shares the beam schema with Python and takes the same advisory locks.

#[allow(clippy::wildcard_imports)] // The error catalog (`beam_errors`).
pub(crate) use beam_errors::*;

pub mod concurrency;
mod controller;
pub mod idempotency;
pub mod identity;
mod memory;
mod pg;
mod pg_revisions;
mod records;
pub mod retry;
pub mod revisions;
mod status;

pub use controller::{
    Admission, EnsureRequest, Ensured, ExecutionConfig, ExecutionController, StopReason,
};
pub use memory::InMemoryExecutionRecords;
pub use pg::PgExecutionRecords;
pub use pg_revisions::PgRevisionStore;
pub use records::{ExecResult, ExecutionRecords, ExecutionRow, Lease, NewExecution, RevisionPin};
pub use status::{transition, ExecutionStatus};
