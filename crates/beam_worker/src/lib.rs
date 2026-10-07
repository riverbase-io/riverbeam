//! Worker-side task dispatch for riverbeam.
//!
//! Worker RPC handlers:
//! `invoke_agent`, `ingest_knowledge_document`, `delete_knowledge_document`,
//! `remember_memory`, `forget_memory`. Phase 2 implements `invoke_agent`;
//! the rest return `BEM-092` until their owning phases land.
//!
//! The transport binding (NATS via `riverbase_task`, or a Redis bridge for the
//! dual-run topology) is intentionally left to the host process; this crate
//! exposes the queue-name namespace and a JSON-in/JSON-out [`BeamWorker::handle_task`]
//! so any consumer loop can drive it.

mod nats;
mod worker;

pub use nats::{NatsWorkerServer, DEFAULT_CONCURRENCY, DEFAULT_TASK_PREFIX};
pub use worker::{BeamWorker, WorkerTask};
