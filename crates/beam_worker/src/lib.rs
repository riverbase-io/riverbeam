//! Worker-side task dispatch for riverbeam.
//!
//! [`BeamWorker::handle_task`] runs one named task (`invoke_agent`,
//! `remember_memory`, `activate_revision`, …) with a JSON payload.
//! [`QueueWorker`] feeds it from the durable work queue
//! (`ref_beam.agent_task`): domain commands insert rows and emit
//! hints, workers claim and run them.

mod local_bus;
mod queue;
pub mod rtc;
mod worker;

pub use beam_store::tasks::WorkerTask;
pub use local_bus::LocalBus;
pub use queue::{QueueConfig, QueueWorker};
pub use rtc::{caller_channel, rtc_stream_enabled, RtcMirrorTransport};
pub use worker::BeamWorker;
