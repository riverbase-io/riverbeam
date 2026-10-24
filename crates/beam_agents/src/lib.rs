//! The agent registry: every agent's metadata. Agents run on a runtime
//! (`beam_runtime`); the registry answers what exists and what it accepts
//! (plan 021 D3).

#[allow(clippy::wildcard_imports)] // The error catalog (`beam_errors`).
pub(crate) use beam_errors::*;

mod registry;

pub use registry::{human_message_from_input, AgentRegistry};
