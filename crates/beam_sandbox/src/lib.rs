//! Path-jailed workspace + working-tree git. Hosts bind `root` and `protected_refs`.

#[allow(clippy::wildcard_imports)] // The error catalog (`beam_errors`).
pub(crate) use beam_errors::*;

mod git;
mod jail;

pub use git::WorkingTree;
pub use jail::SandboxRoot;
pub use riverbase_core::RiverbaseResult as SandboxResult;
