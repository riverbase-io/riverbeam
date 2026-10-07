//! Path-jailed workspace + working-tree git. Hosts bind `root` and `protected_refs`.

mod errors;
#[allow(unused_imports)]
pub(crate) use errors::*;

mod git;
mod jail;

pub use riverbase_core::RiverbaseResult as SandboxResult;
pub use git::WorkingTree;
pub use jail::SandboxRoot;
