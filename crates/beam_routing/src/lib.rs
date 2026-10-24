//! Model routing for riverbeam.

#[allow(clippy::wildcard_imports)] // The error catalog (`beam_errors`).
pub(crate) use beam_errors::*;

pub mod aliases;
mod router;
mod strategies;
mod yaml;

pub use aliases::ModelAliasMap;
pub use router::ModelRouter;
pub use strategies::{
    CapabilityStrategy, CostAwareStrategy, FallbackStrategy, ModalityStrategy, RoutingStrategy,
};
pub use yaml::{load_aliases_from_yaml, load_models_from_yaml};
