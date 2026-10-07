//! Model routing for riverbeam.

mod errors;
#[allow(unused_imports)]
pub(crate) use errors::*;

mod router;
mod strategies;
mod yaml;

pub use router::ModelRouter;
pub use strategies::{
    CapabilityStrategy, CostAwareStrategy, FallbackStrategy, ModalityStrategy, RoutingStrategy,
};
pub use yaml::load_models_from_yaml;
