//! `RiverBeam` orchestrator — invoke pipeline entry point.

mod config;
/// Public error contract shared with Python (plan 005, D3).
pub mod public_errors;
pub(crate) use beam_types::pyjson;
mod river_beam;

pub use beam_errors::*;
pub use config::BeamConfig;
pub use river_beam::{
    assert_public_schemas_compatible, narrowed_max_steps, spec_from_registration, validate_spec,
    RiverBeam, RiverBeamBuilder,
};
pub use riverbase_core::{RiverbaseError, RiverbaseResult};

#[cfg(test)]
mod error_tests {
    #[test]
    fn catalogue_codes_are_bem_prefixed() {
        assert_eq!(super::BEM_020.errcode, "BEM-020");
        assert_eq!(super::BEM_060.http_status, 403);
        assert_eq!(super::BEM_045.http_status, 501);
    }
}
