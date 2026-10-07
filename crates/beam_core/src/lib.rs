//! RiverBeam orchestrator — invoke pipeline entry point.

mod config;
/// Riverbase `BEM-*` error catalogue.
mod errors;
mod river_beam;

pub use config::BeamConfig;
pub use errors::*;
pub use riverbase_core::{RiverbaseError, RiverbaseResult};
pub use river_beam::RiverBeam;

#[cfg(test)]
mod error_tests {
    #[test]
    fn catalogue_codes_are_bem_prefixed() {
        assert_eq!(super::BEM_020.errcode, "BEM-020");
        assert_eq!(super::BEM_060.http_status, 403);
        assert_eq!(super::BEM_045.http_status, 501);
    }
}
