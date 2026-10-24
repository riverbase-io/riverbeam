//! Plan 019: the production check refuses `BEAM_TRACE_PRIVACY=full_debug`.
//! Its own test binary, since the privacy mode is process-wide.

use beam_core::{BeamConfig, RiverBeam};
use beam_telemetry::PrivacyMode;

#[test]
fn full_debug_privacy_is_refused_in_production() {
    beam_telemetry::set_privacy_mode(Some(PrivacyMode::FullDebug));
    let beam = RiverBeam::new(BeamConfig::default());
    let err = beam.validate_production().unwrap_err();
    beam_telemetry::set_privacy_mode(None);
    let text = format!("{err} {}", err.errdata);
    assert!(text.contains("BEAM_TRACE_PRIVACY=full_debug"), "{text}");
}
