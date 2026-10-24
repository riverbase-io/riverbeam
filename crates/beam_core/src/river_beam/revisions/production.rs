//! The production profile's checks (Python `validate_production`).

use super::spec::invalid;
use crate::river_beam::RiverBeam;
use riverbase_core::RiverbaseResult;

impl RiverBeam {
    /// Python `validate_production_settings` (`BEAM_PROFILE=production`):
    /// refuse the development fallbacks.
    ///
    /// # Errors
    /// `BEM-494` naming every problem.
    pub fn validate_production(&self) -> RiverbaseResult<()> {
        let mut problems = Vec::new();
        if matches!(
            self.config.memory_backend.as_str(),
            "in_memory" | "memory" | ""
        ) {
            problems.push("MEMORY_BACKEND=in_memory");
        }
        if !self.store.durable() {
            problems.push("BEAM_DATABASE_URL missing (in-memory store)");
        }
        if !self.executions.records().durable() {
            problems.push("in-memory execution records");
        }
        if !self.security.enforce() {
            problems.push("DisabledSecurityManager");
        }
        if beam_telemetry::privacy_mode() == beam_telemetry::PrivacyMode::FullDebug {
            problems.push("BEAM_TRACE_PRIVACY=full_debug");
        }
        // Python rejects MemorySaver for runtimes that advertise durable execution.
        if !self.runtime_states.durable()
            && self.runtimes.bound_names().iter().any(|n| {
                self.runtimes
                    .find(n)
                    .is_some_and(|r| r.capabilities().durable_execution)
            })
        {
            problems.push("in-memory runtime state for a durable runtime");
        }
        if problems.is_empty() {
            return Ok(());
        }
        Err(invalid(format!(
            "production profile rejects unsafe fallbacks: {}",
            problems.join(", ")
        )))
    }
}
