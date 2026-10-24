//! Usage plans, quota enforcement, and consumption tracking.
//!
//! [`UsagePlan`]/[`UsageLimits`], the [`UsageTracker`] over a pluggable
//! [`UsageStore`] (hermetic [`InMemoryUsageStore`] default), and the
//! pre-dispatch [`UsageGuard`].

#[allow(clippy::wildcard_imports)] // The error catalog (`beam_errors`).
pub(crate) use beam_errors::*;

mod cost;
mod factory;
mod guard;
mod pg;
mod plans;
mod reservation;
mod store;
mod tracker;

pub use cost::{cost_event, cost_usd, CostDims, CostEvent, CostStore, InMemoryCostStore};
pub use factory::{build_reservation_store, build_usage_store, USAGE_BACKENDS};
pub use guard::{GuardResult, OveragePolicy, UsageGuard};
pub use pg::{PgCostStore, PgReservationStore, PgUsageStore};
pub use plans::{UsageLimits, UsagePlan};
pub use reservation::{Reservation, ReservationStore, ReserveRequest};
pub use store::{InMemoryUsageStore, UsageStore};
pub use tracker::{UsageEvent, UsageSummary, UsageTracker};
