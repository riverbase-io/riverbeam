//! Usage plans, quota enforcement, and consumption tracking.
//!
//! [`UsagePlan`]/[`UsageLimits`], the [`UsageTracker`] over a pluggable
//! [`UsageStore`] (hermetic [`InMemoryUsageStore`] default), and the
//! pre-dispatch [`UsageGuard`].

mod guard;
mod plans;
mod store;
mod tracker;

pub use guard::{GuardResult, OveragePolicy, UsageGuard};
pub use plans::{UsageLimits, UsagePlan};
pub use store::{InMemoryUsageStore, UsageStore};
pub use tracker::{UsageEvent, UsageSummary, UsageTracker};
