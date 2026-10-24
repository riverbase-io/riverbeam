//! Security rule knowledge-base engine.
//!
//! Re-backs the security checker on [`river_reed`]: the six security domains
//! (input, output, rag, tool, memory, runtime) are seeded as GRL rule sets in
//! a `river_reed` knowledge base ([`seed`]/[`grl`]), evaluated through
//! [`SecurityEngine`] (a dedicated worker thread owning the non-`Send`
//! `RuleManager`), and aggregated into a [`SecurityDecision`] by
//! [`verdict_from_narrations`].
//!
//! Regex scoring stays in Rust ([`PatternLoader`]) and is injected as
//! precomputed `Input.*` facts because GRL cannot call custom Rust functions
//! through `river_reed`'s evaluator. [`SecurityManagement`] is the
//! executor-facing façade; [`DisabledSecurityManager`] is the no-op default.
//!
//! The rule engine and `river_reed` come with the `rules` feature (on by
//! default): `SecurityManager`, `SecurityEngine` and `narrations_from_eval`.
//! Without it the crate keeps the types, the façade, the no-op manager, the
//! tenant ownership checks and the decision stores.

mod codes;
mod decision;
#[cfg(feature = "rules")]
mod engine;
mod enums;
#[cfg(feature = "rules")]
mod facts;
#[cfg(feature = "rules")]
mod grl;
mod manager;
#[cfg(feature = "rules")]
mod mem_store;
pub mod ownership;
mod patterns;
mod policy;
mod request;
#[cfg(feature = "rules")]
mod rule_manager;
#[cfg(feature = "rules")]
mod seed;
pub mod store;
mod verdict;

pub use codes::{RuleNarration, ALLOW_CODE, BLOCK_CODE, FLAG_CODE, HITL_CODE, REJECT_CODE};
pub use decision::SecurityDecision;
#[cfg(feature = "rules")]
pub use engine::{narrations_from_eval, SecurityEngine};
pub use enums::{SecurityAction, SecuritySeverity, SecurityStage, ViolationType};
pub use manager::{DisabledSecurityManager, SecurityManagement, PASSTHROUGH_STAGES};
pub use patterns::PatternLoader;
pub use policy::{HitlConfig, SecurityPolicy, ToolPolicy};
pub use request::{SecurityContext, SecurityRequest};
#[cfg(feature = "rules")]
pub use rule_manager::SecurityManager;
#[cfg(feature = "postgres")]
pub use store::PgSecurityStore;
pub use store::{DecisionRecord, DecisionScope, InMemorySecurityStore, SecurityStore};
pub use verdict::verdict_from_narrations;
