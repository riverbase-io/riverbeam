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
//! precomputed `Input.*` facts ([`facts`]) because GRL cannot call custom Rust
//! functions through `river_reed`'s evaluator. [`SecurityManager`] is the
//! executor-facing façade; [`DisabledSecurityManager`] is the no-op default.

mod codes;
mod decision;
mod engine;
mod enums;
mod facts;
mod grl;
mod manager;
mod mem_store;
mod patterns;
mod policy;
mod request;
mod seed;
mod verdict;

pub use codes::{RuleNarration, ALLOW_CODE, BLOCK_CODE, FLAG_CODE, HITL_CODE, REJECT_CODE};
pub use decision::SecurityDecision;
pub use engine::{narrations_from_eval, SecurityEngine};
pub use enums::{SecurityAction, SecuritySeverity, SecurityStage, ViolationType};
pub use manager::{
    DisabledSecurityManager, SecurityManagement, SecurityManager, PASSTHROUGH_STAGES,
};
pub use patterns::PatternLoader;
pub use policy::{HitlConfig, SecurityPolicy, ToolPolicy};
pub use request::{SecurityContext, SecurityRequest};
pub use verdict::verdict_from_narrations;
