//! Security checks for riverbeam (OWASP Top 10 for LLM Applications).
//!
//! Each [`SecurityStage`] builds typed facts from its request (`facts`): the
//! highest [`Severity`] each kind of [`Detector`] finds, and policy lookups.
//! The rules ([`rules`]) turn the facts into coded narrations, and
//! [`verdict_from_narrations`] aggregates them into a [`SecurityDecision`]
//! that names the patterns that matched.
//! [`SecurityManagement`] is the executor-facing façade: [`SecurityManager`]
//! enforces, [`DisabledSecurityManager`] is the no-op default.

mod codes;
mod decision;
pub mod detect;
mod enums;
mod facts;
mod manager;
pub mod ownership;
mod patterns;
mod policy;
mod request;
mod rule_manager;
pub mod rules;
pub mod store;
mod verdict;

pub use codes::{
    RuleNarration, ALLOW_CODE, BLOCK_CODE, FLAG_CODE, HITL_CODE, MASK_CODE, REJECT_CODE,
};
pub use decision::SecurityDecision;
pub use detect::{
    char_span, mask, mask_flagged, Detector, DetectorKind, Detectors, Finding, PromptEcho,
    RegexDetector, Severity, MASK,
};
pub use enums::{SecurityAction, SecuritySeverity, SecurityStage, ViolationType};
pub use manager::{DisabledSecurityManager, SecurityManagement, PASSTHROUGH_STAGES};
pub use policy::{HitlConfig, SecurityPolicy, ToolPolicy};
pub use request::{SecurityContext, SecurityRequest};
pub use rule_manager::SecurityManager;
#[cfg(feature = "postgres")]
pub use store::PgSecurityStore;
pub use store::{DecisionRecord, DecisionScope, InMemorySecurityStore, SecurityStore};
pub use verdict::verdict_from_narrations;
