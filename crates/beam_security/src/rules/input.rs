//! Input rules: user input, context injection, the model's prompt and a HITL
//! resume.

use super::{narrate, Rule};
use crate::codes::{RuleNarration, ALLOW_CODE, BLOCK_CODE, FLAG_CODE};
use crate::detect::Severity;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InputFacts {
    pub content_empty: bool,
    /// The injection detectors' highest severity.
    pub injection: Option<Severity>,
    /// The role-override detectors' highest severity.
    pub role_override: Option<Severity>,
    /// A HITL resume without a user.
    pub identity_missing: bool,
}

pub(super) const RULES: &[Rule<InputFacts>] = &[
    injection_blocked,
    injection_flagged,
    role_override,
    identity_required,
];

/// PI-001: a blocking injection finding blocks.
fn injection_blocked(f: &InputFacts) -> Option<RuleNarration> {
    if f.content_empty {
        narrate("PI-001", ALLOW_CODE, "input_checked")
    } else if f.injection == Some(Severity::Block) {
        narrate("PI-001", BLOCK_CODE, "injection_blocked")
    } else {
        narrate("PI-001", ALLOW_CODE, "inj_user_input_passed")
    }
}

/// PI-001: a flagging injection finding, and none that blocks, is flagged.
fn injection_flagged(f: &InputFacts) -> Option<RuleNarration> {
    if f.content_empty {
        narrate("PI-001", ALLOW_CODE, "input_checked")
    } else if f.injection == Some(Severity::Flag) {
        narrate("PI-001", FLAG_CODE, "injection_flagged")
    } else {
        None
    }
}

/// PI-008: a role override blocks or is flagged, by its severity.
fn role_override(f: &InputFacts) -> Option<RuleNarration> {
    if f.content_empty {
        return narrate("PI-008", ALLOW_CODE, "input_checked");
    }
    match f.role_override? {
        Severity::Block => narrate("PI-008", BLOCK_CODE, "role_override_detected"),
        Severity::Flag => narrate("PI-008", FLAG_CODE, "role_override_flagged"),
    }
}

/// IAM-001: a HITL resume needs a user.
fn identity_required(f: &InputFacts) -> Option<RuleNarration> {
    if f.identity_missing {
        narrate("IAM-001", BLOCK_CODE, "identity_missing")
    } else {
        narrate("IAM-001", ALLOW_CODE, "identity_ok")
    }
}
