//! Tool rules: a call before it runs and its result after.

use super::{narrate, Rule};
use crate::codes::{RuleNarration, ALLOW_CODE, BLOCK_CODE, FLAG_CODE, HITL_CODE};
use crate::detect::Severity;

/// Whether a call needs a human, as the policy decides it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum HitlOutcome {
    /// The policy says nothing about this tool.
    #[default]
    Unlisted,
    Approved,
    AutoApproved,
    Required,
    NotRequired,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ToolFacts {
    pub denied_global: bool,
    pub denied_allowlist: bool,
    pub hitl: HitlOutcome,
    /// `AFTER_TOOL` with the tool's output to scan.
    pub output_checked: bool,
    /// The injection and role-override detectors' highest severity on it.
    pub output_injection: Option<Severity>,
}

pub(super) const RULES: &[Rule<ToolFacts>] = &[
    denied_global,
    not_in_allowlist,
    hitl_required,
    output_injection,
    allowed_default,
];

/// TOOL-001: a globally denied tool blocks.
fn denied_global(f: &ToolFacts) -> Option<RuleNarration> {
    if f.denied_global {
        narrate("TOOL-001", BLOCK_CODE, "tool_denied_global")
    } else {
        narrate("TOOL-001", ALLOW_CODE, "tool_allowed_by_global_deny_check")
    }
}

/// TOOL-001: with a global allowlist, a tool outside it blocks.
fn not_in_allowlist(f: &ToolFacts) -> Option<RuleNarration> {
    if f.denied_allowlist {
        narrate(
            "TOOL-001",
            BLOCK_CODE,
            "tool_denied_not_in_global_allowlist",
        )
    } else {
        narrate("TOOL-001", ALLOW_CODE, "tool_in_global_allowlist")
    }
}

/// HITL-001: a call that needs a human waits for one.
fn hitl_required(f: &ToolFacts) -> Option<RuleNarration> {
    match f.hitl {
        HitlOutcome::Approved => narrate("HITL-001", ALLOW_CODE, "hitl_approved"),
        HitlOutcome::AutoApproved => narrate("HITL-001", ALLOW_CODE, "hitl_auto_approved"),
        HitlOutcome::Required => narrate("HITL-001", HITL_CODE, "tool_hitl_required"),
        HitlOutcome::NotRequired => narrate("HITL-001", ALLOW_CODE, "tool_hitl_not_required"),
        HitlOutcome::Unlisted => None,
    }
}

/// TOOL-002: injection in a tool's output (indirect injection) blocks or is
/// flagged, by its severity.
fn output_injection(f: &ToolFacts) -> Option<RuleNarration> {
    if !f.output_checked {
        return None;
    }
    match f.output_injection {
        None => narrate("TOOL-002", ALLOW_CODE, "tool_output_checked"),
        Some(Severity::Block) => narrate("TOOL-002", BLOCK_CODE, "tool_output_injection_blocked"),
        Some(Severity::Flag) => narrate("TOOL-002", FLAG_CODE, "tool_output_injection_flagged"),
    }
}

/// TOOL-001: the default allow.
fn allowed_default(_: &ToolFacts) -> Option<RuleNarration> {
    narrate("TOOL-001", ALLOW_CODE, "tool_allowed")
}
