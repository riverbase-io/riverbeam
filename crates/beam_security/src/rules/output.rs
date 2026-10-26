//! Output rules: the model's answer.

use super::{narrate, Rule};
use crate::codes::{RuleNarration, ALLOW_CODE, BLOCK_CODE, FLAG_CODE, MASK_CODE};
use crate::detect::Severity;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OutputFacts {
    pub content_empty: bool,
    /// The secret detectors' highest severity.
    pub secrets: Option<Severity>,
    /// The PII detectors' highest severity.
    pub pii: Option<Severity>,
    /// The request carried the agent's instructions.
    pub echo_checked: bool,
    /// The prompt-echo detector's finding.
    pub prompt_echo: Option<Severity>,
}

pub(super) const RULES: &[Rule<OutputFacts>] = &[secrets, pii, prompt_echo];

/// OUT-001: a secret blocks, or is masked when its detector only flags it.
fn secrets(f: &OutputFacts) -> Option<RuleNarration> {
    match f.secrets.filter(|_| !f.content_empty) {
        None => narrate("OUT-001", ALLOW_CODE, "output_checked"),
        Some(Severity::Block) => narrate("OUT-001", BLOCK_CODE, "secrets_detected"),
        // Masked, as the stream guard masks it.
        Some(Severity::Flag) => narrate("OUT-001", MASK_CODE, "secrets_flagged"),
    }
}

/// OUT-002: PII is masked, or blocks when its detector says so.
fn pii(f: &OutputFacts) -> Option<RuleNarration> {
    match f.pii.filter(|_| !f.content_empty) {
        None => narrate("OUT-002", ALLOW_CODE, "output_checked"),
        Some(Severity::Block) => narrate("OUT-002", BLOCK_CODE, "pii_detected"),
        Some(Severity::Flag) => narrate("OUT-002", MASK_CODE, "pii_detected"),
    }
}

/// OUT-003: output that repeats the agent's instructions blocks (LLM07).
fn prompt_echo(f: &OutputFacts) -> Option<RuleNarration> {
    if !f.echo_checked {
        return None;
    }
    match f.prompt_echo {
        None => narrate("OUT-003", ALLOW_CODE, "output_checked"),
        Some(Severity::Block) => narrate("OUT-003", BLOCK_CODE, "prompt_echo_detected"),
        Some(Severity::Flag) => narrate("OUT-003", FLAG_CODE, "prompt_echo_flagged"),
    }
}
