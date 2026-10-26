//! Memory rules: what an invoke writes to memory.

use super::{narrate, Rule};
use crate::codes::{RuleNarration, ALLOW_CODE, BLOCK_CODE, FLAG_CODE};
use crate::detect::Severity;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MemoryFacts {
    /// No content, or an operation other than `save`.
    pub skip: bool,
    /// The memory-sensitive detectors' highest severity.
    pub sensitive: Option<Severity>,
    /// The injection and role-override detectors' highest severity.
    pub injection: Option<Severity>,
}

pub(super) const RULES: &[Rule<MemoryFacts>] = &[sensitive_reject, poisoning, passes];

/// MEM-001: a stated credential is rejected; a mention is flagged.
fn sensitive_reject(f: &MemoryFacts) -> Option<RuleNarration> {
    if f.skip {
        return narrate("MEM-001", ALLOW_CODE, "memory_checked");
    }
    match f.sensitive {
        None => narrate("MEM-001", ALLOW_CODE, "memory_check_pass"),
        Some(Severity::Block) => narrate("MEM-001", BLOCK_CODE, "memory_sensitive_data_rejected"),
        Some(Severity::Flag) => narrate("MEM-001", FLAG_CODE, "memory_sensitive_data_flagged"),
    }
}

/// MEM-002: an injection in a memory write is rejected, so it cannot reach a
/// later prompt (LLM04).
fn poisoning(f: &MemoryFacts) -> Option<RuleNarration> {
    if f.skip {
        return None;
    }
    match f.injection {
        None => narrate("MEM-002", ALLOW_CODE, "memory_poisoning_not_suspected"),
        Some(Severity::Block) => narrate("MEM-002", BLOCK_CODE, "memory_poisoning_suspected"),
        Some(Severity::Flag) => narrate("MEM-002", FLAG_CODE, "memory_poisoning_suspected"),
    }
}

/// MEM-001: the default pass.
fn passes(_: &MemoryFacts) -> Option<RuleNarration> {
    narrate("MEM-001", ALLOW_CODE, "memory_check_pass")
}
