//! Runtime rules: a run's steps and tokens.

use super::{narrate, Rule};
use crate::codes::{RuleNarration, ALLOW_CODE, BLOCK_CODE, FLAG_CODE};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeFacts {
    pub step_count: u64,
    pub max_steps: u64,
    /// Four fifths of `max_steps`, rounded down.
    pub step_threshold: u64,
    pub token_count: u64,
    pub max_tokens: u64,
}

/// A run's time limit is the execution timeout's (`beam_execution`), not a
/// rule's.
pub(super) const RULES: &[Rule<RuntimeFacts>] = &[max_steps, token_budget, runtime_ok];

/// RUN-001: more steps than the limit blocks; past four fifths of it is flagged.
fn max_steps(f: &RuntimeFacts) -> Option<RuleNarration> {
    if f.step_count > f.max_steps {
        narrate("RUN-001", BLOCK_CODE, "run_max_steps_exceeded")
    } else if f.step_count > f.step_threshold {
        narrate("RUN-001", FLAG_CODE, "run_step_count_approaching")
    } else {
        narrate("RUN-001", ALLOW_CODE, "runtime_ok")
    }
}

/// RUN-003: more tokens than the budget blocks.
fn token_budget(f: &RuntimeFacts) -> Option<RuleNarration> {
    if f.token_count > f.max_tokens {
        narrate("RUN-003", BLOCK_CODE, "run_token_budget_exceeded")
    } else {
        narrate("RUN-003", ALLOW_CODE, "runtime_ok")
    }
}

/// RUN-001: the default pass.
fn runtime_ok(_: &RuntimeFacts) -> Option<RuleNarration> {
    narrate("RUN-001", ALLOW_CODE, "runtime_ok")
}
