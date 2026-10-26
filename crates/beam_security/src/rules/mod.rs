//! The security rules, in plain Rust over typed facts.
//!
//! Each area (input, output, tool, memory, runtime) has one facts struct and
//! a list of rules. A rule reads the facts and returns at most one coded
//! narration: its conditions exclude each other. [`evaluate`] returns the
//! narrations in declaration order, which the verdict's stable sort keeps
//! among equal codes.

mod input;
mod memory;
mod output;
mod runtime;
mod tool;

pub use input::InputFacts;
pub use memory::MemoryFacts;
pub use output::OutputFacts;
pub use runtime::RuntimeFacts;
pub use tool::{HitlOutcome, ToolFacts};

use crate::codes::RuleNarration;

/// The facts one stage's rules read.
#[derive(Debug, Clone, PartialEq)]
pub enum Facts {
    Input(InputFacts),
    Output(OutputFacts),
    Tool(ToolFacts),
    Memory(MemoryFacts),
    Runtime(RuntimeFacts),
}

/// The narrations of the rules for `facts`, in declaration order.
#[must_use]
pub fn evaluate(facts: &Facts) -> Vec<RuleNarration> {
    match facts {
        Facts::Input(f) => run(input::RULES, f),
        Facts::Output(f) => run(output::RULES, f),
        Facts::Tool(f) => run(tool::RULES, f),
        Facts::Memory(f) => run(memory::RULES, f),
        Facts::Runtime(f) => run(runtime::RULES, f),
    }
}

/// One rule over an area's facts.
type Rule<F> = fn(&F) -> Option<RuleNarration>;

fn run<F>(rules: &[Rule<F>], facts: &F) -> Vec<RuleNarration> {
    rules.iter().filter_map(|rule| rule(facts)).collect()
}

/// A rule's narration, in the `Option` every rule returns.
#[allow(clippy::unnecessary_wraps)]
fn narrate(rule: &str, code: i32, message: &str) -> Option<RuleNarration> {
    Some(RuleNarration::new(rule, code, message))
}
