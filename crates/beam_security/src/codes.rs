use serde::{Deserialize, Serialize};

pub const ALLOW_CODE: i32 = 200;
pub const FLAG_CODE: i32 = 300;
pub const HITL_CODE: i32 = 350;
pub const BLOCK_CODE: i32 = 400;
pub const REJECT_CODE: i32 = 450;

const DEFAULT_RULESET: &str = "security";
const DEFAULT_REVISION: i32 = 0;

/// A single rule outcome (ports rulepy `RuleNarration`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuleNarration {
    pub rule: String,
    pub code: i32,
    pub message: String,
    pub ruleset: String,
    pub revision: i32,
}

impl RuleNarration {
    #[must_use]
    pub fn new(rule: &str, code: i32, message: &str) -> Self {
        Self {
            rule: rule.to_string(),
            code,
            message: message.to_string(),
            ruleset: DEFAULT_RULESET.to_string(),
            revision: DEFAULT_REVISION,
        }
    }
}
