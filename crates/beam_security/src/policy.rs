use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ToolPolicy {
    #[serde(default)]
    pub hitl_required: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HitlConfig {
    #[serde(default)]
    pub enabled: bool,
}

/// Tenant/agent security policy (ports `SecurityPolicy`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityPolicy {
    #[serde(default)]
    pub global_denied_tools: HashSet<String>,
    #[serde(default)]
    pub global_allowed_tools: HashSet<String>,
    #[serde(default)]
    pub tool_policies: HashMap<String, ToolPolicy>,
    #[serde(default)]
    pub hitl_config: HitlConfig,
    #[serde(default = "default_max_steps")]
    pub max_steps: u64,
    #[serde(default = "default_max_tokens")]
    pub max_tokens: u64,
    #[serde(default = "default_max_recursion")]
    pub max_recursion: u64,
    #[serde(default = "default_max_timeout")]
    pub max_timeout: f64,
}

fn default_max_steps() -> u64 {
    50
}
fn default_max_tokens() -> u64 {
    128_000
}
fn default_max_recursion() -> u64 {
    10
}
fn default_max_timeout() -> f64 {
    30.0
}

impl Default for SecurityPolicy {
    fn default() -> Self {
        Self {
            global_denied_tools: HashSet::new(),
            global_allowed_tools: HashSet::new(),
            tool_policies: HashMap::new(),
            hitl_config: HitlConfig::default(),
            max_steps: default_max_steps(),
            max_tokens: default_max_tokens(),
            max_recursion: default_max_recursion(),
            max_timeout: default_max_timeout(),
        }
    }
}
