//! Tool execution preferences (Python rc8 `tools/preferences.py`, plan 023
//! part C): the modes, the rules tenants, agents and users set, the
//! per-session grants, and the decision the layers resolve to. The resolver
//! is `beam_tools::policy`; the stores are in `beam_store`.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// How long "allow for this conversation" lasts.
pub const GRANT_TTL_HOURS: i64 = 24;

/// What happens to a call.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ToolMode {
    /// Runs without asking.
    #[default]
    Auto,
    /// Waits for a human's approval.
    Ask,
    /// Never runs.
    Deny,
}

impl ToolMode {
    /// `auto` < `ask` < `deny`.
    #[must_use]
    pub fn strictness(self) -> u8 {
        match self {
            Self::Auto => 0,
            Self::Ask => 1,
            Self::Deny => 2,
        }
    }

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Ask => "ask",
            Self::Deny => "deny",
        }
    }
}

/// Which layer decided.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ToolLayer {
    Security,
    Tenant,
    Agent,
    User,
    Grant,
    #[default]
    Default,
}

impl ToolLayer {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Security => "security",
            Self::Tenant => "tenant",
            Self::Agent => "agent",
            Self::User => "user",
            Self::Grant => "grant",
            Self::Default => "default",
        }
    }
}

/// What an approval remembers.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RememberOption {
    /// This call only.
    #[default]
    Once,
    /// The tool in this conversation (a grant, [`GRANT_TTL_HOURS`]).
    Session,
    /// Always, for this user (an exact-name user rule with mode `auto`).
    Always,
}

impl RememberOption {
    /// The option a resume names (`once`, `session`, `always`); `None` for
    /// anything else.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "once" => Some(Self::Once),
            "session" => Some(Self::Session),
            "always" => Some(Self::Always),
            _ => None,
        }
    }
}

/// Whose rule it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RuleScope {
    Tenant,
    User,
}

impl RuleScope {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Tenant => "tenant",
            Self::User => "user",
        }
    }
}

/// Which tools a rule covers: every field that is set must match; an unset
/// field matches any tool. A misspelled key is refused, not read as "any
/// tool".
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolMatch {
    /// The tool name, exact or a glob (`*`, `?`, `[...]`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// `native`, `mcp`, `http` or `client`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// `low`, `medium` or `high`: tools at least this risky.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_risk: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub side_effect: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
}

impl ToolMatch {
    /// The rule for exactly `name`.
    #[must_use]
    pub fn exact(name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
            ..Self::default()
        }
    }

    /// Whether `name` is a glob.
    #[must_use]
    pub fn is_glob(&self) -> bool {
        self.name
            .as_deref()
            .is_some_and(|n| n.contains(['*', '?', '[']))
    }

    /// Refuse a match that could never match a tool: a `source` or
    /// `min_risk` outside its values, or a `category` (riverbeam's tools have
    /// none). Such a `deny` would quietly allow everything.
    ///
    /// # Errors
    /// The reason the match is refused.
    pub fn check(&self) -> Result<(), String> {
        if let Some(source) = &self.source {
            if !["native", "mcp", "http", "client"].contains(&source.as_str()) {
                return Err(format!(
                    "source must be native, mcp, http or client, not {source:?}"
                ));
            }
        }
        if let Some(risk) = &self.min_risk {
            if !["low", "medium", "high"].contains(&risk.as_str()) {
                return Err(format!(
                    "min_risk must be low, medium or high, not {risk:?}"
                ));
            }
        }
        if self.category.is_some() {
            return Err("category is not supported: riverbeam's tools have no categories".into());
        }
        Ok(())
    }

    /// How specific the rule is: no name 0, a glob 1, an exact name 2.
    #[must_use]
    pub fn specificity(&self) -> u8 {
        match &self.name {
            None => 0,
            Some(_) if self.is_glob() => 1,
            Some(_) => 2,
        }
    }
}

/// One rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRule {
    #[serde(rename = "match")]
    pub matcher: ToolMatch,
    pub mode: ToolMode,
    /// Tenant rules only: lower layers may not loosen it.
    #[serde(default)]
    pub locked: bool,
}

impl ToolRule {
    /// Plan 023 D6: a user's `auto` rule needs an exact tool name, so a user
    /// cannot switch every approval off for themselves; globs and empty
    /// matches are allowed for `ask` and `deny`. `locked` is for tenant
    /// rules only.
    ///
    /// # Errors
    /// The reason the rule is refused.
    pub fn check_user_rule(&self) -> Result<(), String> {
        if self.locked {
            return Err("locked is allowed only on tenant rules".into());
        }
        if self.mode == ToolMode::Auto && (self.matcher.name.is_none() || self.matcher.is_glob()) {
            return Err("an auto rule needs an exact tool name".into());
        }
        Ok(())
    }
}

/// A stored rule.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRuleRecord {
    pub rule_id: Uuid,
    pub scope: RuleScope,
    #[serde(default)]
    pub org_id: Option<String>,
    #[serde(default)]
    pub user_id: Option<String>,
    pub rule: ToolRule,
}

/// "Allow for this conversation": `tool_name` runs without asking in
/// `session_id` until `expires_at`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolGrant {
    pub session_id: Uuid,
    pub tool_name: String,
    #[serde(default)]
    pub user_id: Option<String>,
    #[serde(default)]
    pub org_id: Option<String>,
    pub expires_at: DateTime<Utc>,
}

/// What the layers resolved to for one call.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ToolDecision {
    pub mode: ToolMode,
    pub layer: ToolLayer,
    #[serde(default)]
    pub rule: Option<ToolRule>,
    #[serde(default)]
    pub locked: bool,
    #[serde(default)]
    pub reason: Option<String>,
}

impl ToolDecision {
    /// What an approval may remember: `once` only when the decision is locked.
    #[must_use]
    pub fn remember_options(&self) -> Vec<RememberOption> {
        if self.locked {
            vec![RememberOption::Once]
        } else {
            vec![
                RememberOption::Once,
                RememberOption::Session,
                RememberOption::Always,
            ]
        }
    }
}

/// The agent layer of an agent whose `approval_required` tools ask.
#[must_use]
pub fn ask_rules(tools: &[String]) -> Vec<ToolRule> {
    tools
        .iter()
        .map(|name| ToolRule {
            matcher: ToolMatch::exact(name.clone()),
            mode: ToolMode::Ask,
            locked: false,
        })
        .collect()
}

/// Who a call runs for: the layers' keys (Python rc8 `ToolScope`).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ToolScope {
    /// The conversation a grant is keyed on (the root session).
    pub session_id: Option<Uuid>,
    pub user_id: Option<String>,
    pub org_id: Option<String>,
    /// The agent's own rules (its `approval_required` tools ask).
    pub agent_rules: Vec<ToolRule>,
}

impl ToolScope {
    /// The scope of `input`'s run, with `agent_rules`.
    #[must_use]
    pub fn of(input: &crate::AgentInput, agent_rules: Vec<ToolRule>) -> Self {
        let auth = input.auth_context.as_ref();
        Self {
            session_id: Some(input.session_id),
            user_id: auth.and_then(|a| a.user_id.clone()),
            org_id: auth.and_then(|a| a.org_id.clone()),
            agent_rules,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_misspelled_match_key_is_refused() {
        let err = serde_json::from_value::<ToolMatch>(serde_json::json!({"nmae": "x"}));
        assert!(err.is_err());
        let rule: ToolRule =
            serde_json::from_value(serde_json::json!({"match": {"name": "crm__*"}, "mode": "ask"}))
                .unwrap();
        assert_eq!(rule.matcher.specificity(), 1);
        assert!(!rule.locked);
    }

    #[test]
    fn a_user_auto_rule_needs_an_exact_name() {
        let rule = |name: Option<&str>, mode, locked| ToolRule {
            matcher: ToolMatch {
                name: name.map(str::to_owned),
                ..ToolMatch::default()
            },
            mode,
            locked,
        };
        assert!(rule(Some("pay"), ToolMode::Auto, false)
            .check_user_rule()
            .is_ok());
        assert!(rule(Some("pay*"), ToolMode::Auto, false)
            .check_user_rule()
            .is_err());
        assert!(rule(None, ToolMode::Auto, false).check_user_rule().is_err());
        assert!(rule(None, ToolMode::Deny, false).check_user_rule().is_ok());
        assert!(rule(Some("pay"), ToolMode::Ask, true)
            .check_user_rule()
            .is_err());
    }

    #[test]
    fn a_locked_decision_remembers_once_only() {
        let locked = ToolDecision {
            mode: ToolMode::Ask,
            locked: true,
            ..ToolDecision::default()
        };
        assert_eq!(locked.remember_options(), vec![RememberOption::Once]);
        assert_eq!(ToolDecision::default().remember_options().len(), 3);
    }
}
