//! How the layers resolve a call's mode (Python rc8 `tools/preferences.py`,
//! plan 023 part C). Pure: no I/O. The layers, strictest wins within a
//! locked decision: security (riverbeam's `BEFORE_TOOL` stays a hard block
//! that runs on every call, so its ceiling is empty here), tenant, agent,
//! user, then a session grant, which turns an unlocked `ask` into `auto`.

use beam_types::tool_policy::{ToolDecision, ToolLayer, ToolMatch, ToolMode, ToolRule};

use crate::definition::{ToolDefinition, ToolRisk, ToolSource};

/// The rules of the layers above the default.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ToolPolicyLayers {
    pub tenant: Vec<ToolRule>,
    pub agent: Vec<ToolRule>,
    pub user: Vec<ToolRule>,
}

/// What security decided before the rules.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SecurityCeiling {
    pub denied: bool,
    pub approval_required: bool,
    pub reason: Option<String>,
}

fn risk_rank(risk: &str) -> Option<u8> {
    match risk {
        "low" => Some(0),
        "medium" => Some(1),
        "high" => Some(2),
        _ => None,
    }
}

fn risk_name(risk: ToolRisk) -> &'static str {
    match risk {
        ToolRisk::Low => "low",
        ToolRisk::Medium => "medium",
        ToolRisk::High => "high",
    }
}

fn source_name(source: ToolSource) -> &'static str {
    match source {
        ToolSource::Native => "native",
        ToolSource::Mcp => "mcp",
        ToolSource::Http => "http",
        ToolSource::Client => "client",
    }
}

/// `fnmatch.fnmatchcase`: `*` any run, `?` one character, `[...]` a set
/// (`[!...]` its complement, `a-z` a range).
#[must_use]
pub fn glob_matches(pattern: &str, text: &str) -> bool {
    fn class(set: &[char], c: char) -> Option<(bool, usize)> {
        // Returns (matched, length of the class including brackets).
        let mut i = 1;
        let negate = set.get(i) == Some(&'!');
        if negate {
            i += 1;
        }
        let start = i;
        let mut matched = false;
        while i < set.len() {
            if set[i] == ']' && i > start {
                return Some((matched != negate, i + 1));
            }
            if i + 2 < set.len() && set[i + 1] == '-' && set[i + 2] != ']' {
                if set[i] <= c && c <= set[i + 2] {
                    matched = true;
                }
                i += 3;
            } else {
                if set[i] == c {
                    matched = true;
                }
                i += 1;
            }
        }
        None
    }
    fn go(p: &[char], t: &[char]) -> bool {
        match p.first() {
            None => t.is_empty(),
            Some('*') => (0..=t.len()).any(|k| go(&p[1..], &t[k..])),
            Some('?') => !t.is_empty() && go(&p[1..], &t[1..]),
            Some('[') => match (t.first(), class(p, t.first().copied().unwrap_or('\0'))) {
                (Some(_), Some((true, len))) => go(&p[len..], &t[1..]),
                (Some(_), Some((false, _))) | (None, Some(_)) => false,
                // An unclosed `[` is a literal.
                (_, None) => t.first() == Some(&'[') && go(&p[1..], &t[1..]),
            },
            Some(c) => t.first() == Some(c) && go(&p[1..], &t[1..]),
        }
    }
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    go(&p, &t)
}

/// Whether `matcher` covers `definition`.
#[must_use]
pub fn matches(matcher: &ToolMatch, definition: &ToolDefinition) -> bool {
    if let Some(name) = &matcher.name {
        if !glob_matches(name, &definition.name) {
            return false;
        }
    }
    if let Some(source) = &matcher.source {
        if source != source_name(definition.source) {
            return false;
        }
    }
    if let Some(min) = &matcher.min_risk {
        let (Some(min), Some(risk)) = (risk_rank(min), risk_rank(risk_name(definition.risk)))
        else {
            return false;
        };
        if risk < min {
            return false;
        }
    }
    if let Some(side_effect) = matcher.side_effect {
        if side_effect != definition.side_effect {
            return false;
        }
    }
    // riverbeam's tools have no categories, so a category never matches.
    matcher.category.is_none()
}

/// The tool's own mode: `ask` for an approval-gated or high-risk tool.
#[must_use]
pub fn default_mode(definition: &ToolDefinition) -> ToolMode {
    if definition.needs_approval() {
        ToolMode::Ask
    } else {
        ToolMode::Auto
    }
}

/// The rule of `rules` that decides for `definition`: the most specific,
/// then the strictest.
fn pick<'a>(rules: &'a [ToolRule], definition: &ToolDefinition) -> Option<&'a ToolRule> {
    rules
        .iter()
        .filter(|r| matches(&r.matcher, definition))
        .max_by_key(|r| (r.matcher.specificity(), r.mode.strictness()))
}

/// Resolve the security, tenant, agent, user and grant layers for
/// `definition`.
#[must_use]
pub fn resolve_tool_mode(
    definition: &ToolDefinition,
    layers: &ToolPolicyLayers,
    ceiling: &SecurityCeiling,
    granted: bool,
) -> ToolDecision {
    if ceiling.denied {
        return ToolDecision {
            mode: ToolMode::Deny,
            layer: ToolLayer::Security,
            locked: true,
            reason: ceiling.reason.clone(),
            rule: None,
        };
    }
    let mut decision = ToolDecision {
        mode: default_mode(definition),
        layer: ToolLayer::Default,
        ..ToolDecision::default()
    };
    if ceiling.approval_required {
        decision = ToolDecision {
            mode: ToolMode::Ask,
            layer: ToolLayer::Security,
            locked: true,
            reason: ceiling.reason.clone(),
            rule: None,
        };
    }
    for (layer, rules) in [
        (ToolLayer::Tenant, &layers.tenant),
        (ToolLayer::Agent, &layers.agent),
        (ToolLayer::User, &layers.user),
    ] {
        let Some(rule) = pick(rules, definition) else {
            continue;
        };
        if decision.locked && rule.mode.strictness() <= decision.mode.strictness() {
            continue;
        }
        let locked = decision.locked || (rule.locked && layer != ToolLayer::User);
        decision = ToolDecision {
            mode: rule.mode,
            layer,
            rule: Some(rule.clone()),
            locked,
            reason: None,
        };
    }
    if granted && decision.mode == ToolMode::Ask && !decision.locked {
        decision.mode = ToolMode::Auto;
        decision.layer = ToolLayer::Grant;
    }
    decision
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tool(name: &str, risk: ToolRisk) -> ToolDefinition {
        ToolDefinition {
            risk,
            ..ToolDefinition::new(name)
        }
    }

    fn rule(name: Option<&str>, mode: ToolMode, locked: bool) -> ToolRule {
        ToolRule {
            matcher: ToolMatch {
                name: name.map(str::to_owned),
                ..ToolMatch::default()
            },
            mode,
            locked,
        }
    }

    #[test]
    fn globs_match_like_fnmatchcase() {
        assert!(glob_matches("crm__*", "crm__search"));
        assert!(glob_matches("pay?", "pay1"));
        assert!(glob_matches("[a-c]x", "bx"));
        assert!(!glob_matches("[!a-c]x", "bx"));
        assert!(!glob_matches("crm__*", "CRM__search"));
        assert!(glob_matches("exact", "exact"));
    }

    #[test]
    fn the_default_is_the_tools_own_mode() {
        let none = ToolPolicyLayers::default();
        let ceiling = SecurityCeiling::default();
        let low = resolve_tool_mode(&tool("a", ToolRisk::Low), &none, &ceiling, false);
        assert_eq!((low.mode, low.layer), (ToolMode::Auto, ToolLayer::Default));
        let high = resolve_tool_mode(&tool("a", ToolRisk::High), &none, &ceiling, false);
        assert_eq!(high.mode, ToolMode::Ask);
    }

    #[test]
    fn the_most_specific_then_strictest_rule_wins() {
        let layers = ToolPolicyLayers {
            user: vec![
                rule(Some("crm__*"), ToolMode::Deny, false),
                rule(Some("crm__search"), ToolMode::Auto, false),
            ],
            ..ToolPolicyLayers::default()
        };
        let d = resolve_tool_mode(
            &tool("crm__search", ToolRisk::Low),
            &layers,
            &SecurityCeiling::default(),
            false,
        );
        assert_eq!((d.mode, d.layer), (ToolMode::Auto, ToolLayer::User));
    }

    #[test]
    fn a_locked_tenant_rule_cannot_be_loosened() {
        let layers = ToolPolicyLayers {
            tenant: vec![rule(Some("pay"), ToolMode::Ask, true)],
            user: vec![rule(Some("pay"), ToolMode::Auto, false)],
            ..ToolPolicyLayers::default()
        };
        let d = resolve_tool_mode(
            &tool("pay", ToolRisk::Low),
            &layers,
            &SecurityCeiling::default(),
            true,
        );
        assert_eq!(
            (d.mode, d.layer, d.locked),
            (ToolMode::Ask, ToolLayer::Tenant, true)
        );
        // A grant does not open a locked ask either.
        assert_eq!(d.remember_options().len(), 1);
    }

    #[test]
    fn a_grant_turns_an_unlocked_ask_into_auto() {
        let d = resolve_tool_mode(
            &tool("pay", ToolRisk::High),
            &ToolPolicyLayers::default(),
            &SecurityCeiling::default(),
            true,
        );
        assert_eq!((d.mode, d.layer), (ToolMode::Auto, ToolLayer::Grant));
    }

    #[test]
    fn a_locked_ceiling_only_tightens() {
        let layers = ToolPolicyLayers {
            user: vec![rule(None, ToolMode::Deny, false)],
            agent: vec![rule(Some("pay"), ToolMode::Auto, false)],
            ..ToolPolicyLayers::default()
        };
        let ceiling = SecurityCeiling {
            approval_required: true,
            ..SecurityCeiling::default()
        };
        let d = resolve_tool_mode(&tool("pay", ToolRisk::Low), &layers, &ceiling, false);
        assert_eq!(
            (d.mode, d.layer, d.locked),
            (ToolMode::Deny, ToolLayer::User, true)
        );
    }
}
