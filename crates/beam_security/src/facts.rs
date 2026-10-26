//! Builds each stage's typed facts from the request, the policy and the
//! detectors.
//!
//! Detection and policy-set membership are resolved here; the rules
//! ([`crate::rules`]) only read the outcome: each detector kind's highest
//! severity, and the policy's flags.

use serde_json::Value;

use crate::detect::{highest, Detector, DetectorKind, Detectors, Finding, PromptEcho};
use crate::enums::SecurityStage;
use crate::policy::SecurityPolicy;
use crate::request::SecurityRequest;
use crate::rules::{
    Facts, HitlOutcome, InputFacts, MemoryFacts, OutputFacts, RuntimeFacts, ToolFacts,
};

/// The facts for `stage` from the request payload and the policy, and the
/// detectors' findings.
#[must_use]
pub fn build_facts(
    stage: SecurityStage,
    req: &SecurityRequest,
    policy: &SecurityPolicy,
    detectors: &Detectors,
) -> (Facts, Vec<Finding>) {
    let p = &req.payload;
    let content = req.str_field("content");
    let mut findings = Vec::new();
    let mut scan = |kinds: &[DetectorKind]| {
        let found = detectors.scan(kinds, &content);
        let severity = highest(&found);
        findings.extend(found);
        severity
    };

    let facts = match stage {
        // These run the input rules; the HITL resume also requires an
        // identity (IAM-001).
        SecurityStage::BeforeInput
        | SecurityStage::BeforeContextInjection
        | SecurityStage::BeforeLlm
        | SecurityStage::BeforeHitlResume => {
            let user = req
                .security_context
                .as_ref()
                .and_then(|c| c.user_id.as_deref())
                .filter(|u| !u.is_empty());
            Facts::Input(InputFacts {
                content_empty: content.is_empty(),
                injection: scan(&[DetectorKind::Injection]),
                role_override: scan(&[DetectorKind::RoleOverride]),
                identity_missing: stage == SecurityStage::BeforeHitlResume && user.is_none(),
            })
        }
        SecurityStage::AfterLlm | SecurityStage::BeforeOutput => {
            let secrets = scan(&[DetectorKind::Secrets]);
            let pii = scan(&[DetectorKind::Pii]);
            // The agent's instructions, when the caller sends them (OUT-003).
            let echo = PromptEcho::new(&req.str_field("instructions"));
            let echoed = echo.scan(&content);
            let prompt_echo = highest(&echoed);
            findings.extend(echoed);
            Facts::Output(OutputFacts {
                content_empty: content.is_empty(),
                secrets,
                pii,
                echo_checked: !echo.is_empty(),
                prompt_echo,
            })
        }
        SecurityStage::BeforeTool | SecurityStage::AfterTool => {
            let tool = req.str_field("tool_name");
            // The call already ran, so it was approved.
            let hitl_approved = stage == SecurityStage::AfterTool
                || p.get("hitl_approved")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
            // The result the model will read, after the call (TOOL-002).
            let output_checked = stage == SecurityStage::AfterTool && !content.is_empty();
            Facts::Tool(ToolFacts {
                denied_global: policy.global_denied_tools.contains(&tool),
                denied_allowlist: !policy.global_allowed_tools.is_empty()
                    && !policy.global_allowed_tools.contains(&tool),
                hitl: hitl_outcome(&tool, hitl_approved, policy),
                output_checked,
                output_injection: if output_checked {
                    scan(&[DetectorKind::Injection, DetectorKind::RoleOverride])
                } else {
                    None
                },
            })
        }
        SecurityStage::BeforeMemoryWrite => {
            let operation = p.get("operation").and_then(Value::as_str).unwrap_or("save");
            let skip = content.is_empty() || operation != "save";
            Facts::Memory(MemoryFacts {
                skip,
                sensitive: if skip {
                    None
                } else {
                    scan(&[DetectorKind::MemorySensitive])
                },
                injection: if skip {
                    None
                } else {
                    scan(&[DetectorKind::Injection, DetectorKind::RoleOverride])
                },
            })
        }
        SecurityStage::Runtime => {
            let count = |key: &str| p.get(key).and_then(Value::as_u64).unwrap_or(0);
            Facts::Runtime(RuntimeFacts {
                step_count: count("step_count"),
                max_steps: policy.max_steps,
                // Python `int(max_steps * 0.80)`.
                step_threshold: policy.max_steps * 4 / 5,
                token_count: count("token_count"),
                max_tokens: policy.max_tokens,
            })
        }
    };
    (facts, findings)
}

/// Whether `tool` needs a human (Python `_tool_hitl_required`).
fn hitl_outcome(tool: &str, approved: bool, policy: &SecurityPolicy) -> HitlOutcome {
    if approved {
        return HitlOutcome::Approved;
    }
    if policy
        .hitl_config
        .auto_approve_tools
        .iter()
        .any(|spec| !spec.is_empty() && (spec == tool || fnmatch(tool, spec)))
    {
        return HitlOutcome::AutoApproved;
    }
    match policy.tool_policies.get(tool) {
        Some(tp) if tp.hitl_required => HitlOutcome::Required,
        Some(_) => HitlOutcome::NotRequired,
        None if policy.hitl_config.enabled => HitlOutcome::Required,
        None => HitlOutcome::Unlisted,
    }
}

/// Python `fnmatch.fnmatch` on POSIX (case-sensitive): `*`, `?`, `[seq]`, `[!seq]`.
#[must_use]
pub fn fnmatch(name: &str, pattern: &str) -> bool {
    fn go(n: &[char], p: &[char]) -> bool {
        match p.first() {
            None => n.is_empty(),
            Some('*') => (0..=n.len()).any(|i| go(&n[i..], &p[1..])),
            Some('?') => !n.is_empty() && go(&n[1..], &p[1..]),
            Some('[') => {
                let Some(end) = p.iter().skip(1).position(|c| *c == ']').map(|i| i + 1) else {
                    return n.first() == Some(&'[') && go(&n[1..], &p[1..]);
                };
                let Some(c) = n.first() else { return false };
                let (negate, set) = match p.get(1) {
                    Some('!') => (true, &p[2..end]),
                    _ => (false, &p[1..end]),
                };
                let mut hit = false;
                let mut i = 0;
                while i < set.len() {
                    if i + 2 < set.len() && set[i + 1] == '-' {
                        hit |= set[i] <= *c && *c <= set[i + 2];
                        i += 3;
                    } else {
                        hit |= set[i] == *c;
                        i += 1;
                    }
                }
                hit != negate && go(&n[1..], &p[end + 1..])
            }
            Some(ch) => n.first() == Some(ch) && go(&n[1..], &p[1..]),
        }
    }
    let n: Vec<char> = name.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    go(&n, &p)
}

#[cfg(test)]
mod tests {
    use super::fnmatch;

    #[test]
    fn fnmatch_matches_python() {
        assert!(fnmatch("search_web", "search_*"));
        assert!(fnmatch("tool1", "tool?"));
        assert!(fnmatch("b", "[abc]"));
        assert!(!fnmatch("d", "[abc]"));
        assert!(fnmatch("d", "[!abc]"));
        assert!(fnmatch("m", "[a-z]"));
        assert!(!fnmatch("Search", "search*"));
    }
}
