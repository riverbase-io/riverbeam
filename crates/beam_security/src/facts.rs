//! Builds the `Input.*` fact JSON for each stage.
//!
//! All regex scoring and policy-set membership is resolved here in Rust; the GRL
//! documents ([`crate::grl`]) only see precomputed numbers and booleans.

use serde_json::{json, Value};

use crate::enums::SecurityStage;
use crate::patterns::PatternLoader;
use crate::policy::SecurityPolicy;
use crate::request::SecurityRequest;

/// Construct the fact object for `stage` from the request payload + policy.
#[must_use]
pub fn build_facts(stage: SecurityStage, req: &SecurityRequest, policy: &SecurityPolicy) -> Value {
    let p = &req.payload;
    let content = req.str_field("content");

    match stage {
        // Python routes these through the input KB (`stages.ROUTES`); the
        // HITL resume fact also requires an identity (IAM-001).
        SecurityStage::BeforeInput
        | SecurityStage::BeforeContextInjection
        | SecurityStage::BeforeLlm
        | SecurityStage::BeforeHitlResume => {
            let user = req
                .security_context
                .as_ref()
                .and_then(|c| c.user_id.as_deref())
                .filter(|u| !u.is_empty());
            json!({
                "content_empty": content.is_empty(),
                "injection_score": PatternLoader::score_injection(&content),
                "role_override_score": PatternLoader::score_role_override(&content),
                "identity_missing": stage == SecurityStage::BeforeHitlResume && user.is_none(),
            })
        }
        SecurityStage::AfterLlm | SecurityStage::BeforeOutput => json!({
            "content_empty": content.is_empty(),
            "secrets_score": PatternLoader::score_secrets(&content),
            "pii_score": PatternLoader::score_pii(&content),
        }),
        SecurityStage::BeforeTool | SecurityStage::AfterTool => {
            let tool = req.str_field("tool_name");
            // The call already ran, so it was approved.
            let hitl_approved = stage == SecurityStage::AfterTool
                || p.get("hitl_approved")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
            let denied_global = policy.global_denied_tools.contains(&tool);
            let denied_allowlist = !policy.global_allowed_tools.is_empty()
                && !policy.global_allowed_tools.contains(&tool);
            json!({
                "always": true,
                "tool_denied_global": denied_global,
                "tool_denied_allowlist": denied_allowlist,
                "hitl_outcome": hitl_outcome(&tool, hitl_approved, policy),
            })
        }
        SecurityStage::BeforeMemoryWrite => {
            let operation = p.get("operation").and_then(Value::as_str).unwrap_or("save");
            let confidence = p.get("confidence").and_then(Value::as_f64).unwrap_or(0.0);
            let skip = content.is_empty() || operation != "save";
            let score = if skip {
                0.0
            } else {
                PatternLoader::score_memory_sensitive(&content)
            };
            json!({ "always": true, "memory_skip": skip, "sensitive_score": score, "confidence": confidence })
        }
        SecurityStage::Runtime => {
            let step_count = p.get("step_count").and_then(Value::as_u64).unwrap_or(0);
            let token_count = p.get("token_count").and_then(Value::as_u64).unwrap_or(0);
            let recursion_depth = p
                .get("recursion_depth")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let loop_detected = p
                .get("loop_detected")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let elapsed_seconds = p
                .get("elapsed_seconds")
                .and_then(Value::as_f64)
                .unwrap_or(0.0);
            // Python `int(max_steps * 0.80)`.
            let step_threshold = policy.max_steps * 4 / 5;
            json!({
                "always": true,
                "step_count": step_count,
                "max_steps": policy.max_steps,
                "step_threshold": step_threshold,
                "token_count": token_count,
                "max_tokens": policy.max_tokens,
                "recursion_depth": recursion_depth,
                "max_recursion": policy.max_recursion,
                "loop_detected": loop_detected,
                "elapsed_seconds": elapsed_seconds,
                "max_timeout": policy.max_timeout,
            })
        }
    }
}

/// Python `_tool_hitl_required` as a code: 0 no narration, 1 approved,
/// 2 auto-approved, 3 approval required, 4 not required for this tool.
fn hitl_outcome(tool: &str, approved: bool, policy: &SecurityPolicy) -> u8 {
    if approved {
        return 1;
    }
    if policy
        .hitl_config
        .auto_approve_tools
        .iter()
        .any(|spec| !spec.is_empty() && (spec == tool || fnmatch(tool, spec)))
    {
        return 2;
    }
    match policy.tool_policies.get(tool) {
        Some(tp) if tp.hitl_required => 3,
        Some(_) => 4,
        None if policy.hitl_config.enabled => 3,
        None => 0,
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
