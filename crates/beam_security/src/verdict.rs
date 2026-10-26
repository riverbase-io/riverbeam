use serde_json::{json, Map, Value};

use crate::codes::{RuleNarration, BLOCK_CODE, FLAG_CODE, HITL_CODE, MASK_CODE, REJECT_CODE};
use crate::decision::SecurityDecision;
use crate::detect::{highest, Finding};
use crate::enums::{SecurityAction, SecuritySeverity, ViolationType};

fn code_to_action(code: i32) -> SecurityAction {
    match code {
        FLAG_CODE => SecurityAction::Warn,
        MASK_CODE => SecurityAction::Mask,
        HITL_CODE => SecurityAction::RequireApproval,
        BLOCK_CODE | REJECT_CODE => SecurityAction::Deny,
        _ => SecurityAction::Allow, // ALLOW_CODE + unknown
    }
}

fn message_to_violation(message: &str) -> Option<ViolationType> {
    // The first message part found names the violation, so a longer message
    // comes before a shorter one it contains.
    const MAP: &[(&str, ViolationType)] = &[
        // Indirect injection: the label is the retrieved-content one.
        ("tool_output_injection", ViolationType::InjectionRagChunk),
        ("injection_blocked", ViolationType::InjectionUserInput),
        ("injection_flagged", ViolationType::InjectionUserInput),
        ("role_override", ViolationType::InjectionRoleOverride),
        ("memory_sensitive_data", ViolationType::MemorySensitiveData),
        ("secrets_detected", ViolationType::OutputSecretDetected),
        ("secrets_flagged", ViolationType::OutputSecretDetected),
        ("pii_detected", ViolationType::OutputPiiDetected),
        // The instructions are confidential (LLM07).
        ("prompt_echo", ViolationType::OutputSecretDetected),
        ("tool_denied_global", ViolationType::UnauthorizedTool),
        (
            "tool_denied_not_in_global_allowlist",
            ViolationType::UnauthorizedTool,
        ),
        ("tool_hitl_required", ViolationType::HitlRequiredNotObtained),
        (
            "memory_poisoning_suspected",
            ViolationType::MemoryPoisoningSuspected,
        ),
        ("run_max_steps_exceeded", ViolationType::RunMaxStepsExceeded),
        (
            "run_token_budget_exceeded",
            ViolationType::RunTokenBudgetExceeded,
        ),
        ("identity_missing", ViolationType::IdentityMissing),
    ];
    MAP.iter()
        .find(|(prefix, _)| message.contains(prefix))
        .map(|(_, v)| *v)
}

fn score_from_message(message: &str) -> f64 {
    let text = message.to_lowercase();
    if text.contains("blocked") || text.contains("exceeded") || text.contains("denied") {
        1.0
    } else if text.contains("flagged")
        || text.contains("detected")
        || text.contains("suspicious")
        || text.contains("high")
    {
        0.7
    } else if text.contains("warning") || text.contains("approaching") {
        0.5
    } else {
        0.8
    }
}

fn severity_from_score(score: f64) -> SecuritySeverity {
    if score >= 0.9 {
        SecuritySeverity::Critical
    } else if score >= 0.7 {
        SecuritySeverity::High
    } else if score >= 0.45 {
        SecuritySeverity::Medium
    } else {
        SecuritySeverity::Low
    }
}

fn narration_to_value(n: &RuleNarration) -> Value {
    json!({
        "message": n.message,
        "code": n.code,
        "rule": n.rule,
        "ruleset": n.ruleset,
        "revision": n.revision,
    })
}

/// Aggregate narrations into a [`SecurityDecision`] (ports `verdict_from_narrations`).
#[must_use]
pub fn verdict_from_narrations(
    narrations: &[RuleNarration],
    sanitized_content: Option<String>,
    extra_metadata: Option<Map<String, Value>>,
) -> SecurityDecision {
    if narrations.is_empty() {
        return SecurityDecision::allow();
    }

    // Stable sort by code descending (matches Python `sorted(..., reverse=True)`).
    let mut sorted: Vec<&RuleNarration> = narrations.iter().collect();
    sorted.sort_by_key(|n| std::cmp::Reverse(n.code));

    let top = sorted[0];
    let action = code_to_action(top.code);
    let violation = message_to_violation(&top.message);
    let score = score_from_message(&top.message);

    let mut meta = Map::new();
    meta.insert(
        "narrations".into(),
        Value::Array(sorted.iter().map(|n| narration_to_value(n)).collect()),
    );
    meta.insert("narration_count".into(), json!(narrations.len()));
    if let Some(v) = violation {
        meta.insert(
            "violation_type".into(),
            serde_json::to_value(v).unwrap_or(Value::Null),
        );
    }
    if let Some(extra) = extra_metadata {
        for (k, v) in extra {
            meta.insert(k, v);
        }
    }

    let mut matched_rules: Vec<String> = Vec::new();
    for n in &sorted {
        if !n.rule.is_empty() && !matched_rules.contains(&n.rule) {
            matched_rules.push(n.rule.clone());
        }
    }

    let messages: Vec<&str> = sorted
        .iter()
        .filter(|n| !n.message.is_empty())
        .map(|n| n.message.as_str())
        .collect();
    let narration = if messages.is_empty() {
        matched_rules.join(" ")
    } else {
        messages.join(" ")
    };

    let user_message = if action == SecurityAction::Deny {
        Some("This request could not be completed for security reasons.".to_string())
    } else {
        None
    };

    SecurityDecision {
        action,
        severity: severity_from_score(score),
        matched_rules,
        narration,
        user_message,
        metadata: meta,
        violation_type: violation,
        score,
        sanitized_content,
    }
}

/// The detectors' part of a decision: the ids of the patterns that matched
/// (`metadata["matched_patterns"]`, in order, once each), and the score and
/// severity of the highest finding.
pub(crate) fn apply_findings(decision: &mut SecurityDecision, findings: &[Finding]) {
    let Some(top) = highest(findings) else {
        return;
    };
    let mut ids: Vec<&str> = Vec::new();
    for finding in findings {
        if !ids.contains(&finding.pattern_id.as_str()) {
            ids.push(&finding.pattern_id);
        }
    }
    decision
        .metadata
        .insert("matched_patterns".into(), json!(ids));
    decision.score = top.score();
    decision.severity = severity_from_score(decision.score);
}
