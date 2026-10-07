use serde_json::{json, Map, Value};

use crate::codes::{RuleNarration, BLOCK_CODE, FLAG_CODE, HITL_CODE, REJECT_CODE};
use crate::decision::SecurityDecision;
use crate::enums::{SecurityAction, SecuritySeverity, ViolationType};

fn code_to_action(code: i32) -> SecurityAction {
    match code {
        FLAG_CODE => SecurityAction::Warn,
        HITL_CODE => SecurityAction::RequireApproval,
        BLOCK_CODE | REJECT_CODE => SecurityAction::Deny,
        _ => SecurityAction::Allow, // ALLOW_CODE + unknown
    }
}

fn message_to_violation(message: &str) -> Option<ViolationType> {
    // Ordered to match the Python dict insertion order.
    const MAP: &[(&str, ViolationType)] = &[
        ("injection_blocked", ViolationType::InjectionUserInput),
        ("injection_flagged", ViolationType::InjectionUserInput),
        ("injection_detected", ViolationType::InjectionUserInput),
        ("role_override", ViolationType::InjectionRoleOverride),
        ("memory_sensitive_data", ViolationType::MemorySensitiveData),
        ("secrets_redacted", ViolationType::OutputSecretDetected),
        ("secrets_detected", ViolationType::OutputSecretDetected),
        ("pii_detected", ViolationType::OutputPiiDetected),
        ("unsafe_content_detected", ViolationType::OutputUnsafeContent),
        ("rag_cross_tenant", ViolationType::RagTenantFilterViolation),
        ("rag_chunk_no_metadata", ViolationType::RagSensitiveDocument),
        ("rag_sensitive_document", ViolationType::RagSensitiveDocument),
        ("rag_injection_detected", ViolationType::InjectionRagChunk),
        ("tool_denied_global", ViolationType::UnauthorizedTool),
        ("tool_denied_not_in_global_allowlist", ViolationType::UnauthorizedTool),
        ("tool_denied_agent", ViolationType::UnauthorizedTool),
        ("tool_denied_agent_not_allowed", ViolationType::UnauthorizedTool),
        ("tool_denied_plan", ViolationType::UnauthorizedTool),
        ("tool_denied_plan_not_allowed", ViolationType::UnauthorizedTool),
        ("tool_hitl_required", ViolationType::HitlRequiredNotObtained),
        ("memory_cross_tenant", ViolationType::MemoryScopeViolation),
        ("memory_poisoning_suspected", ViolationType::MemoryPoisoningSuspected),
        ("run_max_steps_exceeded", ViolationType::RunMaxStepsExceeded),
        ("run_timeout_exceeded", ViolationType::RunTimeoutExceeded),
        ("run_token_budget_exceeded", ViolationType::RunTokenBudgetExceeded),
        ("run_loop_detected", ViolationType::RunLoopDetected),
        ("run_recursion_exceeded", ViolationType::RunRecursionExceeded),
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
    sorted.sort_by(|a, b| b.code.cmp(&a.code));

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
