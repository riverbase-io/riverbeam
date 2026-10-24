//! GRL rule documents for the security stages.
//!
//! Regex scoring cannot run inside GRL (the `rust-rule-engine` custom-function
//! hook is not wired through `river_reed`'s evaluator), so all OWASP pattern
//! scores and policy-derived flags are precomputed in Rust ([`crate::facts`])
//! and injected as numeric/boolean `Input.*` facts. Each GRL document only
//! compares thresholds and writes the coded narration fields
//! `Result.rule` / `Result.code` / `Result.message`, which the
//! [`narration_from_wm`] adapter lifts back into [`RuleNarration`]s.

use serde_json::Value;

use crate::codes::RuleNarration;
use crate::enums::SecurityStage;

/// Rule set names (one knowledge base, one rule set per security domain).
pub const RS_INPUT: &str = "input";
pub const RS_OUTPUT: &str = "output";
pub const RS_RAG: &str = "rag";
pub const RS_TOOL: &str = "tool";
pub const RS_MEMORY: &str = "memory";
pub const RS_RUNTIME: &str = "runtime";

/// `(rule_name, grl_document, priority)` triples for each rule set.
pub type RuleSpec = (&'static str, &'static str, i32);

/// All rule sets, each rule mirroring one Python rule function (same key and
/// order, ALLOW narrations included), so verdicts aggregate the same way.
#[must_use]
pub fn rulesets() -> Vec<(&'static str, Vec<RuleSpec>)> {
    vec![
        (
            RS_INPUT,
            vec![
                ("input_inj_blocked", INPUT_INJ_BLOCKED, 0),
                ("input_inj_flagged", INPUT_INJ_FLAGGED, 0),
                ("input_role_override", INPUT_ROLE_OVERRIDE, 0),
                ("input_identity_required", INPUT_IDENTITY_REQUIRED, 0),
            ],
        ),
        (
            RS_OUTPUT,
            vec![
                ("out_secrets", OUTPUT_SECRETS, 0),
                ("out_pii", OUTPUT_PII, 0),
            ],
        ),
        (RS_RAG, vec![("rag_injection", RAG_INJECTION, 0)]),
        (
            RS_TOOL,
            vec![
                ("tool_denied_global", TOOL_DENIED_GLOBAL, 0),
                ("tool_not_in_allowlist", TOOL_NOT_IN_ALLOWLIST, 0),
                ("tool_hitl_required", TOOL_HITL_REQUIRED, 0),
                ("tool_allowed_default", TOOL_ALLOWED_DEFAULT, 0),
            ],
        ),
        (
            RS_MEMORY,
            vec![
                ("mem_sensitive_reject", MEM_SENSITIVE_REJECT, 0),
                ("mem_poisoning", MEM_POISONING, 0),
                ("mem_passes", MEM_PASSES, 0),
            ],
        ),
        (
            RS_RUNTIME,
            vec![
                ("run_max_steps", RUN_MAX_STEPS, 0),
                ("run_timeout", RUN_TIMEOUT, 0),
                ("run_token_budget", RUN_TOKEN_BUDGET, 0),
                ("run_loop", RUN_LOOP, 0),
                ("run_recursion", RUN_RECURSION, 0),
                ("runtime_ok", RUNTIME_OK, 0),
            ],
        ),
    ]
}

/// A rule's position in declaration order (Python runs a KB's rules in
/// `gen_rules` order, and the verdict's stable sort keeps it among equal codes).
#[must_use]
pub fn rule_order(name: &str) -> usize {
    rulesets()
        .iter()
        .flat_map(|(_, rules)| rules.iter().map(|(n, _, _)| *n))
        .position(|n| n == name)
        .unwrap_or(usize::MAX)
}

/// Maps a routable [`SecurityStage`] to the rule set that evaluates it.
///
/// Mirrors the Python stage routing (`security/engines/routing.py`): there is no
/// dedicated RAG stage — `BeforeContextInjection` reuses the input rules — but
/// the `rag` rule set is still seeded for direct/forward use.
#[must_use]
#[allow(clippy::unnecessary_wraps)] // Every stage routes today; callers keep the unrouted case.
pub fn ruleset_for_stage(stage: SecurityStage) -> Option<&'static str> {
    match stage {
        SecurityStage::BeforeInput
        | SecurityStage::BeforeContextInjection
        | SecurityStage::BeforeLlm
        | SecurityStage::BeforeHitlResume => Some(RS_INPUT),
        SecurityStage::AfterLlm | SecurityStage::BeforeOutput => Some(RS_OUTPUT),
        SecurityStage::BeforeTool | SecurityStage::AfterTool => Some(RS_TOOL),
        SecurityStage::BeforeMemoryWrite => Some(RS_MEMORY),
        SecurityStage::Runtime => Some(RS_RUNTIME),
    }
}

/// Lifts a rule's GRL working memory into a coded narration, or `None` when the
/// rule did not fire (no `Result` written).
#[must_use]
pub fn narration_from_wm(wm: &Value) -> Option<RuleNarration> {
    let res = wm.get("Result")?;
    let code = i32::try_from(res.get("code").and_then(Value::as_i64)?).ok()?;
    let rule = res.get("rule").and_then(Value::as_str).unwrap_or("");
    let message = res.get("message").and_then(Value::as_str).unwrap_or("");
    Some(RuleNarration::new(rule, code, message))
}

const INPUT_INJ_BLOCKED: &str = r#"
rule "PI001_0" salience 30 {
    when
        Input.content_empty == true
    then
        Result.rule = "PI-001";
        Result.code = 200;
        Result.message = "input_checked";
}
rule "PI001_1" salience 20 {
    when
        Input.content_empty == false && Input.injection_score >= 0.7
    then
        Result.rule = "PI-001";
        Result.code = 400;
        Result.message = "injection_blocked";
}
rule "PI001_2" salience 10 {
    when
        Input.content_empty == false && Input.injection_score < 0.7
    then
        Result.rule = "PI-001";
        Result.code = 200;
        Result.message = "inj_user_input_passed";
}
"#;

const INPUT_INJ_FLAGGED: &str = r#"
rule "PI001_0" salience 20 {
    when
        Input.content_empty == true
    then
        Result.rule = "PI-001";
        Result.code = 200;
        Result.message = "input_checked";
}
rule "PI001_1" salience 10 {
    when
        Input.content_empty == false && Input.injection_score >= 0.4 && Input.injection_score < 0.7
    then
        Result.rule = "PI-001";
        Result.code = 300;
        Result.message = "injection_flagged";
}
"#;

const INPUT_ROLE_OVERRIDE: &str = r#"
rule "PI008_0" salience 20 {
    when
        Input.content_empty == true
    then
        Result.rule = "PI-008";
        Result.code = 200;
        Result.message = "input_checked";
}
rule "PI008_1" salience 10 {
    when
        Input.content_empty == false && Input.role_override_score >= 0.4
    then
        Result.rule = "PI-008";
        Result.code = 400;
        Result.message = "role_override_detected";
}
"#;

const INPUT_IDENTITY_REQUIRED: &str = r#"
rule "IAM001_0" salience 20 {
    when
        Input.identity_missing == true
    then
        Result.rule = "IAM-001";
        Result.code = 400;
        Result.message = "identity_missing";
}
rule "IAM001_1" salience 10 {
    when
        Input.identity_missing == false
    then
        Result.rule = "IAM-001";
        Result.code = 200;
        Result.message = "identity_ok";
}
"#;

const OUTPUT_SECRETS: &str = r#"
rule "OUT001_0" salience 30 {
    when
        Input.content_empty == true
    then
        Result.rule = "OUT-001";
        Result.code = 200;
        Result.message = "output_checked";
}
rule "OUT001_1" salience 20 {
    when
        Input.content_empty == false && Input.secrets_score >= 0.4
    then
        Result.rule = "OUT-001";
        Result.code = 400;
        Result.message = "secrets_detected";
}
rule "OUT001_2" salience 10 {
    when
        Input.content_empty == false && Input.secrets_score < 0.4
    then
        Result.rule = "OUT-001";
        Result.code = 200;
        Result.message = "output_checked";
}
"#;

const OUTPUT_PII: &str = r#"
rule "OUT002_0" salience 30 {
    when
        Input.content_empty == true
    then
        Result.rule = "OUT-002";
        Result.code = 200;
        Result.message = "output_checked";
}
rule "OUT002_1" salience 20 {
    when
        Input.content_empty == false && Input.pii_score >= 0.4
    then
        Result.rule = "OUT-002";
        Result.code = 300;
        Result.message = "pii_detected";
}
rule "OUT002_2" salience 10 {
    when
        Input.content_empty == false && Input.pii_score < 0.4
    then
        Result.rule = "OUT-002";
        Result.code = 200;
        Result.message = "output_checked";
}
"#;

const TOOL_DENIED_GLOBAL: &str = r#"
rule "TOOL001_0" salience 20 {
    when
        Input.tool_denied_global == true
    then
        Result.rule = "TOOL-001";
        Result.code = 400;
        Result.message = "tool_denied_global";
}
rule "TOOL001_1" salience 10 {
    when
        Input.tool_denied_global == false
    then
        Result.rule = "TOOL-001";
        Result.code = 200;
        Result.message = "tool_allowed_by_global_deny_check";
}
"#;

const TOOL_NOT_IN_ALLOWLIST: &str = r#"
rule "TOOL001_0" salience 20 {
    when
        Input.tool_denied_allowlist == true
    then
        Result.rule = "TOOL-001";
        Result.code = 400;
        Result.message = "tool_denied_not_in_global_allowlist";
}
rule "TOOL001_1" salience 10 {
    when
        Input.tool_denied_allowlist == false
    then
        Result.rule = "TOOL-001";
        Result.code = 200;
        Result.message = "tool_in_global_allowlist";
}
"#;

const TOOL_HITL_REQUIRED: &str = r#"
rule "HITL001_0" salience 40 {
    when
        Input.hitl_outcome == 1
    then
        Result.rule = "HITL-001";
        Result.code = 200;
        Result.message = "hitl_approved";
}
rule "HITL001_1" salience 30 {
    when
        Input.hitl_outcome == 2
    then
        Result.rule = "HITL-001";
        Result.code = 200;
        Result.message = "hitl_auto_approved";
}
rule "HITL001_2" salience 20 {
    when
        Input.hitl_outcome == 3
    then
        Result.rule = "HITL-001";
        Result.code = 350;
        Result.message = "tool_hitl_required";
}
rule "HITL001_3" salience 10 {
    when
        Input.hitl_outcome == 4
    then
        Result.rule = "HITL-001";
        Result.code = 200;
        Result.message = "tool_hitl_not_required";
}
"#;

const TOOL_ALLOWED_DEFAULT: &str = r#"
rule "TOOL001_0" salience 10 {
    when
        Input.always == true
    then
        Result.rule = "TOOL-001";
        Result.code = 200;
        Result.message = "tool_allowed";
}
"#;

const RAG_INJECTION: &str = r#"
rule "RAG001_0" salience 20 {
    when
        Input.injection_score >= 0.7
    then
        Result.rule = "RAG-001";
        Result.code = 400;
        Result.message = "rag_injection_detected";
}
rule "RAG002_1" salience 10 {
    when
        Input.injection_score >= 0.4 && Input.injection_score < 0.7
    then
        Result.rule = "RAG-002";
        Result.code = 300;
        Result.message = "rag_chunk_flagged";
}
"#;

const MEM_SENSITIVE_REJECT: &str = r#"
rule "MEM001_0" salience 30 {
    when
        Input.memory_skip == true
    then
        Result.rule = "MEM-001";
        Result.code = 200;
        Result.message = "memory_checked";
}
rule "MEM001_1" salience 20 {
    when
        Input.memory_skip == false && Input.sensitive_score >= 0.4
    then
        Result.rule = "MEM-001";
        Result.code = 400;
        Result.message = "memory_sensitive_data_rejected";
}
rule "MEM001_2" salience 10 {
    when
        Input.memory_skip == false && Input.sensitive_score < 0.4
    then
        Result.rule = "MEM-001";
        Result.code = 200;
        Result.message = "memory_check_pass";
}
"#;

const MEM_POISONING: &str = r#"
rule "MEM005_0" salience 20 {
    when
        Input.confidence >= 0.7
    then
        Result.rule = "MEM-005";
        Result.code = 300;
        Result.message = "memory_poisoning_suspected";
}
rule "MEM005_1" salience 10 {
    when
        Input.confidence < 0.7
    then
        Result.rule = "MEM-005";
        Result.code = 200;
        Result.message = "memory_poisoning_not_suspected";
}
"#;

const MEM_PASSES: &str = r#"
rule "MEM001_0" salience 10 {
    when
        Input.always == true
    then
        Result.rule = "MEM-001";
        Result.code = 200;
        Result.message = "memory_check_pass";
}
"#;

const RUN_MAX_STEPS: &str = r#"
rule "RUN001_0" salience 30 {
    when
        Input.step_count > Input.max_steps
    then
        Result.rule = "RUN-001";
        Result.code = 400;
        Result.message = "run_max_steps_exceeded";
}
rule "RUN001_1" salience 20 {
    when
        Input.step_count <= Input.max_steps && Input.step_count > Input.step_threshold
    then
        Result.rule = "RUN-001";
        Result.code = 300;
        Result.message = "run_step_count_approaching";
}
rule "RUN001_2" salience 10 {
    when
        Input.step_count <= Input.step_threshold
    then
        Result.rule = "RUN-001";
        Result.code = 200;
        Result.message = "runtime_ok";
}
"#;

const RUN_TIMEOUT: &str = r#"
rule "RUN002_0" salience 20 {
    when
        Input.elapsed_seconds > Input.max_timeout
    then
        Result.rule = "RUN-002";
        Result.code = 400;
        Result.message = "run_timeout_exceeded";
}
rule "RUN002_1" salience 10 {
    when
        Input.elapsed_seconds <= Input.max_timeout
    then
        Result.rule = "RUN-002";
        Result.code = 200;
        Result.message = "runtime_ok";
}
"#;

const RUN_TOKEN_BUDGET: &str = r#"
rule "RUN003_0" salience 20 {
    when
        Input.token_count > Input.max_tokens
    then
        Result.rule = "RUN-003";
        Result.code = 400;
        Result.message = "run_token_budget_exceeded";
}
rule "RUN003_1" salience 10 {
    when
        Input.token_count <= Input.max_tokens
    then
        Result.rule = "RUN-003";
        Result.code = 200;
        Result.message = "runtime_ok";
}
"#;

const RUN_LOOP: &str = r#"
rule "RUN004_0" salience 20 {
    when
        Input.loop_detected == true
    then
        Result.rule = "RUN-004";
        Result.code = 400;
        Result.message = "run_loop_detected";
}
rule "RUN004_1" salience 10 {
    when
        Input.loop_detected == false
    then
        Result.rule = "RUN-004";
        Result.code = 200;
        Result.message = "runtime_ok";
}
"#;

const RUN_RECURSION: &str = r#"
rule "RUN005_0" salience 20 {
    when
        Input.recursion_depth > Input.max_recursion
    then
        Result.rule = "RUN-005";
        Result.code = 400;
        Result.message = "run_recursion_exceeded";
}
rule "RUN005_1" salience 10 {
    when
        Input.recursion_depth <= Input.max_recursion
    then
        Result.rule = "RUN-005";
        Result.code = 200;
        Result.message = "runtime_ok";
}
"#;

const RUNTIME_OK: &str = r#"
rule "RUN001_0" salience 10 {
    when
        Input.always == true
    then
        Result.rule = "RUN-001";
        Result.code = 200;
        Result.message = "runtime_ok";
}
"#;
