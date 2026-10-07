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

/// All rule sets seeded into the knowledge base, in declaration order.
#[must_use]
pub fn rulesets() -> Vec<(&'static str, Vec<RuleSpec>)> {
    vec![
        (RS_INPUT, vec![("input_injection", INPUT_INJECTION, 0), ("input_role_override", INPUT_ROLE_OVERRIDE, 0)]),
        (RS_OUTPUT, vec![("output_secrets", OUTPUT_SECRETS, 0), ("output_pii", OUTPUT_PII, 0)]),
        (RS_RAG, vec![("rag_injection", RAG_INJECTION, 0)]),
        (
            RS_TOOL,
            vec![
                ("tool_denied_global", TOOL_DENIED_GLOBAL, 0),
                ("tool_denied_allowlist", TOOL_DENIED_ALLOWLIST, 0),
                ("tool_hitl_required", TOOL_HITL_REQUIRED, 0),
            ],
        ),
        (RS_MEMORY, vec![("memory_sensitive", MEMORY_SENSITIVE, 0), ("memory_poisoning", MEMORY_POISONING, 0)]),
        (
            RS_RUNTIME,
            vec![
                ("run_steps", RUN_STEPS, 0),
                ("run_timeout", RUN_TIMEOUT, 0),
                ("run_tokens", RUN_TOKENS, 0),
                ("run_loop", RUN_LOOP, 0),
                ("run_recursion", RUN_RECURSION, 0),
            ],
        ),
    ]
}

/// Maps a routable [`SecurityStage`] to the rule set that evaluates it.
///
/// Mirrors the Python stage routing (`security/engines/routing.py`): there is no
/// dedicated RAG stage — `BeforeContextInjection` reuses the input rules — but
/// the `rag` rule set is still seeded for direct/forward use.
#[must_use]
pub fn ruleset_for_stage(stage: SecurityStage) -> Option<&'static str> {
    match stage {
        SecurityStage::BeforeInput | SecurityStage::BeforeContextInjection => Some(RS_INPUT),
        SecurityStage::AfterLlm | SecurityStage::BeforeOutput => Some(RS_OUTPUT),
        SecurityStage::BeforeTool => Some(RS_TOOL),
        SecurityStage::BeforeMemoryWrite => Some(RS_MEMORY),
        SecurityStage::Runtime => Some(RS_RUNTIME),
        SecurityStage::BeforeLlm | SecurityStage::AfterTool | SecurityStage::BeforeHitlResume => None,
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

// --- input (PI-001 injection, PI-008 role override) ------------------------

const INPUT_INJECTION: &str = r#"
rule "PI001_block" salience 20 {
    when
        Input.injection_score >= 0.7
    then
        Result.rule = "PI-001";
        Result.code = 400;
        Result.message = "injection_blocked";
}
rule "PI001_flag" salience 10 {
    when
        Input.injection_score >= 0.4 && Input.injection_score < 0.7
    then
        Result.rule = "PI-001";
        Result.code = 300;
        Result.message = "injection_flagged";
}
"#;

const INPUT_ROLE_OVERRIDE: &str = r#"
rule "PI008_role_override" salience 10 {
    when
        Input.role_override_score >= 0.4
    then
        Result.rule = "PI-008";
        Result.code = 400;
        Result.message = "role_override_detected";
}
"#;

// --- output (OUT-001 secrets, OUT-002 pii) ---------------------------------

const OUTPUT_SECRETS: &str = r#"
rule "OUT001_secrets" salience 10 {
    when
        Input.secrets_score >= 0.4
    then
        Result.rule = "OUT-001";
        Result.code = 400;
        Result.message = "secrets_detected";
}
"#;

const OUTPUT_PII: &str = r#"
rule "OUT002_pii" salience 10 {
    when
        Input.pii_score >= 0.4
    then
        Result.rule = "OUT-002";
        Result.code = 300;
        Result.message = "pii_detected";
}
"#;

// --- rag (RAG-001 block, RAG-002 flag) -------------------------------------

const RAG_INJECTION: &str = r#"
rule "RAG001_block" salience 20 {
    when
        Input.injection_score >= 0.7
    then
        Result.rule = "RAG-001";
        Result.code = 400;
        Result.message = "rag_injection_detected";
}
rule "RAG002_flag" salience 10 {
    when
        Input.injection_score >= 0.4 && Input.injection_score < 0.7
    then
        Result.rule = "RAG-002";
        Result.code = 300;
        Result.message = "rag_chunk_flagged";
}
"#;

// --- tool (TOOL-001 deny, HITL-001 approval) -------------------------------

const TOOL_DENIED_GLOBAL: &str = r#"
rule "TOOL001_global" salience 10 {
    when
        Input.tool_denied_global == true
    then
        Result.rule = "TOOL-001";
        Result.code = 400;
        Result.message = "tool_denied_global";
}
"#;

const TOOL_DENIED_ALLOWLIST: &str = r#"
rule "TOOL001_allowlist" salience 10 {
    when
        Input.tool_denied_allowlist == true
    then
        Result.rule = "TOOL-001";
        Result.code = 400;
        Result.message = "tool_denied_not_in_global_allowlist";
}
"#;

const TOOL_HITL_REQUIRED: &str = r#"
rule "HITL001_required" salience 10 {
    when
        Input.tool_hitl_required == true
    then
        Result.rule = "HITL-001";
        Result.code = 350;
        Result.message = "tool_hitl_required";
}
"#;

// --- memory (MEM-001 sensitive, MEM-005 poisoning) -------------------------

const MEMORY_SENSITIVE: &str = r#"
rule "MEM001_sensitive" salience 10 {
    when
        Input.memory_sensitive == true
    then
        Result.rule = "MEM-001";
        Result.code = 400;
        Result.message = "memory_sensitive_data_rejected";
}
"#;

const MEMORY_POISONING: &str = r#"
rule "MEM005_poisoning" salience 10 {
    when
        Input.confidence >= 0.7
    then
        Result.rule = "MEM-005";
        Result.code = 300;
        Result.message = "memory_poisoning_suspected";
}
"#;

// --- runtime (RUN-001..005) -----------------------------------------------

const RUN_STEPS: &str = r#"
rule "RUN001_block" salience 20 {
    when
        Input.step_count > Input.max_steps
    then
        Result.rule = "RUN-001";
        Result.code = 400;
        Result.message = "run_max_steps_exceeded";
}
rule "RUN001_flag" salience 10 {
    when
        Input.step_approaching == true
    then
        Result.rule = "RUN-001";
        Result.code = 300;
        Result.message = "run_step_count_approaching";
}
"#;

const RUN_TIMEOUT: &str = r#"
rule "RUN002_timeout" salience 10 {
    when
        Input.elapsed_seconds > Input.max_timeout
    then
        Result.rule = "RUN-002";
        Result.code = 400;
        Result.message = "run_timeout_exceeded";
}
"#;

const RUN_TOKENS: &str = r#"
rule "RUN003_tokens" salience 10 {
    when
        Input.token_count > Input.max_tokens
    then
        Result.rule = "RUN-003";
        Result.code = 400;
        Result.message = "run_token_budget_exceeded";
}
"#;

const RUN_LOOP: &str = r#"
rule "RUN004_loop" salience 10 {
    when
        Input.loop_detected == true
    then
        Result.rule = "RUN-004";
        Result.code = 400;
        Result.message = "run_loop_detected";
}
"#;

const RUN_RECURSION: &str = r#"
rule "RUN005_recursion" salience 10 {
    when
        Input.recursion_depth > Input.max_recursion
    then
        Result.rule = "RUN-005";
        Result.code = 400;
        Result.message = "run_recursion_exceeded";
}
"#;
