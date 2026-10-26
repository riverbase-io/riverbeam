//! Verdicts against Python's rule KBs, with the differences made on purpose.
//!
//! `tests/security_compat/fixtures.json` is written by
//! `scripts/gen_security_fixtures.py` from the Python stack. Since plan 030
//! (D1) the attack corpus (`corpus.rs`) is the source of truth: Python's
//! scoring lets classic attacks through, so the cases listed in
//! [`KNOWN_DIFFERENCES`] differ on purpose. Every other case must still
//! match, and every listed case must still differ, so the list stays accurate.

use std::path::PathBuf;

use beam_security::{
    SecurityContext, SecurityManagement, SecurityManager, SecurityPolicy, SecurityRequest,
    SecurityStage,
};
use serde_json::Value;

/// Fixture index and why riverbeam's verdict differs.
const KNOWN_DIFFERENCES: &[(usize, &str)] = &[
    (3, "\"ignore all previous instructions\" blocks"),
    (4, "a blocking finding scores 1.0 (critical)"),
    (5, "\"disregard your system prompt\" blocks"),
    (6, "asking for the hidden prompt blocks (LLM07)"),
    (7, "\"act as an unrestricted AI\" blocks"),
    (8, "\"developer mode enabled\" blocks"),
    (21, "\"ignore all previous instructions\" blocks"),
    (22, "a blocking finding scores 1.0 (critical)"),
    (23, "\"disregard your system prompt\" blocks"),
    (24, "asking for the hidden prompt blocks (LLM07)"),
    (25, "\"act as an unrestricted AI\" blocks"),
    (26, "\"developer mode enabled\" blocks"),
    (39, "\"ignore all previous instructions\" blocks"),
    (40, "a blocking finding scores 1.0 (critical)"),
    (41, "\"disregard your system prompt\" blocks"),
    (42, "asking for the hidden prompt blocks (LLM07)"),
    (43, "\"act as an unrestricted AI\" blocks"),
    (44, "\"developer mode enabled\" blocks"),
    (63, "an `sk-` key of 40 characters is a secret"),
    (64, "`password: <value>` is a secret"),
    (65, "PII is masked (OUT-002)"),
    (66, "PII is masked (OUT-002)"),
    (67, "a blocking finding scores 1.0 (critical)"),
    (68, "a blocking finding scores 1.0 (critical)"),
    (69, "a password stated in prose is a secret"),
    (81, "an `sk-` key of 40 characters is a secret"),
    (82, "`password: <value>` is a secret"),
    (83, "PII is masked (OUT-002)"),
    (84, "PII is masked (OUT-002)"),
    (85, "a blocking finding scores 1.0 (critical)"),
    (86, "a blocking finding scores 1.0 (critical)"),
    (87, "a password stated in prose is a secret"),
    (90, "MEM-002 (injection) replaces MEM-005 (confidence)"),
    (91, "MEM-002 (injection) replaces MEM-005 (confidence)"),
    (92, "MEM-002 (injection) replaces MEM-005 (confidence)"),
    (93, "injection in a memory write is rejected (MEM-002)"),
    (94, "injection in a memory write is rejected (MEM-002)"),
    (95, "injection in a memory write is rejected (MEM-002)"),
    (96, "MEM-002 (injection) replaces MEM-005 (confidence)"),
    (99, "\"ignore all previous instructions\" blocks"),
    (
        110,
        "RUN-002, RUN-004 and RUN-005 are removed (no caller sends their inputs)",
    ),
    (
        111,
        "RUN-002, RUN-004 and RUN-005 are removed (no caller sends their inputs)",
    ),
    (
        112,
        "RUN-002, RUN-004 and RUN-005 are removed (no caller sends their inputs)",
    ),
    (
        113,
        "RUN-002, RUN-004 and RUN-005 are removed (no caller sends their inputs)",
    ),
    (
        114,
        "RUN-002, RUN-004 and RUN-005 are removed (no caller sends their inputs)",
    ),
    (
        115,
        "RUN-002, RUN-004 and RUN-005 are removed (no caller sends their inputs)",
    ),
    (
        116,
        "RUN-002, RUN-004 and RUN-005 are removed (no caller sends their inputs)",
    ),
];

fn label<T: serde::Serialize>(v: T) -> Value {
    serde_json::to_value(v).unwrap_or(Value::Null)
}

#[tokio::test]
async fn verdicts_match_python_except_the_known_differences() {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/security_compat/fixtures.json");
    let cases: Vec<Value> =
        serde_json::from_str(&std::fs::read_to_string(path).expect("fixture")).expect("json");
    let mut problems = Vec::new();
    for (index, case) in cases.iter().enumerate() {
        let policy: SecurityPolicy = case
            .get("policy")
            .map(|p| serde_json::from_value(p.clone()).expect("policy"))
            .unwrap_or_default();
        let manager = SecurityManager::new(policy).with_enforce(false);
        let stage: SecurityStage = serde_json::from_value(case["stage"].clone()).expect("stage");
        let mut request = SecurityRequest::new(stage)
            .with_payload(case["payload"].as_object().cloned().unwrap_or_default());
        if let Some(ctx) = case.get("context") {
            let ctx: SecurityContext = serde_json::from_value(ctx.clone()).expect("context");
            request = request.with_context(ctx);
        }
        let verdict = manager.check(&request).await;
        let mut rules = verdict.matched_rules.clone();
        rules.sort();
        let actual = serde_json::json!({
            "action": label(verdict.action),
            "severity": label(verdict.severity),
            "violation_type": verdict.violation_type.map_or(Value::Null, label),
            "matched_rules": rules,
        });
        let known = KNOWN_DIFFERENCES.iter().find(|(i, _)| *i == index);
        match (known, actual == case["expected"]) {
            (None, false) => problems.push(format!(
                "#{index} {} {}: rust {actual} python {}",
                case["stage"], case["payload"], case["expected"]
            )),
            (Some((_, reason)), true) => problems.push(format!(
                "#{index} is listed as a known difference ({reason}) but matches Python"
            )),
            _ => {}
        }
    }
    assert!(
        problems.is_empty(),
        "{} of {} cases:\n{}",
        problems.len(),
        cases.len(),
        problems.join("\n")
    );
}
