//! GRL verdicts against Python's rule KBs.
//!
//! `tests/security_compat/fixtures.json` is written by
//! `scripts/gen_security_fixtures.py` from the Python stack.
#![cfg(feature = "rules")]

use std::path::PathBuf;

use beam_security::{
    SecurityContext, SecurityManagement, SecurityManager, SecurityPolicy, SecurityRequest,
    SecurityStage,
};
use serde_json::Value;

fn label<T: serde::Serialize>(v: T) -> Value {
    serde_json::to_value(v).unwrap_or(Value::Null)
}

#[tokio::test]
async fn verdicts_match_python() {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/security_compat/fixtures.json");
    let cases: Vec<Value> =
        serde_json::from_str(&std::fs::read_to_string(path).expect("fixture")).expect("json");
    let mut mismatches = Vec::new();
    for case in &cases {
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
        if actual != case["expected"] {
            mismatches.push(format!(
                "{} {}: rust {actual} python {}",
                case["stage"], case["payload"], case["expected"]
            ));
        }
    }
    assert!(
        mismatches.is_empty(),
        "{} of {} differ:\n{}",
        mismatches.len(),
        cases.len(),
        mismatches.join("\n")
    );
}
