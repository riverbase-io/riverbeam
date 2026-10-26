//! The attack corpus (`tests/security_corpus/cases.json`): every attack gets
//! its expected action and names the patterns that caught it, and every
//! benign case is allowed.

use std::path::PathBuf;

use beam_security::{
    SecurityContext, SecurityManagement, SecurityManager, SecurityPolicy, SecurityRequest,
    SecurityStage,
};
use serde_json::{Map, Value};

fn label<T: serde::Serialize>(v: T) -> String {
    serde_json::to_value(v)
        .ok()
        .and_then(|v| v.as_str().map(str::to_owned))
        .unwrap_or_default()
}

#[tokio::test]
async fn every_corpus_case_gets_its_expected_action() {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/security_corpus/cases.json");
    let cases: Vec<Value> =
        serde_json::from_str(&std::fs::read_to_string(path).expect("corpus")).expect("json");
    let manager = SecurityManager::new(SecurityPolicy::default());
    let mut wrong = Vec::new();
    let mut benign = 0;
    for case in &cases {
        let stage: SecurityStage = serde_json::from_value(case["stage"].clone()).expect("stage");
        let expect = case["expect"].as_str().expect("expect");
        let mut payload = Map::new();
        payload.insert("content".into(), case["text"].clone());
        if stage == SecurityStage::BeforeMemoryWrite {
            payload.insert("operation".into(), Value::String("save".into()));
        }
        for key in ["tool_name", "instructions"] {
            if let Some(value) = case.get(key) {
                payload.insert(key.into(), value.clone());
            }
        }
        let request = SecurityRequest::new(stage)
            .with_payload(payload)
            .with_context(SecurityContext {
                user_id: Some("corpus-user".into()),
                ..SecurityContext::default()
            });
        let decision = manager.check(&request).await;
        let action = label(decision.action);
        let patterns = decision
            .metadata
            .get("matched_patterns")
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        if action != expect || (expect != "allow" && patterns == 0) {
            wrong.push(format!(
                "{} {} {:?}: expected {expect}, got {action} ({})",
                case["owasp"], case["stage"], case["text"], decision.narration
            ));
        }
        if case["owasp"] == "benign" {
            benign += 1;
        }
    }
    assert!(benign >= 30, "the corpus holds {benign} benign cases");
    assert!(
        wrong.is_empty(),
        "{} of {} cases:\n{}",
        wrong.len(),
        cases.len(),
        wrong.join("\n")
    );
}
