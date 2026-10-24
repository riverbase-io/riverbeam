//! The deterministic evaluators score like Python's
//! (`tests/revision_compat/fixtures.json`, written by
//! `scripts/gen_revision_fixtures.py`).

use beam_evals::evaluators::by_name;
use beam_evals::EvaluationCase;
use beam_types::{AgentInput, AgentResult};
use serde_json::Value;
use uuid::Uuid;

#[tokio::test]
async fn evaluators_score_like_python() {
    let fixtures: Value =
        serde_json::from_str(include_str!("../../../tests/revision_compat/fixtures.json"))
            .expect("fixtures");
    for row in fixtures["evaluators"].as_array().expect("evaluators") {
        let case = EvaluationCase {
            id: Uuid::new_v4(),
            dataset_id: Uuid::from_u128(1),
            input: AgentInput::text("q"),
            expected: Some(row["expected"].clone()).filter(|e| !e.is_null()),
        };
        let result: AgentResult = serde_json::from_value(row["result"].clone()).expect("result");
        for (name, want) in row["scores"].as_object().expect("scores") {
            let evaluator = by_name(name).expect("evaluator");
            let got =
                serde_json::to_value(evaluator.evaluate(&case, &result).await).expect("score");
            assert_eq!(&got, want, "{name} on {}", row["expected"]);
        }
    }
}
