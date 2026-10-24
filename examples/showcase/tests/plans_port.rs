//! The showcases' usage plans equal Python's `examples/beam_app` plans,
//! replayed from `fixtures/example_app.json` (`scripts/gen_example_app_fixtures.py`).

use beam_usage::UsagePlan;
use serde_json::Value;
use showcase::host::plans;

fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/example_app.json")).expect("fixture parses")
}

#[test]
fn plans_match_python() {
    let expected: Vec<UsagePlan> =
        serde_json::from_value(fixture()["plans"].clone()).expect("plans");
    assert_eq!(plans::all_plans(), expected);
    assert_eq!(plans::DEFAULT_PLAN, "free");
}
