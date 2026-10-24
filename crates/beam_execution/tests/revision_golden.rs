//! Python's revision contracts (`tests/revision_compat/fixtures.json`,
//! written by `scripts/gen_revision_fixtures.py`): spec dumps and revision
//! hashes, identity hashes, canary buckets, and the lifecycle and
//! compatibility truth tables.

use std::collections::BTreeSet;

use beam_execution::revisions::{
    checkpoint_may_read, compute_revision_hash, may_resume, pick_revision, snapshot_from_spec,
    transition, BoundRuntime, CheckpointVersions, RevisionHashParts,
};
use beam_types::{AgentSpec, CompatibilityPolicy, RevisionStatus};
use serde_json::Value;
use uuid::Uuid;

fn fixtures() -> Value {
    serde_json::from_str(include_str!("../../../tests/revision_compat/fixtures.json"))
        .expect("fixtures")
}

fn policy(raw: &Value) -> CompatibilityPolicy {
    serde_json::from_value(raw.clone()).expect("policy")
}

/// A spec compiled in Python and in Rust has the same
/// revision hash and policy revisions.
#[test]
fn compiled_specs_hash_like_python() {
    for case in fixtures()["specs"].as_array().expect("specs") {
        if case.get("error").is_some() {
            continue;
        }
        let spec: AgentSpec = serde_json::from_value(case["spec"].clone()).expect("spec");
        assert_eq!(spec.canonical_payload(), case["dump"], "{}", case["spec"]);
        let snap = snapshot_from_spec(&spec, &BoundRuntime::default());
        let field = |name: &str| case[name].as_str().map(str::to_owned);
        assert_eq!(
            Some(snap.revision_hash.clone()),
            field("revision_hash"),
            "{}",
            case["spec"]
        );
        assert_eq!(
            snap.toolset_revision,
            field("toolset_revision"),
            "toolset {}",
            case["spec"]
        );
        assert_eq!(
            snap.prompt_revision,
            field("prompt_revision"),
            "prompt {}",
            case["spec"]
        );
        assert_eq!(snap.model_policy_revision, field("model_policy_revision"));
        assert_eq!(
            snap.security_policy_revision,
            field("security_policy_revision"),
            "security {}",
            case["spec"]
        );
        assert_eq!(
            snap.memory_policy_revision,
            field("memory_policy_revision"),
            "memory {}",
            case["spec"]
        );
        assert_eq!(
            snap.knowledge_policy_revision,
            field("knowledge_policy_revision"),
            "knowledge {}",
            case["spec"]
        );
        assert_eq!(
            snap.input_schema_hash,
            field("input_schema_hash"),
            "input {}",
            case["spec"]
        );
        assert_eq!(
            snap.output_schema_hash,
            field("output_schema_hash"),
            "output {}",
            case["spec"]
        );
    }
}

#[test]
fn identity_hashes_match_python() {
    for case in fixtures()["revision_hashes"].as_array().expect("hashes") {
        let args = &case["args"];
        let kw = |k: &str| case["kwargs"][k].as_str();
        let got = compute_revision_hash(
            args[0].as_str().unwrap(),
            args[1].as_str().unwrap(),
            args[2].as_str().unwrap(),
            RevisionHashParts {
                build_id: args[3].as_str(),
                toolset_revision: kw("toolset_revision"),
                prompt_revision: kw("prompt_revision"),
                model_policy_revision: kw("model_policy_revision"),
                security_policy_revision: kw("security_policy_revision"),
            },
        );
        assert_eq!(got, case["expected"].as_str().unwrap());
    }
}

/// Every execution id lands on Python's side of the canary.
#[test]
fn canary_buckets_match_python() {
    let canary = &fixtures()["canary"][0];
    let ids: Vec<Uuid> = canary["ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| Uuid::parse_str(i.as_str().unwrap()).unwrap())
        .collect();
    let (primary, can) = (Uuid::from_u128(1), Uuid::from_u128(2));
    for split in canary["splits"].as_array().unwrap() {
        let percent = i32::try_from(split["percent"].as_i64().unwrap()).unwrap();
        let picks: Vec<&str> = ids
            .iter()
            .map(|id| {
                if pick_revision(primary, Some(can), percent, Some(*id)) == can {
                    "canary"
                } else {
                    "primary"
                }
            })
            .collect();
        let expected: Vec<&str> = split["picks"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p.as_str().unwrap())
            .collect();
        assert_eq!(picks, expected, "{percent}%");
    }
}

/// 20% of 10k new executions within ±2%.
#[test]
fn a_twenty_percent_canary_splits_within_two_points() {
    let (primary, can) = (Uuid::from_u128(1), Uuid::from_u128(2));
    let hits = (0..10_000)
        .filter(|_| pick_revision(primary, Some(can), 20, Some(Uuid::new_v4())) == can)
        .count();
    assert!((1800..=2200).contains(&hits), "{hits}");
}

#[test]
fn lifecycle_and_compatibility_match_python() {
    let f = fixtures();
    for row in f["transitions"].as_array().unwrap() {
        let parse = |k: &str| RevisionStatus::parse(row[k].as_str().unwrap()).unwrap();
        assert_eq!(
            transition(parse("from"), parse("to")).is_ok(),
            row["ok"].as_bool().unwrap(),
            "{row}"
        );
    }
    let (a, b) = (Uuid::from_u128(10), Uuid::from_u128(11));
    for row in f["may_resume"].as_array().unwrap() {
        let current = if row["pinned_same"].as_bool().unwrap() {
            a
        } else {
            b
        };
        let compatible: Option<BTreeSet<Uuid>> =
            row["compatible_given"].as_bool().unwrap().then(|| {
                if row["compatible_with_pinned"].as_bool().unwrap() {
                    BTreeSet::from([a])
                } else {
                    BTreeSet::new()
                }
            });
        assert_eq!(
            may_resume(policy(&row["policy"]), a, current, compatible.as_ref()),
            row["ok"].as_bool().unwrap(),
            "{row}"
        );
    }
    for row in f["checkpoint"].as_array().unwrap() {
        let compatible: Option<BTreeSet<String>> = row["compatible"]
            .as_array()
            .map(|c| c.iter().map(|v| v.as_str().unwrap().to_owned()).collect());
        let v = CheckpointVersions {
            pinned_adapter: row["pinned_adapter"].as_str(),
            deployed_adapter: row["deployed_adapter"].as_str(),
            pinned_library: row["pinned_library"].as_str(),
            deployed_library: row["deployed_library"].as_str(),
        };
        assert_eq!(
            checkpoint_may_read(policy(&row["policy"]), v, compatible.as_ref()),
            row["ok"].as_bool().unwrap(),
            "{row}"
        );
    }
}
