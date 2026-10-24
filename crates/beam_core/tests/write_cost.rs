//! Plan 022 Phase 5: the mean echo invoke on Postgres, against which the
//! single-row write gain is judged (D4: worth it at 10% or more). Ignored:
//! `cargo test --release -p beam_core --test write_cost -- --ignored --nocapture`.

// Timings: sample counts and milliseconds as floats, an index from a quantile.
#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]

use std::time::{Duration, Instant};

use beam_core::{BeamConfig, RiverBeam};
use beam_types::InvokeRequest;
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
#[ignore = "a benchmark: run it in release with --ignored"]
async fn echo_invoke_cost() {
    let Some(store) = beam_store::testdb::store("beam_core_write_cost").await else {
        return;
    };
    let beam = RiverBeam::builder(BeamConfig::default())
        .pg_store(store)
        .build()
        .unwrap();
    beam.register_echo_agent("cost");
    beam.checkin_catalog().await.expect("check in the agent");
    let request = || -> InvokeRequest {
        serde_json::from_value(json!({
            "agent_name": "cost",
            "content": [{"type": "text", "text": "hello"}],
            "session_id": Uuid::new_v4(),
            "auth_context": {"user_id": Uuid::new_v4().to_string()},
        }))
        .expect("request")
    };
    for _ in 0..20 {
        beam.invoke_result(request()).await.expect("warm");
    }
    let mut samples = Vec::new();
    for _ in 0..200 {
        let t = Instant::now();
        beam.invoke_result(request()).await.expect("invoke");
        samples.push(t.elapsed());
    }
    samples.sort();
    let mean = samples.iter().sum::<Duration>().as_secs_f64() / samples.len() as f64;
    println!(
        "echo invoke: mean {:.3} ms, p50 {:.3} ms, p95 {:.3} ms",
        mean * 1e3,
        samples[samples.len() / 2].as_secs_f64() * 1e3,
        samples[samples.len() * 95 / 100].as_secs_f64() * 1e3,
    );
}
