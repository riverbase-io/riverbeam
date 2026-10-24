//! Plan 022 Phase 5: what a single-row write costs on its own unit of work
//! (`put_one`: BEGIN, `set_config`, write, COMMIT) against the same write on
//! a pooled connection. Ignored: run it in release with the parity Postgres,
//! `cargo test --release -p beam_store --test write_cost -- --ignored --nocapture`.

// Timings: sample counts and milliseconds as floats, an index from a quantile.
#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]

use std::time::{Duration, Instant};

use beam_store::entity::agent_session::upsert_agent_session;
use beam_store::{AgentRow, AgentStatus, CatalogStore, ExecutionStore, SessionRow};
use serde_json::json;
use uuid::Uuid;

fn report(label: &str, mut samples: Vec<Duration>) -> f64 {
    samples.sort();
    let mean = samples.iter().sum::<Duration>().as_secs_f64() / samples.len() as f64;
    let at = |q: f64| samples[((samples.len() - 1) as f64 * q) as usize].as_secs_f64();
    println!(
        "{label}: mean {:.3} ms, p50 {:.3} ms, p95 {:.3} ms ({} writes)",
        mean * 1e3,
        at(0.5) * 1e3,
        at(0.95) * 1e3,
        samples.len()
    );
    mean
}

#[tokio::test]
#[ignore = "a benchmark: run it in release with --ignored"]
async fn single_row_write_cost() {
    let Some(store) = beam_store::testdb::store("beam_store_write_cost").await else {
        return;
    };
    let name = format!("cost-{}", Uuid::new_v4().simple());
    let agent = beam_store::agent_id_for(&name);
    store
        .checkin_agents(&[AgentRow {
            id: agent,
            name,
            description: None,
            supported_modalities: vec!["text".into()],
            output_modalities: vec!["text".into()],
            tags: vec![],
            version: None,
            status: AgentStatus::Active,
        }])
        .await
        .expect("agent");
    let session = Uuid::new_v4();
    store
        .ensure_session(&SessionRow {
            id: session,
            agent_id: agent,
            user_id: None,
            profile_id: None,
            org_id: None,
            name: None,
            last_active_at: None,
        })
        .await
        .expect("session");

    let pooled = |store: &beam_store::PgBeamStore| {
        let db = store.db().clone();
        async move {
            let data = json!({"last_active_at": chrono::Utc::now(), "_update": true});
            db.scoped(|conn| {
                Box::pin(async move { upsert_agent_session(conn, session, &data).await })
            })
            .await
            .expect("pooled write");
        }
    };
    for _ in 0..50 {
        store.update_session_activity(session).await.expect("warm");
        pooled(&store).await;
    }
    let (mut unit, mut pool) = (Vec::new(), Vec::new());
    for _ in 0..500 {
        let t = Instant::now();
        store
            .update_session_activity(session)
            .await
            .expect("put_one");
        unit.push(t.elapsed());
        let t = Instant::now();
        pooled(&store).await;
        pool.push(t.elapsed());
    }
    let a = report("A: put_one (unit of work)", unit);
    let b = report("B: pooled connection", pool);
    println!("gain per call (A - B): {:.3} ms", (a - b) * 1e3);
}
