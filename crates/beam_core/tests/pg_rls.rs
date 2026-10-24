//! The orchestrator under the optional row-level security: with
//! `crates/beam_store/sql/rls.sql` applied and RLS on, an invoke in org A
//! stores org A and its user on its execution, and org B cannot see it. Its
//! own test binary, since RLS is process-wide. Needs `BEAM_DATABASE_URL` (a
//! `postgres` superuser); skips without it.

use beam_core::{BeamConfig, RiverBeam};
use beam_store::org_scope::{set_rls_enabled, with_org};
use beam_store::PgBeamStore;
use beam_types::InvokeRequest;
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
async fn an_invoke_is_its_orgs_only() {
    let Some((_, pool)) = beam_store::testdb::rls_pools("beam_core_rls").await else {
        return;
    };
    let store = PgBeamStore::new(pool, beam_store::testdb::schema());
    let beam = RiverBeam::builder(BeamConfig::default())
        .pg_store(store)
        .build()
        .unwrap();
    let name = format!("rls-{}", Uuid::new_v4().simple());
    beam.register_echo_agent(name.clone());
    beam.checkin_catalog().await.expect("check in the agent");
    set_rls_enabled(true);

    let (org_a, org_b, user) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let request: InvokeRequest = serde_json::from_value(json!({
        "agent_name": name,
        "content": [{"type": "text", "text": "hello"}],
        "session_id": Uuid::new_v4(),
        "auth_context": {"user_id": user.to_string(), "org_id": org_a.to_string()},
    }))
    .expect("request");
    let own_org = org_a.to_string();
    let other_org = org_b.to_string();
    let result = with_org(Some(&own_org), beam.invoke_result(request))
        .await
        .expect("invoke in org A");
    let execution = result.execution_id.expect("an execution");

    let seen = with_org(Some(&own_org), beam.executions().records().get(execution))
        .await
        .expect("read in org A")
        .expect("org A sees its execution");
    assert_eq!(seen.org_id, Some(org_a));
    let hidden = with_org(Some(&other_org), beam.executions().records().get(execution))
        .await
        .expect("read in org B");
    assert!(hidden.is_none(), "org B never sees org A's execution");
    set_rls_enabled(false);
}
