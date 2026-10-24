//! Optional row-level security (`crates/beam_store/sql/rls.sql`), plan 005
//! Phase 09: with RLS on, a store scoped to org A never sees org B's rows,
//! and a connection with no org sees none (rc8's policy, plan 020).
//!
//! Needs `BEAM_DATABASE_URL` (a superuser, to migrate and apply the
//! policies); the store then connects as the non-superuser role
//! `beam_rls_test`, since superusers bypass RLS. Skips without a database.

use beam_store::org_scope::{propagate, set_rls_enabled, with_org};
use beam_store::{AgentRow, AgentStatus, CatalogStore, ExecutionStore, PgBeamStore, SessionRow};
use uuid::Uuid;

fn session(org: &str, agent: Uuid) -> SessionRow {
    SessionRow {
        id: Uuid::new_v4(),
        agent_id: agent,
        user_id: None,
        profile_id: None,
        org_id: Some(org.to_owned()),
        name: None,
        last_active_at: None,
    }
}

#[tokio::test]
async fn an_org_scoped_store_sees_only_its_org() {
    // The role's pool still sets the schema's `search_path`, which the beam
    // entities need.
    let Some((_, pool)) = beam_store::testdb::rls_pools("beam_rls").await else {
        return;
    };
    let store = PgBeamStore::new(pool, beam_store::testdb::schema());
    set_rls_enabled(true);

    let name = format!("rls-{}", Uuid::new_v4().simple());
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
        .expect("agents are not org rows");
    let (org_a, org_b) = (Uuid::new_v4().to_string(), Uuid::new_v4().to_string());
    let (a, b) = (session(&org_a, agent), session(&org_b, agent));
    with_org(Some(&org_a), store.ensure_session(&a))
        .await
        .expect("insert a");
    with_org(Some(&org_b), store.ensure_session(&b))
        .await
        .expect("insert b");
    // Writing another org's row is refused (WITH CHECK).
    let foreign = session(&org_b, agent);
    assert!(with_org(Some(&org_a), store.ensure_session(&foreign))
        .await
        .is_err());

    let seen_by_a = with_org(Some(&org_a), async {
        (
            store.get_session(a.id).await.expect("a"),
            store.get_session(b.id).await.expect("b"),
        )
    })
    .await;
    assert!(seen_by_a.0.is_some(), "org A reads its session");
    assert!(seen_by_a.1.is_none(), "org A never sees org B's session");
    // A spawned task keeps the org only through `propagate`.
    let store = std::sync::Arc::new(store);
    let spawned = with_org(Some(&org_a), {
        let store = std::sync::Arc::clone(&store);
        async move { tokio::spawn(propagate(async move { store.get_session(a.id).await })).await }
    })
    .await
    .expect("join");
    assert!(
        spawned.expect("scoped").is_some(),
        "the spawned task reads org A"
    );
    // No org: rc8's policy (`NULLIF(current_setting(..., true), '')`) sees
    // no rows instead of failing (plan 020; it still never shows a row).
    assert!(store.get_session(a.id).await.expect("no org").is_none());
}
