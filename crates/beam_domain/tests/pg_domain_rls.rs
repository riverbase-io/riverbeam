//! The domain under the optional row-level security: with
//! `crates/beam_store/sql/rls.sql` applied and RLS on, an admin in org A
//! changes only org A's rows through riverbase's commands, and the tasks
//! they queue carry org A. Its own test binary, since RLS is process-wide.
//! Needs `BEAM_DATABASE_URL` (a `postgres` superuser); skips without it.

mod support;

use beam_domain::{BeamDomainBundle, BeamSql, DomainStore, SessionRecord, BEAM_NAMESPACE};
use beam_store::PgBeamStore;
use beam_worker::LocalBus;
use riverbase_core::datastore::PostgresDomainLogStore;
use riverbase_core::Domain;
use serde_json::json;
use std::sync::Arc;
use support::{admin, ctx, object, uuid_of};
use uuid::Uuid;

/// A session of `user` in `org`, written as the superuser (no RLS).
async fn seed_session(store: &PgBeamStore, agent: Uuid, user: Uuid, org: Uuid) -> Uuid {
    let session = Uuid::new_v4();
    let caller = beam_domain::Caller {
        user_id: Some(user.to_string()),
        org_id: Some(org.to_string()),
        admin: false,
    };
    DomainStore::for_store(store)
        .record_session(
            &SessionRecord {
                session_id: session,
                agent_id: agent,
                user_id: caller.user_id.clone(),
                org_id: caller.org_id.clone(),
                name: "rls".into(),
                ..SessionRecord::default()
            },
            &caller,
        )
        .await
        .expect("session");
    session
}

#[tokio::test]
async fn an_admin_sees_and_changes_only_their_orgs_rows() {
    let Some((su, role_pool)) = beam_store::testdb::rls_pools("beam_domain_rls").await else {
        return;
    };
    // The agent row is not org-scoped; check it in as the superuser.
    let name = format!("rls-{}", Uuid::new_v4().simple());
    let beam = beam_core::RiverBeam::builder(beam_core::BeamConfig::default())
        .pg_store(su.clone())
        .build()
        .unwrap();
    beam.register_echo_agent(name.clone());
    beam.checkin_catalog().await.expect("catalog");
    let agent = beam_store::agent_id_for(&name);
    let (user_a, org_a) = (Uuid::new_v4(), Uuid::new_v4());
    let (user_b, org_b) = (Uuid::new_v4(), Uuid::new_v4());
    let session_a = seed_session(&su, agent, user_a, org_a).await;
    let session_b = seed_session(&su, agent, user_b, org_b).await;

    // The domain connects as the role, held to the policies.
    let role_store = PgBeamStore::new(role_pool.clone(), beam_store::testdb::schema());
    let logstore = PostgresDomainLogStore::new(role_pool).into_bundle();
    let domain = BeamDomainBundle::spawn_with_namespace(
        BEAM_NAMESPACE,
        &role_store,
        logstore,
        Arc::new(LocalBus::new()),
    )
    .await
    .expect("domain");
    beam_store::org_scope::set_rls_enabled(true);

    // A command in org A changes org A's session: the command's transaction
    // carries `beam.org_id` (`CommandExec`'s per-transaction scope).
    let staff_a = admin(Uuid::new_v4(), org_a);
    domain
        .command_dyn()
        .execute(
            &staff_a,
            "update-session",
            json!({"name": "renamed in A"}),
            object("agent_session", session_a),
        )
        .await
        .expect("org A renames its own session");
    // Org B's session is not there for org A: unknown, not refused by owner.
    let err = domain
        .command_dyn()
        .execute(
            &staff_a,
            "update-session",
            json!({"name": "taken"}),
            object("agent_session", session_b),
        )
        .await
        .unwrap_err();
    assert_eq!(err.errdata["site_code"], "BEM-501", "{err}");

    // A task a command queues in org A carries org A (the policy's WITH
    // CHECK passed on the role's connection).
    let remembered = domain
        .command_dyn()
        .execute(
            &ctx(user_a, org_a),
            "remember-memory",
            json!({"user_id": user_a.to_string(), "content": "org A only"}),
            object("agent_memory", Uuid::new_v4()),
        )
        .await
        .expect("remember in org A");
    beam_store::org_scope::set_rls_enabled(false);
    let task = uuid_of(&remembered, "task_id");
    let row = su
        .db()
        .tasks()
        .get(task)
        .await
        .expect("read")
        .expect("the task");
    assert_eq!(row.org_id, Some(org_a));
    // Riverbase's query engine does not set `beam.org_id` on its
    // connections yet, so domain queries under RLS are not covered here
    // (docs/backlog.md; riverbase-work-items.md).
}
