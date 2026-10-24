//! Plan 025: under the optional RLS, a pooled connection carries no org from
//! one checkout to the next. Before the gateway, `org_scope::prepare` set
//! `beam.org_id` for the whole session, so riverbase's own reads (which never
//! scope) ran as the last caller's org.
//!
//! One connection in the pool, so every checkout reuses it. Needs
//! `BEAM_DATABASE_URL` (a superuser); skips without a database.

use beam_store::org_scope::{set_rls_enabled, with_org};
use beam_store::{AgentRow, AgentStatus, CatalogStore, ExecutionStore, PgBeamStore, SessionRow};
use diesel::sql_types::{Nullable, Text};
use diesel::QueryableByName;
use diesel_async::RunQueryDsl;
use riverbase_core::datastore::dsl::DataQuery;
use riverbase_core::datastore::DataStore;
use serde_json::Value;
use uuid::Uuid;

#[derive(QueryableByName)]
struct Setting {
    #[diesel(sql_type = Nullable<Text>)]
    org: Option<String>,
}

#[tokio::test]
async fn a_pooled_connection_keeps_no_org() {
    let Some((_, pool)) = beam_store::testdb::rls_pools_sized("beam_rls_leak", 1).await else {
        return;
    };
    let store = PgBeamStore::new(pool.clone(), beam_store::testdb::schema());
    set_rls_enabled(true);

    let name = format!("leak-{}", Uuid::new_v4().simple());
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
    let org = Uuid::new_v4().to_string();
    let session = SessionRow {
        id: Uuid::new_v4(),
        agent_id: agent,
        user_id: None,
        profile_id: None,
        org_id: Some(org.clone()),
        name: None,
        last_active_at: None,
    };
    with_org(Some(&org), async {
        store.ensure_session(&session).await.expect("insert");
        assert!(store.get_session(session.id).await.expect("read").is_some());
    })
    .await;

    // Outside any org, on the same (only) connection: riverbase's own read
    // must not see the session, and the connection must hold no org.
    let rows: Vec<Value> = store
        .datastore()
        .query_list(&DataQuery::list("agent_session"))
        .await
        .expect("list");
    let id = session.id.to_string();
    assert!(
        rows.iter().all(|row| row["_id"] != id.as_str()),
        "an unscoped read saw the last caller's org"
    );
    let mut conn = pool.get().await.expect("conn");
    let setting: Vec<Setting> =
        diesel::sql_query("SELECT current_setting('beam.org_id', true) AS org")
            .load(&mut conn)
            .await
            .expect("setting");
    let left = setting
        .into_iter()
        .next()
        .and_then(|s| s.org)
        .unwrap_or_default();
    assert!(left.is_empty(), "the connection kept org {left}");
}
