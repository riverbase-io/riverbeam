//! Writes through the beam entities: a patch keeps the columns it
//! does not name, an insert-only write leaves an existing row, an update-only
//! one a missing row, enum columns write through their cast, generated
//! columns follow, the root resources answer with the identifier, an
//! unchanged catalog check-in touches nothing, and a session's rows copy its
//! owner. Needs `BEAM_DATABASE_URL`; skips without it.

use beam_store::{
    AgentRow, AgentStatus, CatalogStore, ExecutionStore, InteractionRow, InteractionStatus,
    ModelRow, ModelStatus, PgBeamStore, RunRow, RunStatus, SessionRow,
};
use diesel::sql_types::{Jsonb, Uuid as SqlUuid};
use diesel::QueryableByName;
use diesel_async::RunQueryDsl;
use riverbase_core::datastore::DataStore;
use riverbase_core::RiverbaseResult;
use serde_json::{json, Value};
use uuid::Uuid;

async fn store() -> Option<PgBeamStore> {
    beam_store::testdb::store("beam_store_entity_writes").await
}

/// Write `data` to row `id` of `table` through its entity, in a transaction
/// of its own.
async fn write(store: &PgBeamStore, table: &str, id: Uuid, data: Value) -> RiverbaseResult<()> {
    let datastore = store.datastore();
    let resource = datastore.registry().parse(table)?;
    let uow = datastore.begin_command_transaction().await?;
    match datastore
        .upsert(&uow, resource, &id.to_string(), data)
        .await
    {
        Ok(()) => uow.commit().await,
        Err(e) => {
            uow.rollback().await?;
            Err(e)
        }
    }
}

#[derive(QueryableByName)]
struct RowJson {
    #[diesel(sql_type = Jsonb)]
    row: Value,
}

/// Row `id` of `table`, as `to_jsonb` reads it.
async fn read(store: &PgBeamStore, table: &str, id: Uuid) -> Value {
    let mut conn = store.pool().get().await.expect("connection");
    let rows: Vec<RowJson> = diesel::sql_query(format!(
        "SELECT to_jsonb(t) - '_txt' AS row FROM ref_beam.{table} t WHERE t._id = $1"
    ))
    .bind::<SqlUuid, _>(id)
    .load(&mut conn)
    .await
    .expect("read");
    rows.into_iter().next().expect("row").row
}

/// A new agent's columns.
fn agent(name: &str) -> Value {
    json!({
        "name": name,
        "description": "first",
        "supported_modalities": ["text"],
        "output_modalities": ["text"],
        "status": "ACTIVE",
    })
}

#[tokio::test]
async fn patch_keeps_omitted_columns_and_touches() {
    let Some(store) = store().await else {
        return;
    };
    let id = Uuid::new_v4();
    let name = format!("patch-{}", id.simple());
    write(&store, "agent", id, agent(&name))
        .await
        .expect("insert");
    let before = read(&store, "agent", id).await;
    write(&store, "agent", id, json!({"description": "second"}))
        .await
        .expect("patch");
    let after = read(&store, "agent", id).await;
    assert_eq!(after["description"], "second");
    assert_eq!(
        after["name"], name,
        "a column the patch does not name stays"
    );
    assert_eq!(after["status"], "ACTIVE");
    assert_eq!(after["supported_modalities"], json!(["text"]));
    assert_ne!(after["_etag"], before["_etag"], "a new _etag");
    assert!(after["_updated"].is_string(), "{after}");
    // `null` sets NULL.
    write(&store, "agent", id, json!({"description": null}))
        .await
        .expect("null");
    assert!(read(&store, "agent", id).await["description"].is_null());
}

#[tokio::test]
async fn insert_of_an_existing_id_does_nothing() {
    let Some(store) = store().await else {
        return;
    };
    let id = Uuid::new_v4();
    let name = format!("insert-{}", id.simple());
    write(&store, "agent", id, agent(&name))
        .await
        .expect("insert");
    let before = read(&store, "agent", id).await;
    let mut again = agent("someone else");
    again["_insert"] = json!(true);
    write(&store, "agent", id, again)
        .await
        .expect("insert only");
    let after = read(&store, "agent", id).await;
    assert_eq!(after["name"], name);
    assert_eq!(after["_etag"], before["_etag"], "untouched");
}

#[tokio::test]
async fn enum_columns_write_through_the_cast() {
    let Some(store) = store().await else {
        return;
    };
    let id = Uuid::new_v4();
    write(
        &store,
        "model",
        id,
        json!({"name": format!("enum-{}", id.simple()), "provider": "mock", "status": "DEPRECATED"}),
    )
    .await
    .expect("model");
    assert_eq!(read(&store, "model", id).await["status"], "DEPRECATED");

    let plan = Uuid::new_v4();
    write(
        &store,
        "agent_usage_plan",
        plan,
        json!({"name": "enum plan", "limit_allowed_modalities": ["TEXT", "AUDIO"]}),
    )
    .await
    .expect("plan");
    assert_eq!(
        read(&store, "agent_usage_plan", plan).await["limit_allowed_modalities"],
        json!(["TEXT", "AUDIO"])
    );

    let err = write(&store, "model", id, json!({"status": "BOGUS"}))
        .await
        .unwrap_err();
    assert_eq!(err.errcode.as_str(), "BEM-146", "{err}");
    assert_eq!(read(&store, "model", id).await["status"], "DEPRECATED");
}

#[tokio::test]
async fn generated_tenant_follows_org_id() {
    let Some(store) = store().await else {
        return;
    };
    let agent_id = Uuid::new_v4();
    write(
        &store,
        "agent",
        agent_id,
        agent(&format!("tenant-{}", agent_id.simple())),
    )
    .await
    .expect("agent");
    let (session, org) = (Uuid::new_v4(), Uuid::new_v4());
    write(
        &store,
        "agent_session",
        session,
        json!({"agent_id": agent_id, "org_id": org, "name": "t"}),
    )
    .await
    .expect("session");
    let row = read(&store, "agent_session", session).await;
    assert_eq!(row["_tenant"], json!(org.to_string()));
}

#[tokio::test]
async fn root_resources_answer_with_the_identifier() {
    let Some(store) = store().await else {
        return;
    };
    let datastore = store.datastore();
    let agent = datastore.registry().parse("agent").expect("agent");
    let root: Option<Value> = datastore.fetch(agent, "by-name").await.expect("root");
    assert_eq!(root, Some(json!({"_id": "by-name"})));

    let id = Uuid::new_v4();
    write(
        &store,
        "model",
        id,
        json!({"name": format!("fetch-{}", id.simple()), "provider": "mock", "status": "ACTIVE"}),
    )
    .await
    .expect("model");
    let model = datastore.registry().parse("model").expect("model");
    let row: Option<Value> = datastore
        .fetch(model, &id.to_string())
        .await
        .expect("model row");
    assert_eq!(row.expect("found")["provider"], "mock");
}

#[tokio::test]
async fn unchanged_checkin_keeps_etag() {
    let Some(store) = store().await else {
        return;
    };
    let name = format!("checkin-{}", Uuid::new_v4().simple());
    let mut row = AgentRow {
        id: beam_store::agent_id_for(&name),
        name,
        description: Some("one".into()),
        supported_modalities: vec!["text".into()],
        output_modalities: vec!["text".into()],
        tags: vec!["a".into()],
        version: None,
        status: AgentStatus::Active,
    };
    store
        .checkin_agents(std::slice::from_ref(&row))
        .await
        .expect("first");
    let first = read(&store, "agent", row.id).await;
    store
        .checkin_agents(std::slice::from_ref(&row))
        .await
        .expect("same");
    let same = read(&store, "agent", row.id).await;
    assert_eq!(same["_etag"], first["_etag"]);
    assert_eq!(same["_updated"], first["_updated"]);
    row.description = Some("two".into());
    store
        .checkin_agents(std::slice::from_ref(&row))
        .await
        .expect("changed");
    let changed = read(&store, "agent", row.id).await;
    assert_eq!(changed["description"], "two");
    assert_ne!(changed["_etag"], first["_etag"]);

    // A whole-number cost reads back from `to_jsonb` as `0`, not `0.0`.
    let model = ModelRow {
        id: Uuid::new_v4(),
        name: format!("checkin-model-{}", Uuid::new_v4().simple()),
        provider: "mock".into(),
        modalities: vec!["text".into()],
        capabilities: vec![],
        cost_per_1k_tokens: 0.0,
        max_tokens: 4096,
        provider_config: json!({"temperature": 1.0}),
        extra: json!({}),
        status: ModelStatus::Active,
    };
    store
        .checkin_models(std::slice::from_ref(&model))
        .await
        .expect("model");
    let first = read(&store, "model", model.id).await;
    store
        .checkin_models(std::slice::from_ref(&model))
        .await
        .expect("model again");
    assert_eq!(
        read(&store, "model", model.id).await["_etag"],
        first["_etag"]
    );
}

#[tokio::test]
async fn update_only_leaves_a_missing_row_missing() {
    let Some(store) = store().await else {
        return;
    };
    let id = Uuid::new_v4();
    write(
        &store,
        "agent",
        id,
        json!({"description": "nobody", "_update": true}),
    )
    .await
    .expect("no-op");
    let mut conn = store.pool().get().await.expect("connection");
    let rows: Vec<RowJson> =
        diesel::sql_query("SELECT to_jsonb(t) AS row FROM ref_beam.agent t WHERE t._id = $1")
            .bind::<SqlUuid, _>(id)
            .load(&mut conn)
            .await
            .expect("read");
    assert!(rows.is_empty(), "nothing inserted");

    let name = format!("update-{}", id.simple());
    write(&store, "agent", id, agent(&name))
        .await
        .expect("insert");
    write(
        &store,
        "agent",
        id,
        json!({"description": "second", "_update": true}),
    )
    .await
    .expect("update");
    assert_eq!(read(&store, "agent", id).await["description"], "second");
}

#[tokio::test]
async fn session_rows_copy_the_owner() {
    let Some(store) = store().await else {
        return;
    };
    let name = format!("owner-{}", Uuid::new_v4().simple());
    let agent_id = beam_store::agent_id_for(&name);
    store
        .checkin_agents(&[AgentRow {
            id: agent_id,
            name: name.clone(),
            description: None,
            supported_modalities: vec!["text".into()],
            output_modalities: vec!["text".into()],
            tags: vec![],
            version: None,
            status: AgentStatus::Active,
        }])
        .await
        .expect("agent");
    let (user, org) = (Uuid::new_v4(), Uuid::new_v4());
    let session = Uuid::new_v4();
    store
        .ensure_session(&SessionRow {
            id: session,
            agent_id,
            user_id: Some(user.to_string()),
            profile_id: None,
            org_id: Some(org.to_string()),
            name: None,
            last_active_at: None,
        })
        .await
        .expect("session");
    let interaction = Uuid::new_v4();
    store
        .create_interaction(&InteractionRow {
            id: interaction,
            session_id: session,
            execution_id: None,
            agent_id,
            agent_name: name.clone(),
            status: InteractionStatus::InProgress,
            started_at: chrono::Utc::now(),
            ended_at: None,
        })
        .await
        .expect("interaction");
    let row = read(&store, "agent_interaction", interaction).await;
    assert_eq!(row["user_id"], json!(user.to_string()));
    assert_eq!(row["org_id"], json!(org.to_string()));

    // A run with its own org keeps it; the user is the session's.
    let (run, own_org) = (Uuid::new_v4(), Uuid::new_v4());
    store
        .create_run(&RunRow {
            id: run,
            session_id: session,
            interaction_id: interaction,
            execution_id: None,
            attempt_no: 1,
            agent_id,
            agent_name: name,
            status: RunStatus::Running,
            started_at: chrono::Utc::now(),
            ended_at: None,
            org_id: Some(own_org.to_string()),
        })
        .await
        .expect("run");
    let row = read(&store, "agent_run", run).await;
    assert_eq!(row["user_id"], json!(user.to_string()));
    assert_eq!(row["org_id"], json!(own_org.to_string()));

    // Completing the run sets its status and end.
    store
        .complete_run(run, RunStatus::Completed)
        .await
        .expect("complete");
    let row = read(&store, "agent_run", run).await;
    assert_eq!(row["status"], "COMPLETED");
    assert!(row["ended_at"].is_string(), "{row}");
    // Completing a run that is not stored is a no-op, as before.
    store
        .complete_run(Uuid::new_v4(), RunStatus::Completed)
        .await
        .expect("no run, no error");
}

#[tokio::test]
async fn a_dropped_unit_of_work_rolls_back() {
    let Some(store) = store().await else {
        return;
    };
    let datastore = store.datastore();
    let id = Uuid::new_v4();
    let name = format!("dropped-{}", id.simple());
    {
        // A cancelled write: the row is inserted, and the future is dropped
        // before `finish`.
        let uow = beam_store::entity::write::begin(datastore)
            .await
            .expect("begin");
        beam_store::entity::write::put(
            datastore,
            &uow,
            "agent",
            id,
            agent(&format!("{name}-first")),
        )
        .await
        .expect("insert");
    }
    // The same id again: it waits on the dropped transaction's row lock
    // until the rollback, then inserts (a leaked transaction would hold the
    // lock for good).
    let mut again = agent(&name);
    again["_insert"] = json!(true);
    write(&store, "agent", id, again)
        .await
        .expect("insert again");
    assert_eq!(read(&store, "agent", id).await["name"], name);
}
