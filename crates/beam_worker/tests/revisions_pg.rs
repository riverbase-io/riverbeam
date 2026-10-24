//! Agent revisions across restarts on Postgres: a worker binds its agents
//! before it reloads the stored revisions, so a changed agent (here a new
//! tool in its allowlist) must not become a second active revision, and
//! `activate_revision` must move new executions to it. Needs
//! `BEAM_DATABASE_URL`; skips without it.

use std::sync::Arc;

use beam_core::{BeamConfig, RiverBeam};
use beam_runtime::DirectModelRuntime;
use beam_store::PgBeamStore;
use beam_types::AgentMetadata;
use beam_worker::BeamWorker;
use diesel::sql_types::{Nullable, Text, Uuid as SqlUuid};
use diesel::QueryableByName;
use diesel_async::RunQueryDsl;
use serde_json::json;
use uuid::Uuid;

#[derive(QueryableByName)]
struct Revision {
    #[diesel(sql_type = SqlUuid, column_name = _id)]
    id: Uuid,
    #[diesel(sql_type = Text)]
    status: String,
}

#[derive(QueryableByName)]
struct Pinned {
    #[diesel(sql_type = Nullable<SqlUuid>)]
    id: Option<Uuid>,
}

/// A started worker (binds first, then `on_startup` reloads, as
/// the showcase server does) whose agent `name` allows `tools`.
async fn started(store: &PgBeamStore, name: &str, tools: &[&str]) -> Arc<BeamWorker> {
    let metadata: AgentMetadata =
        serde_json::from_value(json!({ "name": name, "tools": tools })).expect("metadata");
    let beam = RiverBeam::builder(BeamConfig::default())
        .pg_store(store.clone())
        .build()
        .unwrap();
    beam.bind_runtime(metadata, Arc::new(DirectModelRuntime::echo()));
    let worker = Arc::new(BeamWorker::new(Arc::new(beam)));
    worker.on_startup().await.expect("startup");
    worker
}

/// Run `agent` once; the revision its execution is pinned to.
async fn run(store: &PgBeamStore, worker: &BeamWorker, agent: &str) -> Uuid {
    let execution = Uuid::new_v4();
    let payload = json!({
        "agent_name": agent,
        "content": [{"type": "text", "text": "hello"}],
        "session_id": Uuid::new_v4(),
        "request_id": Uuid::new_v4(),
        "execution_id": execution,
        "stream": false,
    });
    // The pin is written before the runtime runs; the reply does not matter.
    let _ = worker.handle_task("invoke_agent", payload).await;
    let mut conn = store.pool().get().await.expect("conn");
    let pinned: Pinned = diesel::sql_query(
        "SELECT agent_revision_id AS id FROM ref_beam.agent_execution WHERE _id = $1",
    )
    .bind::<SqlUuid, _>(execution)
    .get_result(&mut conn)
    .await
    .expect("the execution");
    pinned.id.expect("a pinned revision")
}

/// The agent's stored revisions, oldest first.
async fn revisions(store: &PgBeamStore, agent: &str) -> Vec<(Uuid, String)> {
    let mut conn = store.pool().get().await.expect("conn");
    let rows: Vec<Revision> = diesel::sql_query(
        "SELECT _id, status::text AS status FROM ref_beam.agent_revision \
         WHERE agent_name = $1 AND _deleted IS NULL ORDER BY created_at, _id",
    )
    .bind::<Text, _>(agent)
    .load(&mut conn)
    .await
    .expect("revisions");
    rows.into_iter().map(|r| (r.id, r.status)).collect()
}

/// The agent's stored primary (`agent.active_revision_id`).
async fn primary(store: &PgBeamStore, agent: &str) -> Option<Uuid> {
    let mut conn = store.pool().get().await.expect("conn");
    let row: Pinned =
        diesel::sql_query("SELECT active_revision_id AS id FROM ref_beam.agent WHERE name = $1")
            .bind::<Text, _>(agent)
            .get_result(&mut conn)
            .await
            .expect("the agent");
    row.id
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_changed_agent_is_a_draft_after_restart_until_activated() {
    let Some(store) = beam_store::testdb::store("beam_worker_revisions").await else {
        return;
    };
    let name = format!("rev-{}", Uuid::new_v4().simple());

    // First start: the bind is the active primary.
    let first = started(&store, &name, &["alpha"]).await;
    let old = run(&store, &first, &name).await;
    assert_eq!(
        revisions(&store, &name).await,
        vec![(old, "ACTIVE".to_owned())]
    );
    assert_eq!(primary(&store, &name).await, Some(old));

    // Restart with a new tool: a draft revision; runs stay on the primary.
    let restarted = started(&store, &name, &["alpha", "beta"]).await;
    assert_eq!(run(&store, &restarted, &name).await, old);
    let stored = revisions(&store, &name).await;
    assert_eq!(stored.len(), 2, "{stored:?}");
    let new = stored[1].0;
    assert_ne!(new, old);
    assert_eq!(
        stored,
        vec![(old, "ACTIVE".to_owned()), (new, "DRAFT".to_owned())],
        "one active revision"
    );
    assert_eq!(primary(&store, &name).await, Some(old));

    // `activate-revision` (its worker task) makes it the primary.
    let activated = restarted
        .handle_task("activate_revision", json!({ "revision_id": new }))
        .await
        .expect("activate");
    assert_eq!(activated["id"], json!(new));
    assert_eq!(primary(&store, &name).await, Some(new));
    assert_eq!(run(&store, &restarted, &name).await, new);

    // Another restart with the same agent keeps the activation.
    let again = started(&store, &name, &["alpha", "beta"]).await;
    assert_eq!(run(&store, &again, &name).await, new);
    assert_eq!(revisions(&store, &name).await.len(), 2);
}
