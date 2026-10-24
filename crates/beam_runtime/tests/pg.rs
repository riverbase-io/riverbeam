//! Postgres tests for saved runtime state. They run when `BEAM_DATABASE_URL`
//! points at the parity database and use a migrated test database, `beam_test_beam_runtime`.

use std::sync::Arc;
use std::time::Duration;

use beam_execution::{ExecutionRecords, Lease, NewExecution, PgExecutionRecords};
use beam_runtime::state::RuntimeStateStore;
use beam_runtime::{
    collect_events, AgentRuntime, PgRuntimeStateStore, RigAgentDef, RigRuntime, RuntimeContext,
    RuntimeResumeRequest, RuntimeStartRequest, RuntimeStateRecord, ScriptedModel, ScriptedTools,
    ScriptedTurn,
};
use beam_store::{
    agent_id_for, AgentRow, AgentStatus, CatalogStore, ExecutionStore, InteractionRow,
    InteractionStatus, PgBeamStore, RunRow, RunStatus, SessionRow,
};
use beam_types::{ActionResponseEnvelope, ActionResponseStatus, AgentInput, ContentPart};
use chrono::Utc;
use serde_json::{json, Value};
use uuid::Uuid;

struct Db {
    store: PgBeamStore,
    records: Arc<PgExecutionRecords>,
    states: Arc<PgRuntimeStateStore>,
}

async fn db() -> Option<Db> {
    let store = beam_store::testdb::store("beam_runtime").await?;
    let records = Arc::new(PgExecutionRecords::for_store(&store));
    let states = Arc::new(PgRuntimeStateStore::for_store(&store));
    Some(Db {
        store,
        records,
        states,
    })
}

/// An execution with one run, as the pipeline creates them. Returns `(execution, run)`.
async fn execution(db: &Db) -> (Uuid, Uuid) {
    let name = format!("rt-agent-{}", Uuid::new_v4());
    let agent_id = agent_id_for(&name);
    db.store
        .checkin_agents(&[AgentRow {
            id: agent_id,
            name: name.clone(),
            description: None,
            supported_modalities: vec!["text".into()],
            output_modalities: vec!["text".into()],
            tags: vec![],
            version: Some("1.0.0".into()),
            status: AgentStatus::Active,
        }])
        .await
        .unwrap();
    let session_id = Uuid::new_v4();
    db.store
        .ensure_session(&SessionRow {
            id: session_id,
            agent_id,
            user_id: None,
            profile_id: None,
            org_id: None,
            name: None,
            last_active_at: None,
        })
        .await
        .unwrap();
    let execution = db
        .records
        .create(&NewExecution {
            id: Uuid::new_v4(),
            session_id,
            agent_id,
            org_id: None,
            runtime_name: None,
            runtime_ref: None,
            concurrency_key: None,
            timeout_at: None,
            request_id: None,
            request_snapshot: None,
        })
        .await
        .unwrap();
    let interaction = Uuid::new_v4();
    db.store
        .create_interaction(&InteractionRow {
            id: interaction,
            session_id,
            execution_id: Some(execution.id),
            agent_id,
            agent_name: name.clone(),
            status: InteractionStatus::InProgress,
            started_at: Utc::now(),
            ended_at: None,
        })
        .await
        .unwrap();
    let run_id = Uuid::new_v4();
    db.store
        .create_run(&RunRow {
            id: run_id,
            session_id,
            interaction_id: interaction,
            execution_id: Some(execution.id),
            attempt_no: 1,
            agent_id,
            agent_name: name,
            status: RunStatus::Running,
            started_at: Utc::now(),
            ended_at: None,
            org_id: None,
        })
        .await
        .unwrap();
    (execution.id, run_id)
}

async fn claim(db: &Db, run_id: Uuid, worker: &str) -> Lease {
    let lease_id = Uuid::new_v4();
    let fence = db
        .records
        .claim_lease(run_id, worker, lease_id, Duration::from_millis(50))
        .await
        .unwrap();
    Lease {
        run_id,
        lease_id,
        fence,
    }
}

fn record(execution_id: Uuid, run_id: Uuid, status: &str) -> RuntimeStateRecord {
    RuntimeStateRecord {
        state_id: execution_id,
        execution_id,
        run_id,
        runtime_name: "rig".into(),
        run_format: 2,
        adapter_version: "1.0.0".into(),
        library_version: "rig-agent-0.44.0".into(),
        status: status.into(),
        turn: 1,
        state: json!({"x": 1}),
        updated_at: Utc::now(),
    }
}

#[tokio::test]
async fn state_round_trips_and_retention_finds_finished_runs() {
    let Some(db) = db().await else { return };
    let (execution_id, run_id) = execution(&db).await;
    let lease = claim(&db, run_id, "w1").await;
    db.states
        .save(&record(execution_id, run_id, "running"), Some(&lease))
        .await
        .unwrap();
    let loaded = db.states.load(execution_id).await.unwrap().unwrap();
    assert_eq!(
        (loaded.status.as_str(), loaded.state.clone()),
        ("running", json!({"x": 1}))
    );
    assert!(db
        .states
        .set_status(execution_id, "completed")
        .await
        .unwrap());
    assert!(db
        .states
        .retention_scan(Duration::ZERO)
        .await
        .unwrap()
        .contains(&execution_id));
    assert!(!db
        .states
        .retention_scan(Duration::from_hours(1))
        .await
        .unwrap()
        .contains(&execution_id));
    assert!(db.states.health().await.healthy);
    assert!(db.states.purge(execution_id).await.unwrap());
    assert!(db.states.load(execution_id).await.unwrap().is_none());
}

#[tokio::test]
async fn stale_worker_cannot_save_state() {
    let Some(db) = db().await else { return };
    let (execution_id, run_id) = execution(&db).await;
    let stale = claim(&db, run_id, "w1").await;
    db.states
        .save(&record(execution_id, run_id, "running"), Some(&stale))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    let current = claim(&db, run_id, "w2").await;
    let err = db
        .states
        .save(&record(execution_id, run_id, "completed"), Some(&stale))
        .await
        .unwrap_err();
    assert_eq!(err.errcode.as_str(), "BEM-397");
    assert_eq!(
        db.states.load(execution_id).await.unwrap().unwrap().status,
        "running"
    );
    db.states
        .save(&record(execution_id, run_id, "completed"), Some(&current))
        .await
        .unwrap();
}

fn input(text: &str) -> AgentInput {
    AgentInput {
        content: vec![ContentPart::text(text)],
        session_id: Uuid::new_v4(),
        interaction_id: Some(Uuid::new_v4()),
        execution_id: None,
        request_id: Some(Uuid::new_v4()),
        run_id: Uuid::new_v4(),
        runtime_ref: None,
        agent_name: Some("payer".into()),
        model_override: None,
        metadata: Value::Null,
        auth_context: None,
        stream: false,
        actions: None,
        context: None,
        client: None,
    }
}

/// The run is held, the first worker's runtime is
/// dropped, and a fresh runtime over the same database finishes it.
#[tokio::test]
async fn rig_resume_after_kill() {
    let Some(db) = db().await else { return };
    let (execution_id, _) = execution(&db).await;
    let def = RigAgentDef {
        tools: vec!["pay".into()],
        ..RigAgentDef::new("payer")
    };
    let model = ScriptedModel::new([
        ScriptedTurn::tool_call("c1", "pay", json!({})),
        ScriptedTurn::text("paid"),
    ]);
    let tools = ScriptedTools::new().with("pay", true, |_| Ok("ok".into()));
    let ctx = || RuntimeContext {
        model_gateway: Arc::new(model.clone()),
        tool_runtime: Arc::new(tools.clone()),
        ..RuntimeContext::default()
    };
    let first = input("pay");
    let worker_a = RigRuntime::new(def.clone(), db.states.clone());
    let (_, held) = collect_events(worker_a.start(
        RuntimeStartRequest {
            execution_id,
            run_id: first.run_id,
            input: first,
            runtime_ref: None,
        },
        ctx(),
    ))
    .await
    .unwrap();
    let action = held.action_required.unwrap()[0].clone();
    let runtime_ref = worker_a.allocate_ref(execution_id);
    drop(worker_a);

    let worker_b = RigRuntime::new(def, Arc::new(PgRuntimeStateStore::for_store(&db.store)));
    let next = input("yes");
    let (_, done) = collect_events(worker_b.resume(
        RuntimeResumeRequest {
            execution_id,
            run_id: next.run_id,
            runtime_ref,
            actions: vec![ActionResponseEnvelope {
                action_id: action["action_id"].as_str().unwrap().into(),
                lc_action_id: None,
                kind: action["action_request"]["kind"].as_str().unwrap().into(),
                request_id: action["action_request"]["request_id"].as_str().unwrap().into(),
                status: ActionResponseStatus::Submitted,
                payload: Some(json!({"approved": true})),
                errors: vec![],
                responder_id: None,
                responded_at: Utc::now(),
            }],
            input: next,
        },
        ctx(),
    ))
    .await
    .unwrap();
    assert_eq!(done.content[0].text.as_deref(), Some("paid"));
    assert_eq!(
        (model.calls(), tools.executed()),
        (2, vec!["pay".to_owned()])
    );
}

/// Two resumes racing for one held run: the compare-and-set lets exactly one through.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_one_resume_claims_a_held_run() {
    let Some(db) = db().await else { return };
    let (execution_id, run_id) = execution(&db).await;
    db.states
        .save(&record(execution_id, run_id, "interrupted"), None)
        .await
        .unwrap();
    let claims = (0..8).map(|_| {
        let states = db.states.clone();
        tokio::spawn(async move {
            states
                .claim(execution_id, "interrupted", "running", Uuid::new_v4())
                .await
                .unwrap()
        })
    });
    let mut won = 0;
    for claim in claims {
        won += usize::from(claim.await.unwrap());
    }
    assert_eq!(won, 1);
    assert_eq!(
        db.states.load(execution_id).await.unwrap().unwrap().status,
        "running"
    );
}
