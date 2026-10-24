//! Postgres tests for the execution control plane. They run when
//! `BEAM_DATABASE_URL` points at the parity database (`docker-compose.parity.yml`)
//! and use a migrated test database, `beam_test_beam_execution`.

use std::sync::Arc;
use std::time::Duration;

use beam_execution::idempotency::normalize_idempotency_scope;
use beam_execution::{
    EnsureRequest, ExecutionConfig, ExecutionController, ExecutionRecords, ExecutionStatus,
    NewExecution, PgExecutionRecords,
};
use beam_store::{
    agent_id_for, ActionRow, ActionStatus, ActionType, AgentRow, AgentStatus, CatalogStore,
    ExecutionStore, InteractionRow, InteractionStatus, PgBeamStore, RunRow, RunStatus, SessionRow,
};
use chrono::Utc;
use uuid::Uuid;

struct Db {
    store: PgBeamStore,
    records: Arc<PgExecutionRecords>,
}

async fn db() -> Option<Db> {
    let store = beam_store::testdb::store("beam_execution").await?;
    let records = Arc::new(PgExecutionRecords::for_store(&store));
    Some(Db { store, records })
}

/// An agent and a session, which executions reference by foreign key.
async fn session(db: &Db) -> (Uuid, Uuid, String) {
    let name = format!("exec-agent-{}", Uuid::new_v4());
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
    (agent_id, session_id, name)
}

fn new_execution(session_id: Uuid, agent_id: Uuid, key: Option<&str>) -> NewExecution {
    NewExecution {
        id: Uuid::new_v4(),
        session_id,
        agent_id,
        org_id: None,
        runtime_name: None,
        runtime_ref: None,
        concurrency_key: key.map(str::to_owned),
        timeout_at: None,
        request_id: None,
        request_snapshot: None,
    }
}

/// An interaction and run on `execution_id`, for lease tests.
async fn run(db: &Db, agent_id: Uuid, session_id: Uuid, name: &str, execution_id: Uuid) -> Uuid {
    let interaction = Uuid::new_v4();
    db.store
        .create_interaction(&InteractionRow {
            id: interaction,
            session_id,
            execution_id: Some(execution_id),
            agent_id,
            agent_name: name.into(),
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
            execution_id: Some(execution_id),
            attempt_no: 1,
            agent_id,
            agent_name: name.into(),
            status: RunStatus::Running,
            started_at: Utc::now(),
            ended_at: None,
            org_id: None,
        })
        .await
        .unwrap();
    run_id
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn start_or_get_is_idempotent_under_concurrency() {
    let Some(db) = db().await else { return };
    let (agent_id, session_id, name) = session(&db).await;
    let scope =
        normalize_idempotency_scope(Some(&format!("k-{}", Uuid::new_v4())), None, &name, None)
            .unwrap();
    let mut tasks = Vec::new();
    for _ in 0..20 {
        let (records, scope) = (db.records.clone(), scope.clone());
        let new = new_execution(session_id, agent_id, Some(&format!("{session_id}:{name}")));
        tasks.push(tokio::spawn(async move {
            records.start_or_get(&new, &scope).await.map(|r| r.id)
        }));
    }
    let mut ids = Vec::new();
    for task in tasks {
        ids.push(task.await.unwrap().expect("start_or_get"));
    }
    ids.dedup();
    assert_eq!(ids.len(), 1, "all callers get the same execution: {ids:?}");
    assert_eq!(
        db.records.lookup_idempotency(&scope).await.unwrap(),
        Some(ids[0])
    );
}

#[tokio::test]
async fn one_active_start_per_concurrency_key() {
    let Some(db) = db().await else { return };
    let (agent_id, session_id, name) = session(&db).await;
    let key = format!("{session_id}:{name}");
    let first = db
        .records
        .create(&new_execution(session_id, agent_id, Some(&key)))
        .await
        .unwrap();
    let second = db
        .records
        .create(&new_execution(session_id, agent_id, Some(&key)))
        .await
        .unwrap_err();
    assert_eq!(second.errcode.as_str(), "BEM-372");
    // A waiting-for-input execution still blocks; a terminal one frees the slot.
    db.records.increment_attempt(first.id, None).await.unwrap();
    db.records
        .update_status(first.id, ExecutionStatus::WaitingForInput, None, None)
        .await
        .unwrap();
    assert!(db
        .records
        .create(&new_execution(session_id, agent_id, Some(&key)))
        .await
        .is_err());
    db.records
        .update_status(first.id, ExecutionStatus::Cancelled, None, None)
        .await
        .unwrap();
    db.records
        .create(&new_execution(session_id, agent_id, Some(&key)))
        .await
        .expect("slot freed");
}

#[tokio::test]
async fn stale_worker_is_fenced_out() {
    let Some(db) = db().await else { return };
    let (agent_id, session_id, name) = session(&db).await;
    let execution = db
        .records
        .create(&new_execution(session_id, agent_id, None))
        .await
        .unwrap();
    let run_id = run(&db, agent_id, session_id, &name, execution.id).await;

    let short = ExecutionConfig {
        lease_ttl: Duration::from_millis(200),
        worker_id: "w1".into(),
        ..ExecutionConfig::default()
    };
    let w1 = ExecutionController::new(db.records.clone(), short.clone());
    let w2 = ExecutionController::new(
        db.records.clone(),
        ExecutionConfig {
            worker_id: "w2".into(),
            ..short
        },
    );
    let lease1 = w1.claim(run_id).await.unwrap();
    assert_eq!(
        w2.claim(run_id).await.unwrap_err().errcode.as_str(),
        "BEM-375",
        "live lease is owned"
    );
    tokio::time::sleep(Duration::from_millis(400)).await;
    let lease2 = w2.claim(run_id).await.expect("expired lease can be taken");
    assert_eq!(lease2.fence, lease1.fence + 1);
    assert_eq!(
        w1.assert_fence(&lease1).await.unwrap_err().errcode.as_str(),
        "BEM-376"
    );
    assert_eq!(
        db.records
            .heartbeat(&lease1, "w1", Duration::from_secs(1))
            .await
            .unwrap_err()
            .errcode
            .as_str(),
        "BEM-376"
    );
    w2.assert_fence(&lease2).await.unwrap();
    w1.release(&lease1).await.unwrap(); // stale release is a no-op
    w2.assert_fence(&lease2)
        .await
        .expect("stale release did not clear the new lease");
}

#[tokio::test]
async fn expired_lease_moves_execution_to_retrying() {
    let Some(db) = db().await else { return };
    let (agent_id, session_id, name) = session(&db).await;
    let execution = db
        .records
        .create(&new_execution(session_id, agent_id, None))
        .await
        .unwrap();
    db.records
        .increment_attempt(execution.id, None)
        .await
        .unwrap();
    let run_id = run(&db, agent_id, session_id, &name, execution.id).await;
    let worker = ExecutionController::new(
        db.records.clone(),
        ExecutionConfig {
            lease_ttl: Duration::from_millis(100),
            ..ExecutionConfig::default()
        },
    );
    worker.claim(run_id).await.unwrap();
    assert!(
        !db.records
            .mark_retrying(execution.id, Some(run_id))
            .await
            .unwrap(),
        "live lease"
    );
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(worker
        .retry_expired_leases()
        .await
        .unwrap()
        .contains(&execution.id));
    let row = db.records.get(execution.id).await.unwrap().unwrap();
    assert_eq!(row.status, ExecutionStatus::Retrying);
    assert_eq!(
        db.store.get_run(run_id).await.unwrap().unwrap().status,
        RunStatus::Interrupted
    );
    // The next attempt continues the same execution.
    assert_eq!(
        db.records
            .increment_attempt(execution.id, None)
            .await
            .unwrap(),
        2
    );
}

#[tokio::test]
async fn cancel_expires_pending_actions() {
    let Some(db) = db().await else { return };
    let (agent_id, session_id, name) = session(&db).await;
    let execution = db
        .records
        .create(&new_execution(session_id, agent_id, None))
        .await
        .unwrap();
    db.records
        .increment_attempt(execution.id, None)
        .await
        .unwrap();
    let run_id = run(&db, agent_id, session_id, &name, execution.id).await;
    let run_row = db.store.get_run(run_id).await.unwrap().unwrap();
    let action = ActionRow {
        id: Uuid::new_v4(),
        session_id,
        execution_id: Some(execution.id),
        interaction_id: run_row.interaction_id,
        run_id,
        step_id: None,
        message_id: None,
        agent_id,
        action_type: ActionType::Confirmation,
        status: ActionStatus::Pending,
        lc_action_id: None,
        payload: serde_json::json!({}),
        result: None,
        resolved_at: None,
        org_id: None,
        tool_name: None,
        expires_at: None,
        sweep_after: None,
    };
    db.store.create_action(action.clone()).await.unwrap();
    let controller = ExecutionController::new(db.records.clone(), ExecutionConfig::default());
    let mut stop = controller.watch(execution.id, Uuid::new_v4());
    let cancelled = controller.cancel(execution.id).await.unwrap();
    assert_eq!(cancelled.status, ExecutionStatus::Cancelled);
    assert_eq!(
        db.store.get_action(action.id).await.unwrap().status,
        ActionStatus::Expired
    );
    stop.changed().await.unwrap();
    assert_eq!(*stop.borrow(), Some(beam_execution::StopReason::Cancelled));
    // Like Python, cancelling again is a no-op; cancelling a completed execution is illegal.
    assert_eq!(
        controller.cancel(execution.id).await.unwrap().status,
        ExecutionStatus::Cancelled
    );
    let done = db
        .records
        .create(&new_execution(session_id, agent_id, None))
        .await
        .unwrap();
    db.records.increment_attempt(done.id, None).await.unwrap();
    db.records
        .update_status(done.id, ExecutionStatus::Completed, None, None)
        .await
        .unwrap();
    assert_eq!(
        controller
            .cancel(done.id)
            .await
            .unwrap_err()
            .errcode
            .as_str(),
        "BEM-370"
    );
}

#[tokio::test]
async fn illegal_transitions_are_rejected_in_sql() {
    let Some(db) = db().await else { return };
    let (agent_id, session_id, _) = session(&db).await;
    let execution = db
        .records
        .create(&new_execution(session_id, agent_id, None))
        .await
        .unwrap();
    let err = db
        .records
        .update_status(execution.id, ExecutionStatus::Completed, None, None)
        .await
        .unwrap_err();
    assert_eq!(
        err.errcode.as_str(),
        "BEM-370",
        "created -> completed is illegal"
    );
    db.records
        .increment_attempt(execution.id, None)
        .await
        .unwrap();
    db.records
        .update_status(
            execution.id,
            ExecutionStatus::Failed,
            Some("BEM-430"),
            Some("boom"),
        )
        .await
        .unwrap();
    let row = db.records.get(execution.id).await.unwrap().unwrap();
    assert_eq!(
        (row.last_error_code.as_deref(), row.current_attempt),
        (Some("BEM-430"), 1)
    );
    assert_eq!(
        db.records
            .increment_attempt(execution.id, None)
            .await
            .unwrap_err()
            .errcode
            .as_str(),
        "BEM-370"
    );
}

/// Plan 011: a refused resume hands the execution back to waiting only from
/// `running`, and only while the attempt it read is still the current one.
#[tokio::test]
async fn return_to_waiting_needs_the_current_attempt() {
    let Some(db) = db().await else { return };
    let (agent_id, session_id, name) = session(&db).await;
    let execution = db
        .records
        .create(&new_execution(session_id, agent_id, None))
        .await
        .unwrap();
    db.records
        .increment_attempt(execution.id, None)
        .await
        .unwrap();
    assert_eq!(
        db.records
            .increment_attempt(execution.id, None)
            .await
            .unwrap(),
        2
    );
    assert!(
        !db.records.return_to_waiting(execution.id, 1).await.unwrap(),
        "a later attempt took it over"
    );
    assert_eq!(status_of(&db, execution.id).await, ExecutionStatus::Running);
    assert!(db.records.return_to_waiting(execution.id, 2).await.unwrap());
    assert_eq!(
        status_of(&db, execution.id).await,
        ExecutionStatus::WaitingForInput
    );
    assert!(
        !db.records.return_to_waiting(execution.id, 2).await.unwrap(),
        "only from running"
    );

    // The store lists the execution's runs with their attempt and status.
    let run_id = run(&db, agent_id, session_id, &name, execution.id).await;
    let runs = db.store.list_execution_runs(execution.id).await.unwrap();
    assert_eq!(
        runs.iter()
            .map(|r| (r.id, r.attempt_no, r.status))
            .collect::<Vec<_>>(),
        [(run_id, 1, RunStatus::Running)]
    );
}

async fn status_of(db: &Db, id: Uuid) -> ExecutionStatus {
    db.records.get(id).await.unwrap().unwrap().status
}

#[tokio::test]
async fn ensure_replays_terminal_and_continues_resume() {
    let Some(db) = db().await else { return };
    let (agent_id, session_id, name) = session(&db).await;
    let controller = ExecutionController::new(db.records.clone(), ExecutionConfig::default());
    let req = EnsureRequest {
        execution_id: None,
        session_id,
        agent_id,
        agent_name: name.clone(),
        org_id: None,
        idempotency: None,
        concurrency_key: format!("{session_id}:{name}"),
        request_id: None,
        request_snapshot: Some(serde_json::json!({"agent_name": name})),
        runtime_name: None,
        runtime_ref: None,
        resume: false,
    };
    let first = controller.ensure(&req).await.unwrap();
    assert_eq!((first.attempt_no, first.replay_terminal), (1, false));
    assert_eq!(first.execution.status, ExecutionStatus::Running);
    assert!(first.execution.timeout_at.is_some());
    controller
        .finish(first.execution.id, ExecutionStatus::WaitingForInput)
        .await
        .unwrap();

    let resume = EnsureRequest {
        execution_id: Some(first.execution.id),
        resume: true,
        ..req.clone()
    };
    let resumed = controller.ensure(&resume).await.unwrap();
    assert_eq!(
        (resumed.attempt_no, resumed.execution.status),
        (2, ExecutionStatus::Running)
    );
    assert!(
        resumed.execution.timeout_at > first.execution.timeout_at,
        "each attempt gets a fresh deadline"
    );
    controller
        .finish(first.execution.id, ExecutionStatus::Completed)
        .await
        .unwrap();

    let again = EnsureRequest {
        execution_id: Some(first.execution.id),
        ..req
    };
    assert!(controller.ensure(&again).await.unwrap().replay_terminal);
}

/// The replay route's lookup: a session's executions, oldest first, and no
/// other session's.
#[tokio::test]
async fn lists_a_sessions_executions() {
    let Some(db) = db().await else { return };
    let (agent_id, session_id, _) = session(&db).await;
    let (other_agent, other_session, _) = session(&db).await;
    let first = db
        .records
        .create(&new_execution(session_id, agent_id, None))
        .await
        .unwrap();
    let second = db
        .records
        .create(&new_execution(session_id, agent_id, None))
        .await
        .unwrap();
    db.records
        .create(&new_execution(other_session, other_agent, None))
        .await
        .unwrap();
    let listed: Vec<Uuid> = db
        .records
        .list_for_session(session_id)
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.id)
        .collect();
    assert_eq!(listed.len(), 2);
    assert!(listed.contains(&first.id) && listed.contains(&second.id));
}

/// Revisions persist and come back (Python `persist_all` / `reconstruct`):
/// a fresh worker's bind takes the stored id and the stored status, so a
/// validating revision still needs its eval; an execution keeps its pin.
#[tokio::test]
async fn revisions_persist_and_reload() {
    use beam_execution::revisions::{snapshot_from_spec, BoundRuntime, RevisionBook};
    use beam_execution::{PgRevisionStore, RevisionPin};

    let Some(db) = db().await else { return };
    let (agent_id, session_id, name) = session(&db).await;
    let store = PgRevisionStore::for_store(&db.store);
    let book = RevisionBook::new();
    let spec = beam_types::AgentSpec::new(name.clone(), "rig");
    let first = book
        .record_bind(&snapshot_from_spec(&spec, &BoundRuntime::default()))
        .unwrap();
    let mut v2 = spec.clone();
    v2.version = "2.0.0".into();
    let second = book
        .record_bind(&snapshot_from_spec(&v2, &BoundRuntime::default()))
        .unwrap();
    book.mark_validating(second.id).unwrap();
    let mut v3 = spec.clone();
    v3.version = "3.0.0".into();
    let third = book
        .record_bind(&snapshot_from_spec(&v3, &BoundRuntime::default()))
        .unwrap();
    book.activate(third.id, false, None).unwrap();
    book.set_canary(&name, third.id, 25).unwrap();
    store.persist(&book).await.unwrap();
    store.persist(&book).await.unwrap();

    // A fresh worker binds before it reloads.
    let fresh = RevisionBook::new();
    let rebound = fresh
        .record_bind(&snapshot_from_spec(&v2, &BoundRuntime::default()))
        .unwrap();
    assert_eq!(
        rebound.status,
        beam_types::RevisionStatus::Active,
        "a first bind is active locally"
    );
    store.reconstruct(&fresh).await.unwrap();
    assert_eq!(fresh.list_revisions(&name).len(), 3);
    let reloaded = fresh
        .get_revision(second.id)
        .expect("the bind took the stored id");
    assert_eq!(
        reloaded.status,
        beam_types::RevisionStatus::Validating,
        "the stored status wins"
    );
    assert!(fresh.get_revision(rebound.id).is_none());
    assert_eq!(
        fresh.current_for(&name).unwrap().id,
        first.id,
        "the stored traffic wins"
    );
    // The canary split is not stored (rc8's `agent` has no canary columns).
    let traffic = fresh.traffic_for(&name).unwrap();
    assert_eq!((traffic.canary_id, traffic.percent), (None, 0));
    let gated = fresh.activate(second.id, true, None).unwrap_err();
    assert_eq!(gated.errcode.as_str(), "BEM-492");
    // Nothing changed here: a write keeps the stored state.
    store.persist(&fresh).await.unwrap();
    let again = RevisionBook::new();
    store.reconstruct(&again).await.unwrap();
    assert_eq!(
        again.get_revision(second.id).unwrap().status,
        beam_types::RevisionStatus::Validating
    );
    assert_eq!(again.current_for(&name).unwrap().id, first.id);

    let execution = db
        .records
        .create(&new_execution(session_id, agent_id, None))
        .await
        .unwrap();
    let pin = |adapter| RevisionPin {
        revision_id: second.id,
        adapter_version: adapter,
        ..RevisionPin::default()
    };
    db.records
        .set_revision(execution.id, pin(Some("a1")))
        .await
        .unwrap();
    db.records
        .set_revision(execution.id, pin(None))
        .await
        .unwrap();
    let row = db.records.get(execution.id).await.unwrap().unwrap();
    assert_eq!(row.agent_revision_id, Some(second.id));
    assert_eq!(
        row.runtime_adapter_version.as_deref(),
        Some("a1"),
        "None keeps the stored version"
    );
}

/// The stored row's owner and stamps, which `ExecutionRow` does not carry.
async fn stored(
    db: &Db,
    id: Uuid,
) -> (
    Option<Uuid>,
    Option<Uuid>,
    Option<chrono::DateTime<Utc>>,
    Option<chrono::DateTime<Utc>>,
) {
    use beam_store::entity::agent_execution::agent_execution as t;
    use diesel::{ExpressionMethods, QueryDsl};
    use diesel_async::RunQueryDsl;
    let mut conn = db.store.pool().get().await.unwrap();
    t::table
        .filter(t::_id.eq(id))
        .select((t::user_id, t::org_id, t::started_at, t::failed_at))
        .first(&mut conn)
        .await
        .unwrap()
}

/// Plan 015: the entity insert copies the session's owner, as the SQL's
/// subquery on the session did.
#[tokio::test]
async fn an_execution_takes_its_sessions_owner() {
    let Some(db) = db().await else { return };
    let (agent_id, _, _) = session(&db).await;
    let (session_id, user, org) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    db.store
        .ensure_session(&SessionRow {
            id: session_id,
            agent_id,
            user_id: Some(user.to_string()),
            profile_id: None,
            org_id: Some(org.to_string()),
            name: None,
            last_active_at: None,
        })
        .await
        .unwrap();
    let execution = db
        .records
        .create(&new_execution(session_id, agent_id, None))
        .await
        .unwrap();
    assert_eq!(execution.org_id, Some(org));
    let (user_id, org_id, _, _) = stored(&db, execution.id).await;
    assert_eq!((user_id, org_id), (Some(user), Some(org)));
}

/// Plan 015: a status patch names only what changes. The last error stays
/// when an update has none, `failed_at` is stamped, and a second attempt
/// keeps the first `started_at`.
#[tokio::test]
async fn status_updates_keep_the_last_error_and_stamp_once() {
    let Some(db) = db().await else { return };
    let (agent_id, session_id, _) = session(&db).await;
    let execution = db
        .records
        .create(&new_execution(session_id, agent_id, None))
        .await
        .unwrap();
    db.records
        .increment_attempt(execution.id, None)
        .await
        .unwrap();
    let (_, _, first_start, _) = stored(&db, execution.id).await;
    assert!(first_start.is_some(), "the first attempt stamps started_at");
    db.records
        .update_status(execution.id, ExecutionStatus::Failed, Some("BEM-430"), None)
        .await
        .unwrap();
    assert!(db.records.mark_retrying(execution.id, None).await.unwrap());
    assert_eq!(
        db.records
            .increment_attempt(execution.id, None)
            .await
            .unwrap(),
        2
    );
    db.records
        .update_status(execution.id, ExecutionStatus::Failed, None, None)
        .await
        .unwrap();
    let row = db.records.get(execution.id).await.unwrap().unwrap();
    assert_eq!(row.last_error_code.as_deref(), Some("BEM-430"));
    let (_, _, started_at, failed_at) = stored(&db, execution.id).await;
    assert_eq!(started_at, first_start, "a retry keeps the first start");
    assert!(failed_at.is_some());
}

/// Plan 015 (review): a second create of one execution id fails, as the
/// plain `INSERT` did, so two `ensure` calls with one caller-chosen id cannot
/// both create and run it.
#[tokio::test]
async fn a_second_create_of_one_id_fails() {
    let Some(db) = db().await else { return };
    let (agent_id, session_id, _) = session(&db).await;
    let new = new_execution(session_id, agent_id, None);
    db.records.create(&new).await.unwrap();
    assert!(db.records.create(&new).await.is_err());
}

/// Plan 022: the typed read finds an action's execution; an unknown action
/// has none.
#[tokio::test]
async fn execution_for_action_reads_the_actions_execution() {
    let Some(db) = db().await else { return };
    let (agent_id, session_id, name) = session(&db).await;
    let execution = db
        .records
        .create(&new_execution(session_id, agent_id, None))
        .await
        .unwrap();
    db.records
        .increment_attempt(execution.id, None)
        .await
        .unwrap();
    let run_id = run(&db, agent_id, session_id, &name, execution.id).await;
    let run_row = db.store.get_run(run_id).await.unwrap().unwrap();
    let action = ActionRow {
        id: Uuid::new_v4(),
        session_id,
        execution_id: Some(execution.id),
        interaction_id: run_row.interaction_id,
        run_id,
        step_id: None,
        message_id: None,
        agent_id,
        action_type: ActionType::Confirmation,
        status: ActionStatus::Pending,
        lc_action_id: None,
        payload: serde_json::json!({}),
        result: None,
        resolved_at: None,
        org_id: None,
        tool_name: None,
        expires_at: None,
        sweep_after: None,
    };
    db.store.create_action(action.clone()).await.unwrap();
    assert_eq!(
        db.records.execution_for_action(action.id).await.unwrap(),
        Some(execution.id)
    );
    assert_eq!(
        db.records
            .execution_for_action(Uuid::new_v4())
            .await
            .unwrap(),
        None
    );
}
