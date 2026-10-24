//! The durable work queue on Postgres (plan 006 Phase 2): hints start tasks
//! at once, the tick covers lost hints, two workers never run one task
//! twice, and a dead worker's task is requeued or failed. Needs
//! `BEAM_DATABASE_URL`; skips without it.

use std::sync::Arc;
use std::time::Duration;

use beam_core::{BeamConfig, RiverBeam};
use beam_store::tasks::{NewTask, TaskStatus, INVOKE_TASK, TASK_TOPIC};
use beam_store::PgBeamStore;
use beam_worker::{BeamWorker, LocalBus, QueueConfig, QueueWorker};
use diesel::sql_types::{BigInt, Uuid as SqlUuid};
use diesel::QueryableByName;
use diesel_async::RunQueryDsl;
use riverbase_core::command::MessageBus;
use serde_json::{json, Value};
use tokio::sync::watch;
use uuid::Uuid;

#[derive(QueryableByName)]
struct Counted {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

/// A database per test: a queue loop claims any due task, so tests that
/// share a database would run each other's tasks.
async fn store(test: &str) -> Option<PgBeamStore> {
    let store = beam_store::testdb::store(&format!("beam_worker_{test}")).await?;
    // An aborted earlier run's tasks would be claimed first.
    let mut conn = store.pool().get().await.expect("conn");
    diesel::sql_query("DELETE FROM ref_beam.agent_task")
        .execute(&mut conn)
        .await
        .expect("clear the queue");
    drop(conn);
    Some(store)
}

/// A worker whose beam runs one echo agent, `name`, on `store`.
async fn worker(store: &PgBeamStore, name: &str) -> Arc<BeamWorker> {
    let beam = RiverBeam::builder(BeamConfig::default())
        .pg_store(store.clone())
        .build()
        .unwrap();
    beam.register_echo_agent(name.to_owned());
    beam.checkin_catalog().await.expect("catalog");
    Arc::new(BeamWorker::new(Arc::new(beam)))
}

fn config(poll: Duration) -> QueueConfig {
    QueueConfig {
        concurrency: 4,
        poll,
        lease: Duration::from_secs(5),
        ..QueueConfig::default()
    }
}

/// An `invoke_agent` task for `agent` with a pre-assigned execution.
fn invoke(agent: &str) -> NewTask {
    let execution = Uuid::new_v4();
    let payload = json!({
        "agent_name": agent,
        "content": [{"type": "text", "text": "hello"}],
        "session_id": Uuid::new_v4(),
        "request_id": Uuid::new_v4(),
        "execution_id": execution,
        "stream": false,
    });
    NewTask {
        execution_id: Some(execution),
        ..NewTask::new(INVOKE_TASK, payload)
    }
}

async fn enqueue(store: &PgBeamStore, task: &NewTask) {
    store.db().tasks().enqueue(task).await.expect("enqueue");
}

async fn status(store: &PgBeamStore, id: Uuid) -> TaskStatus {
    store
        .db()
        .tasks()
        .get(id)
        .await
        .expect("get")
        .expect("task")
        .status()
        .expect("known status")
}

/// Wait up to `within` for task `id` to reach `want`.
async fn reaches(store: &PgBeamStore, id: Uuid, want: TaskStatus, within: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + within;
    while tokio::time::Instant::now() < deadline {
        if status(store, id).await == want {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

fn start(queue: Arc<QueueWorker>) -> watch::Sender<bool> {
    let (tx, rx) = watch::channel(false);
    tokio::spawn(queue.run(rx));
    tx
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hint_starts_a_task_without_the_tick() {
    let Some(store) = store("hint").await else {
        return;
    };
    let name = format!("queue-{}", Uuid::new_v4().simple());
    let bus = Arc::new(LocalBus::new());
    // The tick would wait ten minutes: only the hint can start the task.
    let queue = Arc::new(
        QueueWorker::new(
            worker(&store, &name).await,
            store.clone(),
            config(Duration::from_mins(10)),
        )
        .with_bus(bus.clone()),
    );
    let stop = start(queue);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let task = invoke(&name);
    enqueue(&store, &task).await;
    bus.publish(TASK_TOPIC, json!({"task_id": task.id}))
        .await
        .expect("hint");
    assert!(
        reaches(
            &store,
            task.id,
            TaskStatus::Succeeded,
            Duration::from_secs(5)
        )
        .await,
        "the hinted task did not finish"
    );
    let _ = stop.send(true);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_tick_runs_a_task_without_a_hint() {
    let Some(store) = store("tick").await else {
        return;
    };
    let name = format!("queue-{}", Uuid::new_v4().simple());
    let queue = Arc::new(QueueWorker::new(
        worker(&store, &name).await,
        store.clone(),
        config(Duration::from_millis(50)),
    ));
    let stop = start(queue);
    let task = invoke(&name);
    enqueue(&store, &task).await;
    assert!(
        reaches(
            &store,
            task.id,
            TaskStatus::Succeeded,
            Duration::from_secs(5)
        )
        .await
    );
    let _ = stop.send(true);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_workers_run_each_task_once() {
    let Some(store) = store("race").await else {
        return;
    };
    let name = format!("queue-{}", Uuid::new_v4().simple());
    let bus = Arc::new(LocalBus::new());
    let a = Arc::new(
        QueueWorker::new(
            worker(&store, &name).await,
            store.clone(),
            config(Duration::from_millis(20)),
        )
        .with_bus(bus.clone()),
    );
    let b = Arc::new(
        QueueWorker::new(
            worker(&store, &name).await,
            store.clone(),
            config(Duration::from_millis(20)),
        )
        .with_bus(bus.clone()),
    );
    let stop_a = start(a);
    let stop_b = start(b);
    let mut ids = Vec::new();
    for _ in 0..20 {
        let task = invoke(&name);
        enqueue(&store, &task).await;
        bus.publish(TASK_TOPIC, json!({"task_id": task.id}))
            .await
            .expect("hint");
        ids.push((task.id, task.execution_id.expect("execution")));
    }
    for (id, _) in &ids {
        assert!(
            reaches(&store, *id, TaskStatus::Succeeded, Duration::from_secs(20)).await,
            "task {id} did not finish"
        );
    }
    let mut conn = store.pool().get().await.expect("conn");
    for (_, execution) in &ids {
        let runs: Vec<Counted> = diesel::sql_query(
            "SELECT count(*) AS n FROM ref_beam.agent_run WHERE execution_id = $1",
        )
        .bind::<SqlUuid, _>(*execution)
        .load(&mut conn)
        .await
        .expect("count runs");
        assert_eq!(
            runs[0].n, 1,
            "execution {execution} ran {} times",
            runs[0].n
        );
    }
    let _ = stop_a.send(true);
    let _ = stop_b.send(true);
}

#[tokio::test]
async fn a_dead_workers_task_is_requeued_or_failed() {
    let Some(store) = store("dead").await else {
        return;
    };
    let name = format!("queue-{}", Uuid::new_v4().simple());
    let queue = QueueWorker::new(
        worker(&store, &name).await,
        store.clone(),
        config(Duration::from_mins(10)),
    );
    let retryable = NewTask::new("reconcile_now", json!({}));
    let one_shot = NewTask {
        retryable: false,
        ..NewTask::new("run_evaluation", json!({}))
    };
    enqueue(&store, &retryable).await;
    enqueue(&store, &one_shot).await;
    let mut conn = store.pool().get().await.expect("conn");
    // A worker claims both and dies: its leases lapse.
    for id in [retryable.id, one_shot.id] {
        store
            .db()
            .tasks()
            .claim_by_id(id, "dead-worker", Duration::from_secs(5))
            .await
            .expect("claim")
            .expect("claimed");
    }
    diesel::sql_query(
        "UPDATE ref_beam.agent_task SET lease_expires_at = now() - interval '1 second' WHERE _id IN ($1, $2)",
    )
    .bind::<SqlUuid, _>(retryable.id)
    .bind::<SqlUuid, _>(one_shot.id)
    .execute(&mut conn)
    .await
    .expect("expire leases");
    drop(conn);
    let requeued = queue.sweep().await.expect("sweep");
    assert!(requeued.contains(&retryable.id), "{requeued:?}");
    assert_eq!(status(&store, retryable.id).await, TaskStatus::Queued);
    assert_eq!(status(&store, one_shot.id).await, TaskStatus::Failed);
    // The dead worker cannot record an outcome any more.
    let written = store
        .db()
        .tasks()
        .complete(one_shot.id, "dead-worker", &json!({}), &json!({}))
        .await
        .expect("complete");
    assert!(!written);
}

#[tokio::test]
async fn an_unknown_task_fails_once_with_its_error() {
    let Some(store) = store("unknown").await else {
        return;
    };
    let name = format!("queue-{}", Uuid::new_v4().simple());
    let queue = QueueWorker::new(
        worker(&store, &name).await,
        store.clone(),
        config(Duration::from_mins(10)),
    );
    let task = NewTask::new(format!("nope_{}", Uuid::new_v4().simple()), json!({}));
    enqueue(&store, &task).await;
    // Other tests' tasks may be due too: run until this one is done.
    let mut seen = None;
    for _ in 0..50 {
        match queue.run_next().await.expect("run") {
            Some(row) if row.id == task.id => {
                seen = Some(row);
                break;
            }
            Some(_) => {}
            None => break,
        }
    }
    let row = seen.expect("the task ran");
    assert_eq!(row.status(), Some(TaskStatus::Failed));
    assert_eq!(row.attempt_count, 1);
    // The public code for callers, the site code for operators.
    let code = |key: &str| {
        row.error
            .as_ref()
            .and_then(|e| e.get(key))
            .and_then(Value::as_str)
    };
    assert_eq!(code("errcode"), Some("FLR-001"), "{:?}", row.error);
    assert_eq!(code("site_code"), Some("BEM-091"), "{:?}", row.error);
}

/// A run whose session is busy waits for it (plan 006 follow-up): it goes
/// back to the queue as often as needed without using its attempts, so a
/// second message to a session whose model is still answering is not lost.
#[tokio::test]
async fn a_busy_session_waits_without_using_attempts() {
    let Some(store) = store("busy").await else {
        return;
    };
    let task = invoke("busy-agent");
    enqueue(&store, &task).await;
    let busy = json!({"errcode": "BEM-429", "site_code": "BEM-372"});
    // More busy turns than the task's three attempts.
    for _ in 0..5 {
        store
            .db()
            .tasks()
            .claim_by_id(task.id, "worker-a", Duration::from_secs(5))
            .await
            .expect("claim")
            .expect("due again at once");
        let written = store
            .db()
            .tasks()
            .wait_busy(task.id, "worker-a", &busy, Duration::ZERO)
            .await
            .expect("wait");
        assert!(written);
    }
    let row = store
        .db()
        .tasks()
        .get(task.id)
        .await
        .expect("get")
        .expect("task");
    assert_eq!(row.status(), Some(TaskStatus::Queued));
    assert_eq!(row.attempt_count, 0, "waiting is not an attempt");
    assert_eq!(row.error, Some(busy), "the last wait is visible");
    // Only the lease holder may put it back.
    store
        .db()
        .tasks()
        .claim_by_id(task.id, "worker-a", Duration::from_secs(5))
        .await
        .expect("claim")
        .expect("claimed");
    let foreign = store
        .db()
        .tasks()
        .wait_busy(task.id, "worker-b", &json!({}), Duration::ZERO)
        .await
        .expect("wait");
    assert!(!foreign);
}
