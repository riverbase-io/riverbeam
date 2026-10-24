//! `RunWaiter`'s live path: subscribe, run the command, stop at
//! the first terminal frame, or answer `Pending` when the wait ends. Needs
//! `BEAM_DATABASE_URL` and `NATS_URL`; skips without them.

use std::sync::Arc;
use std::time::Duration;

use showcase::host::runs::{RunEnd, RunWaiter};
use beam_core::{BeamConfig, RiverBeam};
use beam_domain::{BeamDomainBundle, BEAM_NAMESPACE};
use beam_runtime::{RigAgentDef, ScriptedModel, ScriptedTools, ScriptedTurn};
use beam_store::PgBeamStore;
use beam_stream::{BeamStream, NatsTransport};
use beam_worker::{BeamWorker, LocalBus, QueueConfig, QueueWorker};
use riverbase_core::base::AggregateRoot;
use riverbase_core::datastore::PostgresDomainLogStore;
use riverbase_core::{CommandTarget, EngineContext};
use serde_json::{json, Value};
use uuid::Uuid;

/// Riverbase requires a tenant on every command.
const TENANT: Uuid = Uuid::from_u128(0x6265_616d_0000_4000_8000_0000_0000_0002);

struct Harness {
    waiter: RunWaiter,
    agent: Uuid,
    ctx: EngineContext,
}

/// A beam with a rig agent over `model`, the domain, and a waiter on NATS.
/// With `run_queue`, a background loop runs the queued tasks.
async fn harness(label: &str, model: &ScriptedModel, run_queue: bool) -> Option<Harness> {
    let store: PgBeamStore = beam_store::testdb::store(label).await?;
    let Ok(url) = std::env::var("NATS_URL") else {
        eprintln!("skipping: NATS_URL not set");
        return None;
    };
    let transport = Arc::new(NatsTransport::connect(&url).await.expect("connect nats"));
    let beam = RiverBeam::builder(BeamConfig::default())
        .pg_store(store.clone())
        .stream(Arc::new(BeamStream::new(transport.clone())))
        .model_caller(Arc::new(model.clone()))
        .tool_executor(Arc::new(ScriptedTools::new()))
        .build()
        .unwrap();
    let name = format!("waiter-{}", Uuid::new_v4().simple());
    let metadata = serde_json::from_value(json!({"name": name})).expect("metadata");
    beam.bind_rig_agent(metadata, RigAgentDef::new(&name));
    beam.checkin_catalog().await.expect("catalog");
    let beam = Arc::new(beam);
    let bus = Arc::new(LocalBus::new());
    if run_queue {
        let queue = Arc::new(QueueWorker::new(
            Arc::new(BeamWorker::new(beam.clone())),
            store.clone(),
            QueueConfig {
                concurrency: 4,
                poll: Duration::from_mins(10),
                lease: Duration::from_secs(10),
                ..QueueConfig::default()
            },
        ));
        tokio::spawn(async move {
            loop {
                if !matches!(queue.run_next().await, Ok(Some(_))) {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        });
    }
    let logstore = PostgresDomainLogStore::new(store.pool().clone()).into_bundle();
    let domain = BeamDomainBundle::spawn_with_namespace(BEAM_NAMESPACE, &store, logstore, bus)
        .await
        .expect("domain");
    let mut ctx = EngineContext::new(BEAM_NAMESPACE);
    ctx.actor.user_id = Some(Uuid::new_v4());
    ctx.actor.tenant = Some(TENANT);
    ctx.organization_id = Some(Uuid::new_v4());
    ctx.roles = vec![beam_domain::USER_ROLE.into()];
    Some(Harness {
        waiter: RunWaiter::new(Arc::new(domain), transport, beam)
            .with_wait(Duration::from_secs(20)),
        agent: beam_store::agent_id_for(&name),
        ctx,
    })
}

fn target(agent: Uuid) -> CommandTarget {
    CommandTarget::Object(AggregateRoot::new("agent", agent.to_string()))
}

fn turn(session: Uuid, key: &str) -> Value {
    json!({
        "session_id": session,
        "content": [{"type": "text", "text": "hello"}],
        "idempotency_key": key,
    })
}

/// A completed run ends `Done`; a repeat of its idempotency key reads the
/// end from the replay; a failed run ends `Failed`.
#[tokio::test]
async fn runs_end_done_replayed_and_failed() {
    let model = ScriptedModel::new([
        ScriptedTurn::text("hello back"),
        ScriptedTurn::Fail("boom".into()),
    ]);
    let Some(h) = harness("showcase_run_waiter", &model, true).await else {
        return;
    };
    let session = Uuid::new_v4();
    let key = Uuid::new_v4().to_string();
    let first = h
        .waiter
        .run(&h.ctx, "invoke", turn(session, &key), target(h.agent))
        .await
        .expect("run");
    let RunEnd::Done(result) = first else {
        panic!("expected Done, got {first:?}");
    };
    assert_eq!(result["content"][0]["text"], "hello back", "{result}");

    let again = h
        .waiter
        .run(&h.ctx, "invoke", turn(session, &key), target(h.agent))
        .await
        .expect("repeat");
    assert!(matches!(again, RunEnd::Done(_)), "{again:?}");
    assert_eq!(model.calls(), 1, "the repeat runs nothing");

    let failed = h
        .waiter
        .run(
            &h.ctx,
            "invoke",
            turn(session, &Uuid::new_v4().to_string()),
            target(h.agent),
        )
        .await
        .expect("run");
    assert!(matches!(failed, RunEnd::Failed(_)), "{failed:?}");
}

/// With no worker, the wait ends and the run is still `running`.
#[tokio::test]
async fn a_run_nobody_works_on_is_pending_after_the_wait() {
    let model = ScriptedModel::new([]);
    let Some(h) = harness("showcase_run_waiter_idle", &model, false).await else {
        return;
    };
    let waiter = h.waiter.with_wait(Duration::from_millis(200));
    let end = waiter
        .run(
            &h.ctx,
            "invoke",
            turn(Uuid::new_v4(), &Uuid::new_v4().to_string()),
            target(h.agent),
        )
        .await
        .expect("run");
    let RunEnd::Pending(ack) = end else {
        panic!("expected Pending, got {end:?}");
    };
    assert_eq!(ack["status"], "running", "{ack}");
}
