//! Shared setup for the domain's Postgres tests: a test database, a beam
//! with one echo agent, the domain on riverbase's engines with an in-process
//! bus, and a queue worker that runs what the commands queue.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use beam_core::{BeamConfig, RiverBeam, RiverBeamBuilder};
use beam_domain::{BeamDomainBundle, BeamSql, DomainStore, SessionRecord, BEAM_NAMESPACE};
use beam_store::tasks::TaskRow;
use beam_store::PgBeamStore;
use beam_worker::{BeamWorker, LocalBus, QueueConfig, QueueWorker};
use riverbase_core::base::AggregateRoot;
use riverbase_core::datastore::PostgresDomainLogStore;
use riverbase_core::query::QueryRequest;
use riverbase_core::{CommandTarget, EngineContext};
use serde_json::Value;
use uuid::Uuid;

/// Riverbase requires a tenant on every command (plan 006 D7).
pub const TENANT: Uuid = Uuid::from_u128(0x6265_616d_0000_4000_8000_0000_0000_0001);

/// A test database for `label` (a database per test binary, or per test
/// when its queue runs).
pub async fn store(label: &str) -> Option<PgBeamStore> {
    beam_store::testdb::store(label).await
}

/// Delete every queued task: for databases one test owns, where the queue
/// runs and an aborted earlier run's leftovers would be claimed first.
pub async fn clear_queue(store: &PgBeamStore) {
    use diesel_async::RunQueryDsl;
    let mut conn = store.pool().get().await.expect("conn");
    diesel::sql_query("DELETE FROM ref_beam.agent_task")
        .execute(&mut conn)
        .await
        .expect("clear the queue");
}

/// A `beam-user` in `org`.
pub fn ctx(user: Uuid, org: Uuid) -> EngineContext {
    let mut ctx = EngineContext::new(BEAM_NAMESPACE);
    ctx.actor.user_id = Some(user);
    ctx.actor.tenant = Some(TENANT);
    ctx.organization_id = Some(org);
    ctx.roles = vec![beam_domain::USER_ROLE.into()];
    ctx
}

/// A `beam-admin` in `org`.
pub fn admin(user: Uuid, org: Uuid) -> EngineContext {
    let mut ctx = ctx(user, org);
    ctx.roles = vec![beam_domain::ADMIN_ROLE.into()];
    ctx
}

pub fn list(limit: u64, query: Value) -> QueryRequest {
    QueryRequest {
        limit,
        page: 1,
        user_query: Some(query),
        count: true,
        ..QueryRequest::default()
    }
}

#[allow(clippy::needless_pass_by_value)] // Ids of any type, by value at the call sites.
pub fn object(resource: &str, id: impl ToString) -> CommandTarget {
    CommandTarget::Object(AggregateRoot::new(resource, id.to_string()))
}

pub fn collection(resource: &str) -> CommandTarget {
    CommandTarget::collection(resource)
}

pub struct World {
    pub store: PgBeamStore,
    pub domain: BeamDomainBundle,
    pub bus: Arc<LocalBus>,
    pub queue: Arc<QueueWorker>,
    pub agent_id: Uuid,
    pub agent_name: String,
    pub session: Uuid,
    /// `(user, org)` of the session's owner.
    pub owner: (Uuid, Uuid),
}

impl World {
    pub fn owner_ctx(&self) -> EngineContext {
        ctx(self.owner.0, self.owner.1)
    }

    pub fn agent(&self) -> CommandTarget {
        object("agent", self.agent_id)
    }

    /// Run queued tasks here until `id` has run, and return its row.
    pub async fn run_task(&self, id: Uuid) -> TaskRow {
        for _ in 0..100 {
            match self.queue.run_next().await.expect("run a task") {
                Some(row) if row.id == id => return row,
                Some(_) => {}
                None => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
        panic!("task {id} never ran");
    }

    pub async fn task(&self, id: Uuid) -> TaskRow {
        self.store
            .db()
            .tasks()
            .get(id)
            .await
            .expect("get")
            .expect("task")
    }
}

pub fn uuid_of(value: &Value, key: &str) -> Uuid {
    value[key]
        .as_str()
        .and_then(|s| Uuid::parse_str(s).ok())
        .unwrap_or_else(|| panic!("{key} in {value}"))
}

/// The domain over `store` with a fresh echo agent and one session owned by
/// a new user in a new org.
pub async fn world(store: &PgBeamStore) -> World {
    world_with(store, |builder, name| {
        let beam = builder.build().unwrap();
        beam.register_echo_agent(name);
        beam
    })
    .await
}

/// [`world`] with the agent `setup` builds and binds: it gets the builder
/// (already on `store`) and the world's agent name.
pub async fn world_with(
    store: &PgBeamStore,
    setup: impl FnOnce(RiverBeamBuilder, &str) -> RiverBeam,
) -> World {
    let agent_name = format!("domain-{}", Uuid::new_v4().simple());
    let beam = setup(
        RiverBeam::builder(BeamConfig::default()).pg_store(store.clone()),
        &agent_name,
    );
    beam.checkin_catalog().await.expect("catalog");
    let agent_id = beam_store::agent_id_for(&agent_name);
    let worker = Arc::new(BeamWorker::new(Arc::new(beam)));
    let bus = Arc::new(LocalBus::new());
    let queue = Arc::new(
        QueueWorker::new(
            worker,
            store.clone(),
            QueueConfig {
                concurrency: 4,
                poll: Duration::from_mins(10),
                lease: Duration::from_secs(10),
                ..QueueConfig::default()
            },
        )
        .with_bus(bus.clone()),
    );
    let logstore = PostgresDomainLogStore::new(store.pool().clone()).into_bundle();
    let domain =
        BeamDomainBundle::spawn_with_namespace(BEAM_NAMESPACE, store, logstore, bus.clone())
            .await
            .expect("domain");
    let owner = (Uuid::new_v4(), Uuid::new_v4());
    let session = Uuid::new_v4();
    let caller = beam_domain::Caller {
        user_id: Some(owner.0.to_string()),
        org_id: Some(owner.1.to_string()),
        admin: false,
    };
    DomainStore::for_store(store)
        .record_session(
            &SessionRecord {
                session_id: session,
                agent_id,
                user_id: caller.user_id.clone(),
                org_id: caller.org_id.clone(),
                name: "hello".into(),
                ..SessionRecord::default()
            },
            &caller,
        )
        .await
        .expect("session");
    World {
        store: store.clone(),
        domain,
        bus,
        queue,
        agent_id,
        agent_name,
        session,
        owner,
    }
}
