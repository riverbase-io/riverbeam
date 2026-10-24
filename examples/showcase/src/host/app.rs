//! Shared wiring for the showcase worker and server binaries.
//!
//! Builds a [`RiverBeam`] with audit to stdout, the showcases' usage plans and
//! knowledge (pgvector when DSN + extension exist). The showcases' agents are
//! bound by [`crate::host::queue::start_queue`] on the built beam.

use std::sync::Arc;

use beam_audit::{AuditLogger, StdoutSink};
use beam_core::{BeamConfig, RiverBeam};
use beam_gateway::{GatewayFactory, RigProviderFactory};
use beam_knowledge::{build_knowledge, KnowledgeBuildRequest};
use beam_routing::{load_aliases_from_yaml, load_models_from_yaml, ModelRouter};
use beam_store::PgBeamStore;
use beam_stream::BeamStream;
use beam_tools::{
    InMemoryToolIdempotencyStore, PgToolIdempotencyStore, ToolIdempotencyStore, ToolRegistry,
    ToolRuntime, ToolService,
};
use beam_usage::{build_reservation_store, build_usage_store, UsageGuard, UsageTracker};
use riverbase_core::config::RiverbaseConfig;
use riverbase_core::transport::{connect_stream_bus, StreamBus};
use riverbase_core::RiverbaseResult;

use crate::host::plans::{all_plans, DEFAULT_PLAN};

/// Default NATS server address (`NATS_URL`).
pub const DEFAULT_NATS_URL: &str = "nats://127.0.0.1:4222";

/// NATS server address from `NATS_URL` or the default.
#[must_use]
pub fn nats_url() -> String {
    std::env::var("NATS_URL").unwrap_or_else(|_| DEFAULT_NATS_URL.to_string())
}

/// The riverbase bus the API's command outbox publishes queue hints to:
/// `RIVERBASE_BUS_URL`, else `BUS_URL`, else [`nats_url`]. The worker must
/// subscribe to the same bus, or tasks wait for its polling tick.
#[must_use]
pub fn bus_url() -> String {
    std::env::var("RIVERBASE_BUS_URL")
        .or_else(|_| std::env::var("BUS_URL"))
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(nats_url)
}

/// Riverbase's bus at [`bus_url`] (NATS, Postgres or Redis): the queue
/// hints, and the riverbase RTC channels run frames are mirrored to. The
/// API and the worker must reach the same bus.
///
/// # Errors
/// The bus URL is invalid or the bus is unreachable.
pub async fn connect_bus() -> RiverbaseResult<Arc<dyn StreamBus>> {
    let mut config = RiverbaseConfig::default();
    config.bus.url = bus_url();
    config.db_url = database_url().unwrap_or_default();
    connect_stream_bus(&config).await
}

/// Postgres connection string from `BEAM_DATABASE_URL`, `DATABASE_URL`, or
/// `FLRS_DATABASE_URL`. The runtime requires Postgres (no in-memory fallback).
#[must_use]
pub fn database_url() -> Option<String> {
    std::env::var("BEAM_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .or_else(|_| std::env::var("FLRS_DATABASE_URL"))
        .ok()
}

/// Connect the Postgres store. The schema must already be migrated
/// (`showcase-worker migrate`); nothing is created on startup. Give it to the
/// beam with `RiverBeamBuilder::pg_store`, which also puts execution records
/// on the same database.
///
/// # Errors
/// Returns an error if no database URL is configured, the connection fails, or
/// the schema is not migrated.
pub async fn connect_store() -> Result<PgBeamStore, Box<dyn std::error::Error>> {
    let url = database_url()
        .ok_or("no database URL: set BEAM_DATABASE_URL, DATABASE_URL, or FLRS_DATABASE_URL")?;
    let store = PgBeamStore::connect(&url).await?;
    store.ensure_migrated().await?;
    Ok(store)
}

/// Apply pending beam migrations to the beam schema (`BEAM_SQL_SCHEMA`).
///
/// # Errors
/// Returns an error if no database URL is configured or a migration fails.
pub async fn run_migrations() -> Result<Vec<i32>, Box<dyn std::error::Error>> {
    let url = database_url()
        .ok_or("no database URL: set BEAM_DATABASE_URL, DATABASE_URL, or FLRS_DATABASE_URL")?;
    // Connecting also applies riverbase's framework migrations.
    let store = PgBeamStore::connect(&url).await?;
    Ok(beam_store::migrate::migrate(&url, store.db().schema()).await?)
}

/// Build the showcases' [`RiverBeam`] on `store` with all subsystems wired, once
/// knowledge, usage, tools, models and audit. No agents yet:
/// [`crate::host::queue::start_queue`] binds every showcase on the built beam.
///
/// # Errors
/// The beam's build errors (`RiverBeamBuilder::build`), and
/// `USAGE_BACKEND=sql`'s when its stores cannot be built.
pub async fn build_beam(
    stream: Arc<BeamStream>,
    store: &PgBeamStore,
) -> Result<RiverBeam, Box<dyn std::error::Error>> {
    // Python's environment keys (SECURITY_*, AUDIT_*, KNOWLEDGE_ENABLED, ...).
    let mut beam = RiverBeam::builder(BeamConfig::from_env())
        .pg_store(store.clone())
        .stream(stream);

    let dim = usize::try_from(beam.config().knowledge_embedding_dim).unwrap_or(1536);
    // Python `KNOWLEDGE_ENABLED` (default true): no manager when off.
    let knowledge = if beam.config().knowledge_enabled {
        // On the beam database's own pool (plan 025 D8).
        build_knowledge(KnowledgeBuildRequest {
            db: Some(store.db().clone()),
            ..KnowledgeBuildRequest::from_env(dim)
        })
        .await
    } else {
        Err(beam_core::BEM_045.with_data("KNOWLEDGE_ENABLED is off"))
    };
    match knowledge {
        // Each showcase ingests its own corpus into its org's scope.
        Ok(knowledge) => beam = beam.knowledge(Arc::new(knowledge)),
        Err(err) => eprintln!("warning: knowledge factory failed ({err}); RAG disabled"),
    }

    // USAGE_BACKEND=sql needs its stores; in memory, a failure only warns.
    let sql_usage = std::env::var("USAGE_BACKEND").is_ok_and(|b| b.trim() == "sql");
    match usage_accounting(Some(store)) {
        Ok((guard, tracker)) => {
            beam = beam.usage_plans(guard, tracker, all_plans(), Some(DEFAULT_PLAN.into()));
        }
        Err(err) if sql_usage => return Err(err),
        Err(err) => eprintln!("warning: usage accounting disabled ({err})"),
    }
    beam = beam.tools(tool_service(Some(store)));
    match load_models() {
        Ok(Some((router, gateway))) => beam = beam.router(router).gateway(gateway),
        Ok(None) => {}
        Err(err) => {
            eprintln!("warning: MODELS_CONFIG_PATH not loaded ({err}); built-in dev model only");
        }
    }
    // AUDIT_SINKS picks the sinks (`sql` writes agent_event); without it the
    // audit goes to stdout only.
    if std::env::var("AUDIT_SINKS").is_err() {
        beam = beam.audit(Arc::new(AuditLogger::new(vec![Arc::new(StdoutSink)])));
    }
    Ok(beam.build()?)
}

/// The routing and the model gateway [`load_models`] reads.
pub type Models = (ModelRouter, Arc<GatewayFactory>);

/// Models from `MODELS_CONFIG_PATH` (with its `aliases`) and `DEFAULT_MODEL`:
/// the routing, and the model gateway every runtime calls through. `None`
/// without the variable (the built-in dev model stays).
///
/// # Errors
/// Returns an error if the file cannot be read or holds duplicate models.
pub fn load_models() -> Result<Option<Models>, Box<dyn std::error::Error>> {
    let Ok(path) = std::env::var("MODELS_CONFIG_PATH") else {
        return Ok(None);
    };
    let models = load_models_from_yaml(&path)?;
    let aliases = load_aliases_from_yaml(&path)?;
    let default_model = std::env::var("DEFAULT_MODEL")
        .ok()
        .filter(|m| !m.is_empty());
    let router = ModelRouter::new(models.clone(), default_model)?.with_aliases(aliases.clone());
    let gateway =
        Arc::new(GatewayFactory::new(models, Arc::new(RigProviderFactory)).with_aliases(aliases));
    Ok(Some((router, gateway)))
}

/// Usage counters from `USAGE_BACKEND` (`in_memory` by default, or `sql`),
/// for the showcases' plans.
/// With `sql`, reservations are shared through the database. SQL usage keys
/// users by UUID, as Python's schema does.
///
/// # Errors
/// Returns an error for an unknown backend, or `sql` without `store`.
pub fn usage_accounting(
    store: Option<&PgBeamStore>,
) -> Result<(Arc<UsageGuard>, Arc<UsageTracker>), Box<dyn std::error::Error>> {
    let backend = std::env::var("USAGE_BACKEND").unwrap_or_default();
    let sql = backend.trim() == "sql";
    let usage = build_usage_store(&backend, store)?;
    let tracker = Arc::new(UsageTracker::new(usage));
    let mut guard = UsageGuard::new(tracker.clone());
    if sql {
        if let Some(reservations) = build_reservation_store(store) {
            guard = guard.with_reservations(reservations);
        }
    }
    Ok((Arc::new(guard), tracker))
}

/// The tool runtime: an empty catalog (apps register tools and MCP servers;
/// the beam adds `knowledge_search` when knowledge is on), and idempotency in
/// Postgres when `store` is given, so a remembered side effect survives a
/// restart.
#[must_use]
pub fn tool_service(store: Option<&PgBeamStore>) -> Arc<ToolService> {
    let idempotency: Arc<dyn ToolIdempotencyStore> = match store {
        Some(store) => Arc::new(PgToolIdempotencyStore::for_store(store)),
        None => Arc::new(InMemoryToolIdempotencyStore::new()),
    };
    // Tool rules and grants (plan 023 part C), on the same storage.
    let (rules, grants): (
        Arc<dyn beam_store::ToolRuleStore>,
        Arc<dyn beam_store::ToolGrantStore>,
    ) = match store {
        Some(store) => {
            let policy = Arc::new(beam_store::PgToolPolicy::for_store(store));
            (policy.clone(), policy)
        }
        None => (
            Arc::new(beam_store::ToolRules::default()),
            Arc::new(beam_store::ToolGrants::default()),
        ),
    };
    Arc::new(ToolService::new(
        Arc::new(ToolRegistry::new().with_mcp(beam_tools::McpSettings::from_env())),
        Arc::new(ToolRuntime::new(idempotency).with_policy(rules, grants)),
    ))
}
