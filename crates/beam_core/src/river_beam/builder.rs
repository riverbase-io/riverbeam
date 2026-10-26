//! [`RiverBeamBuilder`]: every wiring choice of a [`RiverBeam`], resolved once
//! by [`RiverBeamBuilder::build`] (plan 016 D1). The storage is picked first;
//! each store, sink and service the host did not give follows from it, so no
//! later call can leave a beam half on one storage and half on another.

use std::sync::Arc;

use beam_agents::AgentRegistry;
use beam_audit::AuditLogger;
use beam_context::{
    ContextBuilder, InMemoryManifestStore, ManifestStore, ProfileResolver, ResourceResolver,
    SqlManifestStore,
};
use beam_evals::EvaluationStore;
use beam_execution::{
    ExecutionConfig, ExecutionController, ExecutionRecords, InMemoryExecutionRecords,
    PgExecutionRecords,
};
use beam_gateway::GatewayFactory;
use beam_knowledge::KnowledgeManager;
use beam_memory::{build_transcript_store, TranscriptManager};
use beam_routing::ModelRouter;
use beam_runtime::{
    InMemoryRuntimeStateStore, ModelCaller, NoModel, PgRuntimeStateStore, RuntimeRegistry,
    RuntimeStateStore, ToolExecutor,
};
use beam_security::{
    Detector, DetectorKind, Detectors, SecurityManagement, SecurityPolicy, SecurityStore,
};
use beam_store::{BeamStore, InMemoryStore, LegalHoldStore, OpsStore, PgBeamStore};
use beam_stream::BeamStream;
use beam_tools::{knowledge_search_tool, ToolService};
use beam_usage::{CostStore, InMemoryCostStore, PgCostStore, UsageGuard, UsagePlan, UsageTracker};
use riverbase_core::RiverbaseResult;

use crate::config::BeamConfig;

use super::{
    build_audit, build_security, count_tokens, default_dev_models, ModelSource, RiverBeam,
    ToolSource, UsageAccounting,
};

/// Where a beam keeps its rows.
enum Storage {
    /// Everything in memory (the default).
    Memory,
    /// The host's [`BeamStore`]; execution records stay in memory, so it must
    /// not be durable (an invoke refuses the mix with `BEM-380`).
    Store(Arc<dyn BeamStore>),
    /// Postgres for the store, execution records, runtime state, cost, ops,
    /// revisions, evals, security decisions and the `sql` audit sink.
    Postgres(PgBeamStore),
}

/// Quota enforcement, tracking and plans ([`RiverBeamBuilder::usage_plans`]).
struct Usage {
    guard: Arc<UsageGuard>,
    tracker: Arc<UsageTracker>,
    plans: Vec<UsagePlan>,
    default_plan: Option<String>,
}

/// The wiring of a [`RiverBeam`]: start with [`RiverBeam::builder`], choose,
/// then [`Self::build`]. Anything left unset follows the storage (in memory
/// unless [`Self::pg_store`] or [`Self::store`]) and the [`BeamConfig`].
#[must_use]
pub struct RiverBeamBuilder {
    config: BeamConfig,
    storage: Storage,
    stream: Option<Arc<BeamStream>>,
    knowledge: Option<Arc<KnowledgeManager>>,
    tools: Option<Arc<ToolService>>,
    tool_executor: Option<Arc<dyn ToolExecutor>>,
    model_caller: Option<Arc<dyn ModelCaller>>,
    router: Option<ModelRouter>,
    gateway: Option<Arc<GatewayFactory>>,
    usage: Option<Usage>,
    audit: Option<Arc<AuditLogger>>,
    security: Option<Arc<dyn SecurityManagement>>,
    security_policy: Option<SecurityPolicy>,
    security_detectors: Vec<(DetectorKind, Arc<dyn Detector>)>,
    transcript: Option<TranscriptManager>,
    manifests: Option<Arc<dyn ManifestStore>>,
    cost_store: Option<Arc<dyn CostStore>>,
    ops: Option<Arc<dyn OpsStore>>,
    holds: Option<Arc<dyn LegalHoldStore>>,
    eval_store: Option<Arc<dyn EvaluationStore>>,
    profile_resolver: Option<Arc<dyn ProfileResolver>>,
    resource_resolver: Option<Arc<dyn ResourceResolver>>,
}

impl RiverBeam {
    /// Start wiring a beam with `config`. See [`RiverBeamBuilder`].
    pub fn builder(config: BeamConfig) -> RiverBeamBuilder {
        RiverBeamBuilder {
            config,
            storage: Storage::Memory,
            stream: None,
            knowledge: None,
            tools: None,
            tool_executor: None,
            model_caller: None,
            router: None,
            gateway: None,
            usage: None,
            audit: None,
            security: None,
            security_policy: None,
            security_detectors: Vec::new(),
            transcript: None,
            manifests: None,
            cost_store: None,
            ops: None,
            holds: None,
            eval_store: None,
            profile_resolver: None,
            resource_resolver: None,
        }
    }
}

impl RiverBeamBuilder {
    /// The config this beam is built with.
    #[must_use]
    pub fn config(&self) -> &BeamConfig {
        &self.config
    }

    /// Postgres for both the beam store and execution records (one database:
    /// executions are foreign keys of runs, messages and actions), and for
    /// every store below that is not set.
    pub fn pg_store(mut self, store: PgBeamStore) -> Self {
        self.storage = Storage::Postgres(store);
        self
    }

    /// A store of the host's own, with in-memory execution records. It must
    /// not be durable ([`Self::build`] refuses that: run rows reference
    /// execution rows); use [`Self::pg_store`] for Postgres.
    pub fn store(mut self, store: Arc<dyn BeamStore>) -> Self {
        self.storage = Storage::Store(store);
        self
    }

    /// Publish streaming invokes' chunks to subscribers.
    pub fn stream(mut self, stream: Arc<BeamStream>) -> Self {
        self.stream = Some(stream);
        self
    }

    /// Knowledge: the `knowledge` context source retrieves on each turn, and
    /// with [`Self::tools`] the `knowledge_search` tool is registered.
    pub fn knowledge(mut self, knowledge: Arc<KnowledgeManager>) -> Self {
        self.knowledge = Some(knowledge);
        self
    }

    /// The tool runtime and catalog: runtimes call it, and agents
    /// see only the registered tools they declare.
    pub fn tools(mut self, service: Arc<ToolService>) -> Self {
        self.tools = Some(service);
        self
    }

    /// The tool runtime when there is no [`Self::tools`] catalog (tests and
    /// hosts with their own executor). [`Self::build`] refuses both.
    pub fn tool_executor(mut self, executor: Arc<dyn ToolExecutor>) -> Self {
        self.tool_executor = Some(executor);
        self
    }

    /// The model gateway runtimes call. The default refuses every
    /// call. Not with [`Self::gateway`] (`BEM-149`, plan 021 D4).
    pub fn model_caller(mut self, caller: Arc<dyn ModelCaller>) -> Self {
        self.model_caller = Some(caller);
        self
    }

    /// The model catalog and routing (default: the built-in dev model).
    pub fn router(mut self, router: ModelRouter) -> Self {
        self.router = Some(router);
        self
    }

    /// Route model calls through the gateway: each invoke gets the router's
    /// chain (the routed model plus failover) behind retry, timeout and circuit rules.
    /// Not with [`Self::model_caller`] (`BEM-149`, plan 021 D4).
    pub fn gateway(mut self, factory: Arc<GatewayFactory>) -> Self {
        self.gateway = Some(factory);
        self
    }

    /// Quota enforcement and usage tracking with one plan, the default.
    pub fn usage(
        self,
        guard: Arc<UsageGuard>,
        tracker: Arc<UsageTracker>,
        plan: UsagePlan,
    ) -> Self {
        let name = plan.name.clone();
        self.usage_plans(guard, tracker, vec![plan], Some(name))
    }

    /// Usage plans by name. A user's profile picks the plan; a user without one
    /// gets `default_plan` and a profile for it.
    pub fn usage_plans(
        mut self,
        guard: Arc<UsageGuard>,
        tracker: Arc<UsageTracker>,
        plans: Vec<UsagePlan>,
        default_plan: Option<String>,
    ) -> Self {
        self.usage = Some(Usage {
            guard,
            tracker,
            plans,
            default_plan,
        });
        self
    }

    /// The audit logger. Default: `AUDIT_SINKS` from the config, with the
    /// `sql` sink on Postgres.
    pub fn audit(mut self, audit: Arc<AuditLogger>) -> Self {
        self.audit = Some(audit);
        self
    }

    /// The security manager. Default: an enforcing [`beam_security::SecurityManager`]
    /// from `SECURITY_ENABLED` / `SECURITY_ENFORCE` with [`Self::security_policy`]
    /// (decisions in Postgres on [`Self::pg_store`]), as Python's
    /// `build_components` does; tests that need none pass
    /// [`beam_security::DisabledSecurityManager`].
    pub fn security(mut self, security: Arc<dyn SecurityManagement>) -> Self {
        self.security = Some(security);
        self
    }

    /// The app's security policy
    /// for the default manager. [`Self::build`] refuses it with
    /// [`Self::security`], which brings its own policy.
    pub fn security_policy(mut self, policy: SecurityPolicy) -> Self {
        self.security_policy = Some(policy);
        self
    }

    /// A detector the config-built security manager runs beside the
    /// built-ins of `kind` (a classifier or a guard model, say). Excludes
    /// [`Self::security`], which brings its own detectors.
    pub fn security_detector(mut self, kind: DetectorKind, detector: Arc<dyn Detector>) -> Self {
        self.security_detectors.push((kind, detector));
        self
    }

    /// The session transcript. Default: `MEMORY_BACKEND` over the store.
    pub fn transcript(mut self, transcript: TranscriptManager) -> Self {
        self.transcript = Some(transcript);
        self
    }

    /// Where context manifests go. Default: in memory, or the store's for
    /// [`Self::store`] and [`Self::pg_store`].
    pub fn manifest_store(mut self, manifests: Arc<dyn ManifestStore>) -> Self {
        self.manifests = Some(manifests);
        self
    }

    /// Where cost events go. Default: in memory, or Postgres.
    pub fn cost_store(mut self, store: Arc<dyn CostStore>) -> Self {
        self.cost_store = Some(store);
        self
    }

    /// The stream outbox and dead letters. Default: in memory, or Postgres.
    pub fn ops(mut self, ops: Arc<dyn OpsStore>) -> Self {
        self.ops = Some(ops);
        self
    }

    /// Legal holds. Default: in memory, or Postgres (`agent_legal_hold`), so a hold placed anywhere stops every process's deletion.
    pub fn holds(mut self, holds: Arc<dyn LegalHoldStore>) -> Self {
        self.holds = Some(holds);
        self
    }

    /// Evaluations and feedback. Default: in memory, or Postgres.
    pub fn eval_store(mut self, store: Arc<dyn EvaluationStore>) -> Self {
        self.eval_store = Some(store);
        self
    }

    /// Execution timing and worker identity (default: the config's).
    pub fn execution_config(mut self, config: ExecutionConfig) -> Self {
        self.config.execution = config;
        self
    }

    /// `MEMORY_LONGTERM_ENABLED` (the `ltm` context source) and
    /// `MEMORY_LONGTERM_AUTO_REMEMBER`, overriding the config.
    pub fn long_term_memory(mut self, enabled: bool, auto_remember: bool) -> Self {
        self.config.longterm_enabled = enabled;
        self.config.auto_remember = auto_remember;
        self
    }

    /// The `profile` context source's resolver (none: profiles are unavailable).
    pub fn profile_resolver(mut self, resolver: Arc<dyn ProfileResolver>) -> Self {
        self.profile_resolver = Some(resolver);
        self
    }

    /// The `resources` context source's resolver (default: authorize, never fetch).
    pub fn resource_resolver(mut self, resolver: Arc<dyn ResourceResolver>) -> Self {
        self.resource_resolver = Some(resolver);
        self
    }

    /// The beam. Agents are bound on it afterwards ([`RiverBeam::bind_rig_agent`],
    /// [`RiverBeam::register_echo_agent`]), so they always see its final stores.
    ///
    /// # Errors
    /// `BEM-144` for a `MEMORY_BACKEND` with no transcript backend, `BEM-147`
    /// for an unknown `AUDIT_SINKS` key, `BEM-149` for choices that exclude
    /// each other ([`Self::tools`] with [`Self::tool_executor`], [`Self::gateway`] with
    /// [`Self::model_caller`], [`Self::security`] with
    /// [`Self::security_policy`] or [`Self::security_detector`], a durable
    /// [`Self::store`]), and an invalid
    /// router's error.
    #[allow(clippy::too_many_lines)] // One line per field of the beam it assembles.
    pub fn build(self) -> RiverbaseResult<RiverBeam> {
        let Self {
            config,
            storage,
            stream,
            knowledge,
            tools,
            tool_executor,
            model_caller,
            router,
            gateway,
            usage,
            audit,
            security,
            security_policy,
            security_detectors,
            transcript,
            manifests,
            cost_store,
            ops,
            holds,
            eval_store,
            profile_resolver,
            resource_resolver,
        } = self;
        if tools.is_some() && tool_executor.is_some() {
            return Err(crate::BEM_149.with_data("tools and tool_executor"));
        }
        // Plan 021 D4: a run's models come from one place.
        if gateway.is_some() && model_caller.is_some() {
            return Err(crate::BEM_149.with_data("gateway and model_caller"));
        }
        if security.is_some() && security_policy.is_some() {
            return Err(crate::BEM_149.with_data("security and security_policy"));
        }
        if security.is_some() && !security_detectors.is_empty() {
            return Err(crate::BEM_149.with_data("security and security_detector"));
        }
        if matches!(&storage, Storage::Store(store) if store.durable()) {
            return Err(crate::BEM_149.with_data("a durable store needs pg_store"));
        }
        let pg = match &storage {
            Storage::Postgres(store) => Some(store.clone()),
            Storage::Memory | Storage::Store(_) => None,
        };
        let follows_store = !matches!(storage, Storage::Memory);
        let store: Arc<dyn BeamStore> = match storage {
            Storage::Memory => Arc::new(InMemoryStore::new()),
            Storage::Store(store) => store,
            Storage::Postgres(store) => Arc::new(store),
        };

        let (records, runtime_states) = execution_storage(pg.as_ref());
        let transcript = if let Some(transcript) = transcript {
            transcript
        } else {
            let manager = TranscriptManager::new(build_transcript_store(
                &config.memory_backend,
                &store,
                config.max_stored_messages,
            )?)
            .with_counter(Arc::new(count_tokens));
            // Durable summaries on Postgres (plan 023 B3), for a durable
            // transcript only: an in-memory one restarts its sequences, and a
            // stored summary's coverage would then hide new messages.
            let durable = matches!(config.memory_backend.trim(), "sql" | "sql_cached");
            match &pg {
                Some(pg) if durable => {
                    manager.with_summaries(Arc::new(beam_memory::PgSummaryStore(
                        beam_store::session_summary::PgSessionSummaries::for_store(pg),
                    )))
                }
                _ => manager,
            }
        };
        let manifests = manifests.unwrap_or_else(|| {
            if follows_store {
                Arc::new(SqlManifestStore::new(store.clone()))
            } else {
                Arc::new(InMemoryManifestStore::default())
            }
        });

        let detectors = security_detectors
            .into_iter()
            .fold(Detectors::builtin(), |set, (kind, detector)| {
                set.with(kind, detector)
            });
        let security = resolve_security(
            &config,
            security,
            &security_policy.unwrap_or_default(),
            detectors,
            pg.as_ref(),
        );
        let audit = resolve_audit(&config, audit, pg.as_ref())?;
        let cost_store = cost_store.unwrap_or_else(|| match &pg {
            Some(pg) => Arc::new(PgCostStore::for_store(pg)),
            None => Arc::new(InMemoryCostStore::new()),
        });
        let ops = ops.unwrap_or_else(|| match &pg {
            Some(pg) => Arc::new(beam_store::PgOpsStore::for_store(pg)),
            None => Arc::new(beam_store::InMemoryOps::new()),
        });
        let holds = holds.unwrap_or_else(|| match &pg {
            Some(pg) => Arc::new(beam_store::PgLegalHolds::for_store(pg)),
            None => Arc::new(beam_store::LegalHolds::new()),
        });
        let eval_store = eval_store.unwrap_or_else(|| match &pg {
            Some(pg) => Arc::new(beam_evals::PgEvaluationStore::for_store(pg)),
            None => Arc::new(beam_evals::InMemoryEvaluationStore::new()),
        });

        // One tool runtime: the catalog when there is one, with
        // `knowledge_search` registered in it once.
        if let (Some(tools), Some(knowledge)) = (&tools, &knowledge) {
            tools
                .registry()
                .register(knowledge_search_tool(knowledge.clone()));
        }
        let tool_source = match (tools, tool_executor) {
            (Some(tools), _) => ToolSource::Catalog(tools),
            (None, Some(executor)) => ToolSource::Executor(executor),
            (None, None) => ToolSource::None,
        };
        let models = match gateway {
            Some(factory) => ModelSource::Gateway(factory),
            None => ModelSource::Caller(model_caller.unwrap_or_else(|| Arc::new(NoModel))),
        };
        let router = match router {
            Some(router) => router,
            None => ModelRouter::new(default_dev_models(), config.default_model.clone())?,
        };
        let usage = usage.map(|u| UsageAccounting {
            guard: u.guard,
            tracker: u.tracker,
            plans: u.plans.into_iter().map(|p| (p.name.clone(), p)).collect(),
            default_plan: u.default_plan,
        });

        Ok(RiverBeam {
            registry: Arc::new(AgentRegistry::new()),
            router,
            store,
            knowledge,
            transcript: Arc::new(transcript),
            context_builder: ContextBuilder::default(),
            manifests,
            profile_resolver,
            resource_resolver,
            usage,
            cost_store,
            models,
            audit,
            security,
            beam_stream: stream,
            executions: Arc::new(ExecutionController::new(records, config.execution.clone())),
            runtimes: Arc::new(RuntimeRegistry::new()),
            runtime_states,
            tool_source,
            ops,
            holds,
            revisions: Arc::new(beam_execution::revisions::RevisionBook::new()),
            revision_store: pg.as_ref().map(beam_execution::PgRevisionStore::for_store),
            eval_store,
            config,
        })
    }
}

/// Execution records and runtime state on the beam's storage (runtime state
/// is fenced by the records' leases).
fn execution_storage(
    pg: Option<&PgBeamStore>,
) -> (Arc<dyn ExecutionRecords>, Arc<dyn RuntimeStateStore>) {
    if let Some(pg) = pg {
        return (
            Arc::new(PgExecutionRecords::for_store(pg)),
            Arc::new(PgRuntimeStateStore::for_store(pg)),
        );
    }
    let records: Arc<dyn ExecutionRecords> = Arc::new(InMemoryExecutionRecords::new());
    let states = InMemoryRuntimeStateStore::new().fenced_by(records.clone());
    (records, Arc::new(states))
}

/// The host's security manager, or the config-built one (its decisions in
/// Postgres on [`RiverBeamBuilder::pg_store`]).
fn resolve_security(
    config: &BeamConfig,
    security: Option<Arc<dyn SecurityManagement>>,
    policy: &SecurityPolicy,
    detectors: Detectors,
    pg: Option<&PgBeamStore>,
) -> Arc<dyn SecurityManagement> {
    if let Some(security) = security {
        return security;
    }
    let decisions = pg.map(|pg| {
        Arc::new(beam_security::PgSecurityStore::for_store(pg)) as Arc<dyn SecurityStore>
    });
    build_security(config, policy.clone(), detectors, decisions)
}

/// The host's audit logger, or `AUDIT_SINKS` from the config (with the `sql`
/// sink on Postgres).
fn resolve_audit(
    config: &BeamConfig,
    audit: Option<Arc<AuditLogger>>,
    pg: Option<&PgBeamStore>,
) -> RiverbaseResult<Arc<AuditLogger>> {
    if let Some(audit) = audit {
        return Ok(audit);
    }
    let sql =
        pg.map(|pg| Arc::new(beam_audit::SqlSink::for_store(pg)) as Arc<dyn beam_audit::AuditSink>);
    Ok(Arc::new(build_audit(config, sql.as_ref())?))
}
