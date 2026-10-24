//! Building and wiring a [`RiverBeam`]: its stores, services and agents.

use std::sync::Arc;

use beam_agents::AgentRegistry;
use beam_context::ManifestStore;
use beam_execution::ExecutionController;
use beam_knowledge::KnowledgeManager;
use beam_memory::TranscriptManager;
use beam_routing::ModelRouter;
use beam_runtime::{
    AgentRuntime, EchoRuntime, RigAgentDef, RigRuntime, RuntimeCapabilities, RuntimeRegistry,
    RuntimeStateStore,
};
use beam_store::{agent_id_for, BeamStore};
use beam_stream::BeamStream;
use beam_tools::ToolService;
use uuid::Uuid;

use crate::config::BeamConfig;

use super::RiverBeam;

impl RiverBeam {
    /// An in-memory beam: `RiverBeam::builder(config).build()` (plan 016 D3),
    /// for tests and quick starts.
    ///
    /// # Panics
    /// When `config` cannot be built ([`RiverBeam::builder`]'s errors:
    /// an unknown `MEMORY_BACKEND` or `AUDIT_SINKS`, security rules that do
    /// not load). [`BeamConfig::default`] always builds.
    #[must_use]
    pub fn new(config: BeamConfig) -> Self {
        Self::builder(config)
            .build()
            .unwrap_or_else(|e| panic!("RiverBeam::new: {e} {}", e.errdata))
    }

    #[must_use]
    pub fn transcript(&self) -> &Arc<TranscriptManager> {
        &self.transcript
    }

    #[must_use]
    pub fn manifest_store(&self) -> &Arc<dyn ManifestStore> {
        &self.manifests
    }

    // --- runtimes ------------------------------------------

    /// Bind an agent to a runtime: every agent runs on
    /// one (plan 021 D3).
    pub fn bind_runtime(
        &self,
        metadata: beam_types::AgentMetadata,
        runtime: Arc<dyn AgentRuntime>,
    ) {
        let name = metadata.name.clone();
        // The bind records an immutable revision.
        self.record_registration(&metadata, runtime.as_ref());
        self.registry.register(metadata);
        self.runtimes.bind(name, runtime);
    }

    /// Bind a rig agent loop. Its runs are saved in [`Self::runtime_states`]:
    /// in Postgres on a beam built with [`super::RiverBeamBuilder::pg_store`].
    #[allow(clippy::must_use_candidate)] // Binding is the effect; the handle is optional.
    pub fn bind_rig_agent(
        &self,
        mut metadata: beam_types::AgentMetadata,
        def: RigAgentDef,
    ) -> Arc<RigRuntime> {
        // The agent declares the tools its definition may call.
        if metadata.tools.is_empty() {
            metadata.tools.clone_from(&def.tools);
        }
        let runtime = Arc::new(RigRuntime::new(def, self.runtime_states.clone()));
        self.bind_runtime(metadata, runtime.clone());
        runtime
    }

    #[must_use]
    pub fn runtimes(&self) -> &Arc<RuntimeRegistry> {
        &self.runtimes
    }

    /// Where runtimes save run state (in memory, or Postgres on a Postgres beam).
    #[must_use]
    pub fn runtime_states(&self) -> &Arc<dyn RuntimeStateStore> {
        &self.runtime_states
    }

    /// The capabilities of the runtime bound to `agent_name`, with Python's field names.
    #[must_use]
    pub fn agent_capabilities(&self, agent_name: &str) -> Option<RuntimeCapabilities> {
        self.runtimes.find(agent_name).map(|r| r.capabilities())
    }

    pub(super) fn runtime_for_agent(&self, agent_id: Uuid) -> Option<Arc<dyn AgentRuntime>> {
        self.runtimes
            .bound_names()
            .into_iter()
            .find(|name| agent_id_for(name) == agent_id)
            .and_then(|name| self.runtimes.find(&name))
    }

    /// The execution control plane (cancel, retry, drain).
    #[must_use]
    pub fn executions(&self) -> &Arc<ExecutionController> {
        &self.executions
    }

    /// The attached stream, if any (the work queue publishes the error frame
    /// of a run that fails before it starts).
    #[must_use]
    pub fn beam_stream(&self) -> Option<&Arc<BeamStream>> {
        self.beam_stream.as_ref()
    }

    #[must_use]
    pub fn store(&self) -> &Arc<dyn BeamStore> {
        &self.store
    }

    #[must_use]
    pub fn tools(&self) -> Option<&Arc<ToolService>> {
        self.tool_source.catalog()
    }

    #[must_use]
    pub fn knowledge(&self) -> Option<&Arc<KnowledgeManager>> {
        self.knowledge.as_ref()
    }

    #[must_use]
    pub fn config(&self) -> &BeamConfig {
        &self.config
    }

    #[must_use]
    pub fn registry(&self) -> &AgentRegistry {
        &self.registry
    }

    #[must_use]
    pub fn router(&self) -> &ModelRouter {
        &self.router
    }

    /// Bind agent `name` to [`EchoRuntime`]: it replies `Echo: <text>`
    /// (tests and local runs).
    pub fn register_echo_agent(&self, name: impl Into<String>) {
        let metadata = beam_types::AgentMetadata {
            name: name.into(),
            description: "Echo agent for tests and local dev".into(),
            supported_modalities: vec![beam_types::Modality::Text],
            output_modalities: vec![beam_types::Modality::Text],
            tags: vec![],
            version: "1.0.0".into(),
            final_nodes: Vec::new(),
            output_contract: None,
            tools: Vec::new(),
        };
        self.bind_runtime(metadata, Arc::new(EchoRuntime::new()));
    }

    #[must_use]
    pub fn list_agents(&self) -> Vec<beam_types::AgentMetadata> {
        self.registry.list_agents()
    }
}
