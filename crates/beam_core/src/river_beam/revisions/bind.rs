//! Binding, publishing and pinning revisions (Python
//! `orchestration/invocation/publication.py` and the revision steps of
//! `persistence.py`).

use super::spec::{
    check_model, context_spec, invalid, provider_label, spec_from_registration, validate_runtime,
    validate_spec, UNBOUND_RUNTIME,
};
use crate::river_beam::RiverBeam;
use beam_audit::{AuditAction, AuditEvent};
use beam_context::policy::ContextSpec;
use beam_execution::revisions::{
    snapshot_from_spec, AgentRevision, BoundRuntime, DeployedRuntime, RevisionBook,
    RevisionSnapshot,
};
use beam_runtime::AgentRuntime;
use beam_types::{AgentInput, AgentMetadata, AgentSpec, ModelConfig};
use riverbase_core::RiverbaseResult;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::sync::Arc;
use uuid::Uuid;

impl RiverBeam {
    /// The revision directory.
    #[must_use]
    pub fn revisions(&self) -> &Arc<RevisionBook> {
        &self.revisions
    }

    /// Validate the spec (and, with
    /// `resolve_refs`, that its tools and model exist here) and build its
    /// revision snapshot, without running the agent.
    ///
    /// # Errors
    /// `BEM-494` with Python's message.
    pub fn compile_spec(
        &self,
        spec: &AgentSpec,
        runtime: Option<&dyn AgentRuntime>,
        resolve_refs: bool,
    ) -> RiverbaseResult<RevisionSnapshot> {
        validate_spec(spec)?;
        if let Some(runtime) = runtime {
            validate_runtime(&spec.runtime, runtime)?;
        }
        if resolve_refs {
            self.resolve_spec_refs(spec)?;
        }
        Ok(snapshot_from_spec(
            spec,
            &BoundRuntime {
                adapter_version: runtime.map(|r| r.adapter_version().to_owned()),
                library_version: runtime.map(|r| r.library_version().to_owned()),
            },
        ))
    }

    /// Python `_resolve_tools` and `_resolve_model` (no knowledge catalog
    /// to check collections against, as in Python without one).
    pub(super) fn resolve_spec_refs(&self, spec: &AgentSpec) -> RiverbaseResult<()> {
        if let Some(tools) = self.tool_source.catalog() {
            let known = tools.registry().list_tool_names();
            if let Some(missing) = spec.tools.allowlist.iter().find(|t| !known.contains(t)) {
                return Err(invalid(format!("missing tool: {missing}")));
            }
        }
        let pin = spec.model.route.as_deref().or(spec.model.name.as_deref());
        let Some(pin) = pin else {
            if spec.model.provider.is_some() {
                return Err(invalid("model provider requires a pinned model name"));
            }
            return Ok(());
        };
        let selected = self
            .router
            .resolve_name(pin)?
            .ok_or_else(|| invalid(format!("missing model: {pin}")))?;
        check_model(spec, pin, &selected)
    }

    /// Record the revision of an agent bound without a spec (Python
    /// `bind_agent` through `spec_from_registration`).
    pub(in crate::river_beam) fn record_registration(
        &self,
        metadata: &AgentMetadata,
        runtime: &dyn AgentRuntime,
    ) {
        let spec = spec_from_registration(metadata, runtime.runtime_name());
        match self.compile_spec(&spec, Some(runtime), false) {
            Ok(snapshot) => {
                let recorded = self.revisions.record_bind(&snapshot).and_then(|row| {
                    self.revisions.set_compatibility(
                        row.id,
                        spec.execution.compatibility,
                        BTreeSet::new(),
                    )
                });
                if let Err(e) = recorded {
                    tracing::warn!(agent = %metadata.name, error = %e, "recording the bind's revision failed");
                }
            }
            Err(e) => {
                tracing::warn!(agent = %metadata.name, error = %e, "the bound agent's spec does not compile");
            }
        }
    }

    /// Compile `spec` against this beam and `runtime`,
    /// bind the agent, record the revision (the first is active, later ones
    /// drafts) and audit `AGENT_PUBLISHED`.
    ///
    /// # Errors
    /// `BEM-494`; store errors.
    pub async fn publish_agent(
        &self,
        spec: AgentSpec,
        runtime: Arc<dyn AgentRuntime>,
    ) -> RiverbaseResult<AgentRevision> {
        let snapshot = self.compile_spec(&spec, Some(runtime.as_ref()), true)?;
        let metadata = AgentMetadata {
            name: spec.name.clone(),
            description: spec.description.clone(),
            supported_modalities: vec![beam_types::Modality::Text],
            output_modalities: vec![beam_types::Modality::Text],
            tags: spec.tags.clone(),
            version: spec.version.clone(),
            final_nodes: Vec::new(),
            output_contract: None,
            tools: spec.tools.allowlist.clone(),
        };
        self.registry.register(metadata);
        self.runtimes.bind(spec.name.clone(), runtime);
        let row = self.revisions.record_bind(&snapshot)?;
        let row = self.revisions.set_compatibility(
            row.id,
            spec.execution.compatibility,
            BTreeSet::new(),
        )?;
        self.persist_revisions().await?;
        self.audit
            .log(AuditEvent::new(AuditAction::AgentPublished).agent_name(Some(spec.name.clone())))
            .await;
        Ok(row)
    }

    /// Write what changed in the revision book.
    pub(super) async fn persist_revisions(&self) -> RiverbaseResult<()> {
        match &self.revision_store {
            Some(store) => store.persist(&self.revisions).await,
            None => Ok(()),
        }
    }

    /// Load stored revisions and traffic (startup).
    ///
    /// # Errors
    /// Store errors.
    pub async fn reconstruct_revisions(&self) -> RiverbaseResult<()> {
        match &self.revision_store {
            Some(store) => store.reconstruct(&self.revisions).await,
            None => Ok(()),
        }
    }

    /// Python `_resolve_revision` after `ensure`: a pinned execution resumes
    /// under its compatibility policy; a new one is pinned to the traffic's
    /// pick (canary by execution id), or to `named` (an eval case's
    /// revision). The pin, the runtime versions and a spec's narrower
    /// timeout are written to the execution.
    ///
    /// # Errors
    /// `BEM-490` when `named` is not a revision of the invoked agent.
    #[allow(clippy::too_many_lines)]
    pub(in crate::river_beam) async fn resolve_revision(
        &self,
        input: &mut AgentInput,
        execution: &mut beam_execution::ExecutionRow,
        named: Option<Uuid>,
    ) -> RiverbaseResult<()> {
        let agent = input.agent_name.clone().unwrap_or_else(|| "agent".into());
        let runtime = self.runtimes.find(&agent);
        let runtime_name = runtime.as_ref().map_or_else(
            || UNBOUND_RUNTIME.to_owned(),
            |r| r.runtime_name().to_owned(),
        );
        // The stored pin wins over this worker's memory (another worker may
        // have moved it); Python restores it on every invoke.
        if let Some(pinned) = execution.agent_revision_id {
            if self.revisions.pin_of(execution.id) != Some(pinned) {
                self.revisions.forget_pin(execution.id);
                self.revisions
                    .restore_pin(execution.id, pinned, &agent, &runtime_name)?;
            }
        }
        let resuming = self.revisions.pin_of(execution.id).is_some();
        let mut migrated_ref = None;
        let revision = if resuming {
            let deployed = DeployedRuntime {
                adapter_version: runtime.as_ref().map(|r| r.adapter_version().to_owned()),
                library_version: runtime.as_ref().map(|r| r.library_version().to_owned()),
                can_read_checkpoint: None,
                // What wrote the checkpoint, as the execution recorded it.
                pinned_versions: Some((
                    execution.runtime_adapter_version.clone(),
                    execution.runtime_library_version.clone(),
                )),
            };
            let stored_ref = execution
                .runtime_ref
                .clone()
                .or_else(|| input.runtime_ref.clone());
            let resolved = self.revisions.resolve_resume(
                execution.id,
                &agent,
                stored_ref.as_ref(),
                &deployed,
            )?;
            // MIGRATABLE: the migrated ref is what the runtime resumes from.
            if resolved.runtime_ref.is_some() && resolved.runtime_ref != stored_ref {
                migrated_ref.clone_from(&resolved.runtime_ref);
                execution.runtime_ref.clone_from(&resolved.runtime_ref);
                input.runtime_ref.clone_from(&resolved.runtime_ref);
            }
            resolved.revision
        } else {
            let picked = match named {
                Some(id) => Some(
                    self.revisions
                        .get_revision(id)
                        .filter(|r| r.agent_name == agent)
                        .ok_or_else(|| {
                            crate::BEM_490.with_data(format!("unknown revision {id} of {agent}"))
                        })?,
                ),
                None => self
                    .revisions
                    .pick_for_new(&agent, Some(execution.id))
                    .and_then(|id| self.revisions.get_revision(id)),
            };
            let revision = match picked {
                Some(revision) => revision,
                None => self
                    .revisions
                    .pin_revision(&agent, "1.0.0", &runtime_name)?,
            };
            self.revisions.bind_execution(execution.id, revision.id)?;
            revision
        };
        let spec = revision.spec();
        // A spec narrows this attempt's deadline,
        // counted from now: a resumed or retried execution is not timed out
        // by its first attempt's clock.
        let timeout_at = spec
            .as_ref()
            .and_then(|s| s.execution.timeout_s)
            .filter(|t| *t > 0)
            .map(|t| {
                let narrowed = chrono::Utc::now() + chrono::Duration::seconds(t);
                execution
                    .timeout_at
                    .map_or(narrowed, |current| current.min(narrowed))
            })
            .filter(|t| Some(*t) != execution.timeout_at);
        if execution.agent_revision_id != Some(revision.id)
            || timeout_at.is_some()
            || migrated_ref.is_some()
        {
            // A new pin records the versions that will write its checkpoints;
            // a resume keeps the versions that wrote them.
            let new_pin = execution.agent_revision_id.is_none();
            self.executions
                .records()
                .set_revision(
                    execution.id,
                    beam_execution::RevisionPin {
                        revision_id: revision.id,
                        adapter_version: revision
                            .runtime_adapter_version
                            .as_deref()
                            .filter(|_| new_pin),
                        library_version: revision
                            .runtime_library_version
                            .as_deref()
                            .filter(|_| new_pin),
                        timeout_at,
                        runtime_ref: migrated_ref.as_ref(),
                    },
                )
                .await?;
            if new_pin {
                execution
                    .runtime_adapter_version
                    .clone_from(&revision.runtime_adapter_version);
                execution
                    .runtime_library_version
                    .clone_from(&revision.runtime_library_version);
            }
            execution.agent_revision_id = Some(revision.id);
            if timeout_at.is_some() {
                execution.timeout_at = timeout_at;
            }
        }
        // Python `invocation_dry_run(spec.tools.dry_run)`.
        if spec.as_ref().is_some_and(|s| s.tools.dry_run) {
            if let Value::Object(map) = &mut input.metadata {
                map.insert("dry_run".into(), Value::Bool(true));
            } else {
                input.metadata = json!({"dry_run": true});
            }
        }
        if self.revisions.is_dirty() {
            if let Err(e) = self.persist_revisions().await {
                tracing::warn!(error = %e, "persisting revisions failed");
            }
        }
        Ok(())
    }

    /// The spec the invoke's execution is pinned to.
    pub(in crate::river_beam) fn pinned_spec(&self, input: &AgentInput) -> Option<AgentSpec> {
        self.revisions
            .snapshot_for_execution(input.execution_id?)?
            .spec()
    }

    /// The spec's pinned model (by route or
    /// name, through the router) and credential ref.
    pub(in crate::river_beam) fn apply_model_policy(
        &self,
        model: ModelConfig,
        spec: Option<&AgentSpec>,
    ) -> RiverbaseResult<ModelConfig> {
        let Some(spec) = spec else { return Ok(model) };
        let mut selected = model;
        if let Some(pin) = spec.model.route.as_deref().or(spec.model.name.as_deref()) {
            let resolved = self
                .router
                .resolve_name(pin)?
                .ok_or_else(|| invalid(format!("missing model: {pin}")))?;
            check_model(spec, pin, &resolved)?;
            selected = resolved;
        } else if let Some(provider) = &spec.model.provider {
            let actual = provider_label(&selected);
            if &actual != provider {
                return Err(invalid(format!(
                    "model provider mismatch: {actual} != {provider}"
                )));
            }
        }
        if let Some(credential) = &spec.model.credential_ref {
            selected.credential_ref = Some(credential.clone());
            selected.api_key = None;
        }
        Ok(selected)
    }

    /// The context-policy spec and tool allowlist a pinned spec narrows to.
    pub(in crate::river_beam) fn spec_context(
        &self,
        spec: &AgentSpec,
    ) -> (ContextSpec, Vec<String>) {
        let registered = self
            .tool_source
            .catalog()
            .map(|t| t.registry().list_tool_names())
            .unwrap_or_default();
        let tools = spec
            .tools
            .allowlist
            .iter()
            .filter(|t| registered.contains(t))
            .cloned()
            .collect();
        (context_spec(spec), tools)
    }

    // --- revision lifecycle ----------------
}
