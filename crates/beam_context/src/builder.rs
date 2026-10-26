//! Typed context orchestration (Python `context/builder.py` and
//! `context/registry.py`). Sources retrieve; the composer selects.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Instant;

use beam_gateway::TokenCounter;
use beam_knowledge::Hit;
use beam_types::{AgentInput, ModelConfig, Spotlight};
use chrono::{DateTime, Utc};
use futures_util::future::join_all;
use riverbase_core::RiverbaseResult;
use serde_json::{json, Value};

use crate::composer::{model_output_tokens, model_window, ComposeOptions, ContextComposer};
use crate::item::{dropped_source, unique_decisions};
use crate::policy::{compile_context_policy, ContextSpec, FailMode, ResolvedContextPolicy};
use crate::services::{
    AuthorizedToolset, ContextBuildRequest, ContextBuildResult, ContextServices, SourceArtifact,
    SourceDiagnostic, SourceResult,
};
use crate::sources::{builtin_sources, ContextSource};
use crate::types::{
    default_sources, is_io_source, registry_order, DecisionAction, ExecutionContext, Trust,
};

/// Unique sources, iterated in registry order.
#[derive(Clone)]
pub struct ContextSourceRegistry {
    sources: Vec<Arc<dyn ContextSource>>,
}

/// `summary` follows `history`.
#[must_use]
pub fn source_enabled(source: &str, policy: &ResolvedContextPolicy) -> bool {
    if source == "summary" {
        policy.enables("history")
    } else {
        policy.enables(source)
    }
}

impl ContextSourceRegistry {
    /// # Errors
    /// `BEM-486` when two sources claim the same name.
    pub fn new(sources: Vec<Arc<dyn ContextSource>>) -> RiverbaseResult<Self> {
        let mut registry = Self {
            sources: Vec::new(),
        };
        for source in sources {
            registry.register(source)?;
        }
        Ok(registry)
    }

    /// # Errors
    /// `BEM-486` for a duplicate source name.
    pub fn register(&mut self, source: Arc<dyn ContextSource>) -> RiverbaseResult<()> {
        if self.get(source.source()).is_some() {
            return Err(crate::BEM_486.with_data(format!(
                "duplicate context provider for source {}",
                source.source()
            )));
        }
        self.sources.push(source);
        Ok(())
    }

    #[must_use]
    pub fn get(&self, name: &str) -> Option<&Arc<dyn ContextSource>> {
        self.sources.iter().find(|s| s.source() == name)
    }

    #[must_use]
    pub fn ordered(&self) -> Vec<&Arc<dyn ContextSource>> {
        registry_order()
            .into_iter()
            .filter_map(|name| self.get(name))
            .collect()
    }

    #[must_use]
    pub fn enabled(&self, policy: &ResolvedContextPolicy) -> Vec<&Arc<dyn ContextSource>> {
        self.ordered()
            .into_iter()
            .filter(|s| source_enabled(s.source(), policy))
            .collect()
    }

    #[must_use]
    pub fn disabled_sources(&self, policy: &ResolvedContextPolicy) -> Vec<&'static str> {
        registry_order()
            .into_iter()
            .filter(|name| {
                self.get(name).is_some() && !source_enabled(name, policy) && *name != "summary"
            })
            .collect()
    }
}

impl Default for ContextSourceRegistry {
    /// Python `builtin_registry`.
    fn default() -> Self {
        Self {
            sources: builtin_sources(),
        }
    }
}

/// Builds a turn's context: resolves the sources, then composes the envelope.
#[derive(Clone, Default)]
pub struct ContextBuilder {
    pub registry: ContextSourceRegistry,
}

fn raise_if_deadline(execution: &ExecutionContext) -> RiverbaseResult<()> {
    if execution.deadline.is_some_and(|d| Utc::now() > d) {
        return Err(crate::BEM_483.with_data("context build deadline exceeded"));
    }
    Ok(())
}

fn stamp_latency(mut result: SourceResult, started: Instant) -> SourceResult {
    let latency = started.elapsed().as_secs_f64() * 1000.0;
    let mut diagnostic = result
        .diagnostics
        .take()
        .unwrap_or_else(|| SourceDiagnostic::new("", result.items.len()));
    diagnostic.latency_ms = Some(latency);
    result.diagnostics = Some(diagnostic);
    result
}

/// What a failed or timed-out I/O source records (Python files a failed
/// `summary` under its own name and a timed-out one under `history`).
fn failed_source(source: &str, reason: &str) -> SourceResult {
    let decision_source = match (source, reason) {
        ("summary", "timeout") => "history",
        _ => source,
    };
    SourceResult::dropped(
        dropped_source(decision_source, reason),
        SourceDiagnostic::failed(source, reason),
    )
}

/// The policy source a failure mode is looked up under.
fn fail_source(source: &str) -> &str {
    if source == "summary" {
        "history"
    } else {
        source
    }
}

impl ContextBuilder {
    /// Python `ContextBuilder.build`.
    ///
    /// # Errors
    /// `BEM-482` when cancelled, `BEM-483` past the deadline, `BEM-480` on
    /// overflow, and a fail-closed source's error.
    #[allow(clippy::too_many_lines)]
    pub async fn build(
        &self,
        request: &ContextBuildRequest,
    ) -> RiverbaseResult<ContextBuildResult> {
        if request.execution.cancelled {
            return Err(crate::BEM_482.with_data("context build cancelled"));
        }
        raise_if_deadline(&request.execution)?;
        let policy = &request.policy;

        let enabled = self.registry.enabled(policy);
        let mut extra: Vec<_> = self
            .registry
            .disabled_sources(policy)
            .into_iter()
            .map(|s| dropped_source(s, "disabled"))
            .collect();
        extra.extend(
            default_sources()
                .iter()
                .filter(|s| !policy.enables(s))
                .map(|s| dropped_source(s, "disabled")),
        );
        if policy.knowledge_mode == "tool" {
            for row in &mut extra {
                if row.source == "knowledge" {
                    row.reason = "tool_only".into();
                }
            }
            extra.push(dropped_source("knowledge", "tool_only"));
        }

        let mut by_source: HashMap<&'static str, SourceResult> = HashMap::new();
        for source in enabled.iter().filter(|s| !is_io_source(s.source())) {
            let started = Instant::now();
            let result = source.resolve(request).await?;
            by_source.insert(source.source(), stamp_latency(result, started));
        }
        let io: Vec<&Arc<dyn ContextSource>> = enabled
            .iter()
            .copied()
            .filter(|s| is_io_source(s.source()))
            .collect();
        if !io.is_empty() {
            by_source.extend(invoke_io(&io, request).await?);
        }

        let mut items = Vec::new();
        let mut artifacts = Vec::new();
        let mut diagnostics = BTreeMap::new();
        let mut timings = BTreeMap::new();
        for source in self.registry.ordered() {
            let Some(result) = by_source.remove(source.source()) else {
                continue;
            };
            items.extend(result.items);
            extra.extend(result.decisions);
            artifacts.extend(result.artifacts);
            if let Some(diagnostic) = result.diagnostics {
                if let Some(latency) = diagnostic.latency_ms {
                    timings.insert(source.source().to_owned(), latency);
                }
                diagnostics.insert(source.source().to_owned(), diagnostic);
            }
        }

        let schemas = artifacts
            .iter()
            .find_map(|a| match a {
                SourceArtifact::ToolSchemas(s) => Some(s.clone()),
                SourceArtifact::CitationHits(_) => None,
            })
            .unwrap_or_default();
        let hits: Vec<Hit> = artifacts
            .iter()
            .find_map(|a| match a {
                SourceArtifact::CitationHits(h) => Some(h.clone()),
                SourceArtifact::ToolSchemas(_) => None,
            })
            .unwrap_or_default();
        let (envelope, mut manifest) = ContextComposer {
            counter: request.counter,
            spotlight: request.spotlight,
        }
        .compose(
            &items,
            &request.model,
            policy,
            ComposeOptions {
                revision_id: request.revision_id.clone(),
                tool_schemas: schemas,
                configured_sources: default_sources(),
                extra_decisions: unique_decisions(extra),
            },
        )?;
        if !timings.is_empty() {
            manifest.source_timings = timings;
        }
        let selected: Vec<&str> = manifest
            .decisions
            .iter()
            .filter(|d| d.action == DecisionAction::Selected)
            .map(|d| d.item_id.as_str())
            .collect();
        let citations = citations_for_selected(&items, &selected, &hits);
        let selected_untrusted = items
            .iter()
            .filter(|i| selected.contains(&i.id.as_str()) && i.trust == Trust::Untrusted)
            .cloned()
            .collect();
        Ok(ContextBuildResult {
            envelope,
            manifest,
            execution: request.execution.clone(),
            toolset: AuthorizedToolset {
                names: request.tool_names.clone(),
                schema_revision: Some(policy.serializer_revision.clone()),
            },
            citations,
            provider_diagnostics: diagnostics,
            artifacts,
            selected_untrusted,
        })
    }
}

/// I/O sources run concurrently under the deadline. A
/// failure or timeout drops the source unless its policy fails closed.
async fn invoke_io(
    sources: &[&Arc<dyn ContextSource>],
    request: &ContextBuildRequest,
) -> RiverbaseResult<HashMap<&'static str, SourceResult>> {
    raise_if_deadline(&request.execution)?;
    let policy = &request.policy;
    let all = join_all(sources.iter().map(|source| async move {
        let started = Instant::now();
        match source.resolve(request).await {
            Ok(result) => Ok((source.source(), stamp_latency(result, started))),
            Err(e) if policy.fail_mode(fail_source(source.source())) == FailMode::FailClosed => {
                Err(e)
            }
            Err(e) => {
                tracing::warn!(source = source.source(), error = %e, "context source failed");
                Ok((source.source(), failed_source(source.source(), "failed")))
            }
        }
    }));
    let rows = match remaining(request.execution.deadline) {
        None => all.await,
        Some(left) => {
            if let Ok(rows) = tokio::time::timeout(left, all).await {
                rows
            } else {
                if sources
                    .iter()
                    .any(|s| policy.fail_mode(s.source()) == FailMode::FailClosed)
                {
                    return Err(crate::BEM_483.with_data("context sources timed out"));
                }
                return Ok(sources
                    .iter()
                    .map(|s| (s.source(), failed_source(s.source(), "timeout")))
                    .collect());
            }
        }
    };
    rows.into_iter().collect()
}

fn remaining(deadline: Option<DateTime<Utc>>) -> Option<std::time::Duration> {
    deadline.map(|d| (d - Utc::now()).to_std().unwrap_or_default())
}

/// Python `citations_for_selected` with `citations_from_hits`: one citation
/// per selected knowledge hit that names a UUID document.
fn citations_for_selected(
    items: &[crate::types::ContextItem],
    selected: &[&str],
    hits: &[Hit],
) -> Vec<Value> {
    let cited: Vec<&str> = items
        .iter()
        .filter(|i| selected.contains(&i.id.as_str()))
        .filter_map(|i| i.citation_id.as_deref())
        .collect();
    if cited.is_empty() {
        return Vec::new();
    }
    hits.iter()
        .filter(|hit| cited.contains(&format!("cite:{}", hit.id).as_str()))
        .filter_map(|hit| {
            let mut citation = beam_knowledge::provenance::citation_from_hit(hit)?;
            if let Some(map) = citation.as_object_mut() {
                map.insert("citation_id".into(), json!(format!("cite:{}", hit.id)));
            }
            Some(citation)
        })
        .collect()
}

/// The token budget the history source pages against.
#[must_use]
pub fn history_budget(model: &ModelConfig, policy: &ResolvedContextPolicy) -> u64 {
    policy
        .token_budget
        .filter(|b| *b > 0)
        .unwrap_or_else(|| model_window(model).saturating_sub(model_output_tokens(model)))
        .max(1)
}

/// The execution context of an invoke: its identity, auth and request facts.
#[must_use]
pub fn execution_context_from(
    input: &AgentInput,
    deadline: Option<DateTime<Utc>>,
    cancelled: bool,
) -> ExecutionContext {
    let auth = input.auth_context.clone();
    ExecutionContext {
        tenant_id: crate::types::normalize_identity(
            auth.as_ref().and_then(|a| a.org_id.as_deref()),
        ),
        user_id: input.user_id(),
        execution_id: input.execution_id,
        run_id: Some(input.run_id),
        deadline,
        cancelled,
        auth,
    }
}

/// Inputs to [`assemble_build_request`] besides the agent input.
#[derive(Clone, Default)]
pub struct BuildOptions {
    pub spec: Option<ContextSpec>,
    pub policy: Option<ResolvedContextPolicy>,
    pub tool_names: Vec<String>,
    pub source_budgets: HashMap<String, u64>,
    pub revision_id: Option<String>,
    pub deadline: Option<DateTime<Utc>>,
    pub cancelled: bool,
    /// How untrusted text reaches the model (`BEAM_SECURITY_SPOTLIGHT`).
    pub spotlight: Spotlight,
}

/// Everything a context build needs for one invoke.
#[must_use]
pub fn assemble_build_request(
    input: &AgentInput,
    model: &ModelConfig,
    services: ContextServices,
    options: BuildOptions,
) -> ContextBuildRequest {
    let request = input.context.clone();
    let policy = options.policy.unwrap_or_else(|| {
        compile_context_policy(options.spec.as_ref(), request.as_ref(), &options.tool_names)
    });
    let mut budgets = options.source_budgets;
    budgets
        .entry("history".into())
        .or_insert_with(|| history_budget(model, &policy));
    ContextBuildRequest {
        execution: execution_context_from(input, options.deadline, options.cancelled),
        request,
        source_budgets: budgets,
        revision_id: options.revision_id,
        spec: options.spec,
        services,
        counter: TokenCounter,
        spotlight: options.spotlight,
        tool_names: options.tool_names,
        model: model.clone(),
        agent_input: input.clone(),
        policy,
    }
}
