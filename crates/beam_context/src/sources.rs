//! The 11 context sources (Python `context/sources/`). Sources return
//! candidates and decisions; they never inject messages.

use std::sync::Arc;

use async_trait::async_trait;
use beam_gateway::TokenCounter;
use beam_knowledge::Hit;
use beam_types::{
    AgentInput, AgentMessage, AgentMessageRole, ContentPart, ContentPartType, Modality,
    ModelConfig, RequestContext,
};
use chrono::{DateTime, Utc};
use futures_util::future::join_all;
use riverbase_core::RiverbaseResult;
use serde_json::{json, Value};
use tokio::sync::Semaphore;

use crate::composer::{model_output_tokens, model_window};
use crate::item::{dropped_item, dropped_resource, dropped_source, NewItem};
use crate::policy::FailMode;
use crate::resolvers::AuthorizedResourceResolver;
use crate::services::{
    ContextBuildRequest, ResourceResolver, SourceArtifact, SourceDiagnostic, SourceResult,
    ToolCatalog,
};
use crate::types::{ContextItem, ContextRole, Trust};
use crate::units::history_units;

/// One context source: resolves its items for a build.
#[async_trait]
pub trait ContextSource: Send + Sync {
    fn source(&self) -> &'static str;

    /// # Errors
    /// A fail-closed failure, or a hard error such as a modality mismatch.
    async fn resolve(&self, request: &ContextBuildRequest) -> RiverbaseResult<SourceResult>;
}

/// Python `str(value)` for a JSON value: strings bare, the rest as `repr`.
fn py_str(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        other => beam_types::pyjson::py_repr(other),
    }
}

// --- instructions -----------------------------------------------------------

/// The agent's instructions as one required item.
#[must_use]
pub fn instruction_items(text: Option<&str>, counter: &TokenCounter) -> Vec<ContextItem> {
    let Some(text) = text.map(str::trim).filter(|t| !t.is_empty()) else {
        return Vec::new();
    };
    let mut item = NewItem::new(
        "instructions",
        text,
        ContextRole::Developer,
        "instructions",
        Trust::Developer,
    );
    item.required = true;
    item.priority = 100;
    item.provenance = Some("agent_spec.prompt".into());
    vec![item.build(counter)]
}

pub struct InstructionSource;

#[async_trait]
impl ContextSource for InstructionSource {
    fn source(&self) -> &'static str {
        "instructions"
    }

    async fn resolve(&self, request: &ContextBuildRequest) -> RiverbaseResult<SourceResult> {
        let text = request
            .spec
            .as_ref()
            .and_then(|s| s.instructions.as_deref());
        let items = instruction_items(text, &request.counter);
        let n = items.len();
        Ok(SourceResult::items(
            items,
            SourceDiagnostic::new(self.source(), n),
        ))
    }
}

// --- current ----------------------------------------------------------------

fn modality_label(m: Modality) -> &'static str {
    match m {
        Modality::Text => "text",
        Modality::Image => "image",
        Modality::Audio => "audio",
        Modality::Video => "video",
    }
}

/// The current turn, lossless.
///
/// # Errors
/// `BEM-481` when a part's modality is not one the model supports.
pub fn current_input_items(
    input: &AgentInput,
    counter: &TokenCounter,
    model: Option<&ModelConfig>,
) -> RiverbaseResult<Vec<ContextItem>> {
    let parts = input.content.clone();
    let text = parts
        .iter()
        .filter_map(|p| p.text.as_deref().filter(|t| !t.is_empty()))
        .collect::<Vec<_>>()
        .join(" ");
    let has_media = parts.iter().any(|p| p.part_type != ContentPartType::Text);
    if text.trim().is_empty() && !has_media {
        return Ok(Vec::new());
    }
    if let Some(model) = model {
        for part in &parts {
            let modality = part.detected_modality();
            if modality != Modality::Text && !model.modalities.contains(&modality) {
                return Err(crate::BEM_481.with_data(format!(
                    "unsupported modality: {}",
                    modality_label(modality)
                )));
            }
        }
    }
    let mut item = NewItem::new(
        format!("current:{}", input.run_id),
        text,
        ContextRole::Human,
        "current",
        Trust::User,
    );
    item.parts = parts;
    item.required = true;
    item.priority = 90;
    item.model = model.map(|m| m.name.clone());
    Ok(vec![item.build(counter)])
}

pub struct CurrentInputSource;

#[async_trait]
impl ContextSource for CurrentInputSource {
    fn source(&self) -> &'static str {
        "current"
    }

    async fn resolve(&self, request: &ContextBuildRequest) -> RiverbaseResult<SourceResult> {
        let items =
            current_input_items(&request.agent_input, &request.counter, Some(&request.model))?;
        let n = items.len();
        Ok(SourceResult::items(
            items,
            SourceDiagnostic::new(self.source(), n),
        ))
    }
}

// --- workflow ---------------------------------------------------------------

/// A resume carries a required marker.
#[must_use]
pub fn workflow_items(input: &AgentInput, counter: &TokenCounter) -> Vec<ContextItem> {
    if input.actions.as_ref().is_none_or(Vec::is_empty) {
        return Vec::new();
    }
    let mut item = NewItem::new(
        "workflow:resume",
        "resume pending human action",
        ContextRole::System,
        "workflow",
        Trust::System,
    );
    item.required = true;
    item.priority = 80;
    vec![item.build(counter)]
}

pub struct WorkflowSource;

#[async_trait]
impl ContextSource for WorkflowSource {
    fn source(&self) -> &'static str {
        "workflow"
    }

    async fn resolve(&self, request: &ContextBuildRequest) -> RiverbaseResult<SourceResult> {
        let items = workflow_items(&request.agent_input, &request.counter);
        let n = items.len();
        Ok(SourceResult::items(
            items,
            SourceDiagnostic::new(self.source(), n),
        ))
    }
}

// --- tools ------------------------------------------------------------------

fn falsy(v: &Value) -> bool {
    match v {
        Value::Null => true,
        Value::Object(o) => o.is_empty(),
        Value::Array(a) => a.is_empty(),
        Value::String(s) => s.is_empty(),
        Value::Bool(b) => !b,
        Value::Number(_) => false,
    }
}

/// The authorized tools' schemas, in order.
#[must_use]
pub fn freeze_tool_schemas(catalog: Option<&Arc<dyn ToolCatalog>>, names: &[String]) -> Vec<Value> {
    let Some(catalog) = catalog else {
        return Vec::new();
    };
    names
        .iter()
        .filter_map(|name| catalog.get_definition(name))
        .map(|d| {
            let parameters = if falsy(&d.parameters) {
                json!({})
            } else {
                d.parameters
            };
            json!({"name": d.name, "description": d.description, "parameters": parameters})
        })
        .collect()
}

/// One required item carrying the
/// schemas' token cost.
#[must_use]
pub fn tool_schema_items(schemas: &[Value], counter: &TokenCounter) -> Vec<ContextItem> {
    if schemas.is_empty() {
        return Vec::new();
    }
    let names: Vec<String> = schemas
        .iter()
        .map(|s| s.get("name").map(py_str).unwrap_or_default())
        .collect();
    let mut item = ContextItem::new("tools", ContextRole::System, "tools", Trust::System);
    item.content = format!("tools:{}", names.join(","));
    item.required = true;
    item.priority = 70;
    item.token_estimate = counter.count_tool_schemas(schemas, None).tokens;
    vec![item.synced()]
}

pub struct ToolSource;

#[async_trait]
impl ContextSource for ToolSource {
    fn source(&self) -> &'static str {
        "tools"
    }

    async fn resolve(&self, request: &ContextBuildRequest) -> RiverbaseResult<SourceResult> {
        let schemas = freeze_tool_schemas(request.services.tools.as_ref(), &request.tool_names);
        let items = tool_schema_items(&schemas, &request.counter);
        let n = items.len();
        Ok(SourceResult {
            items,
            artifacts: vec![SourceArtifact::ToolSchemas(schemas)],
            diagnostics: Some(SourceDiagnostic::new(self.source(), n)),
            ..SourceResult::default()
        })
    }
}

// --- history ----------------------------------------------------------------

/// Page-size heuristic, not a token budget.
pub const HISTORY_OVERFETCH_RATIO: u64 = 25;

/// The adapter appends the current turn once.
#[must_use]
pub fn exclude_current_turn(history: Vec<AgentMessage>, input: &AgentInput) -> Vec<AgentMessage> {
    history
        .into_iter()
        .filter(|m| m.run_id != Some(input.run_id))
        .filter(|m| input.request_id.is_none() || m.request_id != input.request_id)
        .collect()
}

fn history_role(role: AgentMessageRole) -> ContextRole {
    match role {
        AgentMessageRole::System => ContextRole::System,
        AgentMessageRole::Human => ContextRole::Human,
        AgentMessageRole::Ai => ContextRole::Ai,
        AgentMessageRole::Tool => ContextRole::Tool,
    }
}

/// One item per message, grouped into units.
#[must_use]
pub fn history_items(messages: &[AgentMessage], counter: &TokenCounter) -> Vec<ContextItem> {
    let mut out = Vec::new();
    for (unit_index, unit) in history_units(messages).into_iter().enumerate() {
        let unit_id = format!("history-unit:{unit_index}");
        for message in unit {
            let text = message.text_content();
            let mut parts = message.content.clone();
            let has_result = message.tool_result.as_ref().is_some_and(|r| !falsy(r));
            if parts.is_empty() && message.tool_calls.is_empty() && !has_result {
                if text.is_empty() {
                    continue;
                }
                parts = vec![ContentPart::text(text.clone())];
            }
            let role = history_role(message.role);
            // Python trusts every non-human history message. The transcript's
            // merged summary quotes user turns, so it stays untrusted here: it
            // is injection-checked and never becomes system text.
            let summary = message
                .metadata
                .get(beam_memory::transcript::SUMMARY_MARKER)
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let trust = if summary {
                Trust::Untrusted
            } else if role == ContextRole::Human {
                Trust::User
            } else {
                Trust::System
            };
            let mut item = NewItem::new(
                format!("history:{}", message.id),
                text,
                role,
                "history",
                trust,
            );
            item.parts = parts;
            item.priority = i64::try_from(unit_index).unwrap_or(i64::MAX);
            item.unit_id = Some(unit_id.clone());
            item.name.clone_from(&message.name);
            item.tool_call_id.clone_from(&message.tool_call_id);
            item.tool_calls.clone_from(&message.tool_calls);
            item.tool_result.clone_from(&message.tool_result);
            out.push(item.build(counter));
        }
    }
    out
}

pub struct HistorySource;

#[async_trait]
impl ContextSource for HistorySource {
    fn source(&self) -> &'static str {
        "history"
    }

    async fn resolve(&self, request: &ContextBuildRequest) -> RiverbaseResult<SourceResult> {
        let Some(memory) = &request.services.memory else {
            return Ok(SourceResult::dropped(
                dropped_source("history", "unavailable"),
                SourceDiagnostic::failed(self.source(), "unavailable"),
            ));
        };
        let budget = request
            .source_budgets
            .get("history")
            .copied()
            .unwrap_or_else(|| {
                let window = model_window(&request.model);
                let output = model_output_tokens(&request.model);
                request
                    .policy
                    .token_budget
                    .filter(|b| *b > 0)
                    .unwrap_or_else(|| window.saturating_sub(output))
                    .max(1)
            });
        // Fetch generously; the composer owns selection under the budget.
        let page = (budget / HISTORY_OVERFETCH_RATIO).max(64);
        let history = memory
            .load_history(
                request.agent_input.session_id,
                Some(usize::try_from(page).unwrap_or(usize::MAX)),
            )
            .await?;
        let history = exclude_current_turn(history, &request.agent_input);
        let items = history_items(&history, &request.counter);
        let n = items.len();
        Ok(SourceResult::items(
            items,
            SourceDiagnostic::new(self.source(), n).backend("memory"),
        ))
    }
}

// --- summary ----------------------------------------------------------------

/// A valid summary is an untrusted history item.
#[must_use]
pub fn summary_items(text: Option<&str>, counter: &TokenCounter) -> Vec<ContextItem> {
    let Some(text) = text.map(str::trim).filter(|t| !t.is_empty()) else {
        return Vec::new();
    };
    let mut item = NewItem::new(
        "summary",
        text,
        ContextRole::Human,
        "history",
        Trust::Untrusted,
    );
    item.priority = 50;
    item.unit_id = Some("summary".into());
    item.provenance = Some("session_context_summary".into());
    vec![item.build(counter)]
}

pub struct SummarySource;

#[async_trait]
impl ContextSource for SummarySource {
    fn source(&self) -> &'static str {
        "summary"
    }

    async fn resolve(&self, request: &ContextBuildRequest) -> RiverbaseResult<SourceResult> {
        let Some(reader) = &request.services.summaries else {
            return Ok(SourceResult::empty(SourceDiagnostic::new(self.source(), 0)));
        };
        let row = reader
            .get(&request.agent_input.session_id.to_string())
            .await
            .filter(|r| !r.invalidated);
        let Some(row) = row else {
            return Ok(SourceResult::empty(
                SourceDiagnostic::new(self.source(), 0).cache("miss"),
            ));
        };
        let items = summary_items(Some(&row.text), &request.counter);
        let cache = if items.is_empty() { "stale" } else { "hit" };
        let n = items.len();
        Ok(SourceResult::items(
            items,
            SourceDiagnostic::new(self.source(), n).cache(cache),
        ))
    }
}

// --- knowledge --------------------------------------------------------------

fn hit_collection(hit: &Hit) -> String {
    hit.metadata
        .get("collection")
        .filter(|v| !falsy(v))
        .map(py_str)
        .unwrap_or_default()
}

/// Python `str(hit.scope or "") or None`.
fn scope_text(hit: &Hit) -> Option<String> {
    hit.scope.map(|s| s.as_str().to_owned())
}

/// Hits as untrusted evidence with citation ids.
#[must_use]
pub fn knowledge_hit_items(hits: &[Hit], counter: &TokenCounter) -> Vec<ContextItem> {
    let mut out = Vec::new();
    for (index, hit) in hits.iter().enumerate() {
        if hit.text.is_empty() {
            continue;
        }
        let hit_id = if hit.id.is_empty() {
            index.to_string()
        } else {
            hit.id.clone()
        };
        let citation_id = format!("cite:{hit_id}");
        let provenance = hit
            .source
            .clone()
            .filter(|s| !s.is_empty())
            .or_else(|| hit.document_id.clone().filter(|d| !d.is_empty()))
            .unwrap_or_else(|| hit_collection(hit));
        let mut item = NewItem::new(
            format!("knowledge:{hit_id}"),
            format!("[UNTRUSTED EVIDENCE {citation_id}] {}", hit.text),
            ContextRole::System,
            "knowledge",
            Trust::Untrusted,
        );
        item.priority = 40;
        item.provenance = Some(provenance);
        item.citation_id = Some(citation_id);
        item.tenant_scope = scope_text(hit);
        out.push(item.build(counter));
    }
    out
}

pub struct KnowledgeSource;

#[async_trait]
impl ContextSource for KnowledgeSource {
    fn source(&self) -> &'static str {
        "knowledge"
    }

    async fn resolve(&self, request: &ContextBuildRequest) -> RiverbaseResult<SourceResult> {
        let dropped = |reason: &str| {
            Ok(SourceResult::dropped(
                dropped_source("knowledge", reason),
                SourceDiagnostic::failed("knowledge", reason),
            ))
        };
        if request.policy.knowledge_mode == "tool" {
            return dropped("tool_only");
        }
        let Some(knowledge) = &request.services.knowledge else {
            return dropped("unavailable");
        };
        let query = request.query();
        if query.is_empty() {
            return dropped("empty");
        }
        let hits = match knowledge
            .retrieve(&query, 6, &request.retrieval_scope())
            .await
        {
            Ok(hits) => hits,
            Err(e) => {
                if request.policy.fail_mode("knowledge") == FailMode::FailClosed {
                    return Err(e);
                }
                return dropped("failed");
            }
        };
        let hits: Vec<Hit> = if request.policy.collections.is_empty() {
            hits
        } else {
            hits.into_iter()
                .filter(|hit| {
                    let collection = hit_collection(hit);
                    let key = if collection.is_empty() {
                        hit.source.clone().unwrap_or_default()
                    } else {
                        collection
                    };
                    request.policy.collections.contains(&key)
                })
                .collect()
        };
        let items = knowledge_hit_items(&hits, &request.counter);
        let n = items.len();
        Ok(SourceResult {
            items,
            artifacts: vec![SourceArtifact::CitationHits(hits)],
            diagnostics: Some(SourceDiagnostic::new(self.source(), n).backend("knowledge")),
            ..SourceResult::default()
        })
    }
}

// --- ltm --------------------------------------------------------------------

/// Python's `repr(float)` for a score: shortest round-trip text, with `.0`
/// on whole numbers.
fn py_float(value: f64) -> String {
    let text = value.to_string();
    if value.is_finite() && !text.contains(['.', 'e', 'E']) {
        format!("{text}.0")
    } else {
        text
    }
}

/// Remembered claims, untrusted unless the policy promotes them.
#[must_use]
pub fn ltm_items(hits: &[Hit], counter: &TokenCounter, trust: Trust) -> Vec<ContextItem> {
    let mut out = Vec::new();
    for (index, hit) in hits.iter().enumerate() {
        if hit.text.is_empty() {
            continue;
        }
        let memory_id = if hit.id.is_empty() {
            index.to_string()
        } else {
            hit.id.clone()
        };
        let memory_type = hit.metadata.get("type").filter(|v| !falsy(v)).map(py_str);
        let rendered = format!(
            "remembered claim ({}): {} [score={}]",
            memory_type.as_deref().unwrap_or("semantic"),
            hit.text,
            py_float(hit.score)
        );
        let mut item = NewItem::new(
            format!("ltm:{memory_id}"),
            rendered,
            ContextRole::System,
            "ltm",
            trust,
        );
        item.priority = 30;
        item.memory_id = Some(memory_id);
        item.memory_type = memory_type;
        item.confidence = hit.metadata.get("confidence").and_then(Value::as_f64);
        item.consent = hit
            .metadata
            .get("consent")
            .filter(|v| !v.is_null())
            .map(|v| !falsy(v));
        out.push(item.build(counter));
    }
    out
}

pub struct LongTermMemorySource;

#[async_trait]
impl ContextSource for LongTermMemorySource {
    fn source(&self) -> &'static str {
        "ltm"
    }

    async fn resolve(&self, request: &ContextBuildRequest) -> RiverbaseResult<SourceResult> {
        let empty = || Ok(SourceResult::empty(SourceDiagnostic::new("ltm", 0)));
        let Some(reader) = &request.services.long_term_memory else {
            return empty();
        };
        if request.agent_input.user_id().is_none() {
            return empty();
        }
        let query = request.query();
        if query.is_empty() {
            return empty();
        }
        let k = request
            .source_budgets
            .get("ltm")
            .copied()
            .filter(|k| *k > 0)
            .unwrap_or(5);
        let hits = match reader
            .recall(
                &query,
                usize::try_from(k).unwrap_or(5),
                &request.retrieval_scope(),
            )
            .await
        {
            Ok(hits) => hits,
            Err(e) => {
                if request.policy.fail_mode("ltm") == FailMode::FailClosed {
                    return Err(e);
                }
                return Ok(SourceResult::empty(SourceDiagnostic::failed(
                    self.source(),
                    "failed",
                )));
            }
        };
        let trust = if request.policy.promote_ltm_trust {
            Trust::System
        } else {
            Trust::Untrusted
        };
        let items = ltm_items(&hits, &request.counter, trust);
        let n = items.len();
        Ok(SourceResult::items(
            items,
            SourceDiagnostic::new(self.source(), n).backend("ltm"),
        ))
    }
}

// --- profile ----------------------------------------------------------------

pub struct ProfileSource;

#[async_trait]
impl ContextSource for ProfileSource {
    fn source(&self) -> &'static str {
        "profile"
    }

    async fn resolve(&self, request: &ContextBuildRequest) -> RiverbaseResult<SourceResult> {
        let profile_id = request
            .request
            .as_ref()
            .and_then(|r| r.profile_id.as_deref())
            .filter(|p| !p.is_empty());
        let Some(profile_id) = profile_id else {
            return Ok(SourceResult::empty(SourceDiagnostic::new(self.source(), 0)));
        };
        let Some(resolver) = &request.services.profiles else {
            return Ok(SourceResult::dropped(
                dropped_source("profile", "unavailable"),
                SourceDiagnostic::failed(self.source(), "unavailable"),
            ));
        };
        let profile = match resolver.resolve(profile_id, &request.execution).await {
            Ok(profile) => profile,
            Err(reason) => {
                let reason = if reason.is_empty() {
                    "unavailable".to_owned()
                } else {
                    reason
                };
                return Ok(SourceResult::dropped(
                    dropped_source("profile", &reason),
                    SourceDiagnostic::failed(self.source(), &reason),
                ));
            }
        };
        let mut item = NewItem::new(
            format!("profile:{}", profile.profile_id),
            profile.text,
            ContextRole::Human,
            "profile",
            Trust::Untrusted,
        );
        item.priority = 25;
        item.provenance = Some("profile_resolver".into());
        item.tenant_scope = profile.tenant_id;
        Ok(SourceResult::items(
            vec![item.build(&request.counter)],
            SourceDiagnostic::new(self.source(), 1).backend("profile"),
        ))
    }
}

// --- facts ------------------------------------------------------------------

/// `(admitted, expired)`.
#[must_use]
pub fn partition_facts(
    request: Option<&RequestContext>,
    counter: &TokenCounter,
    now: DateTime<Utc>,
) -> (Vec<ContextItem>, Vec<ContextItem>) {
    let Some(request) = request else {
        return (Vec::new(), Vec::new());
    };
    let mut admitted = Vec::new();
    let mut expired = Vec::new();
    for fact in &request.facts {
        let mut item = NewItem::new(
            format!("fact:{}", fact.key),
            format!("{}={}", fact.key, py_str(&fact.value)),
            ContextRole::Human,
            "facts",
            Trust::Untrusted,
        );
        item.priority = 20;
        item.sensitivity = fact.sensitivity;
        item.expiry = fact.expiry;
        let item = item.build(counter);
        if fact.expiry.is_some_and(|e| e <= now) {
            expired.push(item);
        } else {
            admitted.push(item);
        }
    }
    (admitted, expired)
}

pub struct FactSource;

#[async_trait]
impl ContextSource for FactSource {
    fn source(&self) -> &'static str {
        "facts"
    }

    async fn resolve(&self, request: &ContextBuildRequest) -> RiverbaseResult<SourceResult> {
        let (admitted, expired) =
            partition_facts(request.request.as_ref(), &request.counter, Utc::now());
        let n = admitted.len();
        Ok(SourceResult {
            items: admitted,
            decisions: expired.iter().map(|i| dropped_item(i, "expired")).collect(),
            diagnostics: Some(SourceDiagnostic::new(self.source(), n)),
            ..SourceResult::default()
        })
    }
}

// --- resources --------------------------------------------------------------

const RESOURCE_CONCURRENCY: usize = 4;

pub struct ResourceSource;

#[async_trait]
impl ContextSource for ResourceSource {
    fn source(&self) -> &'static str {
        "resources"
    }

    async fn resolve(&self, request: &ContextBuildRequest) -> RiverbaseResult<SourceResult> {
        let Some(resources) = request
            .request
            .as_ref()
            .map(|r| &r.resources)
            .filter(|r| !r.is_empty())
        else {
            return Ok(SourceResult::empty(SourceDiagnostic::new(self.source(), 0)));
        };
        let resolver: Arc<dyn ResourceResolver> =
            request.services.resources.clone().unwrap_or_else(|| {
                Arc::new(AuthorizedResourceResolver::new(
                    &request.policy.resource_schemes,
                    &request.policy.resource_hosts,
                    request.policy.max_resource_bytes,
                ))
            });
        let semaphore = Semaphore::new(RESOURCE_CONCURRENCY);
        let rows = join_all(resources.iter().enumerate().map(|(index, resource)| {
            let resolver = resolver.clone();
            let limiter = &semaphore;
            async move {
                let outcome = {
                    let _permit = limiter.acquire().await;
                    resolver.resolve(resource, &request.execution).await
                };
                let item_id = format!("resource:{}:{index}", resource.kind);
                match outcome {
                    Ok(text) if !text.is_empty() => {
                        let origin = if resource.resolved_text.is_some() {
                            "claimed"
                        } else {
                            "fetched"
                        };
                        let mut item = NewItem::new(
                            item_id,
                            text,
                            ContextRole::Human,
                            "resources",
                            Trust::Untrusted,
                        );
                        item.priority = 15;
                        item.provenance = Some(format!("{origin}:{}", resource.uri));
                        Ok(item.build(&request.counter))
                    }
                    Ok(_) => Err(dropped_resource(&item_id, "unavailable")),
                    Err(reason) => {
                        let reason = if reason.is_empty() {
                            "unavailable".to_owned()
                        } else {
                            reason
                        };
                        Err(dropped_resource(&item_id, &reason))
                    }
                }
            }
        }))
        .await;
        let mut result = SourceResult::default();
        for row in rows {
            match row {
                Ok(item) => result.items.push(item),
                Err(decision) => result.decisions.push(decision),
            }
        }
        result.diagnostics =
            Some(SourceDiagnostic::new(self.source(), result.items.len()).backend("resource"));
        Ok(result)
    }
}

/// Python `builtin_registry`'s providers, in registry order.
#[must_use]
pub fn builtin_sources() -> Vec<Arc<dyn ContextSource>> {
    vec![
        Arc::new(InstructionSource),
        Arc::new(CurrentInputSource),
        Arc::new(WorkflowSource),
        Arc::new(ToolSource),
        Arc::new(HistorySource),
        Arc::new(SummarySource),
        Arc::new(KnowledgeSource),
        Arc::new(LongTermMemorySource),
        Arc::new(ProfileSource),
        Arc::new(FactSource),
        Arc::new(ResourceSource),
    ]
}
