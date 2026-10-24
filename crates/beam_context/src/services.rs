//! Invocation-scoped collaborators for context sources (Python
//! `context/services.py`), and the request/result records sources exchange
//! (Python `context/contract.py`).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use beam_gateway::TokenCounter;
use beam_knowledge::Hit;
use beam_memory::{SessionSummary, SummaryBook, SummaryStore, TranscriptManager};
use beam_types::{AgentInput, AgentMessage, ModelConfig, RequestContext, ResourceRef};
use riverbase_core::RiverbaseResult;
use serde_json::Value;
use uuid::Uuid;

use crate::policy::{ContextSpec, ResolvedContextPolicy};
use crate::types::{ContextItem, ExecutionContext, ManifestDecision};

#[async_trait]
pub trait MemoryReader: Send + Sync {
    /// # Errors
    /// Transcript store errors.
    async fn load_history(
        &self,
        session_id: Uuid,
        limit: Option<usize>,
    ) -> RiverbaseResult<Vec<AgentMessage>>;
}

#[async_trait]
impl MemoryReader for TranscriptManager {
    async fn load_history(
        &self,
        session_id: Uuid,
        limit: Option<usize>,
    ) -> RiverbaseResult<Vec<AgentMessage>> {
        Self::load_history(self, session_id, limit).await
    }
}

/// Who is asking, for ACL-filtered retrieval.
#[derive(Debug, Clone, Default)]
pub struct RetrievalScope {
    pub agent_id: Option<Uuid>,
    pub user_id: Option<String>,
    pub org_id: Option<String>,
}

#[async_trait]
pub trait KnowledgeRetriever: Send + Sync {
    /// # Errors
    /// Retrieval errors.
    async fn retrieve(
        &self,
        query: &str,
        k: usize,
        scope: &RetrievalScope,
    ) -> RiverbaseResult<Vec<Hit>>;
}

/// Python `knowledge.retrieve(query, k=6, agent_id=..., user_id=..., org_id=...)`:
/// the caller's identity is the visibility filter (no scope).
#[async_trait]
impl KnowledgeRetriever for beam_knowledge::KnowledgeManager {
    async fn retrieve(
        &self,
        query: &str,
        k: usize,
        scope: &RetrievalScope,
    ) -> RiverbaseResult<Vec<Hit>> {
        let filters = beam_knowledge::RetrievalFilter {
            agent_id: scope.agent_id.map(|a| a.to_string()),
            user_id: scope.user_id.clone(),
            org_id: scope.org_id.clone(),
            ..beam_knowledge::RetrievalFilter::default()
        };
        Self::retrieve(self, query, k, filters).await
    }
}

/// Python `knowledge.recall(query=..., user_id=..., agent_id=..., k=...)`.
#[async_trait]
impl LongTermMemoryReader for beam_knowledge::KnowledgeManager {
    async fn recall(
        &self,
        query: &str,
        k: usize,
        scope: &RetrievalScope,
    ) -> RiverbaseResult<Vec<Hit>> {
        let Some(user) = scope.user_id.clone() else {
            return Ok(Vec::new());
        };
        let request = beam_knowledge::RecallRequest {
            agent_id: scope.agent_id.map(|a| a.to_string()),
            k,
            ..beam_knowledge::RecallRequest::new(query, user)
        };
        Self::recall(self, &request).await
    }
}

#[async_trait]
pub trait LongTermMemoryReader: Send + Sync {
    /// # Errors
    /// Recall errors.
    async fn recall(
        &self,
        query: &str,
        k: usize,
        scope: &RetrievalScope,
    ) -> RiverbaseResult<Vec<Hit>>;
}

/// A tool's model-visible schema.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolSchema {
    pub name: String,
    pub description: String,
    pub parameters: Value,
}

pub trait ToolCatalog: Send + Sync {
    fn get_definition(&self, name: &str) -> Option<ToolSchema>;
}

impl ToolCatalog for beam_tools::ToolRegistry {
    fn get_definition(&self, name: &str) -> Option<ToolSchema> {
        self.get(name).map(|d| ToolSchema {
            name: d.name,
            description: d.description,
            parameters: d.input_schema,
        })
    }
}

/// A caller's profile, as the profile source reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedProfile {
    pub profile_id: String,
    pub text: String,
    pub tenant_id: Option<String>,
    pub user_id: Option<String>,
}

/// A resolver's answer: the value, or the reason it was refused.
pub type Resolution<T> = Result<T, String>;

#[async_trait]
pub trait ProfileResolver: Send + Sync {
    async fn resolve(
        &self,
        profile_id: &str,
        execution: &ExecutionContext,
    ) -> Resolution<ResolvedProfile>;
}

#[async_trait]
pub trait ResourceResolver: Send + Sync {
    async fn resolve(
        &self,
        resource: &ResourceRef,
        execution: &ExecutionContext,
    ) -> Resolution<String>;
}

/// The session's summary for the `summary` source. Async: summaries may be
/// in Postgres (plan 023 B3).
#[async_trait]
pub trait SummaryReader: Send + Sync {
    async fn get(&self, session_id: &str) -> Option<SessionSummary>;
}

#[async_trait]
impl SummaryReader for SummaryBook {
    async fn get(&self, session_id: &str) -> Option<SessionSummary> {
        Self::get(self, session_id)
    }
}

/// A [`SummaryStore`]'s summaries as a [`SummaryReader`]; a read that fails
/// is logged and reads as none.
pub struct StoredSummaries(pub Arc<dyn SummaryStore>);

#[async_trait]
impl SummaryReader for StoredSummaries {
    async fn get(&self, session_id: &str) -> Option<SessionSummary> {
        match self.0.get(session_id).await {
            Ok(summary) => summary,
            Err(e) => {
                tracing::warn!(session_id, error = %e, "session summary read failed");
                None
            }
        }
    }
}

/// The services the context sources read.
#[derive(Clone, Default)]
pub struct ContextServices {
    pub memory: Option<Arc<dyn MemoryReader>>,
    pub knowledge: Option<Arc<dyn KnowledgeRetriever>>,
    pub long_term_memory: Option<Arc<dyn LongTermMemoryReader>>,
    pub tools: Option<Arc<dyn ToolCatalog>>,
    pub profiles: Option<Arc<dyn ProfileResolver>>,
    pub resources: Option<Arc<dyn ResourceResolver>>,
    pub summaries: Option<Arc<dyn SummaryReader>>,
}

/// The pinned policy and authenticated request
/// every source receives.
#[derive(Clone)]
pub struct ContextBuildRequest {
    pub policy: ResolvedContextPolicy,
    pub model: ModelConfig,
    pub request: Option<RequestContext>,
    pub execution: ExecutionContext,
    pub agent_input: AgentInput,
    pub source_budgets: HashMap<String, u64>,
    pub revision_id: Option<String>,
    pub spec: Option<ContextSpec>,
    pub services: ContextServices,
    pub counter: TokenCounter,
    pub tool_names: Vec<String>,
}

impl ContextBuildRequest {
    /// The text parts of the current input (Python joins non-empty texts).
    #[must_use]
    pub fn query(&self) -> String {
        self.agent_input
            .content
            .iter()
            .filter_map(|p| p.text.as_deref().filter(|t| !t.is_empty()))
            .collect::<Vec<_>>()
            .join(" ")
            .trim()
            .to_owned()
    }

    #[must_use]
    pub fn retrieval_scope(&self) -> RetrievalScope {
        RetrievalScope {
            agent_id: self
                .agent_input
                .agent_name
                .as_deref()
                .map(beam_store::agent_id_for),
            user_id: self.agent_input.user_id(),
            org_id: self
                .agent_input
                .auth_context
                .as_ref()
                .and_then(|a| a.org_id.clone()),
        }
    }
}

/// What one source did in a build: items, timing, cache.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SourceDiagnostic {
    pub source: String,
    pub latency_ms: Option<f64>,
    pub candidate_count: usize,
    pub backend: Option<String>,
    pub cache: Option<String>,
    pub failure: Option<String>,
}

impl SourceDiagnostic {
    #[must_use]
    pub fn new(source: &str, candidate_count: usize) -> Self {
        Self {
            source: source.to_owned(),
            candidate_count,
            ..Self::default()
        }
    }

    #[must_use]
    pub fn failed(source: &str, failure: &str) -> Self {
        Self {
            source: source.to_owned(),
            failure: Some(failure.to_owned()),
            ..Self::default()
        }
    }

    #[must_use]
    pub fn backend(mut self, backend: &str) -> Self {
        self.backend = Some(backend.to_owned());
        self
    }

    #[must_use]
    pub fn cache(mut self, cache: &str) -> Self {
        self.cache = Some(cache.to_owned());
        self
    }
}

/// Python `SourceArtifact`, by artifact type.
#[derive(Debug, Clone, PartialEq)]
pub enum SourceArtifact {
    ToolSchemas(Vec<Value>),
    CitationHits(Vec<Hit>),
}

/// A source's items and its diagnostic.
#[derive(Debug, Clone, Default)]
pub struct SourceResult {
    pub items: Vec<ContextItem>,
    pub decisions: Vec<ManifestDecision>,
    pub diagnostics: Option<SourceDiagnostic>,
    pub artifacts: Vec<SourceArtifact>,
}

impl SourceResult {
    #[must_use]
    pub fn items(items: Vec<ContextItem>, diagnostics: SourceDiagnostic) -> Self {
        Self {
            items,
            diagnostics: Some(diagnostics),
            ..Self::default()
        }
    }

    #[must_use]
    pub fn dropped(decision: ManifestDecision, diagnostics: SourceDiagnostic) -> Self {
        Self {
            decisions: vec![decision],
            diagnostics: Some(diagnostics),
            ..Self::default()
        }
    }

    #[must_use]
    pub fn empty(diagnostics: SourceDiagnostic) -> Self {
        Self {
            diagnostics: Some(diagnostics),
            ..Self::default()
        }
    }
}

/// The tools a turn may advertise.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuthorizedToolset {
    pub names: Vec<String>,
    pub schema_revision: Option<String>,
}

/// A built context: the envelope, the manifest and what was selected.
#[derive(Debug, Clone)]
pub struct ContextBuildResult {
    pub envelope: crate::types::ContextEnvelope,
    pub manifest: crate::types::ContextManifest,
    pub execution: ExecutionContext,
    pub toolset: AuthorizedToolset,
    pub citations: Vec<Value>,
    pub provider_diagnostics: BTreeMap<String, SourceDiagnostic>,
    pub artifacts: Vec<SourceArtifact>,
    pub selected_untrusted: Vec<ContextItem>,
}

impl ContextBuildResult {
    #[must_use]
    pub fn tools(&self) -> &[String] {
        &self.toolset.names
    }
}
