//! The native `knowledge_search` tool (Python `knowledge/search_tool.py`).
//!
//! Caller-supplied `scope`, `user_id` and `agent_id` are ignored: filters come
//! from the invoke's auth context only, so a model cannot widen what it sees.

use std::sync::Arc;

use async_trait::async_trait;
use beam_knowledge::{KnowledgeManager, KnowledgeScope, RetrievalFilter};
use riverbase_core::RiverbaseResult;
use serde_json::{json, Value};

use crate::definition::{ToolCallContext, ToolDefinition, ToolHandler};

pub const KNOWLEDGE_SEARCH: &str = "knowledge_search";

const DESCRIPTION: &str = "Search the agent's knowledge base for relevant chunks of indexed \
documents. Returns a list of {text, score, source} entries.";

/// Python's `KnowledgeSearchInput.model_json_schema()`, verbatim.
fn input_schema() -> Value {
    json!({"$defs": {"KnowledgeScope": {"enum": ["global", "org", "agent", "user"], "title": "KnowledgeScope", "type": "string"}},
           "properties": {
               "query": {"description": "Natural-language search query.", "title": "Query", "type": "string"},
               "k": {"default": 6, "description": "Number of chunks to return.", "maximum": 50, "minimum": 1, "title": "K", "type": "integer"},
               "scope": {"anyOf": [{"$ref": "#/$defs/KnowledgeScope"}, {"type": "null"}], "default": null,
                         "description": "Ignored. Scope is taken from the request security context."},
               "agent_id": {"anyOf": [{"type": "string"}, {"type": "null"}], "default": null,
                            "description": "Ignored. Agent is taken from the security context.", "title": "Agent Id"},
               "user_id": {"anyOf": [{"type": "string"}, {"type": "null"}], "default": null,
                           "description": "Ignored. User is taken from the security context.", "title": "User Id"}},
           "required": ["query"], "title": "KnowledgeSearchInput", "type": "object"})
}

/// The tool definition, bound to `knowledge`. Read-only (`side_effect = false`).
#[must_use]
pub fn knowledge_search_tool(knowledge: Arc<KnowledgeManager>) -> ToolDefinition {
    ToolDefinition {
        description: DESCRIPTION.into(),
        input_schema: input_schema(),
        side_effect: false,
        ..ToolDefinition::new(KNOWLEDGE_SEARCH)
    }
    .with_handler(Arc::new(KnowledgeSearch(knowledge)))
}

struct KnowledgeSearch(Arc<KnowledgeManager>);

#[async_trait]
impl ToolHandler for KnowledgeSearch {
    async fn call(&self, args: Value, ctx: &ToolCallContext) -> RiverbaseResult<Value> {
        let query = args
            .get("query")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let k = args
            .get("k")
            .and_then(Value::as_u64)
            .map_or(6, |k| k.clamp(1, 50));
        // Filters from the auth context only; the caller's scope/user/agent are ignored.
        let auth = ctx.auth.as_ref();
        let tenant = auth
            .and_then(|a| a.org_id.clone())
            .filter(|t| !t.is_empty());
        // Python: org scope when the caller has a tenant; the agent comes from
        // the invoke.
        let filter = RetrievalFilter {
            scope: tenant.is_some().then_some(KnowledgeScope::Org),
            agent_id: ctx
                .input
                .as_ref()
                .and_then(|i| i.agent_name.as_deref())
                .map(|name| beam_store::agent_id_for(name).to_string()),
            user_id: auth.and_then(|a| a.user_id.clone()),
            org_id: tenant,
            ..RetrievalFilter::default()
        };
        let hits = self
            .0
            .retrieve(query, usize::try_from(k).unwrap_or(6), filter)
            .await?;
        Ok(Value::Array(
            hits.into_iter()
                .map(|hit| {
                    json!({"id": hit.id, "text": hit.text, "score": hit.score, "source": hit.source,
                           "document_id": hit.document_id, "chunk_index": hit.chunk_index,
                           "metadata": hit.metadata})
                })
                .collect(),
        ))
    }
}
