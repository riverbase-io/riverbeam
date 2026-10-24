//! What the showcases share besides [`crate::host`]: the caller of a route or
//! a tool, problem replies, tool definitions from closures, the client tools
//! every chat UI runs, long-term memory and knowledge ingest.

use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use beam_core::RiverBeam;
use beam_knowledge::{IngestRequest, KnowledgeManager, KnowledgeScope};
use beam_tools::{ToolCallContext, ToolDefinition, ToolHandler, ToolRegistry, ToolRisk, KNOWLEDGE_SEARCH};
use riverbase_core::{RiverbaseError, RiverbaseResult};
use riverbase_http::Principal;
use serde_json::{json, Value};

use crate::host::http::{principal_roles, principal_user};
use crate::host::tools::{recall_memories_tool, remember_fact_tool};

/// The client tool that shows a form card and waits for the answers.
pub const REQUEST_INPUT: &str = "request_input";
/// The client tool that asks for one uploaded document and waits for what
/// the UI read from it.
pub const REQUEST_DOCUMENT: &str = "request_document";

/// An authenticated caller of a showcase route.
#[derive(Debug, Clone)]
pub struct Caller {
    pub user: String,
    pub org: Option<String>,
    pub roles: Vec<String>,
}

impl Caller {
    /// Whether the caller holds the riverbase role `role`.
    #[must_use]
    pub fn has(&self, role: &str) -> bool {
        self.roles.iter().any(|r| r == role)
    }
}

/// The route's caller, or `401`.
///
/// # Errors
/// The response to send when nobody signed in.
pub fn caller(principal: Option<Extension<Principal>>) -> Result<Caller, Response> {
    let Some(Extension(principal)) = principal else {
        return Err((StatusCode::UNAUTHORIZED, "sign in first").into_response());
    };
    Ok(Caller {
        user: principal_user(&principal),
        org: principal
            .claims
            .get("organization_id")
            .and_then(Value::as_str)
            .map(str::to_owned),
        roles: principal_roles(&principal),
    })
}

/// A riverbase error as the UI reads it: its code, message and detail.
#[must_use]
pub fn problem(err: &RiverbaseError) -> Response {
    let status = StatusCode::from_u16(err.http_status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let detail = err.errdata.get("detail").cloned().unwrap_or(Value::Null);
    (
        status,
        Json(json!({"errcode": err.errcode.as_str(), "errmesg": err.errmesg, "detail": detail})),
    )
        .into_response()
}

/// The run's user id.
#[must_use]
pub fn tool_user(ctx: &ToolCallContext) -> Option<String> {
    ctx.auth
        .as_ref()
        .and_then(|a| a.user_id.clone())
        .or_else(|| ctx.input.as_ref().and_then(|i| i.user_id()))
}

/// The request fact `key`, as the UI sent it with the turn.
#[must_use]
pub fn fact(ctx: &ToolCallContext, key: &str) -> Option<Value> {
    ctx.input
        .as_ref()?
        .context
        .as_ref()?
        .facts
        .iter()
        .find(|f| f.key == key)
        .map(|f| f.value.clone())
}

/// `key` from the tool's arguments, else the request fact of that name.
#[must_use]
pub fn arg_or_fact(args: &Value, ctx: &ToolCallContext, key: &str) -> Option<String> {
    args.get(key)
        .cloned()
        .or_else(|| fact(ctx, key))
        .and_then(|v| v.as_str().map(str::to_owned))
        .filter(|v| !v.trim().is_empty())
}

/// A JSON schema object with `properties` and `required`.
#[must_use]
pub fn object(properties: &Value, required: &[&str]) -> Value {
    json!({"type": "object", "properties": properties, "required": required})
}

/// A [`ToolHandler`] from an async closure over the arguments and the call.
pub struct FnTool<F>(pub F);

#[async_trait]
impl<F, Fut> ToolHandler for FnTool<F>
where
    F: Fn(Value, ToolCallContext) -> Fut + Send + Sync,
    Fut: Future<Output = RiverbaseResult<Value>> + Send,
{
    async fn call(&self, args: Value, ctx: &ToolCallContext) -> RiverbaseResult<Value> {
        (self.0)(args, ctx.clone()).await
    }
}

/// A read-only server tool: low risk, no side effect, so it runs at once.
pub fn read_tool<F, Fut>(name: &str, description: &str, input_schema: Value, handler: F) -> ToolDefinition
where
    F: Fn(Value, ToolCallContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = RiverbaseResult<Value>> + Send + 'static,
{
    ToolDefinition {
        description: description.into(),
        input_schema,
        risk: ToolRisk::Low,
        side_effect: false,
        idempotent: true,
        ..ToolDefinition::new(name)
    }
    .with_handler(Arc::new(FnTool(handler)))
}

/// A server tool that writes the showcase's own records: low risk (it runs
/// at once), with a side effect remembered once per call id.
pub fn write_tool<F, Fut>(name: &str, description: &str, input_schema: Value, handler: F) -> ToolDefinition
where
    F: Fn(Value, ToolCallContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = RiverbaseResult<Value>> + Send + 'static,
{
    ToolDefinition {
        side_effect: true,
        ..read_tool(name, description, input_schema, handler)
    }
}

/// A high-risk server tool: the run holds until a human approves the call.
pub fn approval_tool<F, Fut>(name: &str, description: &str, input_schema: Value, handler: F) -> ToolDefinition
where
    F: Fn(Value, ToolCallContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = RiverbaseResult<Value>> + Send + 'static,
{
    ToolDefinition {
        risk: ToolRisk::High,
        require_approval: true,
        ..write_tool(name, description, input_schema, handler)
    }
}

/// A client tool: the browser that declares it runs it.
#[must_use]
pub fn client_tool(name: &str, description: &str, input_schema: Value) -> ToolDefinition {
    ToolDefinition {
        description: description.into(),
        input_schema,
        ..ToolDefinition::client(name)
    }
}

/// The client tools every showcase's chat runs: a form card and a document
/// upload.
#[must_use]
pub fn shared_client_tools() -> Vec<ToolDefinition> {
    vec![
        client_tool(
            REQUEST_INPUT,
            "Show the user a form card for missing details and wait for the answers. fields is an array of \
             {name, label, type, required}; type is text or number.",
            object(
                &json!({"title": {"type": "string"}, "description": {"type": "string"}, "fields": {"type": "array"}}),
                &["title", "fields"],
            ),
        ),
        client_tool(
            REQUEST_DOCUMENT,
            "Ask the user to upload one document and wait. document_type names it: pay_stub (loan desk), \
             damage_photo (claims) or invoice (accounts payable). The answer names the file and what was \
             read from it.",
            object(
                &json!({"document_type": {"type": "string"}, "title": {"type": "string"}}),
                &["document_type", "title"],
            ),
        ),
    ]
}

/// Register `tools` in `registry`; returns their names in order.
pub fn register_all(registry: &ToolRegistry, tools: Vec<ToolDefinition>) -> Vec<String> {
    tools
        .into_iter()
        .map(|tool| {
            let name = tool.name.clone();
            registry.register(tool);
            name
        })
        .collect()
}

/// The tools every showcase's chat agent may use when the beam has them:
/// `knowledge_search`, and `remember_fact` and `recall_memories` (registered
/// here; registering them again replaces them with the same definitions).
pub fn shared_agent_tools(beam: &RiverBeam, registry: &ToolRegistry) -> Vec<String> {
    let mut names = Vec::new();
    if registry
        .list_tool_names()
        .iter()
        .any(|n| n == KNOWLEDGE_SEARCH)
    {
        names.push(KNOWLEDGE_SEARCH.to_owned());
    }
    if let Some(knowledge) = beam.knowledge() {
        for tool in [
            remember_fact_tool(knowledge.clone()),
            recall_memories_tool(knowledge.clone()),
        ] {
            names.push(tool.name.clone());
            registry.register(tool);
        }
    }
    names
}

/// Ingest a showcase's documents, `(file, title, text)`, into its org's
/// scope (callers search with their org). Ingest dedupes by content, so a
/// restart adds nothing.
///
/// # Errors
/// The knowledge manager fails to ingest.
pub async fn ingest_org_documents(
    knowledge: &KnowledgeManager,
    org: &str,
    documents: &[(&str, &str, &str)],
) -> RiverbaseResult<()> {
    for (file, title, text) in documents {
        knowledge
            .ingest(IngestRequest {
                scope: KnowledgeScope::Org,
                org_id: Some(org.to_owned()),
                source_label: Some((*file).into()),
                filename: Some((*file).into()),
                mime_type: Some("text/markdown".into()),
                ..IngestRequest::text(*text, *title)
            })
            .await?;
    }
    Ok(())
}

/// Whether `name` is a tool several showcases use (MCP bridges, memory,
/// knowledge, the shared client tools), as opposed to one a showcase owns.
#[must_use]
pub fn is_shared_tool(name: &str) -> bool {
    name.contains("__")
        || [
            KNOWLEDGE_SEARCH,
            crate::host::tools::REMEMBER_FACT,
            crate::host::tools::RECALL_MEMORIES,
            REQUEST_INPUT,
            REQUEST_DOCUMENT,
        ]
        .contains(&name)
}
