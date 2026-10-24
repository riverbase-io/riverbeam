//! Tools every showcase may use: long-term memory (`remember_fact`,
//! `recall_memories`) and the MCP servers listed in `MCP_SERVERS_CONFIG_PATH`.
//!
//! The MCP file names its servers; secrets never go in it. Header values
//! (`headers_env`) and a stdio server's environment (`env_from`) name the
//! worker's environment variables to read them from:
//!
//! ```yaml
//! servers:
//!   - name: docs
//!     transport: http
//!     url: https://mcp.example.com/mcp
//!     headers_env: { Authorization: DOCS_MCP_AUTHORIZATION }
//!     allow: [search, get_page]
//!   - name: files
//!     transport: stdio
//!     command: npx
//!     args: ["-y", "@modelcontextprotocol/server-filesystem", "/srv/shared"]
//!     allow: [read_file, list_directory]
//! ```
//!
//! A server without `allow` registers nothing while
//! `BEAM_MCP_REQUIRE_ALLOWLIST` is on (the default). A server that cannot be
//! reached is skipped with a warning.

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use beam_knowledge::{KnowledgeManager, MemoryType, RecallRequest, RememberRequest};
use beam_tools::{McpClient, ToolCallContext, ToolDefinition, ToolHandler, ToolRegistry, ToolRisk};
use riverbase_core::RiverbaseResult;
use serde::Deserialize;
use serde_json::{json, Value};

/// How long one MCP server may take to connect and list its tools.
const MCP_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// The long-term memory tool.
pub const REMEMBER_FACT: &str = "remember_fact";

/// The long-term memory search.
pub const RECALL_MEMORIES: &str = "recall_memories";

/// Whose memory a call reads or writes: the caller (the auth's user id, as
/// the `ltm` context source keys it; the model never names the user) and
/// the running agent's id (the `ltm` source recalls per agent).
fn memory_owner(ctx: &ToolCallContext, tool: &str) -> RiverbaseResult<(String, Option<String>)> {
    let user = ctx
        .auth
        .as_ref()
        .and_then(|a| a.user_id.as_deref())
        .filter(|u| !u.trim().is_empty())
        .ok_or_else(|| beam_core::BEM_004.with_data(format!("{tool} needs a signed-in user")))?;
    let agent_id = ctx
        .input
        .as_ref()
        .and_then(|i| i.agent_name.as_deref())
        .map(|name| beam_store::agent_id_for(name).to_string());
    Ok((user.to_owned(), agent_id))
}

/// `remember_fact`: a long-term memory for the caller, the same write as the
/// `remember-memory` command's task. With `MEMORY_LONGTERM_ENABLED`, later
/// runs read it back into their context (the `ltm` source).
struct RememberFact(Arc<KnowledgeManager>);

#[async_trait]
impl ToolHandler for RememberFact {
    async fn call(&self, args: Value, ctx: &ToolCallContext) -> RiverbaseResult<Value> {
        let fact = args
            .get("fact")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|f| !f.is_empty())
            .ok_or_else(|| beam_core::BEM_004.with_data("fact is required"))?;
        let (user, agent_id) = memory_owner(ctx, REMEMBER_FACT)?;
        let memory_type = args
            .get("type")
            .and_then(Value::as_str)
            .and_then(MemoryType::parse)
            .unwrap_or_default();
        let request = RememberRequest {
            memory_type: memory_type.as_str().into(),
            agent_id,
            source_execution_id: Some(ctx.execution_id.to_string()),
            ..RememberRequest::new(user, fact)
        };
        let memory_id = self.0.remember(request).await?;
        Ok(json!({ "memory_id": memory_id, "type": memory_type.as_str(), "fact": fact }))
    }
}

/// The `remember_fact` definition: it writes the caller's memory, so every
/// call waits for the human's approval.
#[must_use]
pub fn remember_fact_tool(knowledge: Arc<KnowledgeManager>) -> ToolDefinition {
    ToolDefinition {
        description: "Save a fact about the user to their long-term memory, so later \
            conversations can use it. The user approves each save."
            .into(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "fact": { "type": "string", "description": "One short fact, in the third person." },
                "type": {
                    "type": "string",
                    "enum": ["semantic", "preference", "episodic"],
                    "description": "preference for likes and dislikes; episodic for events; semantic otherwise.",
                },
            },
            "required": ["fact"],
        }),
        risk: ToolRisk::Medium,
        side_effect: true,
        idempotent: false,
        require_approval: true,
        ..ToolDefinition::new(REMEMBER_FACT)
    }
    .with_handler(Arc::new(RememberFact(knowledge)))
}

/// `recall_memories`: the caller's long-term memories closest to a query.
/// The `ltm` context source reads them into a run's context only when the
/// agent's revision spec enables long-term memory; this tool lets the agent
/// look them up itself.
struct RecallMemories(Arc<KnowledgeManager>);

#[async_trait]
impl ToolHandler for RecallMemories {
    async fn call(&self, args: Value, ctx: &ToolCallContext) -> RiverbaseResult<Value> {
        let query = args
            .get("query")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|q| !q.is_empty())
            .ok_or_else(|| beam_core::BEM_004.with_data("query is required"))?;
        let k = args
            .get("k")
            .and_then(Value::as_u64)
            .map_or(5, |k| k.clamp(1, 20));
        let (user, agent_id) = memory_owner(ctx, RECALL_MEMORIES)?;
        let request = RecallRequest {
            agent_id,
            k: usize::try_from(k).unwrap_or(5),
            ..RecallRequest::new(query, user)
        };
        let hits = self.0.recall(&request).await?;
        Ok(Value::Array(
            hits.into_iter()
                .map(|hit| {
                    json!({"memory_id": hit.id, "fact": hit.text, "score": hit.score,
                           "type": hit.metadata.get("type")})
                })
                .collect(),
        ))
    }
}

/// The `recall_memories` definition: read-only, so it runs at once.
#[must_use]
pub fn recall_memories_tool(knowledge: Arc<KnowledgeManager>) -> ToolDefinition {
    ToolDefinition {
        description: "Search the user's long-term memory (facts saved earlier with \
            remember_fact). Returns the closest facts with their scores."
            .into(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "query": { "type": "string", "description": "What to look for, e.g. \"the user's preferences\"." },
                "k": { "type": "integer", "minimum": 1, "maximum": 20, "default": 5 },
            },
            "required": ["query"],
        }),
        risk: ToolRisk::Low,
        side_effect: false,
        idempotent: true,
        ..ToolDefinition::new(RECALL_MEMORIES)
    }
    .with_handler(Arc::new(RecallMemories(knowledge)))
}

/// The `MCP_SERVERS_CONFIG_PATH` file.
#[derive(Debug, Default, Deserialize, PartialEq, Eq)]
pub struct McpConfig {
    #[serde(default)]
    pub servers: Vec<McpServerConfig>,
}

/// One MCP server. `allow` lists the (unprefixed) tool names to expose.
#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "transport", rename_all = "lowercase")]
pub enum McpServerConfig {
    Http {
        name: String,
        url: String,
        /// Header name → environment variable holding its value.
        #[serde(default)]
        headers_env: HashMap<String, String>,
        #[serde(default)]
        allow: Option<Vec<String>>,
    },
    Stdio {
        name: String,
        command: String,
        #[serde(default)]
        args: Vec<String>,
        /// The server's variable → the worker's variable holding its value.
        #[serde(default)]
        env_from: HashMap<String, String>,
        #[serde(default)]
        allow: Option<Vec<String>>,
    },
}

impl McpServerConfig {
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            Self::Http { name, .. } | Self::Stdio { name, .. } => name,
        }
    }

    #[must_use]
    pub fn allow(&self) -> Option<HashSet<String>> {
        match self {
            Self::Http { allow, .. } | Self::Stdio { allow, .. } => {
                allow.as_ref().map(|a| a.iter().cloned().collect())
            }
        }
    }
}

/// Parse an MCP config document.
///
/// # Errors
/// Invalid YAML, or a server with an unknown `transport` or missing fields.
pub fn parse_mcp_config(text: &str) -> Result<McpConfig, serde_yaml::Error> {
    serde_yaml::from_str(text)
}

/// The file at `MCP_SERVERS_CONFIG_PATH`, or `None` when the variable is unset.
///
/// # Errors
/// The file cannot be read or parsed.
pub fn mcp_config_from_env() -> Result<Option<McpConfig>, Box<dyn Error>> {
    let Some(path) = std::env::var("MCP_SERVERS_CONFIG_PATH")
        .ok()
        .filter(|p| !p.trim().is_empty())
    else {
        return Ok(None);
    };
    let text = std::fs::read_to_string(path.trim()).map_err(|e| format!("{}: {e}", path.trim()))?;
    Ok(Some(parse_mcp_config(&text)?))
}

/// Read each value from the environment variable it names.
///
/// # Errors
/// The first variable that is unset.
fn resolve_env(names: &HashMap<String, String>) -> Result<HashMap<String, String>, String> {
    names
        .iter()
        .map(|(key, var)| {
            std::env::var(var)
                .map(|value| (key.clone(), value))
                .map_err(|_| format!("environment variable {var} is not set"))
        })
        .collect()
}

async fn connect(server: &McpServerConfig) -> Result<McpClient, String> {
    let connected = match server {
        McpServerConfig::Http {
            name,
            url,
            headers_env,
            ..
        } => McpClient::connect_http(name.clone(), url.clone(), resolve_env(headers_env)?).await,
        McpServerConfig::Stdio {
            name,
            command,
            args,
            env_from,
            ..
        } => McpClient::connect_stdio(name.clone(), command, args, &resolve_env(env_from)?).await,
    };
    connected.map_err(|e| format!("{e} {}", e.errdata))
}

/// Connect every server and register its allowed tools as `<server>__<tool>`.
/// A server that fails or takes longer than 10 seconds is skipped with a
/// warning, and so is one without `allow` while the allowlist is required
/// (it is never started). Returns the registered tool names.
pub async fn connect_mcp_servers(registry: &ToolRegistry, config: &McpConfig) -> Vec<String> {
    let mut names = Vec::new();
    for server in &config.servers {
        if server.allow().is_none() && registry.mcp_settings().require_allowlist {
            eprintln!(
                "warning: MCP server {} skipped (no allow list; BEAM_MCP_REQUIRE_ALLOWLIST is on)",
                server.name()
            );
            continue;
        }
        let client = match tokio::time::timeout(MCP_CONNECT_TIMEOUT, connect(server)).await {
            Ok(Ok(client)) => Arc::new(client),
            Ok(Err(err)) => {
                eprintln!("warning: MCP server {} skipped ({err})", server.name());
                continue;
            }
            Err(_) => {
                eprintln!(
                    "warning: MCP server {} skipped (no answer in {}s)",
                    server.name(),
                    MCP_CONNECT_TIMEOUT.as_secs()
                );
                continue;
            }
        };
        let bridged = registry.register_mcp_server(&client, server.allow().as_ref());
        names.extend(bridged.into_iter().map(|definition| definition.name));
    }
    names
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn remember_fact_needs_a_fact_and_a_signed_in_user() {
        let knowledge = Arc::new(KnowledgeManager::new(
            Arc::new(beam_knowledge::HashEmbedder::new(64)),
            Arc::new(beam_knowledge::InMemoryVectorStore::new()),
        ));
        let tool = remember_fact_tool(knowledge);
        assert!(tool.require_approval);
        let handler = tool.callable.as_ref().expect("handler");
        let anonymous = ToolCallContext::default();
        let err = handler
            .call(json!({"fact": "likes tea"}), &anonymous)
            .await
            .expect_err("no user");
        assert!(
            err.errdata.to_string().contains("signed-in user"),
            "{err} {}",
            err.errdata
        );
        let signed_in = ToolCallContext {
            auth: Some(beam_types::InvokeAuthContext {
                profile_id: None,
                user_id: Some("tester".into()),
                org_id: None,
                mcp_token: None,
            }),
            ..ToolCallContext::default()
        };
        let err = handler
            .call(json!({"fact": "  "}), &signed_in)
            .await
            .expect_err("blank");
        assert!(
            err.errdata.to_string().contains("fact is required"),
            "{err} {}",
            err.errdata
        );
        let out = handler
            .call(
                json!({"fact": "likes tea", "type": "preference"}),
                &signed_in,
            )
            .await
            .expect("saved");
        assert_eq!(out["type"], "preference");
        assert!(out["memory_id"].as_str().is_some_and(|id| !id.is_empty()));
    }

    #[test]
    fn the_config_reads_http_and_stdio_servers() {
        let config = parse_mcp_config(
            "servers:
  - name: docs
    transport: http
    url: http://127.0.0.1:9000/mcp
    headers_env: { Authorization: DOCS_TOKEN }
    allow: [search]
  - name: files
    transport: stdio
    command: npx
    args: [\"-y\", server-filesystem]
",
        )
        .expect("valid config");
        assert_eq!(
            config.servers,
            vec![
                McpServerConfig::Http {
                    name: "docs".into(),
                    url: "http://127.0.0.1:9000/mcp".into(),
                    headers_env: HashMap::from([("Authorization".into(), "DOCS_TOKEN".into())]),
                    allow: Some(vec!["search".into()]),
                },
                McpServerConfig::Stdio {
                    name: "files".into(),
                    command: "npx".into(),
                    args: vec!["-y".into(), "server-filesystem".into()],
                    env_from: HashMap::new(),
                    allow: None,
                },
            ]
        );
        assert_eq!(config.servers[1].allow(), None);
    }

    #[test]
    fn the_showcase_file_parses() {
        let config =
            parse_mcp_config(include_str!("../../mcp_servers.yaml")).expect("valid config");
        let names: Vec<&str> = config.servers.iter().map(McpServerConfig::name).collect();
        assert_eq!(names, ["rates", "credit", "orders", "monitoring", "erp"]);
        assert!(config.servers.iter().all(|s| s.allow().is_some()));
    }

    #[test]
    fn an_unknown_transport_is_refused() {
        let err = parse_mcp_config("servers: [{name: x, transport: websocket, url: 'ws://x'}]")
            .expect_err("unknown transport");
        assert!(err.to_string().contains("websocket"), "{err}");
    }

    #[test]
    fn a_missing_secret_names_its_variable() {
        let names = HashMap::from([(
            "Authorization".to_owned(),
            "SHOWCASE_TEST_SURELY_UNSET_VAR".to_owned(),
        )]);
        let err = resolve_env(&names).expect_err("unset");
        assert!(err.contains("SHOWCASE_TEST_SURELY_UNSET_VAR"), "{err}");
    }

    #[tokio::test]
    async fn an_unreachable_server_is_skipped() {
        let registry = ToolRegistry::new();
        let config = parse_mcp_config(
            "servers: [{name: down, transport: http, url: 'http://127.0.0.1:9/mcp', allow: [x]}]",
        )
        .expect("valid config");
        assert!(connect_mcp_servers(&registry, &config).await.is_empty());
        assert!(registry.list_tool_names().is_empty());
    }
}
