//! MCP client and bridge (Python `tools/adapters/mcp.py`).
//!
//! HTTP servers are spoken to the way Python calls them: JSON-RPC POSTs only
//! (`initialize`, then the request, on one connection), never the GET SSE
//! stream, which lands on another replica behind a load balancer. A
//! terminated session is retried with a fresh one, up to 4 attempts. Stdio
//! servers use the rmcp SDK. Calls made for a user carry the caller's MCP token
//! (`X-MCP-Token`, fluvius `MCP_TOKEN_HEADER`).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, ACCEPT, CONTENT_TYPE};
use riverbase_core::RiverbaseResult;
use rmcp::service::{RoleClient, RunningService};
use rmcp::ServiceExt;
use serde_json::{json, Map, Value};
use tokio::sync::Mutex;

use crate::definition::{ToolCallContext, ToolDefinition, ToolHandler, ToolSource};

/// Python's protocol version for the POST path.
pub const MCP_PROTOCOL_VERSION: &str = "2024-11-05";
const SESSION_ATTEMPTS: u32 = 4;
const RETRY_BACKOFF: Duration = Duration::from_millis(50);
const SESSION_TERMINATED_MARKERS: [&str; 2] = ["session terminated", "session not found"];
const JSONRPC_ACCEPT: &str = "application/json, text/event-stream";

/// The environment a stdio server gets (the MCP SDK's
/// `DEFAULT_INHERITED_ENV_VARS`), plus the server's own `env`.
#[cfg(windows)]
const DEFAULT_INHERITED_ENV_VARS: &[&str] = &[
    "APPDATA",
    "HOMEDRIVE",
    "HOMEPATH",
    "LOCALAPPDATA",
    "PATH",
    "PATHEXT",
    "PROCESSOR_ARCHITECTURE",
    "SYSTEMDRIVE",
    "SYSTEMROOT",
    "TEMP",
    "USERNAME",
    "USERPROFILE",
];
#[cfg(not(windows))]
const DEFAULT_INHERITED_ENV_VARS: &[&str] = &["HOME", "LOGNAME", "PATH", "SHELL", "TERM", "USER"];

/// A tool an MCP server offers.
#[derive(Debug, Clone, PartialEq)]
pub struct McpToolDefinition {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
}

enum Transport {
    Http {
        url: String,
        /// Sent with discovery and calls made without a user.
        headers: HashMap<String, String>,
    },
    Stdio(Mutex<Option<RunningService<RoleClient, ()>>>),
}

pub struct McpClient {
    server_name: String,
    transport: Transport,
    tools: Vec<McpToolDefinition>,
}

fn is_session_terminated(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    SESSION_TERMINATED_MARKERS.iter().any(|m| lower.contains(m))
}

/// A JSON body or a one-shot SSE body.
fn parse_body(content_type: &str, text: &str) -> RiverbaseResult<Option<Value>> {
    if text.trim().is_empty() {
        return Ok(None);
    }
    let trimmed = text.trim_start();
    let sse = content_type
        .to_ascii_lowercase()
        .contains("text/event-stream")
        || trimmed.starts_with("event:")
        || trimmed.starts_with("data:");
    if sse {
        for line in text.lines() {
            if let Some(data) = line.strip_prefix("data:") {
                let data = data.trim();
                if !data.is_empty() && data != "[DONE]" {
                    let parsed: Value = serde_json::from_str(data)
                        .map_err(|e| crate::BEM_472.with_data(e.to_string()))?;
                    if parsed.is_object() {
                        return Ok(Some(parsed));
                    }
                }
            }
        }
        return Err(crate::BEM_472.with_data("MCP SSE response had no JSON data"));
    }
    let parsed: Value =
        serde_json::from_str(text).map_err(|e| crate::BEM_472.with_data(e.to_string()))?;
    if parsed.is_object() {
        Ok(Some(parsed))
    } else {
        Err(crate::BEM_472.with_data("Unexpected MCP JSON body type"))
    }
}

fn raise_if_jsonrpc_error(payload: Option<&Value>) -> RiverbaseResult<()> {
    if let Some(error) = payload.and_then(|p| p.get("error")) {
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .map_or_else(|| error.to_string(), str::to_owned);
        return Err(crate::BEM_472.with_data(message));
    }
    Ok(())
}

/// Python `_tool_result_from_jsonrpc` + `MCPBridgedTool.acall`: text blocks
/// joined by newlines; other blocks as JSON; a result with no blocks as JSON.
fn result_text(result: &Value) -> String {
    let blocks: Vec<String> = result
        .get("content")
        .and_then(Value::as_array)
        .map(|content| {
            content
                .iter()
                .map(|block| match block.get("text") {
                    Some(Value::String(text)) => text.clone(),
                    Some(other) if !other.is_null() => other.to_string(),
                    _ => block.to_string(),
                })
                .collect()
        })
        .unwrap_or_default();
    if blocks.is_empty() && result.as_object().is_some_and(|o| !o.is_empty()) {
        return result.to_string();
    }
    blocks.join("\n")
}

fn header_map(headers: &HashMap<String, String>) -> RiverbaseResult<HeaderMap> {
    let mut map = HeaderMap::new();
    for (name, value) in headers {
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|e| crate::BEM_471.with_data(e.to_string()))?;
        let value =
            HeaderValue::from_str(value).map_err(|e| crate::BEM_471.with_data(e.to_string()))?;
        map.insert(name, value);
    }
    Ok(map)
}

impl McpClient {
    /// Connect over HTTP and list the server's tools.
    ///
    /// # Errors
    /// `BEM-471` when the server cannot be reached or refuses.
    pub async fn connect_http(
        server_name: impl Into<String>,
        url: impl Into<String>,
        headers: HashMap<String, String>,
    ) -> RiverbaseResult<Self> {
        let mut mcp = Self {
            server_name: server_name.into(),
            transport: Transport::Http {
                url: url.into(),
                headers,
            },
            tools: Vec::new(),
        };
        let listed = mcp
            .http_request("tools/list", json!({}), None)
            .await
            .map_err(|e| crate::BEM_471.with_data(format!("{}: {}", mcp.server_name, e.errdata)))?;
        mcp.tools = parse_tools(&listed);
        tracing::info!(server = %mcp.server_name, tools = mcp.tools.len(), "mcp_connected_http");
        Ok(mcp)
    }

    /// Start a stdio server and list its tools.
    ///
    /// # Errors
    /// `BEM-471` when the process cannot start or the handshake fails.
    pub async fn connect_stdio(
        server_name: impl Into<String>,
        command: &str,
        args: &[String],
        env: &HashMap<String, String>,
    ) -> RiverbaseResult<Self> {
        use rmcp::transport::{ConfigureCommandExt, TokioChildProcess};
        let server_name = server_name.into();
        // Python's stdio client passes only a safe environment
        // (`get_default_environment`), never the worker's secrets.
        let process =
            TokioChildProcess::new(tokio::process::Command::new(command).configure(|c| {
                c.args(args).env_clear();
                for key in DEFAULT_INHERITED_ENV_VARS {
                    // The child's environment, read as is (not a setting).
                    if let Ok(value) = std::env::var(key) {
                        if !value.starts_with("()") {
                            c.env(key, value);
                        }
                    }
                }
                for (key, value) in env {
                    c.env(key, value);
                }
            }))
            .map_err(|e| crate::BEM_471.with_data(format!("{server_name}: {e}")))?;
        let service = ()
            .serve(process)
            .await
            .map_err(|e| crate::BEM_471.with_data(format!("{server_name}: {e}")))?;
        let listed = service
            .list_all_tools()
            .await
            .map_err(|e| crate::BEM_471.with_data(format!("{server_name}: {e}")))?;
        let tools = listed
            .into_iter()
            .map(|t| McpToolDefinition {
                name: t.name.to_string(),
                description: t.description.map(|d| d.to_string()).unwrap_or_default(),
                input_schema: Value::Object((*t.input_schema).clone()),
            })
            .collect::<Vec<_>>();
        tracing::info!(server = %server_name, tools = tools.len(), "mcp_connected");
        Ok(Self {
            server_name,
            transport: Transport::Stdio(Mutex::new(Some(service))),
            tools,
        })
    }

    #[must_use]
    pub fn server_name(&self) -> &str {
        &self.server_name
    }

    #[must_use]
    pub fn tools(&self) -> &[McpToolDefinition] {
        &self.tools
    }

    /// One JSON-RPC exchange on a fresh HTTP session: `initialize`, then
    /// `method`, with the session id the server issues (if any). Python
    /// `call_tool_via_jsonrpc_posts`, generalized to any method.
    async fn http_session(
        &self,
        method: &str,
        params: Value,
        extra: Option<&HashMap<String, String>>,
    ) -> RiverbaseResult<Value> {
        let Transport::Http { url, headers } = &self.transport else {
            return Err(crate::BEM_472.with_data("not an HTTP MCP server"));
        };
        // Python `call_tool_as_user` sends only the user's headers, never the
        // connect-time (service) credentials.
        let base = header_map(extra.unwrap_or(headers))?;
        // One client with one connection per session, as Python: `initialize`
        // and the request go over the same TCP connection (and so the same
        // replica behind a load balancer).
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(1)
            .http1_only()
            .build()
            .map_err(|e| crate::BEM_472.with_data(e.to_string()))?;
        let client = &client;
        let post = |body: Value, session: Option<String>| {
            let mut request = client
                .post(url)
                .headers(base.clone())
                .header(ACCEPT, JSONRPC_ACCEPT)
                .header(CONTENT_TYPE, "application/json")
                .json(&body);
            if let Some(session) = session {
                request = request.header("mcp-session-id", session);
            }
            async move {
                let response = request
                    .send()
                    .await
                    .map_err(|e| crate::BEM_472.with_data(e.without_url().to_string()))?;
                let status = response.status();
                let session = response
                    .headers()
                    .get("mcp-session-id")
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_owned);
                let content_type = response
                    .headers()
                    .get(CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or_default()
                    .to_owned();
                let text = response
                    .text()
                    .await
                    .map_err(|e| crate::BEM_472.with_data(e.without_url().to_string()))?;
                if status.as_u16() >= 400 {
                    let snippet: String = text.chars().take(240).collect();
                    return Err(crate::BEM_472.with_data(format!(
                        "MCP HTTP {} for {}: {snippet}",
                        status.as_u16(),
                        body.get("method")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                    )));
                }
                Ok((session, parse_body(&content_type, &text)?))
            }
        };
        let (session, init) = post(
            json!({"jsonrpc": "2.0", "id": uuid::Uuid::new_v4().to_string(), "method": "initialize",
                   "params": {"protocolVersion": MCP_PROTOCOL_VERSION, "capabilities": {},
                              "clientInfo": {"name": "fluvius-beam", "version": "1.0"}}}),
            None,
        )
        .await?;
        raise_if_jsonrpc_error(init.as_ref())?;
        // Python skips `notifications/initialized`: that hop often lands on another replica.
        let (_, reply) = post(
            json!({"jsonrpc": "2.0", "id": uuid::Uuid::new_v4().to_string(), "method": method, "params": params}),
            session,
        )
        .await?;
        raise_if_jsonrpc_error(reply.as_ref())?;
        Ok(reply
            .and_then(|r| r.get("result").cloned())
            .unwrap_or(Value::Null))
    }

    /// [`Self::http_session`] with Python's retry on a terminated session.
    async fn http_request(
        &self,
        method: &str,
        params: Value,
        extra: Option<&HashMap<String, String>>,
    ) -> RiverbaseResult<Value> {
        let mut attempt = 1;
        loop {
            match self.http_session(method, params.clone(), extra).await {
                Ok(result) => return Ok(result),
                Err(e)
                    if attempt < SESSION_ATTEMPTS
                        && is_session_terminated(&e.errdata.to_string()) =>
                {
                    tracing::warn!(server = %self.server_name, attempt, "mcp_session_terminated; retrying with a new session");
                    tokio::time::sleep(RETRY_BACKOFF * attempt).await;
                    attempt += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Call `name` with `args`; `user_headers` are sent with an HTTP call
    /// Returns the result's text.
    ///
    /// # Errors
    /// `BEM-472` for a transport or JSON-RPC error.
    pub async fn call_tool(
        &self,
        name: &str,
        args: Map<String, Value>,
        user_headers: Option<&HashMap<String, String>>,
    ) -> RiverbaseResult<String> {
        match &self.transport {
            Transport::Http { .. } => {
                let result = self
                    .http_request(
                        "tools/call",
                        json!({"name": name, "arguments": args}),
                        user_headers,
                    )
                    .await?;
                Ok(result_text(&result))
            }
            Transport::Stdio(service) => {
                let guard = service.lock().await;
                let service = guard
                    .as_ref()
                    .ok_or_else(|| crate::BEM_472.with_data("MCP client not connected"))?;
                let result = service
                    .call_tool(
                        rmcp::model::CallToolRequestParams::new(name.to_owned())
                            .with_arguments(args),
                    )
                    .await
                    .map_err(|e| crate::BEM_472.with_data(e.to_string()))?;
                let value = serde_json::to_value(&result)
                    .map_err(|e| crate::BEM_472.with_data(e.to_string()))?;
                Ok(result_text(&value))
            }
        }
    }

    pub async fn close(&self) {
        if let Transport::Stdio(service) = &self.transport {
            if let Some(service) = service.lock().await.take() {
                let _ = service.cancel().await;
            }
        }
    }
}

fn parse_tools(listed: &Value) -> Vec<McpToolDefinition> {
    listed
        .get("tools")
        .and_then(Value::as_array)
        .map(|tools| {
            tools
                .iter()
                .filter_map(|t| {
                    Some(McpToolDefinition {
                        name: t.get("name")?.as_str()?.to_owned(),
                        description: t
                            .get("description")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_owned(),
                        input_schema: t.get("inputSchema").cloned().unwrap_or_else(|| json!({})),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// MCP tools as registry definitions named `<server>__<tool>`.
pub struct McpBridge;

impl McpBridge {
    /// `(unprefixed tool name, definition)` for every tool of `client`; a
    /// call's MCP token goes in `token_header`.
    #[must_use]
    pub fn bridge_all(
        client: &Arc<McpClient>,
        token_header: &str,
    ) -> Vec<(String, ToolDefinition)> {
        client
            .tools()
            .iter()
            .map(|tool| {
                let definition = ToolDefinition {
                    description: tool.description.clone(),
                    input_schema: tool.input_schema.clone(),
                    source: ToolSource::Mcp,
                    mcp_server: Some(client.server_name().to_owned()),
                    ..ToolDefinition::new(format!("{}__{}", client.server_name(), tool.name))
                }
                .with_handler(Arc::new(McpHandler {
                    client: client.clone(),
                    tool_name: tool.name.clone(),
                    token_header: token_header.to_owned(),
                }));
                (tool.name.clone(), definition)
            })
            .collect()
    }
}

struct McpHandler {
    client: Arc<McpClient>,
    tool_name: String,
    token_header: String,
}

#[async_trait]
impl ToolHandler for McpHandler {
    async fn call(&self, args: Value, ctx: &ToolCallContext) -> RiverbaseResult<Value> {
        // Python drops `None` arguments before calling.
        let cleaned: Map<String, Value> = args
            .as_object()
            .map(|a| {
                a.iter()
                    .filter(|(_, v)| !v.is_null())
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            })
            .unwrap_or_default();
        // The host's token, else one minted for this call: no
        // token is ever stored with the run.
        let user_headers = crate::mcp_token::token_for(
            ctx.auth.as_ref(),
            crate::mcp_token::McpTokenMinter::global(),
        )?
        .map(|token| HashMap::from([(self.token_header.clone(), token)]));
        let text = self
            .client
            .call_tool(&self.tool_name, cleaned, user_headers.as_ref())
            .await?;
        Ok(Value::String(text))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bodies_parse_like_python() {
        let sse = "event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}\n\n";
        assert!(parse_body("text/event-stream", sse).unwrap().is_some());
        assert!(parse_body("application/json", "").unwrap().is_none());
        assert!(parse_body("application/json", "[1]").is_err());
        let result =
            json!({"content": [{"type": "text", "text": "a"}, {"type": "text", "text": "b"}]});
        assert_eq!(result_text(&result), "a\nb");
        assert!(is_session_terminated("HTTP 404: Session not found"));
    }
}
