//! The showcases' outside services as MCP servers (`showcase-mcp`):
//! deterministic stand-ins, so runs are repeatable.
//!
//! - `/rates/mcp`, `/credit/mcp`: the loan desk's pricing engine and credit
//!   bureau ([`crate::loan_desk::mcp_stub`]; credit wants the bureau key).
//! - `/orders/mcp`: the support desk's order system.
//! - `/monitoring/mcp`: the incident desk's monitoring.
//! - `/erp/mcp`: accounts payable's ERP (purchase orders, goods receipts).
//!
//! They speak JSON-RPC over POST, the way `beam_tools::mcp` calls a server.
//! [`StubServer`] is the protocol the new stubs share: `initialize`,
//! `notifications/*`, `tools/list` and `tools/call` over plain functions.

use std::sync::Arc;

use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{json, Value};

/// Every path the stubs serve.
pub const PATHS: [&str; 5] = [
    "/rates/mcp",
    "/credit/mcp",
    "/orders/mcp",
    "/monitoring/mcp",
    "/erp/mcp",
];

/// The secrets the stubs check.
#[derive(Debug, Clone, Default)]
pub struct Keys {
    /// What `/credit/mcp` expects in `x-bureau-key` (`LOAN_DESK_BUREAU_KEY`).
    pub bureau: String,
}

impl Keys {
    /// The keys from the environment; an unset key refuses every call that
    /// needs it.
    #[must_use]
    pub fn from_env() -> Self {
        let bureau = std::env::var("LOAN_DESK_BUREAU_KEY").unwrap_or_default();
        if bureau.is_empty() {
            eprintln!("warning: LOAN_DESK_BUREAU_KEY is unset; the credit server refuses every pull");
        }
        Self { bureau }
    }
}

/// Every stub on one router.
pub fn router(keys: &Keys) -> Router {
    crate::loan_desk::mcp_stub::router(keys.bureau.clone())
        .merge(StubServer::router(
            "/orders/mcp",
            crate::support::stub::orders(),
        ))
        .merge(StubServer::router(
            "/monitoring/mcp",
            crate::incident::stub::monitoring(),
        ))
        .merge(StubServer::router("/erp/mcp", crate::invoice::stub::erp()))
}

/// What a stub tool answers: its result, or a message the caller sees as a
/// tool error.
pub type StubResult = Result<Value, String>;

/// One tool of a stub server.
pub struct StubTool {
    pub name: &'static str,
    pub description: &'static str,
    pub input_schema: Value,
    pub call: fn(&Value) -> StubResult,
}

/// A stub MCP server: a name and its tools.
pub struct StubServer {
    pub name: &'static str,
    pub tools: Vec<StubTool>,
}

impl StubServer {
    /// The server at `path`.
    pub fn router(path: &str, server: StubServer) -> Router {
        let server = Arc::new(server);
        Router::new().route(
            path,
            post(move |Json(body): Json<Value>| {
                let server = server.clone();
                async move { server.answer(&body) }
            }),
        )
    }

    /// The JSON-RPC answer to `body`.
    #[must_use]
    pub fn answer(&self, body: &Value) -> Response {
        let id = body["id"].clone();
        match body["method"].as_str().unwrap_or_default() {
            "initialize" => {
                let result = json!({
                    "protocolVersion": "2024-11-05",
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": self.name, "version": "1"}
                });
                ([("mcp-session-id", self.name)], reply(&id, &result)).into_response()
            }
            method if method.starts_with("notifications/") => {
                axum::http::StatusCode::ACCEPTED.into_response()
            }
            "tools/list" => {
                let tools: Vec<Value> = self
                    .tools
                    .iter()
                    .map(|t| {
                        json!({"name": t.name, "description": t.description, "inputSchema": t.input_schema})
                    })
                    .collect();
                reply(&id, &json!({ "tools": tools }))
            }
            "tools/call" => {
                let name = body["params"]["name"].as_str().unwrap_or_default();
                let args = &body["params"]["arguments"];
                let result = match self.tools.iter().find(|t| t.name == name) {
                    Some(tool) => match (tool.call)(args) {
                        Ok(value) => text(&value),
                        Err(message) => failure(&message),
                    },
                    None => failure(&format!("unknown tool {name}")),
                };
                reply(&id, &result)
            }
            other => Json(json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {"code": -32601, "message": format!("unknown method {other}")}
            }))
            .into_response(),
        }
    }
}

/// The string argument `key`, or a message naming it.
///
/// # Errors
/// The argument is missing or blank.
pub fn required<'a>(args: &'a Value, key: &str) -> Result<&'a str, String> {
    args[key]
        .as_str()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| format!("{key} is required"))
}

fn reply(id: &Value, result: &Value) -> Response {
    Json(json!({"jsonrpc": "2.0", "id": id, "result": result})).into_response()
}

fn text(value: &Value) -> Value {
    json!({"content": [{"type": "text", "text": value.to_string()}], "isError": false})
}

fn failure(message: &str) -> Value {
    json!({"content": [{"type": "text", "text": message}], "isError": true})
}

#[cfg(test)]
mod tests {
    use super::*;

    fn echo() -> StubServer {
        StubServer {
            name: "echo",
            tools: vec![StubTool {
                name: "say",
                description: "Echo the word.",
                input_schema: json!({"type": "object", "properties": {"word": {"type": "string"}}}),
                call: |args| required(args, "word").map(|w| json!({"said": w})),
            }],
        }
    }

    async fn body(response: Response) -> Value {
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 16).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn a_stub_lists_and_calls_its_tools() {
        let server = echo();
        let listed = body(server.answer(&json!({"id": 1, "method": "tools/list"}))).await;
        assert_eq!(listed["result"]["tools"][0]["name"], "say");
        let called = body(server.answer(&json!({"id": 2, "method": "tools/call",
            "params": {"name": "say", "arguments": {"word": "hi"}}})))
        .await;
        assert_eq!(called["result"]["isError"], false);
        assert!(called["result"]["content"][0]["text"].as_str().unwrap().contains("hi"));
        let refused = body(server.answer(&json!({"id": 3, "method": "tools/call",
            "params": {"name": "say", "arguments": {}}})))
        .await;
        assert_eq!(refused["result"]["isError"], true);
    }
}
