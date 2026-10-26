//! Tool runtime and MCP for riverbeam.
//!
//! Every tool call from a runtime goes through [`ToolRuntime`], as in Python:
//! allowlist, dry run, approval, durable idempotency, the `BEFORE_TOOL` and
//! `AFTER_TOOL` security stages, input validation, timeout and retries.
//! [`ToolService`] is the `beam_runtime::ToolExecutor` runtimes receive. MCP
//! servers are bridged under a default-deny allowlist and get the caller's MCP
//! token, minted per call when the host set none ([`mcp_token`]).

#[allow(clippy::wildcard_imports)] // The error catalog (`beam_errors`).
pub(crate) use beam_errors::*;

mod definition;
pub mod idempotency;
pub mod knowledge;
pub mod mcp;
pub mod mcp_token;
pub mod native;
mod output;
pub mod policy;
mod registry;
mod runtime;
mod service;

pub use definition::{
    ToolCallContext, ToolDefinition, ToolHandler, ToolRisk, ToolSource, CLIENT_TOOL_TIMEOUT_SECONDS,
};
pub use idempotency::{InMemoryToolIdempotencyStore, PgToolIdempotencyStore, ToolIdempotencyStore};
pub use knowledge::{knowledge_search_tool, KNOWLEDGE_SEARCH};
pub use mcp::{McpBridge, McpClient, McpToolDefinition};
pub use output::{bound_tool_output, MAX_RESULT_CHARS};
pub use registry::{McpSettings, ToolRegistry};
pub use runtime::{
    ExecuteContext, ToolPolicyStores, ToolRuntime, ToolRuntimeResult, OUTPUT_WITHHELD,
};
pub use service::ToolService;
