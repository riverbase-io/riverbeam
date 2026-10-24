//! The tool catalog (Python `tools/registry.py`).

use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use riverbase_core::RiverbaseResult;
use serde_json::Value;

use crate::definition::{ToolCallContext, ToolDefinition, ToolHandler};
use crate::mcp::{McpBridge, McpClient};
use crate::native::Tool;

/// The MCP settings a registry reads when a server registers and its tools
/// are called. `Default` reads no environment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpSettings {
    /// Python `BEAM_MCP_REQUIRE_ALLOWLIST` (default on): an MCP server
    /// registered without an allowlist contributes no tools.
    pub require_allowlist: bool,
    /// fluvius's `MCP_TOKEN_HEADER` (default `X-MCP-Token`): the header that
    /// carries a call's MCP token.
    pub token_header: String,
}

impl Default for McpSettings {
    fn default() -> Self {
        Self {
            require_allowlist: true,
            token_header: "X-MCP-Token".into(),
        }
    }
}

impl McpSettings {
    /// The defaults, with `BEAM_MCP_REQUIRE_ALLOWLIST` and `MCP_TOKEN_HEADER`
    /// applied.
    #[must_use]
    pub fn from_env() -> Self {
        let d = Self::default();
        Self {
            require_allowlist: parse_require_allowlist(
                beam_env::string("BEAM_MCP_REQUIRE_ALLOWLIST").as_deref(),
            ),
            token_header: beam_env::string("MCP_TOKEN_HEADER").unwrap_or(d.token_header),
        }
    }
}

/// The allowlist switch from its raw value: only `0`, `false`, `no` or `off`
/// turn it off. Unset, blank or anything else keeps it on, so a mistyped
/// value fails closed.
#[must_use]
pub fn parse_require_allowlist(raw: Option<&str>) -> bool {
    raw.and_then(beam_env::parse_flag).unwrap_or(true)
}

/// Registered definitions in registration order; a name registered again replaces the old one.
#[derive(Default)]
pub struct ToolRegistry {
    definitions: RwLock<Vec<ToolDefinition>>,
    mcp_clients: RwLock<Vec<Arc<McpClient>>>,
    mcp: McpSettings,
}

impl ToolRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// This registry with `settings` for the MCP servers it registers.
    #[must_use]
    pub fn with_mcp(mut self, settings: McpSettings) -> Self {
        self.mcp = settings;
        self
    }

    #[must_use]
    pub fn mcp_settings(&self) -> &McpSettings {
        &self.mcp
    }

    pub fn register(&self, definition: ToolDefinition) {
        tracing::info!(name = %definition.name, source = ?definition.source, "tool_registered");
        if let Ok(mut defs) = self.definitions.write() {
            if let Some(slot) = defs.iter_mut().find(|d| d.name == definition.name) {
                *slot = definition;
            } else {
                defs.push(definition);
            }
        }
    }

    /// Register a native [`Tool`]. Its `confirm_required` becomes `require_approval`.
    pub fn register_native(&self, tool: Arc<dyn Tool>) {
        let spec = tool.spec();
        let definition = ToolDefinition {
            description: spec.description,
            input_schema: spec.parameters,
            require_approval: spec.confirm_required,
            ..ToolDefinition::new(spec.name)
        }
        .with_handler(Arc::new(NativeHandler(tool)));
        self.register(definition);
    }

    /// Bridge an MCP server's tools as `<server>__<tool>`. With `allowed`, only
    /// those (unprefixed) tool names are registered; without it, nothing is
    /// registered while [`McpSettings::require_allowlist`] holds (Python's
    /// production rule).
    pub fn register_mcp_server(
        &self,
        client: &Arc<McpClient>,
        allowed: Option<&HashSet<String>>,
    ) -> Vec<ToolDefinition> {
        if allowed.is_none() && self.mcp.require_allowlist {
            tracing::warn!(server = %client.server_name(), "mcp_server_denied_no_allowlist");
            return Vec::new();
        }
        if crate::mcp_token::McpTokenMinter::global().is_none() {
            // Plan 020 D3: without a secret, calls carry only a host-set token.
            tracing::warn!(
                server = %client.server_name(),
                "mcp_tokens_not_minted: MCP_SECRET_KEY is not set"
            );
        }
        let bridged: Vec<ToolDefinition> = McpBridge::bridge_all(client, &self.mcp.token_header)
            .into_iter()
            .filter(|(tool_name, _)| allowed.is_none_or(|a| a.contains(tool_name)))
            .map(|(_, definition)| definition)
            .collect();
        for definition in &bridged {
            self.register(definition.clone());
        }
        if let Ok(mut clients) = self.mcp_clients.write() {
            clients.push(client.clone());
        }
        tracing::info!(
            server = %client.server_name(),
            tools = ?bridged.iter().map(|d| d.name.as_str()).collect::<Vec<_>>(),
            "mcp_server_registered"
        );
        bridged
    }

    #[must_use]
    pub fn get(&self, name: &str) -> Option<ToolDefinition> {
        self.definitions
            .read()
            .ok()
            .and_then(|d| d.iter().find(|t| t.name == name).cloned())
    }

    #[must_use]
    pub fn list_tools(&self) -> Vec<ToolDefinition> {
        self.definitions
            .read()
            .map(|d| d.clone())
            .unwrap_or_default()
    }

    #[must_use]
    pub fn list_tool_names(&self) -> Vec<String> {
        self.definitions
            .read()
            .map(|d| d.iter().map(|t| t.name.clone()).collect())
            .unwrap_or_default()
    }

    /// Close every MCP connection.
    pub async fn close(&self) {
        let clients: Vec<Arc<McpClient>> = self
            .mcp_clients
            .write()
            .map(|mut c| std::mem::take(&mut *c))
            .unwrap_or_default();
        for client in clients {
            client.close().await;
        }
    }
}

/// A native [`Tool`] behind the runtime.
struct NativeHandler(Arc<dyn Tool>);

#[async_trait]
impl ToolHandler for NativeHandler {
    async fn call(&self, args: Value, ctx: &ToolCallContext) -> RiverbaseResult<Value> {
        if let Some(input) = &ctx.input {
            return self.0.invoke(args, input).await;
        }
        let fallback = beam_types::AgentInput {
            content: Vec::new(),
            session_id: uuid::Uuid::nil(),
            interaction_id: None,
            execution_id: Some(ctx.execution_id),
            request_id: None,
            run_id: uuid::Uuid::nil(),
            runtime_ref: None,
            agent_name: None,
            model_override: None,
            metadata: Value::Null,
            auth_context: ctx.auth.clone(),
            stream: false,
            actions: None,
            context: None,
            client: None,
        };
        self.0.invoke(args, &fallback).await
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_require_allowlist, McpSettings};

    /// Plan 021: the settings' `Default` reads no environment. The only test
    /// here that sets these keys.
    #[test]
    fn default_settings_read_no_environment() {
        std::env::set_var("BEAM_MCP_REQUIRE_ALLOWLIST", "off");
        std::env::set_var("MCP_TOKEN_HEADER", "X-Other");
        let (default, from_env) = (McpSettings::default(), McpSettings::from_env());
        std::env::remove_var("BEAM_MCP_REQUIRE_ALLOWLIST");
        std::env::remove_var("MCP_TOKEN_HEADER");
        assert!(default.require_allowlist);
        assert_eq!(default.token_header, "X-MCP-Token");
        assert!(!from_env.require_allowlist);
        assert_eq!(from_env.token_header, "X-Other");
    }

    /// Plan 019: a blank value keeps the allowlist on. Before, `""` turned it
    /// off.
    #[test]
    fn only_an_explicit_off_turns_the_allowlist_off() {
        for on in [
            None,
            Some(""),
            Some("  "),
            Some("1"),
            Some("true"),
            Some("yes"),
            Some("typo"),
        ] {
            assert!(parse_require_allowlist(on), "{on:?}");
        }
        for off in [Some("0"), Some("false"), Some("No"), Some(" OFF ")] {
            assert!(!parse_require_allowlist(off), "{off:?}");
        }
    }
}
