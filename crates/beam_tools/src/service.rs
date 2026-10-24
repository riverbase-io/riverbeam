//! The runtime-facing tool service: `beam_runtime::ToolExecutor` over the
//! registry and the runtime. Tool definitions become rig `ToolDefinition`s
//! here and nowhere else.

use std::sync::Arc;

use async_trait::async_trait;
use beam_runtime::{ClientAnswer, ToolExecutor, ToolInfo, ToolInvocation};
use beam_types::pyjson;
use beam_types::tool_policy::{RememberOption, ToolDecision, ToolScope};
use rig_core::completion::ToolDefinition as RigToolDefinition;
use rig_core::message::ToolName;
use riverbase_core::RiverbaseResult;
use serde_json::{json, Value};

use crate::definition::ToolCallContext;
use crate::registry::ToolRegistry;
use crate::runtime::{ExecuteContext, ToolRuntime, ToolRuntimeResult};

pub struct ToolService {
    registry: Arc<ToolRegistry>,
    runtime: Arc<ToolRuntime>,
}

impl ToolService {
    #[must_use]
    pub fn new(registry: Arc<ToolRegistry>, runtime: Arc<ToolRuntime>) -> Self {
        Self { registry, runtime }
    }

    #[must_use]
    pub fn registry(&self) -> &Arc<ToolRegistry> {
        &self.registry
    }

    #[must_use]
    pub fn runtime(&self) -> &Arc<ToolRuntime> {
        &self.runtime
    }
}

/// What the model sees for a value: strings as they are, anything else as
/// Python's `json.dumps` (`LangChain`'s tool-message content).
fn content_of(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        other => pyjson::dumps_unicode(other),
    }
}

#[async_trait]
impl ToolExecutor for ToolService {
    fn definitions(&self, names: &[String]) -> Vec<RigToolDefinition> {
        names
            .iter()
            .filter_map(|name| self.registry.get(name))
            .filter_map(|tool| {
                if ToolName::new(tool.name.clone()).is_err() {
                    tracing::warn!(tool = %tool.name, "tool name is not valid for models; not advertised");
                }
                let schema = if tool.input_schema.as_object().is_some_and(|o| !o.is_empty()) {
                    tool.input_schema.clone()
                } else {
                    json!({"type": "object", "properties": {}})
                };
                Some(RigToolDefinition::new(
                    ToolName::new(tool.name.clone()).ok()?,
                    tool.description.clone(),
                    schema,
                ))
            })
            .collect()
    }

    fn requires_approval(&self, name: &str) -> bool {
        self.registry.get(name).is_some_and(|t| t.needs_approval())
    }

    /// The layers for a registered tool (plan 023 C3); an unknown tool is
    /// `auto` here and refused by [`Self::execute`].
    async fn decide(&self, name: &str, scope: &ToolScope) -> RiverbaseResult<ToolDecision> {
        match self.registry.get(name) {
            Some(definition) => self.runtime.decide(&definition, scope).await,
            None => Ok(ToolDecision::default()),
        }
    }

    async fn remember(
        &self,
        name: &str,
        scope: &ToolScope,
        decision: &ToolDecision,
        choice: RememberOption,
    ) -> RiverbaseResult<()> {
        match self.registry.get(name) {
            Some(definition) => {
                self.runtime
                    .remember(&definition, scope, decision, choice)
                    .await
            }
            None => Ok(()),
        }
    }

    async fn execute(&self, call: ToolInvocation) -> RiverbaseResult<String> {
        let definition = self
            .registry
            .get(&call.name)
            .ok_or_else(|| crate::BEM_470.with_data(call.name.clone()))?;
        let args = call.args.clone();
        let result = self
            .runtime
            .execute(&definition, args, &execute_context(call))
            .await?;
        Ok(model_text(result))
    }

    fn tool_info(&self, name: &str) -> Option<ToolInfo> {
        self.registry.get(name).map(|t| ToolInfo {
            description: t.description.clone(),
            risk: serde_json::to_value(t.risk)
                .ok()
                .and_then(|v| v.as_str().map(str::to_owned))
                .unwrap_or_else(|| "medium".into()),
            timeout_seconds: t.timeout_seconds,
            client: t.is_client(),
        })
    }

    async fn client_result(
        &self,
        call: ToolInvocation,
        answer: ClientAnswer,
    ) -> RiverbaseResult<String> {
        let definition = self
            .registry
            .get(&call.name)
            .ok_or_else(|| crate::BEM_470.with_data(call.name.clone()))?;
        let args = call.args.clone();
        let result = self
            .runtime
            .client_result(&definition, args, answer, &execute_context(call))
            .await?;
        Ok(model_text(result))
    }
}

fn execute_context(call: ToolInvocation) -> ExecuteContext {
    let auth = call.input.as_ref().and_then(|i| i.auth_context.clone());
    ExecuteContext {
        call: ToolCallContext {
            execution_id: call.execution_id,
            call_id: call.call_id,
            auth,
            input: call.input,
        },
        tool_revision: None,
        allowed: call.allowed,
        dry_run: call.dry_run,
        approved: call.approved,
    }
}

/// Python's `LangChain` tool returns `{"error": ...}` to the model.
fn model_text(result: ToolRuntimeResult) -> String {
    match result.error {
        Some(error) => content_of(&json!({"error": error})),
        None => content_of(&result.output),
    }
}
