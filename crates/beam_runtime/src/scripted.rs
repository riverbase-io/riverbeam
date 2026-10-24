//! Scripted [`ModelCaller`] and [`ToolExecutor`] for tests: a model that
//! answers from a script, and tools backed by closures. Only with `cfg(test)`
//! or the `testing` feature.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures_util::stream::{self, StreamExt};
use rig_core::completion::{ToolDefinition, Usage};
use rig_core::message::{AssistantContent, ToolCall, ToolFunction, ToolName};
use riverbase_core::RiverbaseResult;
use serde_json::{json, Value};

use crate::seams::{
    ClientAnswer, ModelCall, ModelCaller, ModelReply, ModelStream, ModelStreamItem, ToolExecutor,
    ToolInfo, ToolInvocation,
};

/// One scripted model turn.
#[derive(Debug, Clone)]
pub enum ScriptedTurn {
    Text(String),
    /// `(call id, tool name, arguments)`.
    ToolCalls(Vec<(String, String, Value)>),
    Fail(String),
}

impl ScriptedTurn {
    #[must_use]
    pub fn text(text: impl Into<String>) -> Self {
        Self::Text(text.into())
    }

    #[must_use]
    pub fn tool_call(id: &str, name: &str, args: Value) -> Self {
        Self::ToolCalls(vec![(id.to_owned(), name.to_owned(), args)])
    }
}

/// A model that answers from a script and counts its calls. Clones share the
/// script and the count, so a test can hand one clone to each "worker".
#[derive(Clone, Default)]
pub struct ScriptedModel {
    script: Arc<Mutex<VecDeque<ScriptedTurn>>>,
    calls: Arc<AtomicUsize>,
    seen: Arc<Mutex<Vec<ModelCall>>>,
}

impl ScriptedModel {
    #[must_use]
    pub fn new(turns: impl IntoIterator<Item = ScriptedTurn>) -> Self {
        Self {
            script: Arc::new(Mutex::new(turns.into_iter().collect())),
            ..Self::default()
        }
    }

    /// Model calls made so far.
    #[must_use]
    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    /// The calls received, in order.
    ///
    /// # Panics
    /// When the lock is poisoned.
    #[must_use]
    pub fn seen(&self) -> Vec<ModelCall> {
        self.seen.lock().expect("scripted model lock").clone()
    }
}

impl ModelCaller for ScriptedModel {
    fn stream(&self, call: ModelCall) -> ModelStream {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Ok(mut seen) = self.seen.lock() {
            seen.push(call);
        }
        let turn = self.script.lock().ok().and_then(|mut s| s.pop_front());
        let usage = Usage::new()
            .input_tokens(10)
            .output_tokens(5)
            .total_tokens(15);
        let reply = |choice| {
            Ok(ModelStreamItem::Completed(ModelReply {
                choice,
                usage,
                finish_reason: None,
                raw: json!({"origin": "scripted"}),
                model_used: Some("scripted".into()),
            }))
        };
        let items: Vec<RiverbaseResult<ModelStreamItem>> = match turn {
            None => vec![Err(
                crate::BEM_394.with_data("the model script is exhausted")
            )],
            Some(ScriptedTurn::Fail(reason)) => vec![Err(crate::BEM_394.with_data(reason))],
            Some(ScriptedTurn::Text(text)) => vec![
                Ok(ModelStreamItem::TextDelta(text.clone())),
                reply(vec![AssistantContent::text(text)]),
            ],
            Some(ScriptedTurn::ToolCalls(calls)) => {
                let choice = calls
                    .into_iter()
                    .map(|(id, name, args)| {
                        let args = match args {
                            Value::Object(map) => map,
                            _ => serde_json::Map::new(),
                        };
                        let name = ToolName::new(name).unwrap_or_else(|_| {
                            ToolName::new("unnamed").expect("non-empty tool name")
                        });
                        AssistantContent::ToolCall(ToolCall::from_wire(
                            id,
                            ToolFunction::new(name, Value::Object(args)),
                        ))
                    })
                    .collect();
                vec![reply(choice)]
            }
        };
        stream::iter(items).boxed()
    }
}

type ToolFn = Arc<dyn Fn(&Value) -> RiverbaseResult<String> + Send + Sync>;

struct ScriptedTool {
    definition: ToolDefinition,
    run: ToolFn,
    approval: bool,
    client: bool,
}

/// Tools backed by closures, counting executions per tool.
#[derive(Clone, Default)]
pub struct ScriptedTools {
    tools: Arc<Mutex<HashMap<String, Arc<ScriptedTool>>>>,
    executed: Arc<Mutex<Vec<String>>>,
}

impl ScriptedTools {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a tool; `approval` makes every call wait for a human.
    ///
    /// # Panics
    /// When the lock is poisoned.
    #[must_use]
    pub fn with(
        self,
        name: &str,
        approval: bool,
        run: impl Fn(&Value) -> RiverbaseResult<String> + Send + Sync + 'static,
    ) -> Self {
        let definition = ToolDefinition::new(
            ToolName::new(name).expect("non-empty tool name"),
            format!("scripted tool {name}"),
            json!({"type": "object", "properties": {}}),
        );
        self.tools.lock().expect("scripted tools lock").insert(
            name.to_owned(),
            Arc::new(ScriptedTool {
                definition,
                run: Arc::new(run),
                approval,
                client: false,
            }),
        );
        self
    }

    /// Add a tool the client runs; `approval` also asks the human to confirm.
    ///
    /// # Panics
    /// When the lock is poisoned.
    #[must_use]
    pub fn with_client(self, name: &str, approval: bool) -> Self {
        let definition = ToolDefinition::new(
            ToolName::new(name).expect("non-empty tool name"),
            format!("client tool {name}"),
            json!({"type": "object", "properties": {}}),
        );
        self.tools.lock().expect("scripted tools lock").insert(
            name.to_owned(),
            Arc::new(ScriptedTool {
                definition,
                run: Arc::new(|_| Err(crate::BEM_394.with_data("client tools run on the client"))),
                approval,
                client: true,
            }),
        );
        self
    }

    /// Names of executed tools, in order.
    ///
    /// # Panics
    /// When the lock is poisoned.
    #[must_use]
    pub fn executed(&self) -> Vec<String> {
        self.executed.lock().expect("scripted tools lock").clone()
    }

    fn get(&self, name: &str) -> Option<Arc<ScriptedTool>> {
        self.tools.lock().ok().and_then(|t| t.get(name).cloned())
    }
}

#[async_trait]
impl ToolExecutor for ScriptedTools {
    fn definitions(&self, names: &[String]) -> Vec<ToolDefinition> {
        names
            .iter()
            .filter_map(|n| self.get(n).map(|t| t.definition.clone()))
            .collect()
    }

    fn requires_approval(&self, name: &str) -> bool {
        self.get(name).is_some_and(|t| t.approval)
    }

    async fn execute(&self, call: ToolInvocation) -> RiverbaseResult<String> {
        let tool = self
            .get(&call.name)
            .ok_or_else(|| crate::BEM_394.with_data(format!("unknown tool {}", call.name)))?;
        if let Ok(mut executed) = self.executed.lock() {
            executed.push(call.name.clone());
        }
        (tool.run)(&call.args)
    }

    fn tool_info(&self, name: &str) -> Option<ToolInfo> {
        self.get(name).map(|t| ToolInfo {
            description: t.definition.description.clone(),
            risk: if t.client { "low" } else { "medium" }.into(),
            timeout_seconds: 600.0,
            client: t.client,
        })
    }

    async fn client_result(
        &self,
        call: ToolInvocation,
        answer: ClientAnswer,
    ) -> RiverbaseResult<String> {
        if !self.tool_info(&call.name).is_some_and(|t| t.client) {
            return Err(crate::BEM_394.with_data(format!("{} is not a client tool", call.name)));
        }
        Ok(match (answer.error, answer.output) {
            (Some(error), _) => json!({"error": error}).to_string(),
            (None, Value::String(text)) => text,
            (None, output) => output.to_string(),
        })
    }
}
