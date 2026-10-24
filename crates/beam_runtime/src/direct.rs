//! One model call, no state (Python `runtime/direct.py`).

use async_trait::async_trait;
use beam_types::TokenUsage;
use futures_util::StreamExt;
use rig_agent::run::{prepare_request, RunSpec};
use rig_core::completion::ProviderCapabilities;
use rig_core::message::AssistantContent;
use riverbase_core::RiverbaseResult;
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::content::user_message;
use crate::contract::{
    AgentRuntime, EventStream, RuntimeCapabilities, RuntimeContext, RuntimeExecutionRef,
    RuntimeResumeRequest, RuntimeStartRequest, RuntimeState,
};
use crate::events::{agent_result, ai_message, done_event, drive, token_usage, RuntimeEventType};
use crate::seams::{ModelCall, ModelStreamItem};

pub const DIRECT_ADAPTER_VERSION: &str = "1.0.0";
pub const DIRECT_LIBRARY_VERSION: &str = "direct";

#[derive(Debug, Clone)]
enum Reply {
    /// Python's behavior: `echo: <input text>`, or a fixed reply.
    Echo(Option<String>),
    /// One streamed call through the model gateway.
    Model {
        preamble: Option<String>,
        model: Option<String>,
    },
}

/// Streams one answer. `streaming` is its only capability.
#[derive(Debug, Clone)]
pub struct DirectModelRuntime {
    reply: Reply,
}

impl DirectModelRuntime {
    /// Replies `echo: <input text>`.
    #[must_use]
    pub fn echo() -> Self {
        Self {
            reply: Reply::Echo(None),
        }
    }

    /// Python's `DirectModelRuntime(reply)`.
    #[must_use]
    pub fn with_reply(reply: impl Into<String>) -> Self {
        Self {
            reply: Reply::Echo(Some(reply.into())),
        }
    }

    /// One call through the context's model gateway.
    #[must_use]
    pub fn model(preamble: Option<String>, model: Option<String>) -> Self {
        Self {
            reply: Reply::Model { preamble, model },
        }
    }
}

fn input_text(request: &RuntimeStartRequest) -> String {
    request
        .input
        .content
        .iter()
        .filter_map(|p| p.text.as_deref())
        .collect::<Vec<_>>()
        .join(" ")
}

#[async_trait]
impl AgentRuntime for DirectModelRuntime {
    fn runtime_name(&self) -> &'static str {
        "direct"
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            streaming: true,
            ..RuntimeCapabilities::default()
        }
    }

    fn adapter_version(&self) -> &str {
        DIRECT_ADAPTER_VERSION
    }

    fn library_version(&self) -> &str {
        DIRECT_LIBRARY_VERSION
    }

    fn allocate_ref(&self, execution_id: Uuid) -> RuntimeExecutionRef {
        let mut reference = Map::new();
        reference.insert("id".into(), Value::String(execution_id.to_string()));
        RuntimeExecutionRef {
            runtime: "direct".into(),
            runtime_version: None,
            reference,
        }
    }

    fn start(&self, request: RuntimeStartRequest, context: RuntimeContext) -> EventStream {
        let reply = self.reply.clone();
        drive(
            request.execution_id,
            request.run_id,
            move |mut emitter| async move {
                let input = &request.input;
                let (text, usage, model_used, streamed) = match reply {
                    Reply::Echo(fixed) => (
                        fixed.unwrap_or_else(|| format!("echo: {}", input_text(&request))),
                        TokenUsage::default(),
                        None,
                        false,
                    ),
                    Reply::Model { preamble, model } => {
                        let spec = RunSpec {
                            preamble,
                            ..RunSpec::new()
                        };
                        let prepared = prepare_request(
                            &spec,
                            &ProviderCapabilities::default(),
                            &[],
                            Vec::new(),
                            None,
                            None,
                        )
                        .map_err(|e| crate::BEM_394.with_data(e.to_string()))?;
                        let mut stream = context.model_gateway.stream(ModelCall {
                            execution_id: request.execution_id,
                            run_id: request.run_id,
                            turn: 1,
                            model: input.model_override.clone().or(model),
                            prompt: user_message(&input.content, " "),
                            prepared,
                        });
                        let mut text = String::new();
                        let mut reply = None;
                        while let Some(item) = stream.next().await {
                            match item? {
                                ModelStreamItem::TextDelta(delta) => {
                                    let mut event = emitter.event(RuntimeEventType::MessageDelta);
                                    event.message = Some(ai_message(input, &delta, true));
                                    emitter.emit(event);
                                    text.push_str(&delta);
                                }
                                ModelStreamItem::Completed(done) => reply = Some(done),
                            }
                        }
                        let reply = reply.ok_or_else(|| {
                            crate::BEM_390.with_data("model stream ended without a reply")
                        })?;
                        // A gateway that does not stream sends the whole answer in the reply.
                        let streamed = !text.is_empty();
                        if !streamed {
                            text = reply
                                .choice
                                .iter()
                                .filter_map(|c| match c {
                                    AssistantContent::Text(t) => Some(t.text.as_str()),
                                    _ => None,
                                })
                                .collect();
                        }
                        (text, token_usage(&reply.usage), reply.model_used, streamed)
                    }
                };
                if !streamed {
                    let mut delta = emitter.event(RuntimeEventType::MessageDelta);
                    delta.message = Some(ai_message(input, &text, true));
                    emitter.emit(delta);
                }
                let mut completed = emitter.event(RuntimeEventType::MessageCompleted);
                completed.message = Some(ai_message(input, &text, false));
                emitter.emit(completed);
                let mut result = agent_result(input, request.execution_id, &text, usage);
                result.model_used = model_used;
                let done = done_event(&emitter, &result);
                emitter.emit(done);
                Ok(())
            },
        )
    }

    fn resume(&self, request: RuntimeResumeRequest, _context: RuntimeContext) -> EventStream {
        drive(request.execution_id, request.run_id, |_| async {
            Err(crate::BEM_391.with_data("Runtime 'direct' does not support resume"))
        })
    }

    async fn cancel(
        &self,
        _runtime_ref: &RuntimeExecutionRef,
        _context: &RuntimeContext,
    ) -> RiverbaseResult<()> {
        Err(crate::BEM_391.with_data("DirectModelRuntime does not support cancel"))
    }

    async fn inspect(
        &self,
        _runtime_ref: &RuntimeExecutionRef,
        _context: &RuntimeContext,
    ) -> RiverbaseResult<Option<RuntimeState>> {
        Ok(None)
    }
}
