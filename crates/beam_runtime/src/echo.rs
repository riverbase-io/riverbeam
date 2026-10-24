//! The echo agent's runtime (plan 021 D3): replies `Echo: <input text>`, or
//! `(empty)`, with `len / 4` prompt tokens plus 4 completion tokens. It calls
//! no model; tests and local runs bind it with `register_echo_agent`.

use async_trait::async_trait;
use beam_types::TokenUsage;
use riverbase_core::RiverbaseResult;
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::contract::{
    AgentRuntime, EventStream, RuntimeCapabilities, RuntimeContext, RuntimeExecutionRef,
    RuntimeResumeRequest, RuntimeStartRequest, RuntimeState,
};
use crate::events::{agent_result, ai_message, done_event, drive, RuntimeEventType};

pub const ECHO_RUNTIME_NAME: &str = "echo";

/// Echoes the input text back. `streaming` is its only capability.
#[derive(Debug, Clone, Copy, Default)]
pub struct EchoRuntime;

impl EchoRuntime {
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// The reply and usage for `input`.
    fn reply(input: &beam_types::AgentInput) -> (String, TokenUsage) {
        let text = input
            .content
            .iter()
            .filter_map(|p| p.text.as_deref())
            .collect::<Vec<_>>()
            .join(" ");
        let prompt_tokens = u32::try_from(text.len() / 4).unwrap_or(u32::MAX);
        let usage = TokenUsage {
            prompt_tokens,
            completion_tokens: 4,
            total_tokens: prompt_tokens.saturating_add(4),
            ..TokenUsage::default()
        };
        let reply = if text.is_empty() {
            "(empty)".to_owned()
        } else {
            format!("Echo: {text}")
        };
        (reply, usage)
    }
}

#[async_trait]
impl AgentRuntime for EchoRuntime {
    fn runtime_name(&self) -> &'static str {
        ECHO_RUNTIME_NAME
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            streaming: true,
            ..RuntimeCapabilities::default()
        }
    }

    fn adapter_version(&self) -> &'static str {
        "1.0.0"
    }

    fn library_version(&self) -> &'static str {
        ECHO_RUNTIME_NAME
    }

    fn allocate_ref(&self, execution_id: Uuid) -> RuntimeExecutionRef {
        let mut reference = Map::new();
        reference.insert("id".into(), Value::String(execution_id.to_string()));
        RuntimeExecutionRef {
            runtime: ECHO_RUNTIME_NAME.into(),
            runtime_version: None,
            reference,
        }
    }

    fn start(&self, request: RuntimeStartRequest, _context: RuntimeContext) -> EventStream {
        drive(
            request.execution_id,
            request.run_id,
            move |mut emitter| async move {
                let input = &request.input;
                let (text, usage) = Self::reply(input);
                let mut delta = emitter.event(RuntimeEventType::MessageDelta);
                delta.message = Some(ai_message(input, &text, true));
                emitter.emit(delta);
                let mut completed = emitter.event(RuntimeEventType::MessageCompleted);
                completed.message = Some(ai_message(input, &text, false));
                emitter.emit(completed);
                let result = agent_result(input, request.execution_id, &text, usage);
                let done = done_event(&emitter, &result);
                emitter.emit(done);
                Ok(())
            },
        )
    }

    fn resume(&self, request: RuntimeResumeRequest, _context: RuntimeContext) -> EventStream {
        drive(request.execution_id, request.run_id, |_| async {
            Err(crate::BEM_391.with_data("Runtime 'echo' does not support resume"))
        })
    }

    async fn cancel(
        &self,
        _runtime_ref: &RuntimeExecutionRef,
        _context: &RuntimeContext,
    ) -> RiverbaseResult<()> {
        Err(crate::BEM_391.with_data("EchoRuntime does not support cancel"))
    }

    async fn inspect(
        &self,
        _runtime_ref: &RuntimeExecutionRef,
        _context: &RuntimeContext,
    ) -> RiverbaseResult<Option<RuntimeState>> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use beam_types::AgentInput;
    use futures_util::StreamExt;

    use super::*;

    #[tokio::test]
    async fn echo_replies_with_the_input_and_its_usage() {
        let input = AgentInput {
            agent_name: Some("echo".into()),
            ..AgentInput::text("hello world!")
        };
        let request = RuntimeStartRequest {
            execution_id: Uuid::new_v4(),
            run_id: input.run_id,
            input,
            runtime_ref: None,
        };
        let events: Vec<_> = EchoRuntime::new()
            .start(request, RuntimeContext::default())
            .map(Result::unwrap)
            .collect()
            .await;
        let result = crate::terminal_result(&events).expect("a result");
        assert_eq!(
            result.content[0].text.as_deref(),
            Some("Echo: hello world!")
        );
        let usage = result.token_usage.expect("usage");
        assert_eq!((usage.prompt_tokens, usage.completion_tokens), (3, 4));
        assert_eq!(usage.total_tokens, 7);
    }
}
