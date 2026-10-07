use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use beam_types::AgentInput;

use riverbase_core::RiverbaseResult;
use crate::tool::ToolSpec;

#[derive(Debug, Clone)]
pub enum Completion {
    Text(String),
    ToolCalls(Vec<PendingToolCall>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingToolCall {
    pub id: String,
    pub name: String,
    pub args: Value,
}

#[async_trait]
pub trait CompletionClient: Send + Sync {
    async fn complete(
        &self,
        messages: &[Value],
        tools: &[ToolSpec],
    ) -> RiverbaseResult<Completion>;

    /// Same as [`complete`], with invoke metadata available for host-side BYO routing.
    async fn complete_for(
        &self,
        _input: &AgentInput,
        messages: &[Value],
        tools: &[ToolSpec],
    ) -> RiverbaseResult<Completion> {
        self.complete(messages, tools).await
    }
}

/// Echoes the last user text. Used when no API key is configured.
pub struct EchoClient;

#[async_trait]
impl CompletionClient for EchoClient {
    async fn complete(
        &self,
        messages: &[Value],
        _tools: &[ToolSpec],
    ) -> RiverbaseResult<Completion> {
        let text = messages
            .iter()
            .rev()
            .find_map(|m| {
                if m.get("role").and_then(Value::as_str) == Some("user") {
                    m.get("content").and_then(Value::as_str).map(str::to_string)
                } else {
                    None
                }
            })
            .unwrap_or_default();
        Ok(Completion::Text(if text.is_empty() {
            "(empty)".into()
        } else {
            format!("Echo: {text}")
        }))
    }
}

/// Pops scripted steps (tests and HITL).
pub struct ScriptedClient {
    steps: Mutex<Vec<Completion>>,
}

impl ScriptedClient {
    #[must_use]
    pub fn new(steps: Vec<Completion>) -> Self {
        Self {
            steps: Mutex::new(steps),
        }
    }

    fn next(&self) -> RiverbaseResult<Completion> {
        let mut guard = self
            .steps
            .lock()
            .map_err(|e| crate::BEM_080.with_data(e.to_string()))?;
        if guard.is_empty() {
            return Err(crate::BEM_081.raise());
        }
        Ok(guard.remove(0))
    }
}

#[async_trait]
impl CompletionClient for ScriptedClient {
    async fn complete(
        &self,
        _messages: &[Value],
        _tools: &[ToolSpec],
    ) -> RiverbaseResult<Completion> {
        self.next()
    }
}

/// OpenAI-compatible HTTP (NVIDIA integrate.api, etc.). Key from env, never logged.
pub struct OpenAiCompatible {
    client: reqwest::Client,
    base_url: String,
    api_key: String,
    pub model: String,
    extra_body: Value,
    max_tokens: u32,
}

impl OpenAiCompatible {
    #[must_use]
    pub fn new(base_url: impl Into<String>, api_key: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(60))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
            base_url: base_url.into().trim_end_matches('/').to_string(),
            api_key: api_key.into(),
            model: model.into(),
            extra_body: json!({ "chat_template_kwargs": { "thinking": false } }),
            max_tokens: 4096,
        }
    }

    #[must_use]
    pub fn with_extra_body(mut self, extra: Value) -> Self {
        self.extra_body = extra;
        self
    }

    #[must_use]
    pub fn with_max_tokens(mut self, max_tokens: u32) -> Self {
        self.max_tokens = max_tokens.max(16);
        self
    }

    /// `BEAM_API_KEY` / `GFS_BEAM_API_KEY` / `NVIDIA_API_KEY`.
    #[must_use]
    pub fn from_env() -> Option<Self> {
        let api_key = std::env::var("BEAM_API_KEY")
            .or_else(|_| std::env::var("GFS_BEAM_API_KEY"))
            .or_else(|_| std::env::var("NVIDIA_API_KEY"))
            .ok()
            .filter(|s| !s.is_empty())?;
        let base_url = std::env::var("BEAM_API_BASE_URL")
            .or_else(|_| std::env::var("GFS_BEAM_BASE_URL"))
            .unwrap_or_else(|_| "https://integrate.api.nvidia.com/v1".into());
        let model = std::env::var("BEAM_MODEL")
            .or_else(|_| std::env::var("GFS_BEAM_MODEL"))
            .unwrap_or_else(|_| "deepseek-ai/deepseek-v4-pro-0813".into());
        Some(Self::new(base_url, api_key, model))
    }
}

#[async_trait]
impl CompletionClient for OpenAiCompatible {
    async fn complete(
        &self,
        messages: &[Value],
        tools: &[ToolSpec],
    ) -> RiverbaseResult<Completion> {
        let url = format!("{}/chat/completions", self.base_url);
        let mut body = json!({
            "model": self.model,
            "messages": messages,
            "temperature": 1.0,
            "top_p": 0.95,
            "max_tokens": self.max_tokens,
            "stream": false,
        });
        if let Some(obj) = body.as_object_mut() {
            if let Some(extra) = self.extra_body.as_object() {
                for (k, v) in extra {
                    obj.insert(k.clone(), v.clone());
                }
            }
            if !tools.is_empty() {
                let t: Vec<Value> = tools
                    .iter()
                    .map(|spec| {
                        json!({
                            "type": "function",
                            "function": {
                                "name": spec.name,
                                "description": spec.description,
                                "parameters": spec.parameters,
                            }
                        })
                    })
                    .collect();
                obj.insert("tools".into(), Value::Array(t));
            }
        }
        let resp = self
            .client
            .post(url)
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|e| crate::BEM_082.with_data(e.to_string()))?;
        let status = resp.status();
        let value: Value = resp
            .json()
            .await
            .map_err(|e| crate::BEM_083.with_data(e.to_string()))?;
        if !status.is_success() {
            return Err(crate::BEM_084.with_data(format!(
                "llm http {status}: {}",
                value.get("error").cloned().unwrap_or(value)
            )));
        }
        parse_openai_completion(&value)
    }
}

fn parse_openai_completion(value: &Value) -> RiverbaseResult<Completion> {
    let choice = value
        .pointer("/choices/0/message")
        .ok_or_else(|| crate::BEM_085.raise())?;
    if let Some(calls) = choice.get("tool_calls").and_then(Value::as_array) {
        if !calls.is_empty() {
            let mut out = Vec::new();
            for c in calls {
                let id = c
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let name = c
                    .pointer("/function/name")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let args_raw = c
                    .pointer("/function/arguments")
                    .and_then(Value::as_str)
                    .unwrap_or("{}");
                let args: Value = serde_json::from_str(args_raw).unwrap_or(json!({}));
                out.push(PendingToolCall { id, name, args });
            }
            return Ok(Completion::ToolCalls(out));
        }
    }
    Ok(Completion::Text(message_text(choice)))
}

fn message_text(choice: &Value) -> String {
    if let Some(s) = choice.get("content").and_then(Value::as_str) {
        if !s.trim().is_empty() {
            return s.to_string();
        }
    }
    if let Some(parts) = choice.get("content").and_then(Value::as_array) {
        let joined: String = parts
            .iter()
            .filter_map(|part| {
                part.as_str()
                    .or_else(|| part.get("text").and_then(Value::as_str))
            })
            .collect::<Vec<_>>()
            .join("");
        if !joined.trim().is_empty() {
            return joined;
        }
    }
    for key in ["reasoning_content", "reasoning"] {
        if let Some(s) = choice.get(key).and_then(Value::as_str) {
            if !s.trim().is_empty() {
                return s.to_string();
            }
        }
    }
    String::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_reads_string_content() {
        let value = json!({
            "choices": [{ "message": { "content": "Fix ACL wording" } }]
        });
        match parse_openai_completion(&value).unwrap() {
            Completion::Text(t) => assert_eq!(t, "Fix ACL wording"),
            Completion::ToolCalls(_) => panic!("expected text"),
        }
    }

    #[test]
    fn parse_reads_content_array_and_reasoning() {
        let array = json!({
            "choices": [{ "message": { "content": [{ "type": "text", "text": "Add checklist" }] } }]
        });
        match parse_openai_completion(&array).unwrap() {
            Completion::Text(t) => assert_eq!(t, "Add checklist"),
            Completion::ToolCalls(_) => panic!("expected text"),
        }
        let reasoning = json!({
            "choices": [{ "message": { "content": null, "reasoning_content": "Clarify fail-closed" } }]
        });
        match parse_openai_completion(&reasoning).unwrap() {
            Completion::Text(t) => assert_eq!(t, "Clarify fail-closed"),
            Completion::ToolCalls(_) => panic!("expected text"),
        }
    }

    #[tokio::test]
    async fn nvidia_limerick_when_key_present() {
        let Some(client) = OpenAiCompatible::from_env() else {
            eprintln!("skip: set BEAM_API_KEY (or GFS_BEAM_API_KEY / NVIDIA_API_KEY)");
            return;
        };
        let completion = client
            .complete(
                &[json!({"role":"user","content":"Write a one-line limerick about GPU computing."})],
                &[],
            )
            .await
            .expect("nvidia complete");
        match completion {
            Completion::Text(t) => assert!(!t.trim().is_empty(), "empty completion"),
            Completion::ToolCalls(_) => panic!("unexpected tool call"),
        }
    }

    #[tokio::test]
    async fn nvidia_nemotron_when_key_present() {
        let Some(api_key) = std::env::var("NVIDIA_NEMOTRON_API_KEY")
            .ok()
            .filter(|s| !s.is_empty())
        else {
            eprintln!("skip: set NVIDIA_NEMOTRON_API_KEY");
            return;
        };
        let client = OpenAiCompatible::new(
            "https://integrate.api.nvidia.com/v1",
            api_key,
            "nvidia/nemotron-3-ultra-550b-a55b",
        )
        .with_extra_body(json!({ "chat_template_kwargs": { "enable_thinking": true } }));
        let completion = client
            .complete(
                &[json!({"role":"user","content":"Write a one-line limerick about GPU computing."})],
                &[],
            )
            .await
            .expect("nemotron complete");
        match completion {
            Completion::Text(t) => assert!(!t.trim().is_empty(), "empty completion"),
            Completion::ToolCalls(_) => panic!("unexpected tool call"),
        }
    }
}
