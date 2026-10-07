use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use beam_types::{
    AgentContext, AgentInput, AgentMetadata, AgentResult, ContentPart, Modality, StreamChunk,
    StreamChunkType, TokenUsage, ToolCallRecord,
};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::completion::{Completion, CompletionClient, PendingToolCall};
use riverbase_core::RiverbaseResult;
use crate::registry::Agent;
use crate::skills::{skill_by_name, Skill};
use crate::tool::{Tool, ToolSpec};

/// Tool-loop agent with a pluggable completion client (echo / scripted / OpenAI-compatible).
pub struct RigAgentRunner {
    metadata: AgentMetadata,
    client: Arc<dyn CompletionClient>,
    tools: Vec<Arc<dyn Tool>>,
    skill: Skill,
}

impl RigAgentRunner {
    pub fn new(
        name: impl Into<String>,
        client: Arc<dyn CompletionClient>,
        tools: Vec<Arc<dyn Tool>>,
        skill: Skill,
    ) -> Self {
        let name = name.into();
        Self {
            metadata: AgentMetadata {
                name: name.clone(),
                description: skill.system_prompt.clone(),
                supported_modalities: vec![Modality::Text],
                output_modalities: vec![Modality::Text],
                tags: vec!["rig".into()],
                version: "1.0.0".into(),
            },
            client,
            tools,
            skill,
        }
    }

    pub fn for_skill(
        skill_name: &str,
        client: Arc<dyn CompletionClient>,
        tools: Vec<Arc<dyn Tool>>,
    ) -> Self {
        let skill = skill_by_name(skill_name).unwrap_or_else(|| Skill {
            name: skill_name.into(),
            system_prompt: String::new(),
            tools: tools.iter().map(|t| t.spec().name).collect(),
        });
        let allow: HashSet<String> = skill.tools.iter().cloned().collect();
        let filtered: Vec<Arc<dyn Tool>> = if allow.is_empty() {
            Vec::new()
        } else {
            tools
                .into_iter()
                .filter(|t| allow.contains(&t.spec().name))
                .collect()
        };
        Self::new(skill.name.clone(), client, filtered, skill)
    }

    fn specs(&self) -> Vec<ToolSpec> {
        self.tools.iter().map(|t| t.spec()).collect()
    }

    fn tool(&self, name: &str) -> Option<Arc<dyn Tool>> {
        self.tools.iter().find(|t| t.spec().name == name).cloned()
    }
}

#[async_trait]
impl Agent for RigAgentRunner {
    fn metadata(&self) -> AgentMetadata {
        self.metadata.clone()
    }

    async fn stream(
        &self,
        input: &AgentInput,
        context: &AgentContext,
    ) -> RiverbaseResult<Vec<StreamChunk>> {
        if let Some(actions) = &input.actions {
            return self.resume_actions(input, context, actions).await;
        }

        let mut messages = openai_messages(context, input, &self.skill.system_prompt);
        let mut frames = Vec::new();
        let mut tool_records = Vec::new();
        frames.push(named_frame(
            input,
            &self.metadata.name,
            StreamChunkType::Placeholder,
            "Retrieving allowed pages…",
            None,
            None,
        ));
        frames.push(named_frame(
            input,
            &self.metadata.name,
            StreamChunkType::Thinking,
            "Retrieving allowed pages…",
            None,
            None,
        ));

        for _ in 0..8 {
            let completion = self
                .client
                .complete_for(input, &messages, &self.specs())
                .await?;
            match completion {
                Completion::Text(text) => {
                    frames.push(token_frame(input, &self.metadata.name, &text));
                    frames.push(done_frame(
                        input,
                        &self.metadata.name,
                        &text,
                        tool_records,
                        false,
                        None,
                    ));
                    return Ok(frames);
                }
                Completion::ToolCalls(calls) => {
                    if let Some(interrupt) = self.confirm_interrupt(input, &calls) {
                        frames.push(done_frame(
                            input,
                            &self.metadata.name,
                            "confirmation required",
                            tool_records,
                            true,
                            Some(interrupt),
                        ));
                        return Ok(frames);
                    }
                    for call in calls {
                        frames.push(named_frame(
                            input,
                            &self.metadata.name,
                            StreamChunkType::Tool,
                            &format!("{}…", call.name),
                            Some(call.name.clone()),
                            Some(call.args.clone()),
                        ));
                        let tool = self.tool(&call.name).ok_or_else(|| {
                            crate::BEM_089.with_data(call.name.clone())
                        })?;
                        let result = tool.invoke(call.args.clone(), input).await?;
                        tool_records.push(ToolCallRecord {
                            tool_name: call.name.clone(),
                            args: call.args.clone(),
                            tool_call_id: Some(call.id.clone()),
                            result: Some(result.to_string()),
                            latency_ms: None,
                            usage_metadata: None,
                            step_id: None,
                            result_step_id: None,
                        });
                        messages.push(json!({
                            "role": "assistant",
                            "tool_calls": [{
                                "id": call.id,
                                "type": "function",
                                "function": {
                                    "name": call.name,
                                    "arguments": call.args.to_string(),
                                }
                            }]
                        }));
                        messages.push(json!({
                            "role": "tool",
                            "tool_call_id": call.id,
                            "content": result.to_string(),
                        }));
                    }
                }
            }
        }
        Err(crate::BEM_088.raise())
    }
}

impl RigAgentRunner {
    fn confirm_interrupt(
        &self,
        input: &AgentInput,
        calls: &[PendingToolCall],
    ) -> Option<Vec<Value>> {
        let confirmed = input
            .metadata
            .get("confirm")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if confirmed {
            return None;
        }
        let mut required = Vec::new();
        for call in calls {
            if let Some(tool) = self.tool(&call.name) {
                if tool.spec().confirm_required {
                    required.push(json!({
                        "action_id": Uuid::new_v4().to_string(),
                        "kind": "confirmation",
                        "tool": call.name,
                        "args": call.args,
                    }));
                }
            }
        }
        if required.is_empty() {
            None
        } else {
            Some(required)
        }
    }

    async fn resume_actions(
        &self,
        input: &AgentInput,
        _context: &AgentContext,
        actions: &[beam_types::ActionResponseEnvelope],
    ) -> RiverbaseResult<Vec<StreamChunk>> {
        let mut tool_records = Vec::new();
        let mut texts = Vec::new();
        for action in actions {
            let tool_name = action
                .payload
                .as_ref()
                .and_then(|p| p.get("tool"))
                .and_then(Value::as_str)
                .unwrap_or("");
            let args = action
                .payload
                .as_ref()
                .and_then(|p| p.get("args"))
                .cloned()
                .unwrap_or(json!({}));
            if tool_name.is_empty() {
                continue;
            }
            let tool = self
                .tool(tool_name)
                .ok_or_else(|| crate::BEM_090.with_data(tool_name.to_string()))?;
            let result = tool.invoke(args.clone(), input).await?;
            texts.push(format!("{tool_name}: {result}"));
            tool_records.push(ToolCallRecord {
                tool_name: tool_name.into(),
                args,
                tool_call_id: Some(action.action_id.clone()),
                result: Some(result.to_string()),
                latency_ms: None,
                usage_metadata: None,
                step_id: None,
                result_step_id: None,
            });
        }
        let text = texts.join("\n");
        Ok(vec![done_frame(
            input,
            &self.metadata.name,
            &text,
            tool_records,
            false,
            None,
        )])
    }
}

fn openai_messages(context: &AgentContext, input: &AgentInput, system: &str) -> Vec<Value> {
    let mut out = Vec::new();
    if !system.is_empty() {
        out.push(json!({"role": "system", "content": system}));
    }
    for msg in &context.conversation_history {
        let role = match msg.role {
            beam_types::AgentMessageRole::System => "system",
            beam_types::AgentMessageRole::Human => "user",
            beam_types::AgentMessageRole::Ai => "assistant",
            beam_types::AgentMessageRole::Tool => "tool",
        };
        let content: String = msg
            .content
            .iter()
            .filter_map(|p| p.text.as_deref())
            .collect::<Vec<_>>()
            .join("\n");
        out.push(json!({"role": role, "content": content}));
    }
    let user: String = input
        .content
        .iter()
        .filter_map(|p| p.text.as_deref())
        .collect::<Vec<_>>()
        .join("\n");
    if !user.is_empty() {
        out.push(json!({"role": "user", "content": user}));
    }
    out
}

fn named_frame(
    input: &AgentInput,
    agent: &str,
    chunk_type: StreamChunkType,
    text: &str,
    tool_name: Option<String>,
    tool_args: Option<Value>,
) -> StreamChunk {
    StreamChunk {
        chunk_type,
        data: Some(beam_types::AgentMessage {
            role: beam_types::AgentMessageRole::Ai,
            content: vec![ContentPart::text(text)],
            tool_name,
            tool_args,
            ..Default::default()
        }),
        result: None,
        error: None,
        id: None,
        agent_id: None,
        agent_name: Some(agent.into()),
        session_id: Some(input.session_id.to_string()),
        interaction_id: input.interaction_id.map(|u| u.to_string()),
        run_id: Some(input.run_id.to_string()),
        step_id: None,
        message_id: None,
        timestamp: None,
    }
}

fn token_frame(input: &AgentInput, agent: &str, text: &str) -> StreamChunk {
    named_frame(input, agent, StreamChunkType::Chunk, text, None, None)
}

fn done_frame(
    input: &AgentInput,
    agent: &str,
    text: &str,
    tool_calls: Vec<ToolCallRecord>,
    interrupted: bool,
    action_required: Option<Vec<Value>>,
) -> StreamChunk {
    StreamChunk {
        chunk_type: StreamChunkType::Done,
        data: None,
        result: Some(AgentResult {
            content: vec![ContentPart::text(text)],
            agent_name: agent.into(),
            model_used: None,
            session_id: Some(input.session_id),
            request_id: input.request_id,
            interaction_id: input.interaction_id,
            token_usage: Some(TokenUsage {
                prompt_tokens: 0,
                completion_tokens: 0,
                total_tokens: 0,
            }),
            tool_calls,
            metadata: json!({}),
            latency_ms: Some(0.0),
            interrupted,
            action_required,
        }),
        error: None,
        id: None,
        agent_id: None,
        agent_name: Some(agent.into()),
        session_id: Some(input.session_id.to_string()),
        interaction_id: input.interaction_id.map(|u| u.to_string()),
        run_id: Some(input.run_id.to_string()),
        step_id: None,
        message_id: None,
        timestamp: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::completion::{Completion, ScriptedClient};
    use crate::sandbox_tools::sandbox_tools;
    use beam_sandbox::{SandboxRoot, WorkingTree};
    use beam_types::{ActionResponseEnvelope, ActionResponseStatus, AgentMessageRole};
    use chrono::Utc;
    use tempfile::tempdir;

    fn sample_input(name: &str) -> AgentInput {
        AgentInput {
            content: vec![ContentPart::text("hello")],
            session_id: Uuid::new_v4(),
            interaction_id: Some(Uuid::new_v4()),
            request_id: Some(Uuid::new_v4()),
            run_id: Uuid::new_v4(),
            agent_name: Some(name.into()),
            model_override: None,
            metadata: json!({}),
            auth_context: None,
            stream: false,
            actions: None,
        }
    }

    fn ctx() -> AgentContext {
        AgentContext {
            session_id: "s".into(),
            user_id: None,
            conversation_history: vec![],
            model_config_resolved: None,
            available_tools: vec![],
            metadata: json!({}),
        }
    }

    #[tokio::test]
    async fn scripted_tool_then_text() {
        let dir = tempdir().unwrap();
        let sb = SandboxRoot::new(dir.path()).unwrap();
        let wt = WorkingTree::new(sb, vec!["main".into()]);
        wt.init().unwrap();
        let client = Arc::new(ScriptedClient::new(vec![
            Completion::ToolCalls(vec![PendingToolCall {
                id: "1".into(),
                name: "write_file".into(),
                args: json!({"path": "a.md", "body": "hi"}),
            }]),
            Completion::Text("wrote a.md".into()),
        ]));
        let agent = RigAgentRunner::for_skill("author", client, sandbox_tools(Some(wt.clone())));
        let frames = agent
            .stream(&sample_input("author"), &ctx())
            .await
            .unwrap();
        let done = frames.last().unwrap().result.as_ref().unwrap();
        assert!(!done.interrupted);
        assert_eq!(done.tool_calls.len(), 1);
        assert_eq!(wt.sandbox().read_file("a.md").unwrap(), "hi");
    }

    #[tokio::test]
    async fn git_push_requires_confirm() {
        let dir = tempdir().unwrap();
        let sb = SandboxRoot::new(dir.path()).unwrap();
        let wt = WorkingTree::new(sb, vec!["main".into()]);
        wt.init().unwrap();
        let client = Arc::new(ScriptedClient::new(vec![Completion::ToolCalls(vec![
            PendingToolCall {
                id: "1".into(),
                name: "git_push".into(),
                args: json!({"ref": "change-1"}),
            },
        ])]));
        let agent = RigAgentRunner::for_skill("author", client, sandbox_tools(Some(wt)));
        let frames = agent
            .stream(&sample_input("author"), &ctx())
            .await
            .unwrap();
        let done = frames.last().unwrap().result.as_ref().unwrap();
        assert!(done.interrupted);
        assert!(done.action_required.is_some());
    }

    #[tokio::test]
    async fn resume_runs_confirmed_tool() {
        let dir = tempdir().unwrap();
        let work = dir.path().join("work");
        let bare = dir.path().join("bare.git");
        git2::Repository::init_bare(&bare).unwrap();
        let sb = SandboxRoot::new(&work).unwrap();
        let wt = WorkingTree::new(sb, vec!["main".into()]);
        wt.init().unwrap();
        wt.sandbox().write_file("f.md", "x\n").unwrap();
        wt.add("f.md").unwrap();
        wt.commit("init").unwrap();
        wt.checkout_branch("change-1", true).unwrap();
        wt.add_origin(&format!("file://{}", bare.display())).unwrap();

        let client = Arc::new(ScriptedClient::new(vec![]));
        let agent = RigAgentRunner::for_skill("author", client, sandbox_tools(Some(wt)));
        let mut input = sample_input("author");
        input.actions = Some(vec![ActionResponseEnvelope {
            action_id: Uuid::new_v4().to_string(),
            lc_action_id: None,
            kind: "confirmation".into(),
            request_id: Uuid::new_v4().to_string(),
            status: ActionResponseStatus::Submitted,
            payload: Some(json!({"tool": "git_push", "args": {"ref": "change-1"}})),
            errors: vec![],
            responder_id: Some("u".into()),
            responded_at: Utc::now(),
        }]);
        let frames = agent.stream(&input, &ctx()).await.unwrap();
        let done = frames.last().unwrap().result.as_ref().unwrap();
        assert!(!done.interrupted);
        assert_eq!(done.tool_calls[0].tool_name, "git_push");
        let _ = AgentMessageRole::Ai;
    }

    #[tokio::test]
    async fn stream_emits_thinking_before_token() {
        let client = Arc::new(ScriptedClient::new(vec![Completion::Text("ok".into())]));
        let agent = RigAgentRunner::for_skill("commit-notes", client, vec![]);
        let frames = agent.stream(&sample_input("commit-notes"), &ctx()).await.unwrap();
        assert_eq!(frames[0].chunk_type, StreamChunkType::Placeholder);
        assert_eq!(frames[1].chunk_type, StreamChunkType::Thinking);
        assert!(frames.iter().any(|f| f.chunk_type == StreamChunkType::Chunk));
        assert_eq!(frames.last().unwrap().chunk_type, StreamChunkType::Done);
    }
}
