//! The two services a runtime drives but does not own: model calls (the model
//! gateway) and tool calls (the tool runtime). Both crates
//! implement these traits; scripted versions for tests are in `scripted`
//! (the `testing` feature).

use async_trait::async_trait;
use futures_util::stream::{self, BoxStream, StreamExt};
use rig_agent::run::PreparedRequest;
use rig_core::completion::{FinishReason, Message, ToolDefinition, Usage};
use rig_core::message::AssistantContent;
use riverbase_core::RiverbaseResult;
use serde_json::Value;
use uuid::Uuid;

/// One model turn to send.
#[derive(Debug, Clone)]
pub struct ModelCall {
    pub execution_id: Uuid,
    pub run_id: Uuid,
    /// One-based model-call index in the run.
    pub turn: usize,
    /// The model alias the agent asked for; the gateway resolves it.
    pub model: Option<String>,
    /// The turn's prompt; the provider request is `prepared.apply(CompletionRequest::new(prompt))`.
    pub prompt: Message,
    pub prepared: PreparedRequest,
}

/// A finished model turn.
#[derive(Debug, Clone)]
pub struct ModelReply {
    pub choice: Vec<AssistantContent>,
    pub usage: Usage,
    pub finish_reason: Option<FinishReason>,
    /// The provider's decoded response, kept on the run for diagnostics.
    pub raw: Value,
    /// The concrete model that answered.
    pub model_used: Option<String>,
}

impl ModelReply {
    /// The reply's usage in Beam's counts (plan 023 D2: lets a caller
    /// without rig meter a call).
    #[must_use]
    pub fn token_usage(&self) -> beam_types::TokenUsage {
        crate::events::token_usage(&self.usage)
    }
}

/// Items of a streamed model turn: text deltas, then exactly one reply.
// One reply per turn; boxing it would only add an allocation.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum ModelStreamItem {
    TextDelta(String),
    Completed(ModelReply),
}

pub type ModelStream = BoxStream<'static, RiverbaseResult<ModelStreamItem>>;

/// Sends model turns (implemented by `beam_gateway::ModelGateway`).
pub trait ModelCaller: Send + Sync {
    fn stream(&self, call: ModelCall) -> ModelStream;
}

/// The default when no gateway is wired: every call fails.
pub struct NoModel;

impl ModelCaller for NoModel {
    fn stream(&self, _call: ModelCall) -> ModelStream {
        stream::once(async { Err(crate::BEM_394.with_data("no model gateway is configured")) })
            .boxed()
    }
}

/// One model call's history, fitted to its budget, and what it sends
/// (plan 023 A2).
#[derive(Debug, Clone)]
pub struct Fitted {
    /// The history to send: the run's, with old tool results stubbed and old
    /// units summarized when over budget. The saved run keeps the original.
    pub history: Vec<Message>,
    pub usage: beam_types::ContextUsage,
    /// The call's number in the run (1..N).
    pub step: u32,
    /// What the reducers changed: `(key, reducer)`, for the call's manifest.
    pub transformed: Vec<(String, String)>,
}

/// Fits every model call of one attempt to its budget (rc8
/// `InvocationContextFitter`). The runtime calls [`Self::fit`] before each
/// model call and [`Self::observe`] with what the provider counted.
#[async_trait]
pub trait ContextFitter: Send + Sync {
    /// Fit `history` (before `prompt`) for one call; `preamble` and `tools`
    /// are counted, not changed.
    async fn fit(
        &self,
        preamble: Option<&str>,
        prompt: &Message,
        history: Vec<Message>,
        tools: &[ToolDefinition],
    ) -> Fitted;

    /// The provider's usage for call `step`: calibrates later estimates.
    async fn observe(&self, step: u32, usage: &beam_types::TokenUsage);
}

/// One tool call to execute.
#[derive(Debug, Clone)]
pub struct ToolInvocation {
    pub execution_id: Uuid,
    pub run_id: Uuid,
    pub call_id: String,
    pub name: String,
    pub args: Value,
    /// The invoke: its auth (MCP token, scopes) and metadata (native tools read it).
    pub input: Option<std::sync::Arc<beam_types::AgentInput>>,
    /// Tools this invoke may call; `None` means no restriction (Python
    /// `invocation_tool_allowlist`).
    pub allowed: Option<Vec<String>>,
    /// Evals: side-effecting tools are skipped.
    pub dry_run: bool,
    /// A human approved this call (the HITL resume path).
    pub approved: bool,
}

/// What a hold needs to know about a tool (Python rc8 `ToolDefinition` fields).
#[derive(Debug, Clone, PartialEq)]
pub struct ToolInfo {
    pub description: String,
    /// `low`, `medium` or `high`.
    pub risk: String,
    /// For a client tool: how long the client has to answer.
    pub timeout_seconds: f64,
    /// Run by the calling client (Python rc8 `source = "client"`), not by the executor.
    pub client: bool,
}

/// What a client answered for one of its tool calls.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ClientAnswer {
    pub output: Value,
    pub error: Option<String>,
}

/// Runs tool calls (implemented by `beam_tools::ToolRuntime`).
#[async_trait]
pub trait ToolExecutor: Send + Sync {
    /// Definitions to advertise for the named tools, in the given order. Names
    /// this executor does not know are left out.
    fn definitions(&self, names: &[String]) -> Vec<ToolDefinition>;

    /// Whether a call to `name` must be approved by a human first.
    fn requires_approval(&self, _name: &str) -> bool {
        false
    }

    /// What a call to `name` in `scope` does (plan 023 C3): `auto`, `ask`
    /// or `deny`, and which layer decided. The default is the tool's own
    /// mode ([`Self::requires_approval`]) under the agent's rules; the tool
    /// runtime also reads the tenant's and user's rules and the session's
    /// grants.
    ///
    /// # Errors
    /// A rule or grant that cannot be read: the call is then neither held
    /// nor run (plan 023 D8).
    async fn decide(
        &self,
        name: &str,
        scope: &beam_types::tool_policy::ToolScope,
    ) -> RiverbaseResult<beam_types::tool_policy::ToolDecision> {
        use beam_types::tool_policy::{ToolDecision, ToolLayer, ToolMode};
        let mut decision = ToolDecision {
            mode: if self.requires_approval(name) {
                ToolMode::Ask
            } else {
                ToolMode::Auto
            },
            ..ToolDecision::default()
        };
        if let Some(rule) = scope
            .agent_rules
            .iter()
            .filter(|r| r.matcher.name.as_deref() == Some(name))
            .max_by_key(|r| r.mode.strictness())
        {
            decision = ToolDecision {
                mode: rule.mode,
                layer: ToolLayer::Agent,
                rule: Some(rule.clone()),
                ..ToolDecision::default()
            };
        }
        Ok(decision)
    }

    /// Store what an approval of `name` chose to remember (plan 023 C4):
    /// a session grant, or an "always" user rule. The default stores nothing.
    ///
    /// # Errors
    /// The rule or grant store's.
    async fn remember(
        &self,
        _name: &str,
        _scope: &beam_types::tool_policy::ToolScope,
        _decision: &beam_types::tool_policy::ToolDecision,
        _choice: beam_types::tool_policy::RememberOption,
    ) -> RiverbaseResult<()> {
        Ok(())
    }

    /// Run one call. An `Err` is reported to the model as a failed tool result.
    ///
    /// # Errors
    /// Whatever the tool fails with.
    async fn execute(&self, call: ToolInvocation) -> RiverbaseResult<String>;

    /// What a hold shows about `name`; `None` for a tool this executor does not know.
    fn tool_info(&self, _name: &str) -> Option<ToolInfo> {
        None
    }

    /// Finish a client tool call with the client's answer: the text the model
    /// sees, as [`Self::execute`] would return it.
    ///
    /// # Errors
    /// `BEM-394` when this executor has no client tools; security blocks.
    async fn client_result(
        &self,
        call: ToolInvocation,
        _answer: ClientAnswer,
    ) -> RiverbaseResult<String> {
        Err(crate::BEM_394.with_data(format!("{} is not a client tool", call.name)))
    }
}

/// The default when no tool runtime is wired: no tools.
pub struct NoTools;

#[async_trait]
impl ToolExecutor for NoTools {
    fn definitions(&self, _names: &[String]) -> Vec<ToolDefinition> {
        Vec::new()
    }

    async fn execute(&self, call: ToolInvocation) -> RiverbaseResult<String> {
        Err(crate::BEM_394.with_data(format!("no tool runtime is configured for {}", call.name)))
    }
}
