//! Agent registry and local runners.

mod errors;
#[allow(unused_imports)]
pub(crate) use errors::*;

mod completion;
mod echo;
mod registry;
mod runner;
mod sandbox_tools;
mod skills;
mod tool;

pub use completion::{Completion, CompletionClient, EchoClient, OpenAiCompatible, PendingToolCall, ScriptedClient};
pub use echo::EchoAgent;
pub use registry::{human_message_from_input, terminal_result, Agent, AgentRegistry};
pub use runner::RigAgentRunner;
pub use sandbox_tools::{sandbox_tools, RememberTool};
pub use skills::{builtin_skills, skill_by_name, Skill};
pub use tool::{json_params, Tool, ToolSpec};
