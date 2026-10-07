use std::sync::Arc;

use async_trait::async_trait;
use beam_sandbox::{SandboxRoot, WorkingTree};
use beam_types::AgentInput;
use serde_json::{json, Value};

use riverbase_core::RiverbaseResult;
use crate::tool::{json_params, Tool, ToolSpec};

/// Stateless sandbox/git tools. Bind `metadata.sandbox_root` (and optional
/// `protected_refs`) on each invoke, or pass a tree for tests.
pub fn sandbox_tools(tree: Option<WorkingTree>) -> Vec<Arc<dyn Tool>> {
    let tree = tree.map(Arc::new);
    vec![
        Arc::new(FileTool { tree: tree.clone(), kind: FileOp::List }),
        Arc::new(FileTool { tree: tree.clone(), kind: FileOp::Read }),
        Arc::new(FileTool { tree: tree.clone(), kind: FileOp::Write }),
        Arc::new(GitTool { tree: tree.clone(), kind: GitOp::Status }),
        Arc::new(GitTool { tree: tree.clone(), kind: GitOp::Diff }),
        Arc::new(GitTool { tree: tree.clone(), kind: GitOp::Log }),
        Arc::new(GitTool { tree: tree.clone(), kind: GitOp::Add }),
        Arc::new(GitTool { tree: tree.clone(), kind: GitOp::Commit }),
        Arc::new(GitTool { tree, kind: GitOp::Push }),
    ]
}

#[derive(Clone, Copy)]
enum FileOp {
    List,
    Read,
    Write,
}

struct FileTool {
    tree: Option<Arc<WorkingTree>>,
    kind: FileOp,
}

#[async_trait]
impl Tool for FileTool {
    fn spec(&self) -> ToolSpec {
        match self.kind {
            FileOp::List => ToolSpec {
                name: "list_tree".into(),
                description: "List files under a relative path in the workspace sandbox.".into(),
                confirm_required: false,
                parameters: json_params(json!({ "path": { "type": "string" } }), &[]),
            },
            FileOp::Read => ToolSpec {
                name: "read_file".into(),
                description: "Read a text file from the sandbox.".into(),
                confirm_required: false,
                parameters: json_params(json!({ "path": { "type": "string" } }), &["path"]),
            },
            FileOp::Write => ToolSpec {
                name: "write_file".into(),
                description: "Write a text file in the sandbox.".into(),
                confirm_required: false,
                parameters: json_params(
                    json!({
                        "path": { "type": "string" },
                        "body": { "type": "string" }
                    }),
                    &["path", "body"],
                ),
            },
        }
    }

    async fn invoke(&self, args: Value, input: &AgentInput) -> RiverbaseResult<Value> {
        let tree = resolve_tree(self.tree.as_deref(), input)?;
        let sb = tree.sandbox();
        match self.kind {
            FileOp::List => {
                let path = args.get("path").and_then(Value::as_str).unwrap_or("");
                let entries = sb.list_tree(path)?;
                Ok(json!({ "paths": entries }))
            }
            FileOp::Read => {
                let path = arg_str(&args, "path")?;
                let body = sb.read_file(path)?;
                Ok(json!({ "path": path, "body": body }))
            }
            FileOp::Write => {
                let path = arg_str(&args, "path")?;
                let body = arg_str(&args, "body")?;
                sb.write_file(path, body)?;
                Ok(json!({ "path": path, "written": true }))
            }
        }
    }
}

#[derive(Clone, Copy)]
enum GitOp {
    Status,
    Diff,
    Log,
    Add,
    Commit,
    Push,
}

struct GitTool {
    tree: Option<Arc<WorkingTree>>,
    kind: GitOp,
}

#[async_trait]
impl Tool for GitTool {
    fn spec(&self) -> ToolSpec {
        match self.kind {
            GitOp::Status => ToolSpec {
                name: "git_status".into(),
                description: "Show dirty and untracked paths.".into(),
                confirm_required: false,
                parameters: json_params(json!({}), &[]),
            },
            GitOp::Diff => ToolSpec {
                name: "git_diff".into(),
                description: "Show unstaged/index diff.".into(),
                confirm_required: false,
                parameters: json_params(json!({}), &[]),
            },
            GitOp::Log => ToolSpec {
                name: "git_log".into(),
                description: "Recent commit summaries.".into(),
                confirm_required: false,
                parameters: json_params(json!({ "limit": { "type": "integer" } }), &[]),
            },
            GitOp::Add => ToolSpec {
                name: "git_add".into(),
                description: "Stage a relative path (or '.').".into(),
                confirm_required: false,
                parameters: json_params(json!({ "path": { "type": "string" } }), &["path"]),
            },
            GitOp::Commit => ToolSpec {
                name: "git_commit".into(),
                description: "Commit the index on the current branch.".into(),
                confirm_required: false,
                parameters: json_params(json!({ "message": { "type": "string" } }), &["message"]),
            },
            GitOp::Push => ToolSpec {
                name: "git_push".into(),
                description: "Push the current (or named) branch to origin. Protected refs are denied.".into(),
                confirm_required: true,
                parameters: json_params(json!({ "ref": { "type": "string" } }), &[]),
            },
        }
    }

    async fn invoke(&self, args: Value, input: &AgentInput) -> RiverbaseResult<Value> {
        let tree = resolve_tree(self.tree.as_deref(), input)?;
        match self.kind {
            GitOp::Status => Ok(json!({ "paths": tree.status()? })),
            GitOp::Diff => Ok(json!({ "diff": tree.diff()? })),
            GitOp::Log => {
                let limit = args.get("limit").and_then(Value::as_u64).unwrap_or(20) as usize;
                Ok(json!({ "commits": tree.log(limit)? }))
            }
            GitOp::Add => {
                let path = arg_str(&args, "path")?;
                tree.add(path)?;
                Ok(json!({ "staged": path }))
            }
            GitOp::Commit => {
                let message = arg_str(&args, "message")?;
                let oid = tree.commit(message)?;
                Ok(json!({ "oid": oid, "message": message }))
            }
            GitOp::Push => {
                let r = args.get("ref").and_then(Value::as_str);
                tree.push(r)?;
                Ok(json!({ "pushed": r.unwrap_or("HEAD") }))
            }
        }
    }
}

pub struct RememberTool;

#[async_trait]
impl Tool for RememberTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "remember".into(),
            description: "Store a durable memory item at a named scope.".into(),
            confirm_required: false,
            parameters: json_params(
                json!({
                    "scope_name": { "type": "string" },
                    "scope_id": { "type": "string" },
                    "kind": { "type": "string" },
                    "text": { "type": "string" }
                }),
                &["scope_name", "kind", "text"],
            ),
        }
    }

    async fn invoke(&self, _args: Value, _input: &AgentInput) -> RiverbaseResult<Value> {
        Err(crate::BEM_109.raise())
    }
}

fn resolve_tree(
    held: Option<&WorkingTree>,
    input: &AgentInput,
) -> RiverbaseResult<WorkingTree> {
    if let Some(tree) = held {
        return Ok(tree.clone());
    }
    let root = input
        .metadata
        .get("sandbox_root")
        .and_then(Value::as_str)
        .ok_or_else(|| crate::BEM_107.raise())?;
    let protected: Vec<String> = input
        .metadata
        .get("protected_refs")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_else(|| vec!["main".into(), "master".into()]);
    let sb = SandboxRoot::new(root)?;
    Ok(WorkingTree::new(sb, protected))
}

fn arg_str<'a>(args: &'a Value, key: &str) -> RiverbaseResult<&'a str> {
    args.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| crate::BEM_108.with_data(key.to_string()))
}

