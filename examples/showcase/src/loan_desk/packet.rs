//! Application packets: the pre-check's summary and decision as files in a
//! git working tree inside the sandbox (`LOAN_DESK_SANDBOX_ROOT`), one
//! commit per packet, so every decision has a reviewable history.

use std::sync::Arc;

use async_trait::async_trait;
use beam_sandbox::{SandboxRoot, WorkingTree};
use beam_tools::{ToolCallContext, ToolDefinition, ToolHandler, ToolRisk};
use riverbase_core::RiverbaseResult;
use serde_json::{json, Value};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::loan_desk::errors::{LND_001, LND_007};

pub const WRITE_PACKET: &str = "write_packet";

/// The packet repository. Writes take a lock: git's index is one per tree.
pub struct Packets {
    tree: WorkingTree,
    lock: Mutex<()>,
}

#[allow(clippy::needless_pass_by_value)] // `map_err` hands it over.
fn joined(e: tokio::task::JoinError) -> riverbase_core::RiverbaseError {
    LND_001.with_data(e.to_string())
}

impl Packets {
    /// Open (or create) the repository at `root`.
    ///
    /// # Errors
    /// The directory or the repository cannot be created.
    pub fn open(root: impl AsRef<std::path::Path>) -> RiverbaseResult<Arc<Self>> {
        let tree = WorkingTree::new(SandboxRoot::new(root)?, vec!["refs/heads/main".into()]);
        tree.init()?;
        Ok(Arc::new(Self {
            tree,
            lock: Mutex::new(()),
        }))
    }

    /// The repository at `LOAN_DESK_SANDBOX_ROOT`, or `target/loan-desk-packets`.
    ///
    /// # Errors
    /// As [`Self::open`].
    pub fn from_env() -> RiverbaseResult<Arc<Self>> {
        let root = std::env::var("LOAN_DESK_SANDBOX_ROOT")
            .ok()
            .filter(|r| !r.trim().is_empty())
            .unwrap_or_else(|| {
                concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/../../target/loan-desk-packets"
                )
                .into()
            });
        Self::open(root)
    }

    fn dir(id: Uuid) -> String {
        format!("packets/{id}")
    }

    /// Write the packet's two files and commit them; returns the commit id.
    ///
    /// # Errors
    /// Writing or committing fails.
    pub async fn write(&self, id: Uuid, summary: &str, decision: &str) -> RiverbaseResult<String> {
        let _guard = self.lock.lock().await;
        let tree = self.tree.clone();
        let (summary, decision) = (summary.to_owned(), decision.to_owned());
        tokio::task::spawn_blocking(move || {
            let dir = Self::dir(id);
            tree.sandbox()
                .write_file(&format!("{dir}/summary.md"), &summary)?;
            tree.sandbox()
                .write_file(&format!("{dir}/decision.md"), &decision)?;
            tree.add(&format!("{dir}/summary.md"))?;
            tree.add(&format!("{dir}/decision.md"))?;
            tree.commit(&format!("Packet for application {id}"))
        })
        .await
        .map_err(joined)?
    }

    /// The packet's files and the commits that touched packets for it.
    ///
    /// # Errors
    /// Reading the files or the history fails.
    pub async fn read(&self, id: Uuid) -> RiverbaseResult<Value> {
        let tree = self.tree.clone();
        tokio::task::spawn_blocking(move || {
            let dir = Self::dir(id);
            let mut files = Vec::new();
            if tree.sandbox().resolve(&dir).is_ok_and(|p| p.is_dir()) {
                for rel in tree.sandbox().list_tree(&dir)? {
                    let body = tree.sandbox().read_file(&rel)?;
                    files.push(json!({"path": rel, "body": body}));
                }
            }
            let marker = id.to_string();
            let log: Vec<String> = tree
                .log(200)?
                .into_iter()
                .filter(|summary| summary.contains(&marker))
                .collect();
            Ok(json!({"files": files, "log": log}))
        })
        .await
        .map_err(joined)?
    }
}

struct WritePacket {
    packets: Arc<Packets>,
}

#[async_trait]
impl ToolHandler for WritePacket {
    async fn call(&self, args: Value, _ctx: &ToolCallContext) -> RiverbaseResult<Value> {
        let id = args
            .get("application_id")
            .and_then(Value::as_str)
            .and_then(|s| Uuid::parse_str(s).ok())
            .ok_or_else(|| LND_007.with_data("application_id is missing or not a UUID"))?;
        let text = |key: &str| {
            args.get(key)
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned()
        };
        let commit = self
            .packets
            .write(id, &text("summary_markdown"), &text("decision_markdown"))
            .await?;
        Ok(json!({"committed": commit, "path": Packets::dir(id)}))
    }
}

/// `write_packet`: commits the packet; every call waits for the officer.
#[must_use]
pub fn write_packet_tool(packets: Arc<Packets>) -> ToolDefinition {
    ToolDefinition {
        description:
            "Write the application's packet (summary.md and decision.md) to the packet repository \
            and commit it."
                .into(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "application_id": {"type": "string"},
                "summary_markdown": {"type": "string"},
                "decision_markdown": {"type": "string"}
            },
            "required": ["application_id", "summary_markdown", "decision_markdown"]
        }),
        risk: ToolRisk::Medium,
        side_effect: true,
        idempotent: true,
        require_approval: true,
        ..ToolDefinition::new(WRITE_PACKET)
    }
    .with_handler(Arc::new(WritePacket { packets }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_packet_is_committed_and_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let packets = Packets::open(dir.path()).unwrap();
        let id = Uuid::new_v4();
        let commit = packets.write(id, "# Summary", "Approve").await.unwrap();
        assert_eq!(commit.len(), 40);
        let view = packets.read(id).await.unwrap();
        assert_eq!(view["files"].as_array().unwrap().len(), 2);
        assert_eq!(view["log"], json!([format!("Packet for application {id}")]));
        assert_eq!(
            packets.read(Uuid::new_v4()).await.unwrap()["files"],
            json!([])
        );
    }
}
