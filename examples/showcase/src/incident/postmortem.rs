//! Postmortems: one markdown file per resolved incident, committed to a git
//! working tree inside the sandbox (`INCIDENT_SANDBOX_ROOT`), so every
//! postmortem has a reviewable history.

use std::sync::Arc;

use beam_sandbox::{SandboxRoot, WorkingTree};
use riverbase_core::RiverbaseResult;
use serde_json::{json, Value};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::errors::SHC_001;
use crate::records::Record;

/// The postmortem repository. Writes take a lock: git's index is one per
/// tree.
pub struct Postmortems {
    tree: WorkingTree,
    lock: Mutex<()>,
}

#[allow(clippy::needless_pass_by_value)] // `map_err` hands it over.
fn joined(e: tokio::task::JoinError) -> riverbase_core::RiverbaseError {
    SHC_001.with_data(e.to_string())
}

impl Postmortems {
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

    /// The repository at `INCIDENT_SANDBOX_ROOT`, or
    /// `target/incident-postmortems`.
    ///
    /// # Errors
    /// As [`Self::open`].
    pub fn from_env() -> RiverbaseResult<Arc<Self>> {
        let root = std::env::var("INCIDENT_SANDBOX_ROOT")
            .ok()
            .filter(|r| !r.trim().is_empty())
            .unwrap_or_else(|| {
                concat!(env!("CARGO_MANIFEST_DIR"), "/../../target/incident-postmortems").into()
            });
        Self::open(root)
    }

    /// Write the file `path` and commit it with `message`; returns the
    /// commit id.
    ///
    /// # Errors
    /// Writing or committing fails.
    pub async fn write(&self, path: &str, body: &str, message: &str) -> RiverbaseResult<String> {
        let _guard = self.lock.lock().await;
        let tree = self.tree.clone();
        let (path, body, message) = (path.to_owned(), body.to_owned(), message.to_owned());
        tokio::task::spawn_blocking(move || {
            tree.sandbox().write_file(&path, &body)?;
            tree.add(&path)?;
            tree.commit(&message)
        })
        .await
        .map_err(joined)?
    }

    /// The file `path` (when it exists) and the commits that name the
    /// incident `id`.
    ///
    /// # Errors
    /// Reading the file or the history fails.
    pub async fn read(&self, path: Option<&str>, id: Uuid) -> RiverbaseResult<Value> {
        let tree = self.tree.clone();
        let path = path.map(str::to_owned);
        tokio::task::spawn_blocking(move || {
            let mut files = Vec::new();
            if let Some(rel) = path {
                if tree.sandbox().resolve(&rel).is_ok_and(|p| p.is_file()) {
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

/// Where the postmortem of `incident` lives: `postmortems/<date>-<service>.md`.
#[must_use]
pub fn path_for(date: &str, service: &str) -> String {
    let slug: String = service
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' { c.to_ascii_lowercase() } else { '-' })
        .collect();
    format!("postmortems/{date}-{slug}.md")
}

/// The commit message of an incident's postmortem.
#[must_use]
pub fn commit_message(incident: &Record) -> String {
    format!(
        "Postmortem for incident {}: {}",
        incident.id,
        incident.text("service").unwrap_or("unknown service")
    )
}

/// What the commander writes in a postmortem; the incident supplies the rest.
pub struct Sections<'a> {
    pub summary: &'a str,
    pub impact: &'a str,
    pub root_cause: &'a str,
    pub what_went_well: &'a str,
    pub action_items: &'a [String],
}

fn bullets(items: &[String]) -> String {
    if items.is_empty() {
        return "- None.".into();
    }
    items.iter().map(|i| format!("- {i}")).collect::<Vec<_>>().join("\n")
}

/// The postmortem as markdown, in the order of the postmortem template: the
/// incident's timeline is copied in as it was recorded.
#[must_use]
pub fn render(incident: &Record, date: &str, sections: &Sections<'_>) -> String {
    let service = incident.text("service").unwrap_or("unknown service");
    let severity = incident.text("severity").unwrap_or("unknown");
    let timeline = incident
        .field("timeline")
        .and_then(Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .map(|e| {
                    format!(
                        "- {} ({}): {}",
                        e["at"].as_str().unwrap_or("?"),
                        e["who"].as_str().unwrap_or("?"),
                        e["entry"].as_str().unwrap_or("")
                    )
                })
                .collect::<Vec<_>>()
                .join("\n")
        })
        .filter(|t| !t.is_empty())
        .unwrap_or_else(|| "- No entries.".into());
    format!(
        "# Postmortem: {service}, {date}\n\n\
         Incident {id} · severity {severity}\n\n\
         ## Summary\n\n{summary}\n\n\
         ## Impact\n\n{impact}\n\n\
         ## Timeline\n\n{timeline}\n\n\
         ## Root cause\n\n{root_cause}\n\n\
         ## What went well\n\n{well}\n\n\
         ## Action items\n\n{actions}\n",
        id = incident.id,
        summary = sections.summary.trim(),
        impact = sections.impact.trim(),
        root_cause = sections.root_cause.trim(),
        well = sections.what_went_well.trim(),
        actions = bullets(sections.action_items),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn incident() -> Record {
        let mut r = Record::new("incident", "incident", "u", None, "checkout slow");
        r.data.insert("service".into(), json!("checkout-api"));
        r.data.insert("severity".into(), json!("critical"));
        r.data.insert(
            "timeline".into(),
            json!([{"at": "2026-10-24T14:10:00Z", "who": "Kai Sato", "entry": "Opened the incident."}]),
        );
        r
    }

    #[test]
    fn the_path_names_the_date_and_service() {
        assert_eq!(path_for("2026-10-24", "checkout-api"), "postmortems/2026-10-24-checkout-api.md");
        assert_eq!(path_for("2026-10-24", "My Svc/x"), "postmortems/2026-10-24-my-svc-x.md");
    }

    #[test]
    fn the_markdown_follows_the_template() {
        let sections = Sections {
            summary: "Checkout was slow.",
            impact: "6% of orders failed for 25 minutes.",
            root_cause: "Connection pool v3 held connections.",
            what_went_well: "The rollback was quick.",
            action_items: &["Load test the pool.".to_owned()],
        };
        let body = render(&incident(), "2026-10-24", &sections);
        let order: Vec<_> = ["## Summary", "## Impact", "## Timeline", "## Root cause", "## What went well", "## Action items"]
            .iter()
            .map(|h| body.find(h).unwrap())
            .collect();
        assert!(order.windows(2).all(|w| w[0] < w[1]), "{body}");
        assert!(body.contains("- 2026-10-24T14:10:00Z (Kai Sato): Opened the incident."));
        assert!(body.contains("- Load test the pool."));
    }

    #[tokio::test]
    async fn a_postmortem_is_committed_and_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let repo = Postmortems::open(dir.path()).unwrap();
        let record = incident();
        let path = path_for("2026-10-24", "checkout-api");
        let commit = repo.write(&path, "# Postmortem", &commit_message(&record)).await.unwrap();
        assert_eq!(commit.len(), 40);
        let view = repo.read(Some(&path), record.id).await.unwrap();
        assert_eq!(view["files"][0]["body"], "# Postmortem");
        assert_eq!(view["log"].as_array().unwrap().len(), 1);
        let other = repo.read(Some(&path), Uuid::new_v4()).await.unwrap();
        assert_eq!(other["log"], json!([]));
        assert_eq!(repo.read(None, record.id).await.unwrap()["files"], json!([]));
    }
}
