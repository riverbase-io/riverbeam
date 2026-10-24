//! Revisions on Postgres (Python `RevisionBook.persist_all` / `reconstruct`):
//! an adapter on `db.revisions()`. A canary split is not stored: rc8's
//! `agent` table has no canary columns, so it lives in this process only.

use beam_store::repo::revisions::{RevisionRecord, RevisionWrite};
use beam_store::{Db, PgBeamStore};
use beam_types::{CompatibilityPolicy, RevisionStatus};
use serde_json::{json, Value};

use crate::records::ExecResult;
use crate::revisions::{AgentRevision, RevisionBook, RevisionSnapshot, TrafficPolicy};

/// Persists a [`RevisionBook`] in the beam schema.
#[derive(Clone)]
pub struct PgRevisionStore {
    db: Db,
}

fn db(e: impl std::fmt::Display) -> riverbase_core::RiverbaseError {
    crate::BEM_380.with_data(e.to_string())
}

/// The columns a new revision row is inserted with, beside the ones that
/// change (`persist`), keyed by physical column.
fn revision_columns(r: &AgentRevision) -> serde_json::Map<String, Value> {
    let row = json!({
        "agent_id": r.agent_id,
        "agent_name": r.agent_name,
        "version": r.version,
        "revision_hash": r.revision_hash,
        "runtime_name": r.runtime_name,
        "runtime_adapter_version": r.runtime_adapter_version,
        "code_revision": r.code_revision,
        "build_id": r.build_id,
        "prompt_revision": r.prompt_revision,
        "toolset_revision": r.toolset_revision,
        "model_policy_revision": r.model_policy_revision,
        "security_policy_revision": r.security_policy_revision,
        "memory_policy_revision": r.memory_policy_revision,
        "knowledge_policy_revision": r.knowledge_policy_revision,
        "input_schema_hash": r.input_schema_hash,
        "output_schema_hash": r.output_schema_hash,
        "runtime_config_snapshot": r.runtime_config_snapshot,
        "framework_config_snapshot": r.framework_config_snapshot,
        "beam_version": r.beam_version,
        "runtime_library_version": r.runtime_library_version,
        "application_build_id": r.application_build_id,
        "created_at": r.created_at,
    });
    match row {
        Value::Object(columns) => columns,
        _ => serde_json::Map::new(),
    }
}

fn compatibility(raw: &str) -> CompatibilityPolicy {
    match raw.to_ascii_lowercase().as_str() {
        "compatible" => CompatibilityPolicy::Compatible,
        "migratable" => CompatibilityPolicy::Migratable,
        _ => CompatibilityPolicy::Strict,
    }
}

impl From<RevisionRecord> for AgentRevision {
    fn from(r: RevisionRecord) -> Self {
        Self {
            snapshot: RevisionSnapshot {
                id: r.id,
                agent_id: r.agent_id,
                agent_name: r.agent_name,
                version: r.version,
                revision_hash: r.revision_hash,
                runtime_name: r.runtime_name,
                runtime_adapter_version: r.runtime_adapter_version,
                code_revision: r.code_revision,
                build_id: r.build_id,
                prompt_revision: r.prompt_revision,
                toolset_revision: r.toolset_revision,
                model_policy_revision: r.model_policy_revision,
                security_policy_revision: r.security_policy_revision,
                memory_policy_revision: r.memory_policy_revision,
                knowledge_policy_revision: r.knowledge_policy_revision,
                input_schema_hash: r.input_schema_hash,
                output_schema_hash: r.output_schema_hash,
                runtime_config_snapshot: r
                    .runtime_config_snapshot
                    .unwrap_or(Value::Object(serde_json::Map::new())),
                framework_config_snapshot: r
                    .framework_config_snapshot
                    .unwrap_or(Value::Object(serde_json::Map::new())),
                beam_version: r.beam_version,
                runtime_library_version: r.runtime_library_version,
                application_build_id: r.application_build_id,
            },
            status: RevisionStatus::parse(&r.status).unwrap_or(RevisionStatus::Draft),
            compatibility: compatibility(&r.compatibility_policy),
            compatible_with: r
                .compatible_with
                .and_then(|v| serde_json::from_value(v).ok())
                .unwrap_or_default(),
            compatible_adapter_versions: std::collections::BTreeSet::new(),
            created_at: r.created_at,
            activated_at: r.activated_at,
            retired_at: r.retired_at,
        }
    }
}

impl PgRevisionStore {
    #[must_use]
    pub fn for_store(store: &PgBeamStore) -> Self {
        Self {
            db: store.db().clone(),
        }
    }

    /// Write what changed here (reading the store first if this book never
    /// did): new rows are inserted unless the agent already has a row with
    /// that hash, as Python's `persist_all` skips them; changed rows update
    /// their status, timestamps and compatibility; changed agents their
    /// active revision. One transaction.
    ///
    /// # Errors
    /// Store errors.
    pub async fn persist(&self, book: &RevisionBook) -> ExecResult<()> {
        if !book.is_loaded() {
            self.reconstruct(book).await?;
        }
        let (rows, traffic) = book.export();
        if rows.is_empty() && traffic.is_empty() {
            return Ok(());
        }
        let writes: Vec<RevisionWrite> = rows
            .iter()
            .map(|r| {
                let changes = json!({
                    "status": r.status.as_str().to_uppercase(),
                    "compatibility_policy": r.compatibility.as_str().to_uppercase(),
                    "compatible_with": serde_json::to_value(&r.compatible_with)
                        .unwrap_or(Value::Array(vec![])),
                    "activated_at": r.activated_at,
                    "retired_at": r.retired_at,
                });
                RevisionWrite {
                    id: r.id,
                    agent_name: r.agent_name.clone(),
                    revision_hash: r.revision_hash.clone(),
                    changes: match changes {
                        Value::Object(map) => map,
                        _ => serde_json::Map::new(),
                    },
                    columns: revision_columns(r),
                }
            })
            .collect();
        let active: Vec<(String, uuid::Uuid)> = traffic
            .iter()
            .map(|(name, t)| (name.clone(), t.primary_id))
            .collect();
        self.db
            .revisions()
            .persist(&writes, &active)
            .await
            .map_err(db)?;
        book.mark_persisted(&rows, &traffic);
        Ok(())
    }

    /// Load the stored revisions and traffic into `book`.
    ///
    /// # Errors
    /// Store errors.
    pub async fn reconstruct(&self, book: &RevisionBook) -> ExecResult<()> {
        let (rows, active) = self.db.revisions().load().await.map_err(db)?;
        book.load(
            rows.into_iter().map(AgentRevision::from).collect(),
            active
                .into_iter()
                .map(|(name, primary_id)| {
                    (
                        name,
                        TrafficPolicy {
                            primary_id,
                            canary_id: None,
                            percent: 0,
                        },
                    )
                })
                .collect(),
        );
        Ok(())
    }
}
