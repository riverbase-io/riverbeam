//! Context manifest stores and the privacy-safe inspection view (Python
//! `context/stores/` and `context/inspection.py`). Manifests hold ids,
//! sources, token counts and reasons; never prompt text.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use beam_store::{BeamStore, ManifestRow};
use riverbase_core::RiverbaseResult;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::types::{reconcile_usage, ContextManifest};

/// Where context manifests are kept.
#[async_trait]
pub trait ManifestStore: Send + Sync {
    /// Store `manifest` under `operation_id` (kept as its `operation_id` when unset).
    ///
    /// # Errors
    /// Store errors.
    async fn put(&self, operation_id: &str, manifest: &ContextManifest) -> RiverbaseResult<()>;

    /// # Errors
    /// Store errors.
    async fn get(&self, operation_id: &str) -> RiverbaseResult<Option<ContextManifest>>;

    /// # Errors
    /// Store errors.
    async fn list_for_execution(&self, execution_id: &str)
        -> RiverbaseResult<Vec<ContextManifest>>;

    /// Record the provider's prompt tokens once.
    ///
    /// # Errors
    /// Store errors.
    async fn reconcile(
        &self,
        operation_id: &str,
        actual_prompt_tokens: u64,
    ) -> RiverbaseResult<Option<ContextManifest>> {
        let Some(current) = self.get(operation_id).await? else {
            return Ok(None);
        };
        if current.actual_prompt_tokens.is_some() {
            return Ok(Some(current));
        }
        let updated = reconcile_usage(&current, actual_prompt_tokens);
        self.put(operation_id, &updated).await?;
        Ok(Some(updated))
    }
}

fn stored(operation_id: &str, manifest: &ContextManifest) -> ContextManifest {
    let mut row = manifest.clone();
    if row.operation_id.as_deref().is_none_or(str::is_empty) {
        row.operation_id = Some(operation_id.to_owned());
    }
    row
}

/// Context manifests in process memory.
#[derive(Default)]
pub struct InMemoryManifestStore {
    rows: Mutex<HashMap<String, ContextManifest>>,
}

fn poisoned<T>(_: T) -> riverbase_core::RiverbaseError {
    crate::BEM_485.raise()
}

#[async_trait]
impl ManifestStore for InMemoryManifestStore {
    async fn put(&self, operation_id: &str, manifest: &ContextManifest) -> RiverbaseResult<()> {
        self.rows
            .lock()
            .map_err(poisoned)?
            .insert(operation_id.to_owned(), stored(operation_id, manifest));
        Ok(())
    }

    async fn get(&self, operation_id: &str) -> RiverbaseResult<Option<ContextManifest>> {
        Ok(self
            .rows
            .lock()
            .map_err(poisoned)?
            .get(operation_id)
            .cloned())
    }

    async fn list_for_execution(
        &self,
        execution_id: &str,
    ) -> RiverbaseResult<Vec<ContextManifest>> {
        Ok(self
            .rows
            .lock()
            .map_err(poisoned)?
            .values()
            .filter(|m| m.parent_execution_id.as_deref() == Some(execution_id))
            .cloned()
            .collect())
    }
}

/// Rows in `agent_context_manifest`.
pub struct SqlManifestStore {
    store: Arc<dyn BeamStore>,
}

impl SqlManifestStore {
    #[must_use]
    pub fn new(store: Arc<dyn BeamStore>) -> Self {
        Self { store }
    }
}

fn int(v: u64) -> i32 {
    i32::try_from(v).unwrap_or(i32::MAX)
}

/// The manifest's columns for its `agent_context_manifest` row.
#[must_use]
pub fn manifest_row(manifest: &ContextManifest) -> ManifestRow {
    ManifestRow {
        operation_id: manifest.operation_id.clone().unwrap_or_default(),
        execution_id: manifest
            .parent_execution_id
            .as_deref()
            .and_then(|e| Uuid::parse_str(e).ok()),
        operation_kind: manifest.operation_kind.clone(),
        revision_id: manifest.revision_id.clone(),
        policy_revision: manifest.policy_revision.clone(),
        serializer_revision: manifest.serializer_revision.clone(),
        tokenizer_revision: manifest.tokenizer_revision.clone(),
        input_tokens: int(manifest.input_tokens),
        output_tokens: int(manifest.output_tokens),
        reserve_tokens: int(manifest.reserve_tokens),
        actual_prompt_tokens: manifest.actual_prompt_tokens.map(int),
        estimate_drift: manifest.estimate_drift,
        decisions: serde_json::to_value(&manifest.decisions).unwrap_or(Value::Array(Vec::new())),
        view: inspect_manifest(manifest),
    }
}

/// The view, with columns filling missing keys.
fn row_manifest(row: &ManifestRow) -> RiverbaseResult<ContextManifest> {
    let mut payload = row.view.as_object().cloned().unwrap_or_default();
    let defaults = json!({
        "decisions": row.decisions,
        "operation_id": row.operation_id,
        "operation_kind": row.operation_kind,
        "revision_id": row.revision_id,
        "policy_revision": row.policy_revision,
        "serializer_revision": row.serializer_revision,
        "tokenizer_revision": row.tokenizer_revision,
        "input_tokens": row.input_tokens,
        "output_tokens": row.output_tokens,
        "reserve_tokens": row.reserve_tokens,
        "actual_prompt_tokens": row.actual_prompt_tokens,
        "estimate_drift": row.estimate_drift,
        "parent_execution_id": row.execution_id.map(|e| e.to_string()),
    });
    if let Value::Object(defaults) = defaults {
        for (key, value) in defaults {
            payload.entry(key).or_insert(value);
        }
    }
    // A null in the view means "absent" to pydantic defaults.
    payload.retain(|_, v| !v.is_null());
    serde_json::from_value(Value::Object(payload))
        .map_err(|e| crate::BEM_487.with_data(e.to_string()))
}

#[async_trait]
impl ManifestStore for SqlManifestStore {
    async fn put(&self, operation_id: &str, manifest: &ContextManifest) -> RiverbaseResult<()> {
        self.store
            .put_manifest(&manifest_row(&stored(operation_id, manifest)))
            .await
    }

    async fn get(&self, operation_id: &str) -> RiverbaseResult<Option<ContextManifest>> {
        self.store
            .get_manifest(operation_id)
            .await?
            .as_ref()
            .map(row_manifest)
            .transpose()
    }

    async fn list_for_execution(
        &self,
        execution_id: &str,
    ) -> RiverbaseResult<Vec<ContextManifest>> {
        let Ok(id) = Uuid::parse_str(execution_id) else {
            return Ok(Vec::new());
        };
        self.store
            .list_manifests(id)
            .await?
            .iter()
            .map(row_manifest)
            .collect()
    }
}

/// The privacy-safe view (no raw content).
#[must_use]
pub fn inspect_manifest(manifest: &ContextManifest) -> Value {
    json!({
        "schema_version": manifest.schema_version,
        "policy_revision": manifest.policy_revision,
        "revision_id": manifest.revision_id,
        "operation_id": manifest.operation_id,
        "operation_kind": manifest.operation_kind,
        "tokenizer_revision": manifest.tokenizer_revision,
        "serializer_revision": manifest.serializer_revision,
        "input_tokens": manifest.input_tokens,
        "output_tokens": manifest.output_tokens,
        "reserve_tokens": manifest.reserve_tokens,
        "window_tokens": manifest.window_tokens,
        "actual_prompt_tokens": manifest.actual_prompt_tokens,
        "estimate_drift": manifest.estimate_drift,
        "decisions": manifest.decisions,
        // rc8's per-call fields (plan 023 A2).
        "parent_operation_id": manifest.parent_operation_id,
        "step": manifest.step,
        "usage": manifest.usage,
    })
}
