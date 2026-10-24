//! Knowledge documents and long-term memory.

use std::sync::Arc;

use beam_knowledge::KnowledgeManager;
use beam_security::SecurityStage;
use beam_store::agent_id_for;
use beam_types::{AgentInput, AgentResult};
use serde_json::Value;

use riverbase_core::RiverbaseResult;

use super::{json_truthy, memory_id, result_text, RiverBeam};

impl RiverBeam {
    /// `ingest_knowledge_document` worker task body (Python
    /// `KnowledgeApplicationService.ingest_knowledge_document`); the
    /// `reindex-knowledge` command sends the same task with a `document_id`.
    ///
    /// # Errors
    /// `BEM-045` without knowledge, `BEM-004`/`BEM-245` for a bad payload,
    /// `BEM-047` when the result cannot be serialized, and the ingest's own errors.
    pub async fn ingest_knowledge_document(&self, payload: Value) -> RiverbaseResult<Value> {
        let knowledge = self.require_knowledge()?;
        let text = |key: &str| {
            payload
                .get(key)
                .and_then(Value::as_str)
                .filter(|v| !v.is_empty())
                .map(str::to_owned)
        };
        let filename = text("filename");
        let (source, label) = if let Some(body) = text("text") {
            (
                beam_knowledge::Source::Text(body),
                text("source_url").or_else(|| filename.clone()),
            )
        } else if let Some(media_id) = text("media_id") {
            let label = filename.clone().unwrap_or_else(|| media_id.clone());
            (
                beam_knowledge::Source::Media {
                    media_id,
                    filename: filename.clone(),
                },
                Some(label),
            )
        } else if text("source_url").is_some() {
            // Python cannot parse a URL source either (it raises); Beam never
            // fetches URLs for ingest.
            return Err(crate::BEM_004
                .with_data("source_url ingest needs the media service; send text or media_id"));
        } else {
            return Err(crate::BEM_004
                .with_data("Ingest payload requires one of: text, media_id, source_url"));
        };
        let title = text("title").ok_or_else(|| crate::BEM_004.with_data("title is required"))?;
        let scope = match text("scope") {
            None => beam_knowledge::KnowledgeScope::Global,
            Some(raw) => beam_knowledge::KnowledgeScope::parse(&raw)
                .ok_or_else(|| crate::BEM_245.with_data(raw))?,
        };
        let request = beam_knowledge::IngestRequest {
            source,
            title,
            scope,
            doc_type: text("type").unwrap_or_else(|| "text".into()),
            agent_id: text("agent_id"),
            user_id: text("user_id"),
            org_id: text("org_id"),
            document_id: text("document_id"),
            metadata: payload
                .get("metadata")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default(),
            mime_type: text("mime_type"),
            filename,
            source_label: label,
            embedding_revision: None,
        };
        let result = knowledge.ingest(request).await?;
        serde_json::to_value(result).map_err(|e| crate::BEM_047.with_data(e.to_string()))
    }

    /// `delete_knowledge_document` worker task body.
    ///
    /// # Errors
    /// `BEM-045` without knowledge, `BEM-005` without a `document_id`, and the
    /// store's errors.
    pub async fn delete_knowledge_document(&self, payload: Value) -> RiverbaseResult<Value> {
        let knowledge = self.require_knowledge()?;
        let document_id = payload
            .get("document_id")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| crate::BEM_005.raise())?;
        let deleted = knowledge.delete_document(document_id).await?;
        Ok(serde_json::json!({ "document_id": document_id, "deleted_chunks": deleted }))
    }

    /// `remember_memory` worker task body.
    ///
    /// # Errors
    /// `BEM-045` without knowledge, `BEM-004`/`BEM-245` for a bad payload, and the
    /// store's errors.
    pub async fn remember_memory(&self, payload: Value) -> RiverbaseResult<Value> {
        let knowledge = self.require_knowledge()?;
        let text = |key: &str| payload.get(key).and_then(Value::as_str).map(str::to_owned);
        let user =
            text("user_id").ok_or_else(|| crate::BEM_004.with_data("user_id is required"))?;
        let content = text("content").unwrap_or_default();
        let memory_type = text("type").unwrap_or_else(|| "semantic".into());
        if beam_knowledge::MemoryType::parse(&memory_type).is_none() {
            return Err(crate::BEM_245.with_data(memory_type));
        }
        let request = beam_knowledge::RememberRequest {
            memory_type,
            agent_id: text("agent_id"),
            importance: payload
                .get("importance")
                .and_then(Value::as_i64)
                .unwrap_or(1),
            metadata: payload
                .get("metadata")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default(),
            confidence: payload.get("confidence").and_then(Value::as_f64),
            source_execution_id: text("source_execution_id"),
            // The domain assigns the id when it queues the task.
            memory_id: text("memory_id"),
            ..beam_knowledge::RememberRequest::new(user, content)
        };
        let memory_id = knowledge.remember(request).await?;
        Ok(serde_json::json!({ "memory_id": memory_id }))
    }

    /// `edit_memory` worker task body.
    ///
    /// # Errors
    /// `BEM-045` without knowledge, `BEM-004` without a `memory_id`, and the
    /// store's errors.
    pub async fn edit_memory(&self, payload: Value) -> RiverbaseResult<Value> {
        let knowledge = self.require_knowledge()?;
        let memory_id = memory_id(&payload)?;
        let content = payload.get("content").and_then(Value::as_str);
        let memory_id = knowledge
            .memory_store()
            .edit(&memory_id, content, None)
            .await?;
        Ok(serde_json::json!({ "memory_id": memory_id }))
    }

    /// `expire_memory` worker task body.
    ///
    /// # Errors
    /// `BEM-045` without knowledge, `BEM-004` for a bad payload, and the store's
    /// errors.
    pub async fn expire_memory(&self, payload: Value) -> RiverbaseResult<Value> {
        let knowledge = self.require_knowledge()?;
        let memory_id = memory_id(&payload)?;
        let expires_at = payload
            .get("expires_at")
            .and_then(Value::as_str)
            .and_then(beam_knowledge::longterm::parse_time)
            .ok_or_else(|| crate::BEM_004.with_data("expires_at must be an ISO 8601 time"))?;
        let memory_id = knowledge
            .memory_store()
            .expire(&memory_id, expires_at)
            .await?;
        Ok(serde_json::json!({ "memory_id": memory_id }))
    }

    /// `forget_memory` worker task body.
    ///
    /// # Errors
    /// `BEM-045` without knowledge, `BEM-004` without a `memory_id`, and the
    /// store's errors.
    pub async fn forget_memory(&self, payload: Value) -> RiverbaseResult<Value> {
        let knowledge = self.require_knowledge()?;
        let memory_id = memory_id(&payload)?;
        let deleted = knowledge.forget(&memory_id).await?;
        Ok(serde_json::json!({ "memory_id": memory_id, "deleted": deleted }))
    }

    /// With `MEMORY_LONGTERM_AUTO_REMEMBER` and the
    /// caller's `metadata.memory_consent`, remember `metadata.memory_text`, or
    /// run the extraction pipeline over the answer. Failures are logged.
    pub(super) async fn maybe_auto_remember(&self, input: &AgentInput, result: &AgentResult) {
        let (Some(knowledge), Some(user)) = (
            self.knowledge
                .as_ref()
                .filter(|_| self.config.auto_remember),
            input.user_id(),
        ) else {
            return;
        };
        if !input
            .metadata
            .get("memory_consent")
            .is_some_and(json_truthy)
        {
            return;
        }
        let agent_id = input
            .agent_name
            .as_deref()
            .map(|n| agent_id_for(n).to_string());
        let extracted = input
            .metadata
            .get("memory_text")
            // A falsy value is no text.
            .filter(|v| json_truthy(v))
            .map(|v| v.as_str().map_or_else(|| v.to_string(), str::to_owned))
            .unwrap_or_default();
        let extracted = extracted.trim();
        let outcome = async {
            if !extracted.is_empty() {
                self.check_security(SecurityStage::BeforeMemoryWrite, input, extracted)
                    .await?;
                let mut metadata = serde_json::Map::new();
                metadata.insert(
                    "session_id".into(),
                    Value::String(input.session_id.to_string()),
                );
                metadata.insert(
                    "interaction_id".into(),
                    input
                        .interaction_id
                        .map_or(Value::Null, |i| Value::String(i.to_string())),
                );
                metadata.insert(
                    "agent_name".into(),
                    input.agent_name.clone().map_or(Value::Null, Value::String),
                );
                let request = beam_knowledge::RememberRequest {
                    memory_type: "episodic".into(),
                    agent_id: agent_id.clone(),
                    // Numbers and numeric strings.
                    importance: input
                        .metadata
                        .get("memory_importance")
                        .and_then(|v| {
                            v.as_i64()
                                .or_else(|| v.as_str().and_then(|s| s.trim().parse().ok()))
                        })
                        .unwrap_or(1),
                    metadata,
                    source_execution_id: input.execution_id.map(|e| e.to_string()),
                    ..beam_knowledge::RememberRequest::new(user.clone(), extracted)
                };
                knowledge
                    .memory_store()
                    .maybe_remember(request, self.config.min_importance)
                    .await?;
                return Ok(());
            }
            let answer = result_text(result);
            let answer = answer.trim();
            if answer.is_empty() {
                return Ok(());
            }
            self.check_security(SecurityStage::BeforeMemoryWrite, input, answer)
                .await?;
            beam_knowledge::LtmPipeline::new(knowledge.memory_store().clone())
                .run(
                    answer,
                    &user,
                    agent_id.as_deref(),
                    input.execution_id.map(|e| e.to_string()),
                )
                .await
                .map(|_| ())
        }
        .await;
        if let Err(e) = outcome {
            tracing::warn!(error = %e, "long-term memory auto-remember failed");
        }
    }

    pub(super) fn require_knowledge(&self) -> RiverbaseResult<&Arc<KnowledgeManager>> {
        self.knowledge
            .as_ref()
            .ok_or_else(|| crate::BEM_045.raise())
    }
}
