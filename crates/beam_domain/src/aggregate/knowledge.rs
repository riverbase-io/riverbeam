//! Knowledge documents: `ingest-knowledge`, `reindex-knowledge`,
//! `delete-knowledge`. Each queues a worker task (the worker parses, chunks
//! and embeds). The Casbin policy keeps them to `beam-admin`.

use beam_store::tasks::NewTask;
use riverbase_core::RiverbaseResult;
use serde_json::{json, Value};
use uuid::Uuid;

use super::{queued, BeamAggregate};
use crate::commands::{DeleteKnowledgePayload, IngestKnowledgePayload, ReindexKnowledgePayload};

fn present(value: Option<&str>) -> bool {
    value.is_some_and(|s| !s.trim().is_empty())
}

/// A document needs a source the worker can read: inline text or a stored
/// media file. A `source_url` alone is refused here, as the worker would
/// refuse it (Beam never fetches URLs for ingest).
fn require_source(
    text: Option<&str>,
    media_id: Option<&str>,
    source_url: Option<&str>,
) -> RiverbaseResult<()> {
    if present(text) || present(media_id) {
        Ok(())
    } else if present(source_url) {
        Err(beam_core::BEM_004
            .with_data("source_url ingest needs the media service; send text or media_id"))
    } else {
        Err(beam_core::BEM_502
            .with_data("Ingest payload requires one of: text, media_id, source_url"))
    }
}

/// The document fields both ingest commands queue.
struct Document {
    title: String,
    scope: Option<String>,
    kind: Option<String>,
    agent_id: Option<String>,
    user_id: Option<String>,
    text: Option<String>,
    media_id: Option<String>,
    source_url: Option<String>,
    filename: Option<String>,
    mime_type: Option<String>,
    metadata: Option<Value>,
}

impl Document {
    /// The `ingest_knowledge_document` task for document `document_id`.
    fn task(self, document_id: &str, org_id: Option<&str>) -> NewTask {
        NewTask::new(
            "ingest_knowledge_document",
            json!({
                "document_id": document_id,
                "title": self.title,
                "scope": self.scope,
                "type": self.kind,
                "agent_id": self.agent_id,
                "user_id": self.user_id,
                "text": self.text,
                "media_id": self.media_id,
                "source_url": self.source_url,
                "filename": self.filename,
                "mime_type": self.mime_type,
                "metadata": self.metadata,
                "org_id": org_id,
            }),
        )
    }
}

impl BeamAggregate {
    /// `ingest-knowledge`: a new document. The response carries its id
    /// (the client's, or one assigned here).
    ///
    /// # Errors
    /// `BEM-502` without a source; `BEM-004` with only a `source_url`.
    pub async fn ingest_knowledge(&mut self, p: IngestKnowledgePayload) -> RiverbaseResult<Value> {
        require_source(
            p.text.as_deref(),
            p.media_id.as_deref(),
            p.source_url.as_deref(),
        )?;
        let document_id = p
            .document_id
            .map(|id| id.trim().to_owned())
            .filter(|id| !id.is_empty())
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        let document = Document {
            title: p.title,
            scope: p.scope,
            kind: p.document_type,
            agent_id: p.agent_id,
            user_id: p.user_id,
            text: p.text,
            media_id: p.media_id,
            source_url: p.source_url,
            filename: p.filename,
            mime_type: p.mime_type,
            metadata: p.metadata,
        };
        self.queue_ingest(document, document_id).await
    }

    /// `reindex-knowledge`: ingest an existing document again.
    ///
    /// # Errors
    /// `BEM-003` for a blank `document_id` (a blank one would ingest a new
    /// document); `BEM-502` without a source; `BEM-004` with only a
    /// `source_url`.
    pub async fn reindex_knowledge(
        &mut self,
        p: ReindexKnowledgePayload,
    ) -> RiverbaseResult<Value> {
        let document_id = p.document_id.trim().to_owned();
        if document_id.is_empty() {
            return Err(beam_core::BEM_003.with_data("document_id is required"));
        }
        require_source(
            p.text.as_deref(),
            p.media_id.as_deref(),
            p.source_url.as_deref(),
        )?;
        let document = Document {
            title: p.title,
            scope: p.scope,
            kind: p.document_type,
            agent_id: p.agent_id,
            user_id: p.user_id,
            text: p.text,
            media_id: p.media_id,
            source_url: p.source_url,
            filename: p.filename,
            mime_type: p.mime_type,
            metadata: p.metadata,
        };
        self.queue_ingest(document, document_id).await
    }

    /// Queue `document`'s ingest in the caller's org; the answer carries the
    /// document id.
    async fn queue_ingest(
        &mut self,
        document: Document,
        document_id: String,
    ) -> RiverbaseResult<Value> {
        let caller = self.caller();
        let task = document.task(&document_id, caller.org_id.as_deref());
        let task_id = self.enqueue(task).await?;
        let mut out = queued(task_id, "ingest_knowledge_document");
        out["document_id"] = json!(document_id);
        Ok(out)
    }

    /// `delete-knowledge`: delete a document and its chunks.
    ///
    /// # Errors
    /// Store errors.
    pub async fn delete_knowledge(&mut self, p: DeleteKnowledgePayload) -> RiverbaseResult<Value> {
        let caller = self.caller();
        let task = NewTask::new(
            "delete_knowledge_document",
            json!({ "document_id": p.document_id, "org_id": caller.org_id }),
        );
        let task_id = self.enqueue(task).await?;
        Ok(queued(task_id, "delete_knowledge_document"))
    }
}
