//! Ingestion: parse → chunk → embed → persist (Python `knowledge/ingest.py`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::Utc;
use riverbase_core::RiverbaseResult;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::chunker::{Chunker, RecursiveChunker};
use crate::embedder::Embedder;
use crate::parsers::{AutoParser, DocumentParser, ParseOptions, Source};
use crate::stats::{ingest_stats, IngestStats};
use crate::store::VectorStore;
use crate::types::{Chunk, DocumentStatus, KnowledgeScope, Namespace, VectorRecord};

/// Python `IngestionResult.to_dict()`; `stats` is a Rust extra and not serialized.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IngestionResult {
    pub document_id: String,
    pub chunk_count: usize,
    pub status: DocumentStatus,
    pub skipped: bool,
    pub attempt_count: u32,
    #[serde(skip)]
    pub stats: Option<IngestStats>,
}

impl IngestionResult {
    fn new(document_id: String, chunk_count: usize, status: DocumentStatus) -> Self {
        Self {
            document_id,
            chunk_count,
            status,
            skipped: false,
            attempt_count: 0,
            stats: None,
        }
    }

    fn skipped(document_id: String, chunk_count: usize) -> Self {
        Self {
            skipped: true,
            ..Self::new(document_id, chunk_count, DocumentStatus::Indexed)
        }
    }
}

/// Python `KnowledgeBuilder.ingest` keyword arguments.
#[derive(Debug, Clone)]
pub struct IngestRequest {
    pub source: Source,
    pub title: String,
    pub scope: KnowledgeScope,
    pub doc_type: String,
    pub agent_id: Option<String>,
    pub user_id: Option<String>,
    pub org_id: Option<String>,
    pub document_id: Option<String>,
    pub metadata: Map<String, Value>,
    pub mime_type: Option<String>,
    pub filename: Option<String>,
    pub source_label: Option<String>,
    pub embedding_revision: Option<String>,
}

impl IngestRequest {
    /// Inline text with Python's defaults (global scope, type `text`).
    #[must_use]
    pub fn text(text: impl Into<String>, title: impl Into<String>) -> Self {
        Self {
            source: Source::Text(text.into()),
            title: title.into(),
            scope: KnowledgeScope::Global,
            doc_type: "text".into(),
            agent_id: None,
            user_id: None,
            org_id: None,
            document_id: None,
            metadata: Map::new(),
            mime_type: None,
            filename: None,
            source_label: None,
            embedding_revision: None,
        }
    }
}

/// An `agent_knowledge_document` row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DocumentRow {
    pub id: String,
    pub title: String,
    pub scope: KnowledgeScope,
    pub doc_type: String,
    pub agent_id: Option<String>,
    pub user_id: Option<String>,
    pub org_id: Option<String>,
    pub source: Option<String>,
    pub body: Option<String>,
    pub media_id: Option<String>,
    pub metadata: Map<String, Value>,
    pub status: DocumentStatus,
    pub content_hash: Option<String>,
    pub chunk_count: usize,
    pub embedder_name: Option<String>,
}

/// The document rows beside the chunks.
#[async_trait]
pub trait DocumentRecords: Send + Sync {
    /// # Errors
    /// Store errors.
    async fn upsert_document(&self, row: &DocumentRow) -> RiverbaseResult<()>;

    /// The best match for ingest dedupe: same scope, owners (NULL-safe) and
    /// body hash, an `INDEXED` row first.
    ///
    /// # Errors
    /// Store errors.
    async fn find_by_fingerprint(
        &self,
        scope: KnowledgeScope,
        agent_id: Option<&str>,
        user_id: Option<&str>,
        org_id: Option<&str>,
        content_hash: &str,
    ) -> RiverbaseResult<Option<DocumentRow>>;

    /// The row with this id and body hash.
    ///
    /// # Errors
    /// Store errors.
    async fn find(
        &self,
        document_id: &str,
        content_hash: &str,
    ) -> RiverbaseResult<Option<DocumentRow>>;

    /// # Errors
    /// Store errors.
    async fn delete_document(&self, document_id: &str) -> RiverbaseResult<()>;

    /// Python reconcile's `_fail_stuck_indexing`: documents `INDEXING` for
    /// longer than `minutes` become `FAILED`. Returns how many.
    ///
    /// # Errors
    /// Store errors.
    async fn fail_stuck_indexing(&self, minutes: i64) -> RiverbaseResult<u64> {
        let _ = minutes;
        Ok(0)
    }
}

/// The sha256 of a document's text, as hex.
#[must_use]
pub fn hash_text(text: &str) -> String {
    format!("{:x}", Sha256::digest(text.as_bytes()))
}

fn normalize(id: Option<&str>) -> Option<String> {
    let text = id?.trim();
    if text.is_empty()
        || text.eq_ignore_ascii_case("none")
        || text.eq_ignore_ascii_case("null")
        || text == Uuid::nil().to_string()
    {
        return None;
    }
    Some(text.to_owned())
}

/// Python `KnowledgeBuilder`. Without explicit `document_id`, the same body
/// under the same scope and owners resolves to the existing document: an
/// `INDEXED` one is a no-op, a stuck or failed one is repaired in place.
pub struct KnowledgeBuilder {
    embedder: Arc<dyn Embedder>,
    store: Arc<dyn VectorStore>,
    records: Option<Arc<dyn DocumentRecords>>,
    parser: Arc<dyn DocumentParser>,
    chunker: Arc<dyn Chunker>,
    attempts: Mutex<HashMap<String, (u32, DocumentStatus)>>,
}

impl KnowledgeBuilder {
    #[must_use]
    pub fn new(embedder: Arc<dyn Embedder>, store: Arc<dyn VectorStore>) -> Self {
        Self {
            embedder,
            store,
            records: None,
            parser: Arc::new(AutoParser::default()),
            chunker: Arc::new(RecursiveChunker::default()),
            attempts: Mutex::new(HashMap::new()),
        }
    }

    #[must_use]
    pub fn with_records(mut self, records: Arc<dyn DocumentRecords>) -> Self {
        self.records = Some(records);
        self
    }

    #[must_use]
    pub fn with_parser(mut self, parser: Arc<dyn DocumentParser>) -> Self {
        self.parser = parser;
        self
    }

    #[must_use]
    pub fn with_chunker(mut self, chunker: Arc<dyn Chunker>) -> Self {
        self.chunker = chunker;
        self
    }

    pub fn set_embedder(&mut self, embedder: Arc<dyn Embedder>) {
        self.embedder = embedder;
    }

    /// Ingest attempts made for `document_id` by this builder.
    #[must_use]
    pub fn attempt_count(&self, document_id: &str) -> u32 {
        self.attempts
            .lock()
            .ok()
            .and_then(|a| a.get(document_id).map(|(n, _)| *n))
            .unwrap_or(0)
    }

    #[must_use]
    pub fn document_status(&self, document_id: &str) -> Option<DocumentStatus> {
        self.attempts
            .lock()
            .ok()
            .and_then(|a| a.get(document_id).map(|(_, s)| *s))
    }

    fn bump_attempt(&self, document_id: &str, status: DocumentStatus) -> u32 {
        let Ok(mut attempts) = self.attempts.lock() else {
            return 0;
        };
        let entry = attempts
            .entry(document_id.to_owned())
            .or_insert((0, status));
        entry.0 += 1;
        entry.1 = status;
        entry.0
    }

    fn set_status(&self, document_id: &str, status: DocumentStatus) {
        if let Ok(mut attempts) = self.attempts.lock() {
            attempts
                .entry(document_id.to_owned())
                .or_insert((0, status))
                .1 = status;
        }
    }

    /// Run the pipeline for `request`.
    ///
    /// # Errors
    /// Parser, embedder and store errors; a failed embed or write marks the
    /// document `FAILED` first.
    #[allow(clippy::too_many_lines)]
    pub async fn ingest(&self, request: IngestRequest) -> RiverbaseResult<IngestionResult> {
        let mut opts = ParseOptions {
            mime_type: request.mime_type.clone(),
            ..ParseOptions::default()
        };
        if let Some(filename) = &request.filename {
            opts.metadata
                .insert("filename".into(), Value::String(filename.clone()));
        }
        let blocks = self.parser.parse(&request.source, &opts).await?;
        // Chunking tokenizes the whole document (tiktoken) and hashing reads
        // it all: CPU work kept off the async threads, which the API shares
        // when the server runs the work queue too. A panic stays a panic.
        let chunker = Arc::clone(&self.chunker);
        let (chunks, body, content_hash) = tokio::task::spawn_blocking(move || {
            let chunks = chunker.chunk(&blocks);
            let body = blocks
                .iter()
                .map(|b| b.text.as_str())
                .collect::<Vec<_>>()
                .join("\n\n");
            let content_hash = hash_text(&body);
            (chunks, body, content_hash)
        })
        .await
        .unwrap_or_else(|e| std::panic::resume_unwind(e.into_panic()));
        let mut metadata = request.metadata.clone();

        let document_id = match request.document_id.clone() {
            Some(id) => id,
            None => match self.fingerprint(&request, &content_hash).await? {
                Some((id, count, true)) => return Ok(IngestionResult::skipped(id, count)),
                Some((id, _, false)) => id,
                None => Uuid::new_v4().to_string(),
            },
        };
        let document = |status: DocumentStatus,
                        chunk_count: usize,
                        metadata: &Map<String, Value>| DocumentRow {
            id: document_id.clone(),
            title: request.title.clone(),
            scope: request.scope,
            doc_type: request.doc_type.clone(),
            agent_id: request.agent_id.clone(),
            user_id: normalize(request.user_id.as_deref()),
            org_id: request.org_id.clone(),
            source: request.source_label.clone(),
            body: Some(body.clone()),
            media_id: match &request.source {
                Source::Media { media_id, .. } => Some(media_id.clone()),
                _ => None,
            },
            metadata: metadata.clone(),
            status,
            content_hash: Some(content_hash.clone()),
            chunk_count,
            embedder_name: Some(self.embedder.name()),
        };

        if chunks.is_empty() {
            tracing::warn!(title = %request.title, "ingest produced 0 chunks");
            let attempt = self.bump_attempt(&document_id, DocumentStatus::Failed);
            self.write(&document(DocumentStatus::Failed, 0, &metadata))
                .await?;
            return Ok(IngestionResult {
                attempt_count: attempt,
                ..IngestionResult::new(document_id, 0, DocumentStatus::Failed)
            });
        }
        // Skip fast: the same body is already indexed under this id.
        if let Some(records) = &self.records {
            if let Some(existing) = records.find(&document_id, &content_hash).await? {
                if existing.status == DocumentStatus::Indexed {
                    return Ok(IngestionResult::skipped(existing.id, existing.chunk_count));
                }
            }
        }

        let attempt = self.bump_attempt(&document_id, DocumentStatus::Indexing);
        metadata.insert("attempt_count".into(), Value::from(attempt));
        if let Some(revision) = &request.embedding_revision {
            metadata.insert("embedding_revision".into(), Value::String(revision.clone()));
        }
        self.write(&document(DocumentStatus::Indexing, chunks.len(), &metadata))
            .await?;

        let indexed = async {
            let texts: Vec<String> = chunks.iter().map(|c| c.text.clone()).collect();
            let embeddings = self.embedder.embed(&texts).await?;
            let records =
                self.build_records(&chunks, embeddings, &document_id, &request, &content_hash);
            let count = records.len();
            self.store
                .replace_document_chunks(&document_id, records, Namespace::Knowledge)
                .await?;
            self.write(&document(DocumentStatus::Indexed, count, &metadata))
                .await?;
            Ok::<usize, riverbase_core::RiverbaseError>(count)
        }
        .await;
        match indexed {
            Ok(count) => {
                self.set_status(&document_id, DocumentStatus::Indexed);
                Ok(IngestionResult {
                    attempt_count: attempt,
                    stats: Some(ingest_stats(&chunks)),
                    ..IngestionResult::new(document_id, count, DocumentStatus::Indexed)
                })
            }
            Err(e) => {
                tracing::error!(document_id = %document_id, error = %e, "ingest failed");
                self.set_status(&document_id, DocumentStatus::Failed);
                if let Err(write) = self
                    .write(&document(DocumentStatus::Failed, 0, &metadata))
                    .await
                {
                    tracing::warn!(document_id = %document_id, error = %write, "marking the document failed failed");
                }
                Err(e)
            }
        }
    }

    /// Delete the document's chunks and its row; returns the chunks removed.
    ///
    /// # Errors
    /// Store errors.
    pub async fn delete(&self, document_id: &str) -> RiverbaseResult<usize> {
        let deleted = self
            .store
            .delete(None, Some(document_id), Namespace::Knowledge)
            .await?;
        if let Some(records) = &self.records {
            records.delete_document(document_id).await?;
        }
        Ok(deleted)
    }

    async fn write(&self, row: &DocumentRow) -> RiverbaseResult<()> {
        match &self.records {
            Some(records) => records.upsert_document(row).await,
            None => Ok(()),
        }
    }

    /// `(id, chunk count, indexed)`.
    async fn fingerprint(
        &self,
        request: &IngestRequest,
        content_hash: &str,
    ) -> RiverbaseResult<Option<(String, usize, bool)>> {
        let user = normalize(request.user_id.as_deref());
        // Python matches scope, agent and user; Rust also takes `org_id`
        // on ingest, so the org is part of the fingerprint too.
        let org = normalize(request.org_id.as_deref());
        if let Some(records) = &self.records {
            let row = records
                .find_by_fingerprint(
                    request.scope,
                    request.agent_id.as_deref(),
                    user.as_deref(),
                    org.as_deref(),
                    content_hash,
                )
                .await?;
            return Ok(row.map(|r| {
                let indexed = r.status == DocumentStatus::Indexed;
                (r.id, r.chunk_count, indexed)
            }));
        }
        let found = self
            .store
            .lookup_document_by_body_hash(
                request.scope,
                request.agent_id.as_deref(),
                user.as_deref(),
                org.as_deref(),
                content_hash,
            )
            .await?;
        Ok(found.filter(|(_, n)| *n > 0).map(|(id, n)| (id, n, true)))
    }

    /// Python `_build_records`.
    fn build_records(
        &self,
        chunks: &[Chunk],
        embeddings: Vec<Vec<f32>>,
        document_id: &str,
        request: &IngestRequest,
        body_hash: &str,
    ) -> Vec<VectorRecord> {
        let ingested_at = crate::longterm::iso(Utc::now());
        chunks
            .iter()
            .zip(embeddings)
            .map(|(chunk, embedding)| {
                let mut metadata = chunk.metadata.clone();
                metadata.insert("ingested_at".into(), json!(ingested_at));
                metadata.insert("knowledge_body_hash".into(), json!(body_hash));
                if let Some(revision) = &request.embedding_revision {
                    metadata.insert("embedding_revision".into(), json!(revision));
                }
                let mut record = VectorRecord::new(
                    Uuid::new_v4().to_string(),
                    Namespace::Knowledge,
                    chunk.text.clone(),
                    embedding,
                );
                record.metadata = metadata;
                record.scope = Some(request.scope);
                record.agent_id.clone_from(&request.agent_id);
                record.user_id = normalize(request.user_id.as_deref());
                record.org_id = normalize(request.org_id.as_deref());
                record.document_id = Some(document_id.to_owned());
                record.chunk_index = Some(chunk.chunk_index);
                record.source.clone_from(&request.source_label);
                record.content_hash = Some(hash_text(&chunk.text));
                record.token_count = chunk.token_count;
                record.embedder_name = Some(self.embedder.name());
                record
            })
            .collect()
    }
}
