use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use crate::chunker::{Chunker, SimpleChunker};
use crate::embedder::Embedder;
use riverbase_core::RiverbaseResult;
use crate::stats::{ingest_stats, IngestStats};
use crate::store::VectorStore;
use crate::types::{KnowledgeScope, Namespace, VectorRecord};

/// Outcome of an ingest run (parity with Python `IngestionResult`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IngestionResult {
    pub document_id: Uuid,
    pub chunk_count: usize,
    pub embedder_name: String,
    pub stats: IngestStats,
}

/// Parse → chunk → embed → upsert pipeline.
pub struct KnowledgeBuilder {
    embedder: Arc<dyn Embedder>,
    store: Arc<dyn VectorStore>,
    chunker: Box<dyn Chunker>,
}

impl KnowledgeBuilder {
    #[must_use]
    pub fn new(embedder: Arc<dyn Embedder>, store: Arc<dyn VectorStore>) -> Self {
        Self {
            embedder,
            store,
            chunker: Box::new(SimpleChunker::default()),
        }
    }

    #[must_use]
    pub fn with_chunker(mut self, chunker: Box<dyn Chunker>) -> Self {
        self.chunker = chunker;
        self
    }

    /// Ingest a text document: chunk, embed, and replace its chunks in the store.
    pub async fn ingest(
        &self,
        text: &str,
        document_id: Option<Uuid>,
        scope: Option<KnowledgeScope>,
        agent_id: Option<String>,
        user_id: Option<String>,
        source: Option<String>,
    ) -> RiverbaseResult<IngestionResult> {
        let document_id = document_id.unwrap_or_else(Uuid::new_v4);
        let chunks = self.chunker.chunk(text);
        if chunks.is_empty() {
            return Err(crate::BEM_230.raise());
        }
        let stats = ingest_stats(&chunks);

        let texts: Vec<String> = chunks.iter().map(|c| c.text.clone()).collect();
        let embeddings = self.embedder.embed(&texts).await?;
        let embedder_name = self.embedder.name();

        let records: Vec<VectorRecord> = chunks
            .iter()
            .zip(embeddings)
            .map(|(chunk, embedding)| VectorRecord {
                id: format!("{document_id}:{}", chunk.chunk_index),
                namespace: Namespace::Knowledge,
                text: chunk.text.clone(),
                embedding,
                metadata: json!({ "chunk_index": chunk.chunk_index }),
                scope: scope.clone(),
                agent_id: agent_id.clone(),
                user_id: user_id.clone(),
                document_id: Some(document_id.to_string()),
                chunk_index: Some(chunk.chunk_index),
                source: source.clone(),
                token_count: chunk.token_count,
                embedder_name: Some(embedder_name.clone()),
            })
            .collect();

        self.store
            .replace_document_chunks(
                &document_id.to_string(),
                records,
                Namespace::Knowledge,
            )
            .await?;

        Ok(IngestionResult {
            document_id,
            chunk_count: chunks.len(),
            embedder_name,
            stats,
        })
    }

    pub async fn delete(&self, document_id: Uuid) -> RiverbaseResult<usize> {
        self.store
            .delete(None, Some(&document_id.to_string()), Namespace::Knowledge)
            .await
    }
}
