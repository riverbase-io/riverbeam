//! `KnowledgeManager`: the builder, retriever, long-term memory and embedding
//! revisions behind one API (Python `knowledge/manager.py`).

use std::sync::Arc;

use riverbase_core::RiverbaseResult;
use serde_json::Value;

use crate::builder::{DocumentRecords, IngestRequest, IngestionResult, KnowledgeBuilder};
use crate::chunker::{Chunker, RecursiveChunker};
use crate::embedder::Embedder;
use crate::longterm::{LongTermMemoryStore, RecallRequest, RememberRequest};
use crate::provenance::EmbeddingRevisionBook;
use crate::retriever::{DenseRetriever, Retriever};
use crate::store::VectorStore;
use crate::types::{Hit, Namespace, RetrievalFilter};

pub struct KnowledgeManager {
    embedder: Arc<dyn Embedder>,
    store: Arc<dyn VectorStore>,
    retriever: Arc<dyn Retriever>,
    builder: KnowledgeBuilder,
    records: Option<Arc<dyn DocumentRecords>>,
    chunker: Arc<dyn Chunker>,
    memory: LongTermMemoryStore,
    revisions: Arc<EmbeddingRevisionBook>,
}

impl KnowledgeManager {
    /// Dense retrieval, no document rows (Python without a store manager).
    #[must_use]
    pub fn new(embedder: Arc<dyn Embedder>, store: Arc<dyn VectorStore>) -> Self {
        Self {
            retriever: Arc::new(DenseRetriever::new(embedder.clone(), store.clone())),
            builder: KnowledgeBuilder::new(embedder.clone(), store.clone()),
            records: None,
            chunker: Arc::new(RecursiveChunker::default()),
            memory: LongTermMemoryStore::new(embedder.clone(), store.clone()),
            revisions: Arc::new(EmbeddingRevisionBook::default()),
            embedder,
            store,
        }
    }

    #[must_use]
    pub fn with_retriever(mut self, retriever: Arc<dyn Retriever>) -> Self {
        self.retriever = retriever;
        self
    }

    /// Keep `agent_knowledge_document` rows (Python with a store manager).
    #[must_use]
    pub fn with_records(mut self, records: Arc<dyn DocumentRecords>) -> Self {
        self.records = Some(records);
        self.rebuild();
        self
    }

    #[must_use]
    pub fn with_chunker(mut self, chunker: Arc<dyn Chunker>) -> Self {
        self.chunker = chunker;
        self.rebuild();
        self
    }

    #[must_use]
    pub fn document_records(&self) -> Option<Arc<dyn DocumentRecords>> {
        self.records.clone()
    }

    fn rebuild(&mut self) {
        let builder = KnowledgeBuilder::new(self.embedder.clone(), self.store.clone())
            .with_chunker(self.chunker.clone());
        self.builder = match &self.records {
            Some(records) => builder.with_records(records.clone()),
            None => builder,
        };
    }

    #[must_use]
    pub fn with_revisions(mut self, revisions: Arc<EmbeddingRevisionBook>) -> Self {
        self.revisions = revisions;
        self
    }

    /// Switch embedders (a new revision): ingest and query use it from now on.
    /// The retriever becomes dense; set a hybrid one again with
    /// [`Self::with_retriever`].
    pub fn set_embedder(&mut self, embedder: Arc<dyn Embedder>) {
        self.builder.set_embedder(embedder.clone());
        self.memory = LongTermMemoryStore::new(embedder.clone(), self.store.clone());
        self.retriever = Arc::new(DenseRetriever::new(embedder.clone(), self.store.clone()));
        self.embedder = embedder;
    }

    #[must_use]
    pub fn embedder(&self) -> &Arc<dyn Embedder> {
        &self.embedder
    }

    #[must_use]
    pub fn store(&self) -> &Arc<dyn VectorStore> {
        &self.store
    }

    #[must_use]
    pub fn builder(&self) -> &KnowledgeBuilder {
        &self.builder
    }

    #[must_use]
    pub fn memory_store(&self) -> &LongTermMemoryStore {
        &self.memory
    }

    #[must_use]
    pub fn revisions(&self) -> &Arc<EmbeddingRevisionBook> {
        &self.revisions
    }

    /// Ingest under the active embedding revision unless one is given.
    ///
    /// # Errors
    /// Ingest errors.
    pub async fn ingest(&self, mut request: IngestRequest) -> RiverbaseResult<IngestionResult> {
        if request.embedding_revision.is_none() {
            request.embedding_revision = self.revisions.active().map(|r| r.name);
        }
        self.builder.ingest(request).await
    }

    /// # Errors
    /// Store errors.
    pub async fn delete_document(&self, document_id: &str) -> RiverbaseResult<usize> {
        self.builder.delete(document_id).await
    }

    /// The caller's identity becomes the visibility filter,
    /// restricted to the active embedding revision.
    ///
    /// # Errors
    /// Embedder and store errors.
    pub async fn retrieve(
        &self,
        query: &str,
        k: usize,
        mut filters: RetrievalFilter,
    ) -> RiverbaseResult<Vec<Hit>> {
        let blank = |v: Option<String>| v.filter(|s| !s.trim().is_empty());
        filters.user_id = blank(filters.user_id);
        filters.org_id = blank(filters.org_id);
        filters.agent_id = blank(filters.agent_id);
        if let Some(active) = self.revisions.active() {
            filters
                .extra
                .insert("embedding_revision".into(), Value::String(active.name));
        }
        self.retriever
            .retrieve(query, k, Some(&filters), Namespace::Knowledge)
            .await
    }

    /// # Errors
    /// As [`LongTermMemoryStore::remember`].
    pub async fn remember(&self, request: RememberRequest) -> RiverbaseResult<String> {
        self.memory.remember(request).await
    }

    /// # Errors
    /// As [`LongTermMemoryStore::recall`].
    pub async fn recall(&self, request: &RecallRequest) -> RiverbaseResult<Vec<Hit>> {
        self.memory.recall(request).await
    }

    /// # Errors
    /// Store errors.
    pub async fn forget(&self, memory_id: &str) -> RiverbaseResult<usize> {
        self.memory.forget(memory_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::embedder::MockEmbedder;
    use crate::provenance::EmbeddingRevision;
    use crate::store::InMemoryVectorStore;
    use crate::types::{DocumentStatus, KnowledgeScope};

    fn revision(name: &str, dim: usize, status: &str) -> EmbeddingRevision {
        EmbeddingRevision {
            name: name.into(),
            embedder_name: format!("mock:{name}"),
            embedder_version: "1".into(),
            dimension: dim,
            chunker_name: "recursive".into(),
            chunker_version: "1".into(),
            status: status.into(),
        }
    }

    #[tokio::test]
    async fn ingest_dedupes_and_delete_removes_chunks() {
        let store = Arc::new(InMemoryVectorStore::new());
        let manager = KnowledgeManager::new(Arc::new(MockEmbedder::new(32)), store.clone());
        let first = manager
            .ingest(IngestRequest::text(
                "Paris is the capital of France.",
                "geo",
            ))
            .await
            .unwrap();
        assert_eq!(
            (first.status, first.chunk_count, first.attempt_count),
            (DocumentStatus::Indexed, 1, 1)
        );
        let again = manager
            .ingest(IngestRequest::text(
                "Paris is the capital of France.",
                "geo",
            ))
            .await
            .unwrap();
        assert!(again.skipped);
        assert_eq!(again.document_id, first.document_id);
        assert_eq!(store.len(), 1);

        let hits = manager
            .retrieve("capital of France", 3, RetrievalFilter::default())
            .await
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(
            manager.delete_document(&first.document_id).await.unwrap(),
            1
        );
        assert!(store.is_empty());
    }

    #[tokio::test]
    async fn empty_document_fails_with_an_attempt() {
        let manager = KnowledgeManager::new(
            Arc::new(MockEmbedder::new(32)),
            Arc::new(InMemoryVectorStore::new()),
        );
        let result = manager
            .ingest(IngestRequest::text("   ", "blank"))
            .await
            .unwrap();
        assert_eq!(
            (result.status, result.attempt_count),
            (DocumentStatus::Failed, 1)
        );
    }

    /// Python `test_activate_revision_filters_old_vectors`.
    #[tokio::test]
    async fn retrieval_reads_only_the_active_revision() {
        let store = Arc::new(InMemoryVectorStore::new());
        let mut manager = KnowledgeManager::new(
            Arc::new(MockEmbedder::new(8).with_model("small")),
            store.clone(),
        );
        manager.revisions().register(revision("v1", 8, "active"));
        manager
            .ingest(IngestRequest {
                source_label: Some("a.md".into()),
                ..IngestRequest::text("alpha document about invoices", "a")
            })
            .await
            .unwrap();
        manager.revisions().register(revision("v2", 16, "indexing"));
        manager.set_embedder(Arc::new(MockEmbedder::new(16).with_model("large")));
        manager
            .ingest(IngestRequest {
                source_label: Some("b.md".into()),
                embedding_revision: Some("v2".into()),
                scope: KnowledgeScope::Global,
                ..IngestRequest::text("beta document about invoices", "b")
            })
            .await
            .unwrap();
        // Until v2 is active, retrieval stays on v1 (8-dim vectors).
        let before = manager
            .retrieve("invoices", 6, RetrievalFilter::default())
            .await
            .unwrap();
        assert!(before
            .iter()
            .all(|h| h.metadata["embedding_revision"] == "v1"));
        manager.revisions().activate("v2");
        let hits = manager
            .retrieve("invoices", 6, RetrievalFilter::default())
            .await
            .unwrap();
        assert!(!hits.is_empty());
        assert!(hits.iter().all(
            |h| h.metadata["embedding_revision"] == "v2" && h.source.as_deref() == Some("b.md")
        ));
        let widths: std::collections::HashSet<usize> = store
            .records()
            .iter()
            .filter(|r| r.metadata.get("embedding_revision") == Some(&Value::from("v2")))
            .map(|r| r.embedding.len())
            .collect();
        assert_eq!(widths, [16].into());
    }
}
