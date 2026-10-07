use std::sync::Arc;

use uuid::Uuid;

use crate::builder::{IngestionResult, KnowledgeBuilder};
use crate::embedder::Embedder;
use riverbase_core::RiverbaseResult;
use crate::retriever::{DenseRetriever, Retriever};
use crate::store::VectorStore;
use crate::types::{Hit, KnowledgeScope, Namespace, RetrievalFilter};

/// Bundles embedder + store + retriever + builder (parity with `KnowledgeManager`).
pub struct KnowledgeManager {
    store: Arc<dyn VectorStore>,
    retriever: Arc<dyn Retriever>,
    builder: KnowledgeBuilder,
}

impl KnowledgeManager {
    #[must_use]
    pub fn new(embedder: Arc<dyn Embedder>, store: Arc<dyn VectorStore>) -> Self {
        let retriever = Arc::new(DenseRetriever::new(embedder.clone(), store.clone()));
        let builder = KnowledgeBuilder::new(embedder, store.clone());
        Self {
            store,
            retriever,
            builder,
        }
    }

    pub async fn ingest(
        &self,
        text: &str,
        document_id: Option<Uuid>,
        scope: Option<KnowledgeScope>,
        agent_id: Option<String>,
        user_id: Option<String>,
        source: Option<String>,
    ) -> RiverbaseResult<IngestionResult> {
        self.builder
            .ingest(text, document_id, scope, agent_id, user_id, source)
            .await
    }

    pub async fn delete_document(&self, document_id: Uuid) -> RiverbaseResult<usize> {
        self.builder.delete(document_id).await
    }

    pub async fn retrieve(
        &self,
        query: &str,
        k: usize,
        filters: Option<&RetrievalFilter>,
    ) -> RiverbaseResult<Vec<Hit>> {
        self.retriever
            .retrieve(query, k, filters, Namespace::Knowledge)
            .await
    }

    #[must_use]
    pub fn store(&self) -> &Arc<dyn VectorStore> {
        &self.store
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::embedder::HashEmbedder;
    use crate::store::InMemoryVectorStore;

    fn manager() -> KnowledgeManager {
        KnowledgeManager::new(
            Arc::new(HashEmbedder::new(128)),
            Arc::new(InMemoryVectorStore::new()),
        )
    }

    #[tokio::test]
    async fn ingest_then_retrieve_returns_relevant_chunk() {
        let manager = manager();
        let doc = "Polars is a fast DataFrame library.\n\nRust is a systems programming language.\n\nThe capital of France is Paris.";
        let result = manager.ingest(doc, None, Some(KnowledgeScope::system()), None, None, None)
            .await
            .unwrap();
        assert!(result.chunk_count >= 1);

        let hits = manager.retrieve("what is the capital of France", 1, None).await.unwrap();
        assert_eq!(hits.len(), 1);
        assert!(hits[0].text.to_lowercase().contains("paris"));
    }

    #[tokio::test]
    async fn delete_removes_document_chunks() {
        let manager = manager();
        let result = manager
            .ingest("alpha beta gamma", None, None, None, None, None)
            .await
            .unwrap();
        let removed = manager.delete_document(result.document_id).await.unwrap();
        assert!(removed >= 1);
        let hits = manager.retrieve("alpha", 5, None).await.unwrap();
        assert!(hits.is_empty());
    }
}
