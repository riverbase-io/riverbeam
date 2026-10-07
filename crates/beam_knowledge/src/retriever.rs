use std::sync::Arc;

use async_trait::async_trait;

use crate::embedder::Embedder;
use riverbase_core::RiverbaseResult;
use crate::store::VectorStore;
use crate::types::{Hit, Namespace, RetrievalFilter};

/// Embed a query and return top-k hits.
#[async_trait]
pub trait Retriever: Send + Sync {
    async fn retrieve(
        &self,
        query: &str,
        k: usize,
        filters: Option<&RetrievalFilter>,
        namespace: Namespace,
    ) -> RiverbaseResult<Vec<Hit>>;
}

/// Dense (single-vector cosine) retriever over an [`Embedder`] + [`VectorStore`].
pub struct DenseRetriever {
    embedder: Arc<dyn Embedder>,
    store: Arc<dyn VectorStore>,
}

impl DenseRetriever {
    #[must_use]
    pub fn new(embedder: Arc<dyn Embedder>, store: Arc<dyn VectorStore>) -> Self {
        Self { embedder, store }
    }
}

#[async_trait]
impl Retriever for DenseRetriever {
    async fn retrieve(
        &self,
        query: &str,
        k: usize,
        filters: Option<&RetrievalFilter>,
        namespace: Namespace,
    ) -> RiverbaseResult<Vec<Hit>> {
        let vector = self.embedder.embed_query(query).await?;
        self.store.query(&vector, k, filters, namespace).await
    }
}
