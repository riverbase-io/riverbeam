use std::sync::RwLock;

use async_trait::async_trait;

use riverbase_core::RiverbaseResult;
use crate::types::{Hit, Namespace, RetrievalFilter, VectorRecord};

/// Stores and queries embedding vectors.
#[async_trait]
pub trait VectorStore: Send + Sync {
    async fn upsert(&self, records: Vec<VectorRecord>) -> RiverbaseResult<()>;
    async fn query(
        &self,
        vector: &[f32],
        k: usize,
        filters: Option<&RetrievalFilter>,
        namespace: Namespace,
    ) -> RiverbaseResult<Vec<Hit>>;
    async fn delete(
        &self,
        ids: Option<&[String]>,
        document_id: Option<&str>,
        namespace: Namespace,
    ) -> RiverbaseResult<usize>;

    /// Atomically replace all chunks for `document_id`.
    async fn replace_document_chunks(
        &self,
        document_id: &str,
        records: Vec<VectorRecord>,
        namespace: Namespace,
    ) -> RiverbaseResult<usize> {
        let deleted = self.delete(None, Some(document_id), namespace).await?;
        if !records.is_empty() {
            self.upsert(records).await?;
        }
        Ok(deleted)
    }
}

/// In-memory cosine-similarity vector store (pgvector stand-in).
#[derive(Default)]
pub struct InMemoryVectorStore {
    records: RwLock<Vec<VectorRecord>>,
}

impl InMemoryVectorStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.records.read().map(|r| r.len()).unwrap_or(0)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn cosine(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() {
        return 0.0;
    }
    let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    let na: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let nb: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if na == 0.0 || nb == 0.0 {
        0.0
    } else {
        dot / (na * nb)
    }
}

#[async_trait]
impl VectorStore for InMemoryVectorStore {
    async fn upsert(&self, records: Vec<VectorRecord>) -> RiverbaseResult<()> {
        let mut store = self
            .records
            .write()
            .map_err(|e| crate::BEM_200.with_data(e.to_string()))?;
        for record in records {
            if let Some(existing) = store.iter_mut().find(|r| r.id == record.id) {
                *existing = record;
            } else {
                store.push(record);
            }
        }
        Ok(())
    }

    async fn query(
        &self,
        vector: &[f32],
        k: usize,
        filters: Option<&RetrievalFilter>,
        namespace: Namespace,
    ) -> RiverbaseResult<Vec<Hit>> {
        let store = self
            .records
            .read()
            .map_err(|e| crate::BEM_201.with_data(e.to_string()))?;
        let mut scored: Vec<Hit> = store
            .iter()
            .filter(|r| r.namespace == namespace)
            .filter(|r| filters.map_or(true, |f| f.matches(r)))
            .map(|r| Hit {
                id: r.id.clone(),
                text: r.text.clone(),
                score: cosine(vector, &r.embedding),
                metadata: r.metadata.clone(),
                document_id: r.document_id.clone(),
                chunk_index: r.chunk_index,
                source: r.source.clone(),
                scope: r.scope.clone(),
            })
            .collect();
        scored.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(k);
        Ok(scored)
    }

    async fn delete(
        &self,
        ids: Option<&[String]>,
        document_id: Option<&str>,
        namespace: Namespace,
    ) -> RiverbaseResult<usize> {
        let mut store = self
            .records
            .write()
            .map_err(|e| crate::BEM_202.with_data(e.to_string()))?;
        let before = store.len();
        store.retain(|r| {
            if r.namespace != namespace {
                return true;
            }
            if let Some(ids) = ids {
                if ids.iter().any(|i| i == &r.id) {
                    return false;
                }
            }
            if let Some(doc) = document_id {
                if r.document_id.as_deref() == Some(doc) {
                    return false;
                }
            }
            true
        });
        Ok(before - store.len())
    }
}
