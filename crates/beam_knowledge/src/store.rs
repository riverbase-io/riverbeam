//! The vector store contract and the in-memory store (Python
//! `knowledge/contract.py` `VectorStore` and `knowledge/stores/inmemory.py`).

use std::sync::RwLock;

use async_trait::async_trait;
use riverbase_core::RiverbaseResult;

use crate::types::{Hit, KnowledgeScope, Namespace, RetrievalFilter, VectorRecord};
use crate::visibility::record_is_visible;

/// Stores and queries embedding vectors.
#[async_trait]
pub trait VectorStore: Send + Sync {
    async fn upsert(&self, records: Vec<VectorRecord>) -> RiverbaseResult<()>;

    /// Top-k nearest records visible under `filters`.
    async fn query(
        &self,
        vector: &[f32],
        k: usize,
        filters: Option<&RetrievalFilter>,
        namespace: Namespace,
    ) -> RiverbaseResult<Vec<Hit>>;

    /// Delete by `ids`, else by owning `document_id`; returns the row count.
    async fn delete(
        &self,
        ids: Option<&[String]>,
        document_id: Option<&str>,
        namespace: Namespace,
    ) -> RiverbaseResult<usize>;

    /// Replace every chunk of `document_id`; returns how many were removed.
    /// Stores that span round trips override this with one transaction.
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

    /// One record by id.
    ///
    /// # Errors
    /// `BEM-235` when the store cannot fetch by id.
    async fn get(&self, _id: &str, _namespace: Namespace) -> RiverbaseResult<Option<VectorRecord>> {
        Err(crate::BEM_235.raise())
    }

    /// Lexical search for hybrid retrieval; `None` when the store has none
    /// (Python looks up `search_lexical` with `getattr`).
    async fn search_lexical(
        &self,
        _query: &str,
        _k: usize,
        _filters: Option<&RetrievalFilter>,
        _namespace: Namespace,
    ) -> RiverbaseResult<Option<Vec<Hit>>> {
        Ok(None)
    }

    /// A knowledge document whose chunks carry `body_hash`, with its chunk
    /// count, for ingest dedupe without document rows.
    async fn lookup_document_by_body_hash(
        &self,
        _scope: KnowledgeScope,
        _agent_id: Option<&str>,
        _user_id: Option<&str>,
        _org_id: Option<&str>,
        _body_hash: &str,
    ) -> RiverbaseResult<Option<(String, usize)>> {
        Ok(None)
    }
}

/// Over the shorter length, norms of the whole vectors,
/// a zero norm counted as 1.
#[must_use]
pub fn cosine(a: &[f32], b: &[f32]) -> f64 {
    let dot: f64 = a
        .iter()
        .zip(b)
        .map(|(x, y)| f64::from(*x) * f64::from(*y))
        .sum();
    let norm = |v: &[f32]| {
        let n = v
            .iter()
            .map(|x| f64::from(*x) * f64::from(*x))
            .sum::<f64>()
            .sqrt();
        if n == 0.0 {
            1.0
        } else {
            n
        }
    };
    dot / (norm(a) * norm(b))
}

/// Insertion-ordered rows keyed by
/// `(namespace, id)`, cosine scoring, visibility filtering.
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
        self.records.read().map_or(0, |r| r.len())
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Every record, in insertion order.
    #[must_use]
    pub fn records(&self) -> Vec<VectorRecord> {
        self.records.read().map(|r| r.clone()).unwrap_or_default()
    }

    fn read(&self) -> RiverbaseResult<std::sync::RwLockReadGuard<'_, Vec<VectorRecord>>> {
        self.records
            .read()
            .map_err(|e| crate::BEM_201.with_data(e.to_string()))
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
            match store
                .iter_mut()
                .find(|r| r.namespace == record.namespace && r.id == record.id)
            {
                Some(existing) => *existing = record,
                None => store.push(record),
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
        let store = self.read()?;
        let mut scored: Vec<Hit> = store
            .iter()
            .filter(|r| r.namespace == namespace)
            // Python filters only when filters are given.
            .filter(|r| filters.is_none_or(|f| record_is_visible(r, Some(f))))
            .map(|r| Hit::from_record(r, cosine(vector, &r.embedding)))
            .collect();
        scored.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
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
            let by_id = ids.is_some_and(|ids| !ids.is_empty() && ids.contains(&r.id));
            let by_doc =
                document_id.is_some_and(|d| !d.is_empty() && r.document_id.as_deref() == Some(d));
            !(by_id || by_doc)
        });
        Ok(before - store.len())
    }

    async fn get(&self, id: &str, namespace: Namespace) -> RiverbaseResult<Option<VectorRecord>> {
        Ok(self
            .read()?
            .iter()
            .find(|r| r.namespace == namespace && r.id == id)
            .cloned())
    }

    async fn search_lexical(
        &self,
        query: &str,
        k: usize,
        filters: Option<&RetrievalFilter>,
        namespace: Namespace,
    ) -> RiverbaseResult<Option<Vec<Hit>>> {
        let needle = query.trim().to_lowercase();
        if needle.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let hits: Vec<Hit> = self
            .read()?
            .iter()
            .filter(|r| r.namespace == namespace)
            .filter(|r| filters.is_none_or(|f| record_is_visible(r, Some(f))))
            .filter(|r| r.text.to_lowercase().contains(&needle))
            .take(k)
            .map(|r| Hit::from_record(r, 1.0))
            .collect();
        Ok(Some(hits))
    }

    async fn lookup_document_by_body_hash(
        &self,
        scope: KnowledgeScope,
        agent_id: Option<&str>,
        user_id: Option<&str>,
        org_id: Option<&str>,
        body_hash: &str,
    ) -> RiverbaseResult<Option<(String, usize)>> {
        fn blank(v: Option<&str>) -> Option<&str> {
            v.filter(|s| !s.is_empty())
        }
        let store = self.read()?;
        let mut documents: Vec<&str> = store
            .iter()
            .filter(|r| r.namespace == Namespace::Knowledge && r.scope == Some(scope))
            .filter(|r| blank(r.agent_id.as_deref()) == blank(agent_id))
            .filter(|r| blank(r.user_id.as_deref()) == blank(user_id))
            .filter(|r| blank(r.org_id.as_deref()) == blank(org_id))
            .filter(|r| {
                r.metadata
                    .get("knowledge_body_hash")
                    .and_then(serde_json::Value::as_str)
                    == Some(body_hash)
            })
            .filter_map(|r| r.document_id.as_deref().filter(|d| !d.is_empty()))
            .collect();
        documents.sort_unstable();
        let Some(chosen) = documents.first() else {
            return Ok(None);
        };
        let count = store
            .iter()
            .filter(|r| {
                r.namespace == Namespace::Knowledge && r.document_id.as_deref() == Some(chosen)
            })
            .count();
        Ok(Some(((*chosen).to_owned(), count)))
    }
}
