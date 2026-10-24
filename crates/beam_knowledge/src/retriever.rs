//! Dense and hybrid retrievers (Python `knowledge/retriever.py` and
//! `knowledge/rerank.py`).

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use riverbase_core::RiverbaseResult;
use serde_json::Value;

use crate::embedder::Embedder;
use crate::store::VectorStore;
use crate::types::{Hit, Namespace, RetrievalFilter};

/// Query embeddings kept per retriever (Python: 256, oldest evicted).
const EMBED_CACHE: usize = 256;

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

#[derive(Default)]
struct EmbedCache {
    vectors: HashMap<String, Vec<f32>>,
    order: VecDeque<String>,
}

/// Cosine top-k over one store, with a query
/// embedding cache.
pub struct DenseRetriever {
    embedder: Arc<dyn Embedder>,
    store: Arc<dyn VectorStore>,
    cache: Mutex<EmbedCache>,
}

impl DenseRetriever {
    #[must_use]
    pub fn new(embedder: Arc<dyn Embedder>, store: Arc<dyn VectorStore>) -> Self {
        Self {
            embedder,
            store,
            cache: Mutex::new(EmbedCache::default()),
        }
    }

    async fn query_vector(&self, query: &str) -> RiverbaseResult<Vec<f32>> {
        let key = query.trim().to_owned();
        if let Some(vector) = self
            .cache
            .lock()
            .ok()
            .and_then(|c| c.vectors.get(&key).cloned())
        {
            return Ok(vector);
        }
        let vector = self.embedder.embed_query(query).await?;
        if let Ok(mut cache) = self.cache.lock() {
            if cache.vectors.insert(key.clone(), vector.clone()).is_none() {
                cache.order.push_back(key);
            }
            while cache.order.len() > EMBED_CACHE {
                if let Some(oldest) = cache.order.pop_front() {
                    cache.vectors.remove(&oldest);
                }
            }
        }
        Ok(vector)
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
        if query.trim().is_empty() {
            return Ok(Vec::new());
        }
        let vector = self.query_vector(query).await?;
        self.store.query(&vector, k, filters, namespace).await
    }
}

/// Lexical hits first, then dense, first occurrence of
/// each id, at most `k`.
#[must_use]
pub fn merge_hits(dense: Vec<Hit>, lexical: Vec<Hit>, k: usize) -> Vec<Hit> {
    let mut seen = HashSet::new();
    lexical
        .into_iter()
        .chain(dense)
        .filter(|h| seen.insert(h.id.clone()))
        .take(k)
        .collect()
}

fn words(text: &str) -> HashSet<String> {
    text.to_lowercase()
        .split(|c: char| c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c))
        .filter(|w| !w.is_empty())
        .map(str::to_owned)
        .collect()
}

/// Score plus the count of shared query words, recorded as
/// `metadata.rerank_score`; highest first, ties in input order.
#[must_use]
pub fn rerank(query: &str, hits: Vec<Hit>, k: usize) -> Vec<Hit> {
    let terms = words(query);
    let mut ranked: Vec<(f64, Hit)> = hits
        .into_iter()
        .map(|mut hit| {
            #[allow(clippy::cast_precision_loss)]
            let overlap = terms.intersection(&words(&hit.text)).count() as f64;
            let score = hit.score + overlap;
            hit.metadata.insert(
                "rerank_score".into(),
                serde_json::Number::from_f64(score).map_or(Value::Null, Value::Number),
            );
            (score, hit)
        })
        .collect();
    ranked.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    ranked.into_iter().take(k).map(|(_, hit)| hit).collect()
}

/// Dense plus the store's lexical hits, merged to
/// `2k`, then reranked (`KNOWLEDGE_RERANK`) or cut to `k`.
pub struct HybridRetriever {
    dense: DenseRetriever,
    store: Arc<dyn VectorStore>,
    rerank: bool,
}

impl HybridRetriever {
    #[must_use]
    pub fn new(embedder: Arc<dyn Embedder>, store: Arc<dyn VectorStore>, rerank: bool) -> Self {
        Self {
            dense: DenseRetriever::new(embedder, store.clone()),
            store,
            rerank,
        }
    }
}

#[async_trait]
impl Retriever for HybridRetriever {
    async fn retrieve(
        &self,
        query: &str,
        k: usize,
        filters: Option<&RetrievalFilter>,
        namespace: Namespace,
    ) -> RiverbaseResult<Vec<Hit>> {
        if query.trim().is_empty() {
            return Ok(Vec::new());
        }
        let dense = self.dense.retrieve(query, k, filters, namespace).await?;
        let lexical = self
            .store
            .search_lexical(query, k, filters, namespace)
            .await?
            .unwrap_or_default();
        let merged = merge_hits(dense, lexical, k * 2);
        if self.rerank {
            Ok(rerank(query, merged, k))
        } else {
            Ok(merged.into_iter().take(k).collect())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hit(id: &str, text: &str, score: f64) -> Hit {
        Hit {
            id: id.into(),
            text: text.into(),
            score,
            metadata: serde_json::Map::new(),
            document_id: None,
            chunk_index: None,
            source: None,
            scope: None,
        }
    }

    #[test]
    fn rerank_adds_word_overlap_and_keeps_ties_in_order() {
        let hits = vec![
            hit("a", "nothing shared", 0.9),
            hit("b", "Invoice due date", 0.2),
            hit("c", "also nothing", 0.9),
        ];
        let ranked = rerank("when is the invoice due", hits, 3);
        let ids: Vec<&str> = ranked.iter().map(|h| h.id.as_str()).collect();
        assert_eq!(ids, ["b", "a", "c"]);
        assert_eq!(ranked[0].metadata["rerank_score"], 2.2);
        assert!(
            (ranked[0].score - 0.2).abs() < f64::EPSILON,
            "the score itself is unchanged"
        );
    }

    #[test]
    fn merge_prefers_lexical_and_dedupes() {
        let merged = merge_hits(
            vec![hit("a", "", 0.1), hit("b", "", 0.2)],
            vec![hit("b", "", 1.0), hit("c", "", 1.0)],
            3,
        );
        let ids: Vec<&str> = merged.iter().map(|h| h.id.as_str()).collect();
        assert_eq!(ids, ["b", "c", "a"]);
        assert!((merged[0].score - 1.0).abs() < f64::EPSILON);
    }
}
