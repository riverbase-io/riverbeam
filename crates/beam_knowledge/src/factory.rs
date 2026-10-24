//! Bind the embedder, vector store and retriever from config (Python
//! `knowledge/factory.py`).
//!
//! Differences from Python, on purpose:
//! - With no `KNOWLEDGE_EMBEDDER`, Rust picks an HTTP embedder when an API key
//!   is set, else the hermetic `HashEmbedder` (Python defaults to `mock`).
//! - A database without pgvector uses [`crate::SqlJsonVectorStore`] instead of
//!   falling back to memory, which lost every vector on restart.

use std::sync::Arc;

use riverbase_core::RiverbaseResult;

use crate::chunker::{RecursiveChunker, DEFAULT_CHUNK_OVERLAP, DEFAULT_CHUNK_SIZE};
use crate::embedder::{Embedder, HashEmbedder, HttpEmbedder, MockEmbedder, OpenAiCompatEmbedder};
use crate::manager::KnowledgeManager;
use crate::retriever::{DenseRetriever, HybridRetriever, Retriever};
use crate::store::InMemoryVectorStore;
use beam_env::{flag, string as env};

/// Which embedder the factory should construct (`KNOWLEDGE_EMBEDDER`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbedderKind {
    Hash,
    Mock,
    OpenAi,
    Nvidia,
    /// Python's local `huggingface` embedder, as an HTTP sidecar (D8).
    HuggingFace,
}

/// Which vector store the factory should construct (`KNOWLEDGE_VECTOR_STORE`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreKind {
    InMemory,
    PgVector,
    SqlJson,
}

/// `KNOWLEDGE_RETRIEVER`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetrieverKind {
    Dense,
    Hybrid,
}

/// Inputs for [`build_knowledge`]. Env defaults live in [`KnowledgeBuildRequest::from_env`].
#[derive(Debug, Clone)]
pub struct KnowledgeBuildRequest {
    pub embedder: EmbedderKind,
    pub store: StoreKind,
    pub retriever: RetrieverKind,
    /// `KNOWLEDGE_RERANK` (hybrid only).
    pub rerank: bool,
    pub embedding_dim: usize,
    pub chunk_size: usize,
    pub chunk_overlap: usize,
    pub database_url: Option<String>,
    pub api_key: Option<String>,
    pub base_url: String,
    pub model: String,
    /// `KNOWLEDGE_EMBEDDER_URL`: the sidecar for `huggingface`.
    pub embedder_url: Option<String>,
    /// The beam database to store knowledge on, sharing its pool (plan 025
    /// D8). Without it, `database_url` opens a database of its own.
    #[cfg(feature = "postgres")]
    pub db: Option<beam_store::Db>,
}

impl KnowledgeBuildRequest {
    /// Read `KNOWLEDGE_*` / `BEAM_*` env.
    #[must_use]
    pub fn from_env(embedding_dim: usize) -> Self {
        let api_key = env("BEAM_API_KEY")
            .or_else(|| env("GFS_BEAM_API_KEY"))
            .or_else(|| env("NVIDIA_API_KEY"));
        let base_url = env("BEAM_API_BASE_URL")
            .or_else(|| env("GFS_BEAM_BASE_URL"))
            .unwrap_or_else(|| "https://integrate.api.nvidia.com/v1".into());
        let embedder = match env("KNOWLEDGE_EMBEDDER")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str()
        {
            "openai" => EmbedderKind::OpenAi,
            "nvidia" => EmbedderKind::Nvidia,
            "mock" => EmbedderKind::Mock,
            "hash" => EmbedderKind::Hash,
            "huggingface" => EmbedderKind::HuggingFace,
            _ if api_key.is_some() && base_url.contains("nvidia") => EmbedderKind::Nvidia,
            _ if api_key.is_some() => EmbedderKind::OpenAi,
            _ => EmbedderKind::Hash,
        };
        let retriever = match env("KNOWLEDGE_RETRIEVER")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str()
        {
            "hybrid" => RetrieverKind::Hybrid,
            _ => RetrieverKind::Dense,
        };
        let usize_env =
            |key: &str, default: usize| env(key).and_then(|v| v.parse().ok()).unwrap_or(default);
        let (store, dim) = store_and_dim(
            embedder,
            env("KNOWLEDGE_VECTOR_STORE").as_deref(),
            env("KNOWLEDGE_EMBEDDING_DIM").and_then(|v| v.parse().ok()),
            embedding_dim,
        );
        let model = env("KNOWLEDGE_EMBEDDER_MODEL").unwrap_or_else(|| match embedder {
            EmbedderKind::Nvidia => "nvidia/llama-3.2-nv-embedqa-1b-v1".into(),
            _ => "text-embedding-3-small".into(),
        });
        let database_url = env("BEAM_DATABASE_URL")
            .or_else(|| env("DATABASE_URL"))
            .or_else(|| env("FLRS_DATABASE_URL"))
            .or_else(|| env("BEAM_SQL_DB_DSN"));
        Self {
            embedder,
            store,
            retriever,
            rerank: flag("KNOWLEDGE_RERANK", true),
            embedding_dim: dim,
            chunk_size: usize_env("KNOWLEDGE_CHUNK_SIZE", DEFAULT_CHUNK_SIZE),
            chunk_overlap: usize_env("KNOWLEDGE_CHUNK_OVERLAP", DEFAULT_CHUNK_OVERLAP),
            database_url,
            api_key,
            base_url,
            model,
            embedder_url: env("KNOWLEDGE_EMBEDDER_URL"),
            #[cfg(feature = "postgres")]
            db: None,
        }
    }
}

/// pgvector's column width (`VECTOR(1536)`).
const PGVECTOR_DIM: usize = 1536;
/// The width of NVIDIA's default model (`nvidia/llama-3.2-nv-embedqa-1b-v1`).
const NVIDIA_DIM: usize = 2048;

/// The vector store and embedding width, from `KNOWLEDGE_VECTOR_STORE` and
/// `KNOWLEDGE_EMBEDDING_DIM` (`default_dim` when unset; NVIDIA's model width
/// for NVIDIA). Without an explicit store, vectors that cannot fill
/// `VECTOR(1536)` go to `beam_vector_json` in the same database, and the hash
/// embedder stays in memory. An explicit store is kept, and checked when the
/// knowledge is built.
#[must_use]
pub fn store_and_dim(
    embedder: EmbedderKind,
    store: Option<&str>,
    dim: Option<usize>,
    default_dim: usize,
) -> (StoreKind, usize) {
    let dim = dim
        .unwrap_or(if embedder == EmbedderKind::Nvidia {
            NVIDIA_DIM
        } else {
            default_dim
        })
        .max(1);
    let kind = match store.unwrap_or("pgvector").to_ascii_lowercase().as_str() {
        "inmemory" | "in_memory" | "memory" => StoreKind::InMemory,
        "sqljson" | "sql_json" => StoreKind::SqlJson,
        _ => StoreKind::PgVector,
    };
    if kind != StoreKind::PgVector || store.is_some() {
        return (kind, dim);
    }
    if embedder == EmbedderKind::Hash {
        tracing::warn!("HashEmbedder cannot write VECTOR(1536); using the in-memory store");
        return (StoreKind::InMemory, dim);
    }
    if dim != PGVECTOR_DIM {
        tracing::info!(
            dim,
            "{dim}-wide vectors do not fit VECTOR(1536); using beam_vector_json"
        );
        return (StoreKind::SqlJson, dim);
    }
    (kind, dim)
}

fn embedder(req: &KnowledgeBuildRequest) -> RiverbaseResult<Arc<dyn Embedder>> {
    Ok(match req.embedder {
        EmbedderKind::Hash => Arc::new(HashEmbedder::new(128)),
        EmbedderKind::Mock => {
            Arc::new(MockEmbedder::new(req.embedding_dim).with_model(req.model.clone()))
        }
        EmbedderKind::HuggingFace => {
            let url = req.embedder_url.clone().ok_or_else(|| {
                crate::BEM_231.with_data("KNOWLEDGE_EMBEDDER_URL is required for huggingface")
            })?;
            Arc::new(HttpEmbedder::new(url, req.model.clone(), req.embedding_dim))
        }
        EmbedderKind::OpenAi | EmbedderKind::Nvidia => {
            let key = req.api_key.clone().ok_or_else(|| crate::BEM_231.raise())?;
            if req.embedder == EmbedderKind::Nvidia {
                Arc::new(OpenAiCompatEmbedder::nvidia(
                    req.base_url.clone(),
                    key,
                    req.model.clone(),
                    req.embedding_dim,
                ))
            } else {
                Arc::new(OpenAiCompatEmbedder::openai(
                    req.base_url.clone(),
                    key,
                    req.model.clone(),
                    req.embedding_dim,
                ))
            }
        }
    })
}

/// Build a [`KnowledgeManager`]. Never pairs [`HashEmbedder`] with pgvector.
///
/// # Errors
/// A config error when hash+pgvector are both requested, when the embedder
/// width does not match a pgvector column, or when an HTTP embedder lacks its
/// key or URL. A database without pgvector uses `beam_vector_json`; when the
/// migrations have not run, the first write fails.
pub async fn build_knowledge(req: KnowledgeBuildRequest) -> RiverbaseResult<KnowledgeManager> {
    let embedder = embedder(&req)?;
    if req.embedder == EmbedderKind::Hash && req.store == StoreKind::PgVector {
        return Err(crate::BEM_232.raise());
    }
    if req.store == StoreKind::PgVector {
        if embedder.dimensions() != req.embedding_dim {
            return Err(crate::BEM_233.with_data(format!(
                "{} produces {}-dim vectors but KNOWLEDGE_EMBEDDING_DIM is {}",
                embedder.name(),
                embedder.dimensions(),
                req.embedding_dim
            )));
        }
        if req.embedding_dim != 1536 {
            return Err(crate::BEM_234.with_data(req.embedding_dim.to_string()));
        }
    }
    let manager = match sql_backed(&req, embedder.clone()).await? {
        Some(manager) => manager,
        None => KnowledgeManager::new(embedder.clone(), Arc::new(InMemoryVectorStore::new())),
    };
    let size = req.chunk_size.max(2);
    let chunker = RecursiveChunker::new(size, req.chunk_overlap.min(size - 1));
    let manager = manager.with_chunker(Arc::new(chunker));
    let retriever: Arc<dyn Retriever> = match req.retriever {
        RetrieverKind::Hybrid => Arc::new(HybridRetriever::new(
            embedder,
            manager.store().clone(),
            req.rerank,
        )),
        RetrieverKind::Dense => Arc::new(DenseRetriever::new(embedder, manager.store().clone())),
    };
    Ok(manager.with_retriever(retriever))
}

/// [`KnowledgeBuildRequest::from_env`] then [`build_knowledge`].
///
/// # Errors
/// Same as [`build_knowledge`].
pub async fn build_knowledge_from_env(embedding_dim: usize) -> RiverbaseResult<KnowledgeManager> {
    build_knowledge(KnowledgeBuildRequest::from_env(embedding_dim)).await
}

/// The SQL-backed manager, or `None` for the in-memory store.
#[cfg(feature = "postgres")]
async fn sql_backed(
    req: &KnowledgeBuildRequest,
    embedder: Arc<dyn Embedder>,
) -> RiverbaseResult<Option<KnowledgeManager>> {
    use crate::pg::{PgDocumentRecords, PgVectorStore};
    use crate::sqljson::SqlJsonVectorStore;

    if req.store == StoreKind::InMemory {
        return Ok(None);
    }
    // The app's beam database (one pool), else a database of its own.
    let db = match (&req.db, req.database_url.as_deref()) {
        (Some(db), _) => db.clone(),
        (None, Some(url)) => beam_store::Db::connect(url)
            .await
            .map_err(|e| crate::BEM_203.with_data(e.to_string()))?,
        (None, None) => {
            // Python falls back to memory without a DSN too.
            tracing::warn!(
                "KNOWLEDGE_VECTOR_STORE needs BEAM_DATABASE_URL; using the in-memory store"
            );
            return Ok(None);
        }
    };
    let records: Arc<dyn crate::builder::DocumentRecords> =
        Arc::new(PgDocumentRecords::from_db(db.clone()));
    let pgvector = req.store == StoreKind::PgVector
        && db
            .knowledge_ready()
            .await
            .map_err(|e| e.or(crate::BEM_205))?;
    let store: Arc<dyn crate::store::VectorStore> = if pgvector {
        Arc::new(PgVectorStore::from_db(db))
    } else {
        if req.store == StoreKind::PgVector {
            tracing::warn!("pgvector is unavailable; using beam_vector_json (SqlJsonVectorStore)");
        }
        Arc::new(SqlJsonVectorStore::from_db(db))
    };
    Ok(Some(
        KnowledgeManager::new(embedder, store).with_records(records),
    ))
}

#[cfg(not(feature = "postgres"))]
async fn sql_backed(
    req: &KnowledgeBuildRequest,
    _embedder: Arc<dyn Embedder>,
) -> RiverbaseResult<Option<KnowledgeManager>> {
    if req.store != StoreKind::InMemory {
        tracing::warn!("beam_knowledge built without `postgres`; using the in-memory store");
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(embedder: EmbedderKind, store: StoreKind, dim: usize) -> KnowledgeBuildRequest {
        KnowledgeBuildRequest {
            embedder,
            store,
            retriever: RetrieverKind::Dense,
            rerank: true,
            embedding_dim: dim,
            chunk_size: DEFAULT_CHUNK_SIZE,
            chunk_overlap: DEFAULT_CHUNK_OVERLAP,
            database_url: None,
            api_key: None,
            base_url: String::new(),
            model: String::new(),
            embedder_url: None,
            #[cfg(feature = "postgres")]
            db: None,
        }
    }

    #[tokio::test]
    async fn hash_plus_inmemory_builds() {
        build_knowledge(request(EmbedderKind::Hash, StoreKind::InMemory, 1536))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn pgvector_without_dsn_uses_memory() {
        build_knowledge(request(EmbedderKind::Mock, StoreKind::PgVector, 1536))
            .await
            .unwrap();
    }

    #[test]
    fn wide_embedders_use_the_json_store_unless_pgvector_is_asked_for() {
        // NVIDIA's default model is 2048 wide: it cannot fill VECTOR(1536).
        assert_eq!(
            store_and_dim(EmbedderKind::Nvidia, None, None, 1536),
            (StoreKind::SqlJson, 2048)
        );
        assert_eq!(
            store_and_dim(EmbedderKind::Nvidia, None, Some(1024), 1536),
            (StoreKind::SqlJson, 1024)
        );
        // An explicit store is kept (pgvector then fails the build, BEM-234).
        assert_eq!(
            store_and_dim(EmbedderKind::Nvidia, Some("pgvector"), None, 1536),
            (StoreKind::PgVector, 2048)
        );
        // 1536-wide embedders keep pgvector; the hash embedder stays in memory.
        assert_eq!(
            store_and_dim(EmbedderKind::OpenAi, None, None, 1536),
            (StoreKind::PgVector, 1536)
        );
        assert_eq!(
            store_and_dim(EmbedderKind::Hash, None, None, 1536),
            (StoreKind::InMemory, 1536)
        );
    }

    #[tokio::test]
    async fn invalid_pairs_are_rejected() {
        for req in [
            request(EmbedderKind::Hash, StoreKind::PgVector, 1536),
            request(EmbedderKind::Mock, StoreKind::PgVector, 128),
            request(EmbedderKind::OpenAi, StoreKind::InMemory, 1536),
            request(EmbedderKind::HuggingFace, StoreKind::InMemory, 384),
        ] {
            let Err(err) = build_knowledge(req).await else {
                panic!("expected a config error");
            };
            assert!(err.errcode.as_str().starts_with("BEM-23"));
        }
    }
}
