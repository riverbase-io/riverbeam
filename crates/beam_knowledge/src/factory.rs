//! Bind embedder + vector store from config / env (Python factory parity).

use std::sync::Arc;

use crate::embedder::{Embedder, HashEmbedder, MockEmbedder, OpenAiCompatEmbedder};
use riverbase_core::RiverbaseResult;
use crate::manager::KnowledgeManager;
use crate::store::{InMemoryVectorStore, VectorStore};

/// Which embedder the factory should construct.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbedderKind {
    Hash,
    Mock,
    OpenAi,
    Nvidia,
}

/// Which vector store the factory should construct.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreKind {
    InMemory,
    PgVector,
}

/// Inputs for [`build_knowledge`]. Env defaults live in [`KnowledgeBuildRequest::from_env`].
#[derive(Debug, Clone)]
pub struct KnowledgeBuildRequest {
    pub embedder: EmbedderKind,
    pub store: StoreKind,
    pub embedding_dim: usize,
    pub database_url: Option<String>,
    pub api_key: Option<String>,
    pub base_url: String,
    pub model: String,
}

impl KnowledgeBuildRequest {
    /// Read `KNOWLEDGE_*` / `BEAM_*` env. Hash embedder when no API key.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn from_env(embedding_dim: usize) -> Self {
        let api_key = std::env::var("BEAM_API_KEY")
            .or_else(|_| std::env::var("GFS_BEAM_API_KEY"))
            .or_else(|_| std::env::var("NVIDIA_API_KEY"))
            .ok()
            .filter(|s| !s.is_empty());
        let base_url = std::env::var("BEAM_API_BASE_URL")
            .or_else(|_| std::env::var("GFS_BEAM_BASE_URL"))
            .unwrap_or_else(|_| "https://integrate.api.nvidia.com/v1".into());
        let embedder = match std::env::var("KNOWLEDGE_EMBEDDER")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str()
        {
            "openai" => EmbedderKind::OpenAi,
            "nvidia" => EmbedderKind::Nvidia,
            "mock" => EmbedderKind::Mock,
            "hash" => EmbedderKind::Hash,
            _ if api_key.is_some() && base_url.contains("nvidia") => EmbedderKind::Nvidia,
            _ if api_key.is_some() => EmbedderKind::OpenAi,
            _ => EmbedderKind::Hash,
        };
        let store = match std::env::var("KNOWLEDGE_VECTOR_STORE")
            .unwrap_or_else(|_| "pgvector".into())
            .to_ascii_lowercase()
            .as_str()
        {
            "inmemory" | "memory" => StoreKind::InMemory,
            _ => StoreKind::PgVector,
        };
        let dim = std::env::var("KNOWLEDGE_EMBEDDING_DIM")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(embedding_dim)
            .max(1);
        let model = std::env::var("KNOWLEDGE_EMBEDDER_MODEL").unwrap_or_else(|_| {
            match embedder {
                EmbedderKind::Nvidia => "nvidia/llama-3.2-nv-embedqa-1b-v2".into(),
                _ => "text-embedding-3-small".into(),
            }
        });
        let database_url = std::env::var("BEAM_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .or_else(|_| std::env::var("FLRS_DATABASE_URL"))
            .or_else(|_| std::env::var("BEAM_SQL_DB_DSN"))
            .ok();
        let store = if embedder == EmbedderKind::Hash && store == StoreKind::PgVector {
            tracing::warn!(
                "HashEmbedder cannot write VECTOR(1536); using in-memory cosine store"
            );
            StoreKind::InMemory
        } else {
            store
        };
        Self {
            embedder,
            store,
            embedding_dim: dim,
            database_url,
            api_key,
            base_url,
            model,
        }
    }
}

/// Build a [`KnowledgeManager`]. Never pairs [`HashEmbedder`] with pgvector.
///
/// # Errors
/// A config error when hash+pgvector are both requested, when the
/// embedder width does not match `embedding_dim` for a pgvector store, or when
/// an HTTP embedder is requested without a key.
pub async fn build_knowledge(req: KnowledgeBuildRequest) -> RiverbaseResult<KnowledgeManager> {
    let embedder: Arc<dyn Embedder> = match req.embedder {
        EmbedderKind::Hash => Arc::new(HashEmbedder::new(128)),
        EmbedderKind::Mock => Arc::new(MockEmbedder::new(req.embedding_dim)),
        EmbedderKind::OpenAi | EmbedderKind::Nvidia => {
            let key = req.api_key.clone().ok_or_else(|| crate::BEM_231.raise())?;
            let dim = req.embedding_dim;
            if req.embedder == EmbedderKind::Nvidia {
                Arc::new(OpenAiCompatEmbedder::nvidia(
                    req.base_url.clone(),
                    key,
                    req.model.clone(),
                    dim,
                ))
            } else {
                Arc::new(OpenAiCompatEmbedder::openai(
                    req.base_url.clone(),
                    key,
                    req.model.clone(),
                    dim,
                ))
            }
        }
    };

    if req.embedder == EmbedderKind::Hash && req.store == StoreKind::PgVector {
        return Err(crate::BEM_232.raise());
    }

    let store: Arc<dyn VectorStore> = match req.store {
        StoreKind::InMemory => Arc::new(InMemoryVectorStore::new()),
        StoreKind::PgVector => {
            if embedder.dimensions() != req.embedding_dim {
                return Err(crate::BEM_233.with_data(format!("{} produces {}-dim vectors but KNOWLEDGE_EMBEDDING_DIM is {}", embedder.name(), embedder.dimensions(), req.embedding_dim)));
            }
            if req.embedding_dim != 1536 {
                return Err(crate::BEM_234.with_data(req.embedding_dim.to_string()));
            }
            pgvector_store(&req).await?
        }
    };

    Ok(KnowledgeManager::new(embedder, store))
}

/// [`KnowledgeBuildRequest::from_env`] then [`build_knowledge`].
///
/// # Errors
/// Same as [`build_knowledge`].
pub async fn build_knowledge_from_env(embedding_dim: usize) -> RiverbaseResult<KnowledgeManager> {
    build_knowledge(KnowledgeBuildRequest::from_env(embedding_dim)).await
}

#[cfg(feature = "postgres")]
async fn pgvector_store(req: &KnowledgeBuildRequest) -> RiverbaseResult<Arc<dyn VectorStore>> {
    let Some(url) = req.database_url.as_deref() else {
        tracing::warn!(
            "KNOWLEDGE_VECTOR_STORE=pgvector but no DSN; falling back to InMemoryVectorStore"
        );
        return Ok(Arc::new(InMemoryVectorStore::new()));
    };
    if let Some(store) = crate::pg::PgVectorStore::connect(url).await? {
        Ok(Arc::new(store))
    } else {
        tracing::warn!("pgvector schema unavailable; falling back to InMemoryVectorStore");
        Ok(Arc::new(InMemoryVectorStore::new()))
    }
}

#[cfg(not(feature = "postgres"))]
async fn pgvector_store(_req: &KnowledgeBuildRequest) -> RiverbaseResult<Arc<dyn VectorStore>> {
    tracing::warn!("beam_knowledge built without `postgres`; using InMemoryVectorStore");
    Ok(Arc::new(InMemoryVectorStore::new()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn hash_plus_inmemory_builds() {
        let mgr = build_knowledge(KnowledgeBuildRequest {
            embedder: EmbedderKind::Hash,
            store: StoreKind::InMemory,
            embedding_dim: 1536,
            database_url: None,
            api_key: None,
            base_url: String::new(),
            model: String::new(),
        })
        .await
        .unwrap();
        let _ = mgr.store();
    }

    #[tokio::test]
    async fn mock_dim_must_match_for_pgvector_request_without_dsn_falls_back() {
        let mgr = build_knowledge(KnowledgeBuildRequest {
            embedder: EmbedderKind::Mock,
            store: StoreKind::PgVector,
            embedding_dim: 1536,
            database_url: None,
            api_key: None,
            base_url: String::new(),
            model: String::new(),
        })
        .await
        .unwrap();
        let _ = mgr.store();
    }

    #[tokio::test]
    async fn hash_plus_pgvector_is_rejected() {
        let Err(err) = build_knowledge(KnowledgeBuildRequest {
            embedder: EmbedderKind::Hash,
            store: StoreKind::PgVector,
            embedding_dim: 1536,
            database_url: Some("postgres://unused".into()),
            api_key: None,
            base_url: String::new(),
            model: String::new(),
        })
        .await
        else {
            panic!("expected config error");
        };
        assert!(err.errcode.as_str().starts_with("BEM-23"));
    }

    #[tokio::test]
    async fn pgvector_rejects_non_1536_dim() {
        let Err(err) = build_knowledge(KnowledgeBuildRequest {
            embedder: EmbedderKind::Mock,
            store: StoreKind::PgVector,
            embedding_dim: 128,
            database_url: None,
            api_key: None,
            base_url: String::new(),
            model: String::new(),
        })
        .await
        else {
            panic!("expected config error");
        };
        assert!(err.errcode.as_str().starts_with("BEM-23"));
    }

    #[tokio::test]
    async fn openai_without_key_errors() {
        let Err(err) = build_knowledge(KnowledgeBuildRequest {
            embedder: EmbedderKind::OpenAi,
            store: StoreKind::InMemory,
            embedding_dim: 1536,
            database_url: None,
            api_key: None,
            base_url: "https://example.invalid/v1".into(),
            model: "text-embedding-3-small".into(),
        })
        .await
        else {
            panic!("expected config error");
        };
        assert!(err.errcode.as_str().starts_with("BEM-23"));
    }
}
