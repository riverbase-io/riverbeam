//! RAG knowledge subsystem.
//!
//! The [`Embedder`] / [`VectorStore`] / [`Retriever`] triad plus chunker and
//! builder, bundled by [`KnowledgeManager`]. Default backends are hermetic
//! ([`HashEmbedder`], [`InMemoryVectorStore`]). Enable feature `postgres` for
//! [`PgVectorStore`]; the factory refuses to persist hash vectors into
//! `VECTOR(1536)`. Ingest QA stats use Polars behind `polars-ingest`.

mod errors;
#[allow(unused_imports)]
pub(crate) use errors::*;

mod builder;
mod chunker;
mod embedder;
mod factory;
mod manager;
#[cfg(feature = "postgres")]
mod pg;
mod retriever;
mod stats;
mod store;
mod strategy;
mod types;

pub use builder::{IngestionResult, KnowledgeBuilder};
pub use chunker::{Chunker, SimpleChunker};
pub use embedder::{Embedder, HashEmbedder, MockEmbedder, OpenAiCompatEmbedder};
pub use factory::{
    build_knowledge, build_knowledge_from_env, EmbedderKind, KnowledgeBuildRequest, StoreKind,
};
pub use manager::KnowledgeManager;
#[cfg(feature = "postgres")]
pub use pg::{ensure_knowledge_schema, PgVectorStore};
pub use retriever::{DenseRetriever, Retriever};
pub use stats::{ingest_stats, IngestStats};
pub use store::{InMemoryVectorStore, VectorStore};
pub use strategy::RagContextStrategy;
pub use types::{Chunk, Hit, KnowledgeScope, Namespace, RetrievalFilter, VectorRecord};
