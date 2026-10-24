//! RAG knowledge subsystem.
//!
//! The [`Embedder`] / [`VectorStore`] / [`Retriever`] triad plus chunker and
//! builder, bundled by [`KnowledgeManager`]. Default backends are hermetic
//! ([`HashEmbedder`], [`InMemoryVectorStore`]). Enable feature `postgres` for
//! [`PgVectorStore`]; the factory refuses to persist hash vectors into
//! `VECTOR(1536)`. Ingest QA stats use Polars behind `polars-ingest`.

#[allow(clippy::wildcard_imports)] // The error catalog (`beam_errors`).
pub(crate) use beam_errors::*;

mod blake2b;
mod builder;
mod chunker;
mod embedder;
mod factory;
pub mod longterm;
mod manager;
pub mod parsers;
#[cfg(feature = "postgres")]
mod pg;
pub mod provenance;
mod retriever;
#[cfg(feature = "postgres")]
mod sqljson;
mod stats;
mod store;
mod types;
pub mod visibility;

pub use builder::{
    hash_text, DocumentRecords, DocumentRow, IngestRequest, IngestionResult, KnowledgeBuilder,
};
pub use chunker::{approx_tokens, Chunker, RecursiveChunker, TokenChunker};
pub use embedder::{
    batched_embed, EmbedBatching, Embedder, HashEmbedder, HttpEmbedder, MockEmbedder,
    OpenAiCompatEmbedder,
};
pub use factory::{
    build_knowledge, build_knowledge_from_env, EmbedderKind, KnowledgeBuildRequest, RetrieverKind,
    StoreKind,
};
pub use longterm::{LongTermMemoryStore, LtmPipeline, RecallRequest, RememberRequest};
pub use manager::KnowledgeManager;
pub use parsers::{AutoParser, DocumentParser, MediaReader, ParseOptions, Source};
#[cfg(feature = "postgres")]
pub use pg::{PgDocumentRecords, PgVectorStore};
pub use provenance::{citations_from_hits, EmbeddingRevision, EmbeddingRevisionBook};
pub use retriever::{merge_hits, rerank, DenseRetriever, HybridRetriever, Retriever};
#[cfg(feature = "postgres")]
pub use sqljson::SqlJsonVectorStore;
pub use stats::{ingest_stats, IngestStats};
pub use store::{cosine, InMemoryVectorStore, VectorStore};
pub use types::{
    Chunk, DocumentStatus, Hit, KnowledgeScope, MemoryType, Namespace, ParsedBlock,
    RetrievalFilter, VectorRecord,
};
