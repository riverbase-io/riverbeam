//! Context pipeline for riverbeam (ported from fluvius-beam `context/`).
//!
//! Typed sources return candidates; the deterministic [`ContextComposer`]
//! selects them under a budget compiled from the agent's policy. The
//! [`ContextEnvelope`] is the only model-visible input, and the
//! [`ContextManifest`] records what was selected and dropped, and why, without
//! prompt text.

#[allow(clippy::wildcard_imports)] // The error catalog (`beam_errors`).
pub(crate) use beam_errors::*;

pub mod builder;
pub mod composer;
pub mod item;
pub mod manifest;
pub mod policy;
pub mod resolvers;
pub mod services;
pub mod sources;
pub mod types;
pub mod units;

pub use builder::{assemble_build_request, BuildOptions, ContextBuilder, ContextSourceRegistry};
pub use composer::{ComposeOptions, ContextComposer};
pub use manifest::{inspect_manifest, InMemoryManifestStore, ManifestStore, SqlManifestStore};
pub use policy::{compile_context_policy, ContextSpec, FailMode, ResolvedContextPolicy};
pub use resolvers::{AuthorizedResourceResolver, InMemoryProfileResolver};
pub use services::{
    ContextBuildRequest, ContextBuildResult, ContextServices, KnowledgeRetriever,
    LongTermMemoryReader, MemoryReader, ProfileResolver, ResourceResolver, RetrievalScope,
    SummaryReader, ToolCatalog,
};
pub use types::{
    ContextEnvelope, ContextItem, ContextManifest, ContextRole, DecisionAction, ExecutionContext,
    ManifestDecision, Trust,
};
