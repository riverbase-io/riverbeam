//! Model gateway for riverbeam.
//!
//! Every model call from a runtime goes through a [`ModelGateway`], as in
//! Python: providers built on rig from the catalog's `ModelConfig`
//! (`OpenAI`-compatible, Anthropic, Gemini, Bedrock), per-model retries and timeout, a
//! circuit breaker per model, failover along the router's chain, and the
//! stream-visible rule (no retry after the caller saw tokens). The gateway is
//! the `beam_runtime::ModelCaller` runtimes receive.

#[allow(clippy::wildcard_imports)] // The error catalog (`beam_errors`).
pub(crate) use beam_errors::*;

mod bedrock;
pub mod credentials;
mod factory;
pub mod fit;
mod gateway;
mod providers;
pub mod tokens;

pub use factory::{GatewayFactory, RoutedGateway, FAILOVER_MODELS};
pub use gateway::{Circuits, GatewaySettings, ModelGateway, ModelProvider};
pub use providers::{
    output_tokens, scrub, window_tokens, ProviderFactory, RigModelProvider, RigProviderFactory,
    NVIDIA_BASE_URL, OPENROUTER_BASE_URL,
};
pub use tokens::{TokenCount, TokenCounter};
