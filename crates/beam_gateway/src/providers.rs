//! Provider clients built from `ModelConfig` on rig (D2, D7), following
//! Python's `integrations/langchain/models.py` provider table.

use std::sync::Arc;

use beam_runtime::{ModelCall, ModelReply, ModelStream, ModelStreamItem};
use beam_types::{Modality, ModelConfig, ModelProvider as ProviderKind};
use futures_util::StreamExt;
use rig_core::completion::{CompletionRequest, Message};
use rig_core::message::UserContent;
use rig_core::operation::Completion;
use rig_core::providers::anthropic::AnthropicConfig;
use rig_core::providers::gemini::GeminiConfig;
use rig_core::providers::openai::{self, OpenAIConfig};
use rig_core::streaming::{Item, StreamEvent};
use rig_core::DynModel;
use riverbase_core::RiverbaseResult;

use crate::credentials::{base_url, model_id, resolved_api_key};
use crate::gateway::ModelProvider;

/// Python's NVIDIA default when neither `base_url` nor `NVIDIA_NIM_BASE_URL` is set.
pub const NVIDIA_BASE_URL: &str = "https://integrate.api.nvidia.com/v1";
pub const OPENROUTER_BASE_URL: &str = "https://openrouter.ai/api/v1";

/// Builds the provider for one catalog model.
pub trait ProviderFactory: Send + Sync {
    /// # Errors
    /// `BEM-461` for an invalid provider configuration; `BEM-462` when the
    /// provider needs a key and none resolves.
    fn build(&self, model: &ModelConfig) -> RiverbaseResult<Arc<dyn ModelProvider>>;
}

/// A catalog model on a rig completion model.
pub struct RigModelProvider {
    name: String,
    model: DynModel<Completion>,
    /// The resolved key, scrubbed from every error this provider reports.
    secret: Option<String>,
    /// Python's NVIDIA provider caps output at `output_tokens`.
    max_tokens: Option<u64>,
    /// The model takes images (its catalog modalities list `image`).
    images: bool,
    /// The model's window and output reserve; a window of 0 is
    /// unknown and never refuses.
    window: u64,
    reserve: u64,
}

impl RigModelProvider {
    #[must_use]
    pub fn new(name: impl Into<String>, model: DynModel<Completion>) -> Self {
        Self {
            name: name.into(),
            model,
            secret: None,
            max_tokens: None,
            images: true,
            window: 0,
            reserve: 0,
        }
    }

    /// The model's context window and output reserve: a call
    /// whose prompt plus reserve would not fit fails with `BEM-484` before
    /// the provider is called, and the gateway moves on to the next model.
    #[must_use]
    pub fn with_window(mut self, window: u64, reserve: u64) -> Self {
        self.window = window;
        self.reserve = reserve;
        self
    }

    /// Scrub `secret` from this provider's errors (they reach API clients).
    #[must_use]
    pub fn with_secret(mut self, secret: Option<String>) -> Self {
        self.secret = secret.filter(|s| !s.is_empty());
        self
    }

    #[must_use]
    pub fn with_max_tokens(mut self, max_tokens: Option<u64>) -> Self {
        self.max_tokens = max_tokens;
        self
    }

    /// Whether the model takes images (on by default). Without them, an image
    /// in the turn's prompt fails with `BEM-481` (Python's
    /// `ModalityMismatchError`; the gateway moves on to the next model without
    /// a retry), and images in the history are left out.
    #[must_use]
    pub fn with_images(mut self, images: bool) -> Self {
        self.images = images;
        self
    }
}

fn has_image(message: &Message) -> bool {
    matches!(message, Message::User { content }
        if content.iter().any(|c| matches!(c, UserContent::Image(_))))
}

/// `history` without user images. A user turn that was only images is left
/// out, as the runtimes leave out a turn with no text.
fn without_images(history: Vec<Message>) -> Vec<Message> {
    history
        .into_iter()
        .filter_map(|message| match message {
            Message::User { mut content } => {
                content.retain(|c| !matches!(c, UserContent::Image(_)));
                (!content.is_empty()).then_some(Message::User { content })
            }
            other => Some(other),
        })
        .collect()
}

impl ModelProvider for RigModelProvider {
    fn name(&self) -> &str {
        &self.name
    }

    fn stream(&self, call: &ModelCall) -> ModelStream {
        let mut prepared = call.prepared.clone();
        if prepared.max_tokens.is_none() {
            prepared.max_tokens = self.max_tokens;
        }
        if !self.images {
            if has_image(&call.prompt) {
                let err = crate::BEM_481
                    .with_data(format!("{}: the model does not accept images", self.name));
                return futures_util::stream::once(async move { Err(err) }).boxed();
            }
            prepared.chat_history = without_images(prepared.chat_history);
        }
        let reserve = prepared.max_tokens.unwrap_or(self.reserve);
        let request = prepared.apply(CompletionRequest::new(call.prompt.clone()));
        if self.window > 0 {
            let estimate = crate::tokens::TokenCounter.estimate_request(&request, Some(&self.name));
            if estimate.saturating_add(reserve) > self.window {
                let err = crate::BEM_484.with_data(serde_json::json!({
                    "model": self.name,
                    "estimate": estimate,
                    "window": self.window,
                    "reserve": reserve,
                }));
                return futures_util::stream::once(async move { Err(err) }).boxed();
            }
        }
        let (model, name, secret) = (self.model.clone(), self.name.clone(), self.secret.clone());
        let fail = move |e: &dyn std::fmt::Display| {
            crate::BEM_464.with_data(scrub(&format!("{name}: {e}"), secret.as_deref()))
        };
        let name = self.name.clone();
        async_stream::try_stream! {
            let mut stream = model.stream(request).map_err(|e| fail(&e))?;
            while let Some(item) = stream.next().await {
                let item = item.map_err(|e| fail(&e))?;
                if let Item::Event(StreamEvent::Text { text, .. }) = item {
                    yield ModelStreamItem::TextDelta(text);
                }
            }
            let response = stream.finish().await.map_err(|e| fail(&e))?;
            yield ModelStreamItem::Completed(ModelReply {
                finish_reason: response.finish_reason(),
                choice: response.choice,
                usage: response.usage,
                raw: response.raw,
                model_used: Some(name.clone()),
            });
        }
        .boxed()
    }
}

/// Remove credentials from provider error text: the resolved key itself and
/// any `key=` query value (rig's Gemini client sends the key in the URL, and
/// transport errors print the URL).
#[must_use]
pub fn scrub(text: &str, secret: Option<&str>) -> String {
    let mut out = match secret {
        Some(s) if !s.is_empty() => text.replace(s, "***"),
        _ => text.to_owned(),
    };
    let mut from = 0;
    while let Some(pos) = out[from..].find("key=") {
        let start = from + pos + "key=".len();
        let end = out[start..]
            .find(['&', ')', ' ', '"', '\'', '\n'])
            .map_or(out.len(), |n| start + n);
        if end > start {
            out.replace_range(start..end, "***");
        }
        from = start + 3;
    }
    out
}

/// The production factory: rig's clients on the shared reqwest transport.
#[derive(Debug, Clone, Copy, Default)]
pub struct RigProviderFactory;

/// The variable Python's langchain client reads when the model has no key
/// (`api_key=None`): `ChatOpenAI` (also used for `OpenRouter`), `ChatAnthropic`,
/// `ChatGroq`, `ChatNVIDIA`.
fn default_key_env(provider: ProviderKind) -> Option<&'static str> {
    match provider {
        ProviderKind::Openai | ProviderKind::Openrouter => Some("OPENAI_API_KEY"),
        ProviderKind::Anthropic => Some("ANTHROPIC_API_KEY"),
        ProviderKind::Groq => Some("GROQ_API_KEY"),
        ProviderKind::Nvidia => Some("NVIDIA_API_KEY"),
        _ => None,
    }
}

/// The model's key: `credential_ref.env`, the deprecated `api_key`, then the
/// provider's conventional variable.
fn api_key_for(model: &ModelConfig) -> Option<String> {
    resolved_api_key(model).or_else(|| default_key_env(model.provider).and_then(beam_env::string))
}

fn required_key(model: &ModelConfig) -> RiverbaseResult<String> {
    api_key_for(model).ok_or_else(|| {
        crate::BEM_462.with_data(format!(
            "model {} has no key: set credential_ref.env (or the deprecated api_key)",
            model.name
        ))
    })
}

/// Ollama's OpenAI-compatible API lives under `/v1`; Python's `ChatOllama`
/// takes the server root.
fn ollama_base(base: Option<String>) -> Option<String> {
    base.map(|url| {
        let url = url.trim_end_matches('/').to_owned();
        if url.ends_with("/v1") {
            url
        } else {
            format!("{url}/v1")
        }
    })
}

fn openai_compatible(
    config: OpenAIConfig,
    model: &ModelConfig,
    base: Option<String>,
    id: String,
) -> RigModelProvider {
    let config = match base {
        Some(url) => config.with_base_url(url),
        None => config,
    };
    RigModelProvider::new(model.name.clone(), config.client().chat(id).erase())
}

/// `context_window_tokens`, else
/// `max_tokens` (0 counts as unset, as Python's `or` does).
#[must_use]
pub fn window_tokens(model: &ModelConfig) -> u64 {
    u64::from(
        model
            .context_window_tokens
            .filter(|w| *w > 0)
            .unwrap_or(model.max_tokens),
    )
}

/// The window the overflow guard enforces: only a declared
/// `context_window_tokens`. `max_tokens` is an output limit, so a model that
/// declares no window is not guarded (0), rather than refused for prompts
/// longer than its output limit.
fn declared_window(model: &ModelConfig) -> u64 {
    u64::from(model.context_window_tokens.filter(|w| *w > 0).unwrap_or(0))
}

/// `max_output_tokens`, else a quarter of the window.
#[must_use]
pub fn output_tokens(model: &ModelConfig) -> u64 {
    model
        .max_output_tokens
        .filter(|o| *o > 0)
        .map_or_else(|| (window_tokens(model) / 4).max(1), u64::from)
}

impl ProviderFactory for RigProviderFactory {
    fn build(&self, model: &ModelConfig) -> RiverbaseResult<Arc<dyn ModelProvider>> {
        let provider = match model.provider {
            ProviderKind::Openai => openai_compatible(
                OpenAIConfig::new(required_key(model)?),
                model,
                base_url(model),
                model.name.clone(),
            ),
            // Python allows `custom` without a registered provider: an
            // OpenAI-compatible endpoint at `base_url`.
            ProviderKind::Custom => {
                let base = base_url(model).ok_or_else(|| {
                    crate::BEM_461.with_data(format!(
                        "custom model {} needs provider_config.base_url",
                        model.name
                    ))
                })?;
                openai_compatible(
                    OpenAIConfig::new(resolved_api_key(model).unwrap_or_default()),
                    model,
                    Some(base),
                    model_id(model),
                )
            }
            ProviderKind::Groq => openai_compatible(
                OpenAIConfig::with_key(&openai::wire::GROQ, required_key(model)?),
                model,
                base_url(model),
                model.name.clone(),
            ),
            ProviderKind::Ollama => openai_compatible(
                OpenAIConfig::with_key(
                    &openai::wire::OLLAMA,
                    resolved_api_key(model).unwrap_or_default(),
                ),
                model,
                ollama_base(base_url(model)),
                model.name.clone(),
            ),
            ProviderKind::Nvidia => {
                let base = base_url(model)
                    .or_else(|| beam_env::string("NVIDIA_NIM_BASE_URL"))
                    .unwrap_or_else(|| NVIDIA_BASE_URL.to_owned());
                openai_compatible(
                    OpenAIConfig::new(required_key(model)?),
                    model,
                    Some(base),
                    model_id(model),
                )
                .with_max_tokens(Some(output_tokens(model)))
            }
            ProviderKind::Openrouter => openai_compatible(
                OpenAIConfig::with_key(&openai::wire::OPENROUTER, required_key(model)?),
                model,
                base_url(model).or_else(|| Some(OPENROUTER_BASE_URL.to_owned())),
                model_id(model),
            ),
            ProviderKind::Google => {
                let key = required_key(model)?;
                if key.starts_with("sk-or-") {
                    // Python routes an OpenRouter key through OpenRouter.
                    openai_compatible(
                        OpenAIConfig::with_key(&openai::wire::OPENROUTER, key),
                        model,
                        Some(OPENROUTER_BASE_URL.to_owned()),
                        model_id(model),
                    )
                } else {
                    // Rust adds the native Gemini API (Python requires OpenRouter).
                    let config = GeminiConfig::new(key);
                    let config = match base_url(model) {
                        Some(url) => config.with_base_url(url),
                        None => config,
                    };
                    RigModelProvider::new(
                        model.name.clone(),
                        config.client().completion(model_id(model)).erase(),
                    )
                }
            }
            ProviderKind::Anthropic => {
                let config = AnthropicConfig::new(required_key(model)?);
                let config = match base_url(model) {
                    Some(url) => config.with_base_url(url),
                    None => config,
                };
                // Anthropic requires max_tokens; Python leaves it to langchain's
                // default. Rust sends the model's output budget.
                RigModelProvider::new(
                    model.name.clone(),
                    config.client().completion(model.name.clone()).erase(),
                )
                .with_max_tokens(Some(output_tokens(model)))
            }
            ProviderKind::Bedrock => crate::bedrock::provider(model)?,
        };
        Ok(Arc::new(
            provider
                .with_secret(api_key_for(model))
                .with_images(model.modalities.contains(&Modality::Image))
                .with_window(declared_window(model), output_tokens(model)),
        ))
    }
}
