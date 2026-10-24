//! Provider-aware token estimates (Python `kernel/tokens.py`).
//!
//! With a model name, text is counted with tiktoken (the model's encoding, else
//! `cl100k_base`), as Python does when `tiktoken` is installed. Without one, the
//! conservative bound `(len + 3) / 4` is used. Never an unlabeled `len / 4`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use beam_types::{AgentMessage, ContentPart, ContentPartType};
use rig_core::completion::CompletionRequest;
use serde_json::Value;
use tiktoken_rs::CoreBPE;

pub const CONSERVATIVE: &str = "estimated_conservative";
pub const EXACT: &str = "exact";
pub const MESSAGE_FRAMING: u64 = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenCount {
    pub tokens: u64,
    pub method: &'static str,
}

impl TokenCount {
    fn conservative(tokens: u64) -> Self {
        Self {
            tokens,
            method: CONSERVATIVE,
        }
    }
}

/// Python `MEDIA_COST`, by content part type; other non-text parts cost 100.
fn media_cost(part: &ContentPart) -> u64 {
    let label = serde_json::to_value(part.part_type)
        .ok()
        .and_then(|v| v.as_str().map(str::to_owned))
        .unwrap_or_default();
    match label.as_str() {
        "image_url" | "image_base64" => 765,
        "audio" => 500,
        "video" => 800,
        "file" => 200,
        "media_ref" => 120,
        "reasoning" => 8,
        _ => 100,
    }
}

/// An upper-bound token estimate for text.
#[must_use]
pub fn conservative_text(text: &str) -> u64 {
    if text.is_empty() {
        return 0;
    }
    // Python's `len` counts code points.
    let chars = u64::try_from(text.chars().count()).unwrap_or(u64::MAX);
    chars.div_ceil(4).max(1)
}

/// Loaded encodings, by name (loading one takes milliseconds).
fn encodings() -> &'static Mutex<HashMap<String, Arc<CoreBPE>>> {
    static CACHE: OnceLock<Mutex<HashMap<String, Arc<CoreBPE>>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn encoding_for(model: &str) -> Option<Arc<CoreBPE>> {
    let name = tiktoken_rs::tokenizer::get_tokenizer(model).map_or("cl100k_base", |t| match t {
        tiktoken_rs::tokenizer::Tokenizer::O200kBase => "o200k_base",
        tiktoken_rs::tokenizer::Tokenizer::P50kBase => "p50k_base",
        tiktoken_rs::tokenizer::Tokenizer::P50kEdit => "p50k_edit",
        tiktoken_rs::tokenizer::Tokenizer::R50kBase => "r50k_base",
        tiktoken_rs::tokenizer::Tokenizer::Gpt2 => "gpt2",
        _ => "cl100k_base",
    });
    let mut cache = encodings().lock().ok()?;
    if let Some(bpe) = cache.get(name) {
        return Some(bpe.clone());
    }
    let bpe = match name {
        "o200k_base" => tiktoken_rs::o200k_base(),
        "p50k_base" => tiktoken_rs::p50k_base(),
        "p50k_edit" => tiktoken_rs::p50k_edit(),
        "r50k_base" | "gpt2" => tiktoken_rs::r50k_base(),
        _ => tiktoken_rs::cl100k_base(),
    }
    .ok()?;
    let bpe = Arc::new(bpe);
    cache.insert(name.to_owned(), bpe.clone());
    Some(bpe)
}

/// `None` without text or a model.
fn tiktoken_count(text: &str, model: Option<&str>) -> Option<u64> {
    let model = model.filter(|m| !m.is_empty())?;
    if text.is_empty() {
        return None;
    }
    let bpe = encoding_for(model)?;
    u64::try_from(bpe.encode_ordinary(text).len()).ok()
}

/// Every prompt string in `value` (rig's serialized message parts), and how
/// many images it holds.
fn strings(value: &Value, out: &mut Vec<String>, images: &mut u64) {
    match value {
        Value::String(s) => out.push(s.clone()),
        Value::Array(items) => items.iter().for_each(|v| strings(v, out, images)),
        // rig tags each part with `type`; an image part's data (base64
        // or a URL) is not text, so it counts as one image.
        Value::Object(map) if map.get("type").and_then(Value::as_str) == Some("image") => {
            *images += 1;
        }
        Value::Object(map) => map
            .iter()
            // The tags rig writes on every part are not prompt text.
            .filter(|(k, _)| !matches!(k.as_str(), "role" | "type"))
            .for_each(|(_, v)| strings(v, out, images)),
        _ => {}
    }
}

/// Per-model correction for the conservative estimates, learned from the
/// provider's counts (Python rc8 `TokenCalibration`): an exponential moving
/// average of `actual / estimated`, kept within `[0.6, 1.5]`, per process.
pub mod calibration {
    use std::collections::HashMap;
    use std::sync::{LazyLock, Mutex};

    const ALPHA: f64 = 0.2;
    const FLOOR: f64 = 0.6;
    const CEILING: f64 = 1.5;

    static FACTORS: LazyLock<Mutex<HashMap<String, f64>>> =
        LazyLock::new(|| Mutex::new(HashMap::new()));

    /// The factor for `model` (1.0 until a call was reconciled).
    #[must_use]
    pub fn factor(model: Option<&str>) -> f64 {
        FACTORS
            .lock()
            .ok()
            .and_then(|f| f.get(model.unwrap_or_default()).copied())
            .unwrap_or(1.0)
    }

    /// Learn from one call: the (already scaled) estimate and the provider's count.
    #[allow(clippy::cast_precision_loss)] // Token counts stay far below 2^52.
    pub fn record(model: Option<&str>, estimated: u64, actual: u64) {
        if estimated == 0 || actual == 0 {
            return;
        }
        let Ok(mut factors) = FACTORS.lock() else {
            return;
        };
        let key = model.unwrap_or_default().to_owned();
        let current = factors.get(&key).copied();
        let target =
            (current.unwrap_or(1.0) * actual as f64 / estimated as f64).clamp(FLOOR, CEILING);
        let next = current.map_or(target, |previous| previous + ALPHA * (target - previous));
        factors.insert(key, next);
    }
}

/// Counts serialized roles, parts, schemas and framing.
#[derive(Debug, Clone, Copy, Default)]
pub struct TokenCounter;

impl TokenCounter {
    #[must_use]
    pub fn count_text(&self, text: &str, model: Option<&str>) -> TokenCount {
        tiktoken_count(text, model).map_or_else(
            || TokenCount::conservative(conservative_text(text)),
            |tokens| TokenCount {
                tokens,
                method: EXACT,
            },
        )
    }

    #[must_use]
    pub fn count_parts(&self, parts: &[ContentPart], model: Option<&str>) -> TokenCount {
        if parts.is_empty() {
            return TokenCount::conservative(0);
        }
        let mut total = 0;
        let mut method = CONSERVATIVE;
        for part in parts {
            // Python: `type == "text" or text` (an empty string is falsy).
            if part.part_type == ContentPartType::Text
                || part.text.as_deref().is_some_and(|t| !t.is_empty())
            {
                let counted = self.count_text(part.text.as_deref().unwrap_or(""), model);
                total += counted.tokens;
                method = counted.method;
            } else {
                total += media_cost(part);
            }
        }
        TokenCount {
            tokens: total,
            method,
        }
    }

    #[must_use]
    pub fn count_message(&self, message: &AgentMessage, model: Option<&str>) -> TokenCount {
        let body = self.count_parts(&message.content, model);
        let mut extra = 0;
        if !message.tool_calls.is_empty() {
            extra += conservative_text(&beam_types::pyjson::dumps(&Value::Array(
                message.tool_calls.clone(),
            )));
        }
        if let Some(result) = message.tool_result.as_ref().filter(|r| !is_falsy(r)) {
            extra += conservative_text(&beam_types::pyjson::dumps(result));
        }
        if let Some(name) = message.name.as_deref().filter(|n| !n.is_empty()) {
            extra += conservative_text(name);
        }
        TokenCount {
            tokens: MESSAGE_FRAMING + body.tokens + extra,
            method: body.method,
        }
    }

    /// One rig message's tokens (plan 023's fitter): its text, tool arguments
    /// and results counted as text, an image at the flat image cost, and the
    /// message's framing.
    #[must_use]
    pub fn count_rig_message(
        &self,
        message: &rig_core::completion::Message,
        model: Option<&str>,
    ) -> u64 {
        let mut texts = Vec::new();
        let mut images = 0;
        if let Ok(value) = serde_json::to_value(message) {
            strings(&value, &mut texts, &mut images);
        }
        MESSAGE_FRAMING + self.count_text(&texts.join("\n"), model).tokens + images * 765
    }

    /// The prompt tokens of a provider request (the overflow guard):
    /// every message's text, tool arguments and results counted as text, an
    /// image (a long unbroken string, its data) at the flat image cost, the
    /// documents, the tool schemas, and each message's framing. An estimate
    /// that errs high, as Python's counter does.
    #[must_use]
    pub fn estimate_request(&self, request: &CompletionRequest, model: Option<&str>) -> u64 {
        let mut texts = Vec::new();
        let mut images = 0;
        for message in &request.chat_history {
            if let Ok(value) = serde_json::to_value(message) {
                strings(&value, &mut texts, &mut images);
            }
        }
        for document in &request.documents {
            if let Ok(value) = serde_json::to_value(document) {
                strings(&value, &mut texts, &mut images);
            }
        }
        let text = texts.join("\n");
        let messages = u64::try_from(request.chat_history.len()).unwrap_or(u64::MAX);
        let tools: Vec<Value> = request
            .tools
            .iter()
            .filter_map(|t| serde_json::to_value(t).ok())
            .collect();
        self.count_text(&text, model).tokens
            + images * 765
            + messages * MESSAGE_FRAMING
            + self.count_tool_schemas(&tools, model).tokens
    }

    #[must_use]
    pub fn count_tool_schemas(&self, schemas: &[Value], model: Option<&str>) -> TokenCount {
        if schemas.is_empty() {
            return TokenCount::conservative(0);
        }
        self.count_text(
            &beam_types::pyjson::dumps(&Value::Array(schemas.to_vec())),
            model,
        )
    }

    #[must_use]
    pub fn count_serialized(
        &self,
        messages: &[AgentMessage],
        schemas: &[Value],
        model: Option<&str>,
    ) -> TokenCount {
        let total: u64 = messages
            .iter()
            .map(|m| self.count_message(m, model).tokens)
            .sum();
        let schemas = self.count_tool_schemas(schemas, model);
        TokenCount {
            tokens: total + schemas.tokens,
            method: if schemas.method == EXACT {
                EXACT
            } else {
                CONSERVATIVE
            },
        }
    }
}

/// Python truthiness of a JSON value (`if message.tool_result:`).
fn is_falsy(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::Bool(b) => !b,
        Value::String(s) => s.is_empty(),
        Value::Array(a) => a.is_empty(),
        Value::Object(o) => o.is_empty(),
        Value::Number(n) => n.as_f64() == Some(0.0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Expected counts are Python's `TokenCounter.count_text` with tiktoken 0.12.
    #[test]
    fn counts_match_python_tiktoken() {
        let counter = TokenCounter;
        let vietnamese = "Xin chào, tôi tên là Phú. Tiếng Việt có dấu.";
        let code = "def f(x):\n    return x ** 2  # 😀 emoji";
        let long = "a".repeat(1000);
        for (model, viet) in [
            ("gpt-4o", 16),
            ("gpt-4", 26),
            ("gpt-3.5-turbo", 26),
            ("nemotron-super", 26),
        ] {
            let count = |t: &str| counter.count_text(t, Some(model));
            assert_eq!(
                count("hello world"),
                TokenCount {
                    tokens: 2,
                    method: EXACT
                },
                "{model}"
            );
            assert_eq!(count(vietnamese).tokens, viet, "{model}");
            assert_eq!(count(code).tokens, 14, "{model}");
            assert_eq!(count(&long).tokens, 125, "{model}");
            assert_eq!(count("").method, CONSERVATIVE);
        }
        // No model: the conservative bound over code points.
        assert_eq!(
            counter.count_text(vietnamese, None),
            TokenCount {
                tokens: 11,
                method: CONSERVATIVE
            }
        );
        assert_eq!(counter.count_text("a", None).tokens, 1);
    }

    #[test]
    fn messages_add_framing_and_media() {
        let counter = TokenCounter;
        let message = AgentMessage {
            content: vec![
                ContentPart::text("hello world"),
                ContentPart {
                    part_type: ContentPartType::ImageUrl,
                    text: None,
                    ..ContentPart::text("")
                },
            ],
            name: Some("bob".into()),
            ..AgentMessage::default()
        };
        // 4 framing + 2 text + 765 image + 1 name.
        assert_eq!(counter.count_message(&message, Some("gpt-4o")).tokens, 772);
    }
}
