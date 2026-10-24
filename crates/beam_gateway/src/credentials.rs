//! Execution-time secrets (Python `kernel/credentials.py`).
//!
//! A model's key comes from its `credential_ref.env` variable. The deprecated
//! raw `api_key` is the fallback, as in Python. Keys are never logged and never
//! checked in (`ModelRow` drops both fields).

use beam_types::{CredentialRef, ModelConfig};

/// The `ref.env` variable when it is set and not
/// empty, else `fallback`.
#[must_use]
pub fn resolve_credential(
    credential: Option<&CredentialRef>,
    fallback: Option<&str>,
) -> Option<String> {
    if let Some(env) = credential.and_then(|c| c.env.as_deref()) {
        // A secret named by the model config, read as is (not through
        // beam_env, which trims).
        if let Ok(value) = std::env::var(env) {
            if !value.is_empty() {
                return Some(value);
            }
        }
    }
    fallback.filter(|k| !k.is_empty()).map(str::to_owned)
}

/// The model's key from its credential reference or its `api_key`.
#[must_use]
pub fn resolved_api_key(model: &ModelConfig) -> Option<String> {
    resolve_credential(model.credential_ref.as_ref(), model.api_key.as_deref())
}

/// `provider_config.base_url`, else `extra.base_url`.
#[must_use]
pub fn base_url(model: &ModelConfig) -> Option<String> {
    model
        .provider_config
        .get("base_url")
        .or_else(|| model.extra.get("base_url"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

/// The provider's model id: `provider_config.model_id`, else the catalog name
/// (Python's nvidia, openrouter, google and bedrock providers).
#[must_use]
pub fn model_id(model: &ModelConfig) -> String {
    model
        .provider_config
        .get("model_id")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map_or_else(|| model.name.clone(), str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_wins_over_the_raw_key() {
        let reference = |env: &str| CredentialRef {
            name: "k".into(),
            provider: None,
            env: Some(env.into()),
        };
        // PATH is always set; an unset variable falls back to the raw key.
        let path = std::env::var("PATH").unwrap();
        assert_eq!(
            resolve_credential(Some(&reference("PATH")), Some("raw")),
            Some(path)
        );
        let unset = reference("BEAM_GATEWAY_TEST_UNSET_7C1");
        assert_eq!(
            resolve_credential(Some(&unset), Some("raw")).as_deref(),
            Some("raw")
        );
        assert_eq!(resolve_credential(None, Some("")), None);
    }
}
