//! Short-lived MCP tokens: fluvius's `encode_mcp_token`, minted by
//! the worker at each MCP call from the run's auth ids, so no token is ever
//! queued, stored or logged (rc8 mints one in the command and stores it in
//! the run's request).
//!
//! HS256 over `MCP_SECRET_KEY`, with fluvius's claims: `sub` (the profile,
//! else the user), `aud` (`MCP_TOKEN_AUDIENCE`, default `mcp`), `realm`
//! (`MCP_TOKEN_REALM`, default `default`), `iat`, `exp`
//! (`MCP_TOKEN_TTL_SECONDS`, default 900), `jti`, and `user`, `profile`,
//! `organization`, `iamroles`. riverbeam has only ids, not fluvius's session
//! objects, so those three are `null` and `iamroles` is empty.
//!
//! Known gap (plan 020 review): fluvius's `decode_mcp_token` accepts such a
//! token, but builds the caller only from `user`, `profile` and
//! `organization`, not `sub`. So an MCP server sees an anonymous caller. The
//! cutover gate (`docs/ops/cutover.md`) blocks a deployment whose MCP
//! servers authorize by the caller until the claims carry the identity.

use std::sync::OnceLock;

use beam_types::InvokeAuthContext;
use jsonwebtoken::{encode, EncodingKey, Header};
use riverbase_core::RiverbaseResult;
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

/// Mints MCP tokens with one secret. Its `Debug` never shows the key.
pub struct McpTokenMinter {
    key: EncodingKey,
    audience: String,
    realm: String,
    ttl_seconds: i64,
}

impl std::fmt::Debug for McpTokenMinter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("McpTokenMinter")
            .field("audience", &self.audience)
            .field("realm", &self.realm)
            .field("ttl_seconds", &self.ttl_seconds)
            .finish_non_exhaustive()
    }
}

#[derive(Serialize)]
struct Claims<'a> {
    sub: &'a str,
    aud: &'a str,
    realm: &'a str,
    iat: i64,
    exp: i64,
    jti: String,
    user: Value,
    profile: Value,
    organization: Value,
    iamroles: Vec<String>,
}

impl McpTokenMinter {
    /// A minter for `secret` (fluvius's `MCP_SECRET_KEY`).
    #[must_use]
    pub fn new(secret: &str, audience: &str, realm: &str, ttl_seconds: i64) -> Self {
        Self {
            key: EncodingKey::from_secret(secret.as_bytes()),
            audience: audience.to_owned(),
            realm: realm.to_owned(),
            ttl_seconds: ttl_seconds.max(1),
        }
    }

    /// From `MCP_SECRET_KEY`, `MCP_TOKEN_AUDIENCE`, `MCP_TOKEN_REALM` and
    /// `MCP_TOKEN_TTL_SECONDS`; `None` without a secret (no token is sent,
    /// as when no secret is set).
    #[must_use]
    pub fn from_env() -> Option<Self> {
        // The secret is read as is (not trimmed), as fluvius reads it.
        let secret = std::env::var("MCP_SECRET_KEY")
            .ok()
            .filter(|v| !v.trim().is_empty())?;
        Some(Self::new(
            &secret,
            &beam_env::string("MCP_TOKEN_AUDIENCE").unwrap_or_else(|| "mcp".to_owned()),
            &beam_env::string("MCP_TOKEN_REALM").unwrap_or_else(|| "default".to_owned()),
            beam_env::number("MCP_TOKEN_TTL_SECONDS").unwrap_or(900),
        ))
    }

    /// The process's minter, read from the environment once.
    #[must_use]
    pub fn global() -> Option<&'static Self> {
        static MINTER: OnceLock<Option<McpTokenMinter>> = OnceLock::new();
        MINTER.get_or_init(Self::from_env).as_ref()
    }

    /// A token for `auth`'s profile, else its user.
    ///
    /// # Errors
    /// `BEM-472` when `auth` names neither (fluvius's `MCP-001`) or signing
    /// fails.
    pub fn mint(&self, auth: &InvokeAuthContext) -> RiverbaseResult<String> {
        let sub = auth
            .profile_id
            .as_deref()
            .or(auth.user_id.as_deref())
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| {
                crate::BEM_472.with_data("cannot mint an MCP token: no user or profile")
            })?;
        let now = chrono::Utc::now().timestamp();
        let claims = Claims {
            sub,
            aud: &self.audience,
            realm: &self.realm,
            iat: now,
            exp: now + self.ttl_seconds,
            jti: Uuid::new_v4().simple().to_string(),
            user: Value::Null,
            profile: Value::Null,
            organization: Value::Null,
            iamroles: Vec::new(),
        };
        encode(&Header::default(), &claims, &self.key)
            .map_err(|e| crate::BEM_472.with_data(format!("cannot sign the MCP token: {e}")))
    }
}

/// The token an MCP call sends: the host's own when it set one, else one
/// minted for the run (fail closed: a minting error fails the call), else
/// none when no secret is configured.
///
/// # Errors
/// [`McpTokenMinter::mint`]'s.
pub fn token_for(
    auth: Option<&InvokeAuthContext>,
    minter: Option<&McpTokenMinter>,
) -> RiverbaseResult<Option<String>> {
    let Some(auth) = auth else {
        return Ok(None);
    };
    if let Some(token) = auth.mcp_token.as_deref().filter(|t| !t.is_empty()) {
        return Ok(Some(token.to_owned()));
    }
    match minter {
        Some(minter) => minter.mint(auth).map(Some),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};

    fn auth(user: Option<&str>, token: Option<&str>) -> InvokeAuthContext {
        InvokeAuthContext {
            profile_id: None,
            user_id: user.map(str::to_owned),
            org_id: Some("org-1".into()),
            mcp_token: token.map(str::to_owned),
        }
    }

    /// The header carries a fluvius-shaped JWT for the run's user.
    #[test]
    fn a_minted_token_names_the_user_and_expires() {
        let minter = McpTokenMinter::new("s3cret", "mcp", "beam", 900);
        let token = token_for(Some(&auth(Some("u1"), None)), Some(&minter))
            .unwrap()
            .expect("a token");
        let mut validation = Validation::new(Algorithm::HS256);
        validation.set_audience(&["mcp"]);
        let claims = decode::<Value>(&token, &DecodingKey::from_secret(b"s3cret"), &validation)
            .expect("valid")
            .claims;
        assert_eq!(claims["sub"], "u1");
        assert_eq!(claims["realm"], "beam");
        let ttl = claims["exp"].as_i64().unwrap() - claims["iat"].as_i64().unwrap();
        assert!((1..=900).contains(&ttl), "{ttl}");
        assert!(!format!("{minter:?}").contains("s3cret"));
    }

    #[test]
    fn no_secret_sends_no_token_and_a_host_token_wins() {
        assert_eq!(
            token_for(Some(&auth(Some("u1"), None)), None).unwrap(),
            None
        );
        let minter = McpTokenMinter::new("s3cret", "mcp", "beam", 900);
        assert_eq!(
            token_for(Some(&auth(Some("u1"), Some("host"))), Some(&minter)).unwrap(),
            Some("host".to_owned())
        );
        assert_eq!(token_for(None, Some(&minter)).unwrap(), None);
    }

    #[test]
    fn a_run_without_a_user_cannot_mint() {
        let minter = McpTokenMinter::new("s3cret", "mcp", "beam", 900);
        let err = token_for(Some(&auth(None, None)), Some(&minter)).unwrap_err();
        assert_eq!(err.errcode.as_str(), "BEM-472");
    }
}
