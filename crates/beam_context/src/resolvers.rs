//! Authenticated profile and resource resolvers (Python
//! `context/resolvers.py`). Both fail closed on tenancy and SSRF; the
//! resource resolver authorizes a fetch but never fetches.

use std::collections::{HashMap, HashSet};
use std::net::IpAddr;

use async_trait::async_trait;
use beam_types::ResourceRef;
use chrono::Utc;
use serde_json::Value;

use crate::services::{ProfileResolver, Resolution, ResolvedProfile, ResourceResolver};
use crate::types::{normalize_identity, ExecutionContext};

fn refused<T>(reason: &str) -> Resolution<T> {
    Err(reason.to_owned())
}

fn interrupted(execution: &ExecutionContext) -> Option<&'static str> {
    if execution.cancelled {
        return Some("cancelled");
    }
    if execution.deadline.is_some_and(|d| Utc::now() > d) {
        return Some("deadline");
    }
    None
}

/// Rows of `{text, tenant_id, user_id}`.
#[derive(Debug, Clone, Default)]
pub struct InMemoryProfileResolver {
    rows: HashMap<String, Value>,
}

impl InMemoryProfileResolver {
    #[must_use]
    pub fn new(rows: HashMap<String, Value>) -> Self {
        Self { rows }
    }
}

#[async_trait]
impl ProfileResolver for InMemoryProfileResolver {
    async fn resolve(
        &self,
        profile_id: &str,
        execution: &ExecutionContext,
    ) -> Resolution<ResolvedProfile> {
        if let Some(reason) = interrupted(execution) {
            return refused(reason);
        }
        let Some(row) = self.rows.get(profile_id) else {
            return refused("unavailable");
        };
        // A non-string id still binds
        // the profile to a tenant or user (fail closed, never skipped).
        let field = |key: &str| match row.get(key) {
            None | Some(Value::Null) => None,
            Some(Value::String(s)) => normalize_identity(Some(s)),
            Some(other) => normalize_identity(Some(&other.to_string())),
        };
        let tenant = field("tenant_id");
        let owner = field("user_id");
        if tenant.is_some() && tenant != normalize_identity(execution.tenant_id.as_deref()) {
            return refused("unauthorized");
        }
        if owner.is_some() && owner != normalize_identity(execution.user_id.as_deref()) {
            return refused("unauthorized");
        }
        Ok(ResolvedProfile {
            profile_id: profile_id.to_owned(),
            text: row
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            tenant_id: tenant,
            user_id: owner,
        })
    }
}

const DNS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
const BLOCKED_HOSTS: [&str; 3] = [
    "localhost",
    "localhost.localdomain",
    "metadata.google.internal",
];

/// Python `_BLOCKED_NETWORKS` plus `is_private` / `is_loopback`.
fn blocked_address(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(v4) => {
            let [a, b, ..] = v4.octets();
            v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || a == 0
                || v4.is_broadcast()
                || v4.is_documentation()
                // 100.64.0.0/10 (shared address space) is private to Python.
                || (a == 100 && (64..128).contains(&b))
                // 192.0.0.0/24, 198.18.0.0/15, 240.0.0.0/4 are private to Python too.
                || (a == 192 && b == 0 && v4.octets()[2] == 0)
                || (a == 198 && (b == 18 || b == 19))
                || a >= 240
        }
        IpAddr::V6(v6) => {
            if let Some(mapped) = v6.to_ipv4_mapped() {
                return blocked_address(IpAddr::V4(mapped));
            }
            let first = v6.segments()[0];
            v6.is_loopback()
                || v6.is_unspecified()
                // fc00::/7 unique local, fe80::/10 link local, 2001:db8::/32 documentation.
                || (first & 0xfe00) == 0xfc00
                || (first & 0xffc0) == 0xfe80
                || (first == 0x2001 && v6.segments()[1] == 0x0db8)
        }
    }
}

/// An unresolvable host is treated as private.
async fn is_private_host(host: &str) -> bool {
    if host.is_empty() {
        return true;
    }
    if let Ok(address) = host.parse::<IpAddr>() {
        return blocked_address(address);
    }
    // A host that does not resolve in time is treated as private (fail closed).
    let lookup = tokio::time::timeout(DNS_TIMEOUT, tokio::net::lookup_host((host, 0))).await;
    match lookup.unwrap_or_else(|_| Err(std::io::Error::from(std::io::ErrorKind::TimedOut))) {
        Ok(addresses) => {
            let addresses: Vec<IpAddr> = addresses.map(|a| a.ip()).collect();
            addresses.is_empty() || addresses.into_iter().any(blocked_address)
        }
        Err(_) => true,
    }
}

/// Resolves request resources the policy allows (schemes, hosts, size).
#[derive(Debug, Clone)]
pub struct AuthorizedResourceResolver {
    schemes: HashSet<String>,
    hosts: HashSet<String>,
    max_bytes: u64,
    allowed_mimes: HashSet<String>,
}

impl AuthorizedResourceResolver {
    #[must_use]
    pub fn new(schemes: &[String], hosts: &[String], max_bytes: u64) -> Self {
        let schemes: HashSet<String> = if schemes.is_empty() {
            ["https", "http"].iter().map(|s| (*s).to_owned()).collect()
        } else {
            schemes.iter().map(|s| s.to_lowercase()).collect()
        };
        Self {
            schemes,
            hosts: hosts.iter().map(|h| h.to_lowercase()).collect(),
            max_bytes,
            allowed_mimes: HashSet::new(),
        }
    }

    #[must_use]
    pub fn with_allowed_mimes(mut self, mimes: &[String]) -> Self {
        self.allowed_mimes = mimes.iter().cloned().collect();
        self
    }

    fn mime_refused(&self, resource: &ResourceRef) -> bool {
        resource
            .mime_type
            .as_ref()
            .is_some_and(|m| !self.allowed_mimes.is_empty() && !self.allowed_mimes.contains(m))
    }

    fn oversized_claim(&self, resource: &ResourceRef) -> bool {
        resource
            .max_bytes
            .is_some_and(|b| b > 0 && b > self.max_bytes)
    }

    /// Caller-supplied text; the URI is claimed provenance only.
    fn accept_claimed(&self, resource: &ResourceRef, text: &str) -> Resolution<String> {
        if self.mime_refused(resource) {
            return refused("unsupported_mime");
        }
        if text.len() as u64 > self.max_bytes || self.oversized_claim(resource) {
            return refused("oversized");
        }
        Ok(text.to_owned())
    }

    /// Authorize a fetch. Beam does not fetch, so a valid public URI is
    /// `unavailable`.
    async fn authorize_loader(&self, resource: &ResourceRef) -> Resolution<String> {
        let (scheme, host) = match url::Url::parse(&resource.uri) {
            Ok(url) => (
                url.scheme().to_lowercase(),
                url.host_str()
                    .unwrap_or_default()
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .to_lowercase(),
            ),
            Err(_) => (String::new(), String::new()),
        };
        if !self.schemes.contains(&scheme) || matches!(scheme.as_str(), "file" | "gopher" | "ftp") {
            return refused("ssrf");
        }
        if BLOCKED_HOSTS.contains(&host.as_str()) || host.ends_with(".localhost") {
            return refused("ssrf");
        }
        if !self.hosts.is_empty() && !self.hosts.contains(&host) {
            return refused("unauthorized");
        }
        if is_private_host(&host).await {
            return refused("ssrf");
        }
        if self.mime_refused(resource) {
            return refused("unsupported_mime");
        }
        if self.oversized_claim(resource) {
            return refused("oversized");
        }
        refused("unavailable")
    }
}

#[async_trait]
impl ResourceResolver for AuthorizedResourceResolver {
    async fn resolve(
        &self,
        resource: &ResourceRef,
        execution: &ExecutionContext,
    ) -> Resolution<String> {
        if let Some(reason) = interrupted(execution) {
            return refused(reason);
        }
        if resource.expiry.is_some_and(|e| Utc::now() >= e) {
            return refused("expired");
        }
        match &resource.resolved_text {
            Some(text) => self.accept_claimed(resource, text),
            None => self.authorize_loader(resource).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resource(uri: &str) -> ResourceRef {
        ResourceRef {
            uri: uri.into(),
            kind: "resource".into(),
            expiry: None,
            mime_type: None,
            max_bytes: None,
            resolved_text: None,
        }
    }

    async fn reason(resolver: &AuthorizedResourceResolver, uri: &str) -> String {
        resolver
            .resolve(&resource(uri), &ExecutionContext::default())
            .await
            .unwrap_err()
    }

    #[tokio::test]
    async fn refuses_private_and_metadata_hosts_without_fetching() {
        let resolver = AuthorizedResourceResolver::new(&[], &[], 1_000_000);
        for uri in [
            "http://169.254.169.254/latest/meta-data/",
            "http://127.0.0.1:8080/",
            "http://10.1.2.3/x",
            "http://localhost/",
            "http://metadata.google.internal/",
            "http://[::1]/",
            "http://[fd00::1]/",
            "http://app.localhost/",
            "file:///etc/passwd",
            "gopher://example.com/",
        ] {
            assert_eq!(reason(&resolver, uri).await, "ssrf", "{uri}");
        }
        // A public literal address passes authorization; Beam never fetches.
        assert_eq!(
            reason(&resolver, "https://93.184.216.34/doc").await,
            "unavailable"
        );
    }

    #[tokio::test]
    async fn host_allowlist_and_claimed_text() {
        let resolver = AuthorizedResourceResolver::new(&[], &["docs.example.com".into()], 4);
        assert_eq!(
            reason(&resolver, "https://93.184.216.34/").await,
            "unauthorized"
        );
        let mut claimed = resource("https://anything/");
        claimed.resolved_text = Some("abcd".into());
        let ctx = ExecutionContext::default();
        assert_eq!(resolver.resolve(&claimed, &ctx).await.unwrap(), "abcd");
        claimed.resolved_text = Some("abcde".into());
        assert_eq!(
            resolver.resolve(&claimed, &ctx).await.unwrap_err(),
            "oversized"
        );
    }

    #[tokio::test]
    async fn profiles_fail_closed_across_tenants() {
        let rows = HashMap::from([(
            "p1".to_owned(),
            serde_json::json!({"text": "likes tea", "tenant_id": "org-a", "user_id": "u1"}),
        )]);
        let resolver = InMemoryProfileResolver::new(rows);
        let mut ctx = ExecutionContext {
            tenant_id: Some("org-b".into()),
            user_id: Some("u1".into()),
            ..ExecutionContext::default()
        };
        assert_eq!(
            resolver.resolve("p1", &ctx).await.unwrap_err(),
            "unauthorized"
        );
        ctx.tenant_id = Some("org-a".into());
        assert_eq!(
            resolver.resolve("p1", &ctx).await.unwrap().text,
            "likes tea"
        );
        assert_eq!(
            resolver.resolve("p2", &ctx).await.unwrap_err(),
            "unavailable"
        );
    }
}
