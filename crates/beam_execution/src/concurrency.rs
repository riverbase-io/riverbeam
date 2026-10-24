//! Concurrency scopes (port of fluvius-beam `execution/concurrency.py`).

use crate::identity::coerce_org_id;

/// `normalize_concurrency_scope`: `"<org or empty>:<key>"`, or `None` for an empty key.
#[must_use]
pub fn normalize_concurrency_scope(
    concurrency_key: Option<&str>,
    org_id: Option<&str>,
) -> Option<String> {
    let text = concurrency_key?.trim();
    if text.is_empty() {
        return None;
    }
    let org = coerce_org_id(org_id)
        .map(|o| o.to_string())
        .unwrap_or_default();
    Some(format!("{org}:{text}"))
}

/// The default key Python uses: `"<session_id>:<agent_name>"`.
#[must_use]
pub fn default_concurrency_key(session_id: uuid::Uuid, agent_name: &str) -> String {
    format!("{session_id}:{agent_name}")
}

/// First part of the advisory lock Python takes on a concurrency slot
/// (`hashtext('beam:conc')`); the repository that takes it owns it.
pub use beam_store::repo::executions::CONCURRENCY_LOCK_NAMESPACE;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scopes_like_python() {
        assert_eq!(normalize_concurrency_scope(Some(" "), None), None);
        assert_eq!(
            normalize_concurrency_scope(Some("s:a"), None).as_deref(),
            Some(":s:a")
        );
        let org = uuid::Uuid::new_v4();
        assert_eq!(
            normalize_concurrency_scope(Some("s:a"), Some(&org.to_string())),
            Some(format!("{org}:s:a"))
        );
    }
}
