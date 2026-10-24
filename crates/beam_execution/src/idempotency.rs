//! Idempotency scopes (port of fluvius-beam `execution/idempotency.py`).

use uuid::Uuid;

use crate::identity::{coerce_org_id, normalize_identity, NULL_IDENTITY};

/// Normalized key, user and agent used for lookup and the unique SQL scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdempotencyScope {
    pub key: String,
    pub user_id: Option<String>,
    pub agent_name: String,
    pub org_id: Option<Uuid>,
}

impl IdempotencyScope {
    /// `sql_user_id`: the user as a UUID, or [`NULL_IDENTITY`] when absent.
    /// `None` when the user id is not a UUID (Python raises; callers reject the request).
    #[must_use]
    pub fn sql_user_id(&self) -> Option<Uuid> {
        match &self.user_id {
            None => Some(NULL_IDENTITY),
            Some(u) => Uuid::parse_str(u).ok(),
        }
    }

    /// `lock_parts`: the two strings hashed into Python's
    /// `pg_advisory_xact_lock(hashtext(a), hashtext(b))`, so both stacks take the same lock.
    #[must_use]
    pub fn lock_parts(&self) -> (String, String) {
        (
            self.key.clone(),
            format!(
                "{}\u{1f}{}",
                self.user_id.as_deref().unwrap_or(""),
                self.agent_name
            ),
        )
    }
}

/// `normalize_idempotency_key`: trimmed, `None` when empty.
#[must_use]
pub fn normalize_idempotency_key(key: Option<&str>) -> Option<String> {
    let text = key?.trim();
    (!text.is_empty()).then(|| text.to_owned())
}

/// `normalize_idempotency_scope`: `None` when there is no usable key.
#[must_use]
pub fn normalize_idempotency_scope(
    key: Option<&str>,
    user_id: Option<&str>,
    agent_name: &str,
    org_id: Option<&str>,
) -> Option<IdempotencyScope> {
    Some(IdempotencyScope {
        key: normalize_idempotency_key(key)?,
        user_id: normalize_identity(user_id),
        agent_name: agent_name.trim().to_owned(),
        org_id: coerce_org_id(org_id),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_like_python() {
        assert_eq!(
            normalize_idempotency_scope(Some("  "), None, "a", None),
            None
        );
        let user = Uuid::new_v4().to_string();
        let scope =
            normalize_idempotency_scope(Some(" k1 "), Some(&user), " agent ", Some("not-uuid"))
                .unwrap();
        assert_eq!(scope.key, "k1");
        assert_eq!(scope.agent_name, "agent");
        assert_eq!(scope.org_id, None);
        assert_eq!(
            scope.lock_parts(),
            ("k1".to_owned(), format!("{user}\u{1f}agent"))
        );
        let anonymous = normalize_idempotency_scope(Some("k"), Some("null"), "a", None).unwrap();
        assert_eq!(anonymous.sql_user_id(), Some(NULL_IDENTITY));
        assert_eq!(anonymous.lock_parts().1, "\u{1f}a");
    }
}
