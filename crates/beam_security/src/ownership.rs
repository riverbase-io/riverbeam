//! One tenant-access decision for every owned entity (Python
//! `security/ownership.py`). Absent ids stay absent; a caller without an org
//! never reaches an org's row.

use uuid::Uuid;

/// Entities whose rows belong to a user.
pub const OWNED_ENTITIES: [&str; 18] = [
    "agent",
    "revision",
    "profile",
    "session",
    "execution",
    "message",
    "memory",
    "knowledge",
    "tool",
    "credential",
    "checkpoint",
    "stream",
    "artifact",
    "evaluation",
    "usage",
    "admin",
    "dead_letter",
    "audit",
];

/// A UUID org, or none.
#[must_use]
pub fn coerce_org_id(value: Option<&str>) -> Option<Uuid> {
    value.and_then(|v| Uuid::parse_str(v.trim()).ok())
}

/// Who owns a row (its org and user columns).
#[derive(Debug, Clone, Copy, Default)]
pub struct Owner<'a> {
    pub org_id: Option<&'a str>,
    pub user_id: Option<&'a str>,
}

/// `Err` with Python's message when `caller`
/// may not touch `row` (`TenantAccessError`, `BEM-411`).
///
/// # Errors
/// An unknown entity, a missing caller org for an org row, another tenant's
/// row, or another user's row.
pub fn assert_tenant_access(entity: &str, row: Owner<'_>, caller: Owner<'_>) -> Result<(), String> {
    if !OWNED_ENTITIES.contains(&entity) {
        return Err(format!("unknown owned entity {entity}"));
    }
    let row_org = coerce_org_id(row.org_id);
    let caller_org = coerce_org_id(caller.org_id);
    match (row_org, caller_org) {
        (Some(_), None) => return Err("tenant identity required".into()),
        (Some(r), Some(c)) if r != c => return Err(format!("{entity} belongs to another tenant")),
        _ => {}
    }
    if let (Some(row_user), Some(user)) = (row.user_id, caller.user_id.filter(|u| !u.is_empty())) {
        if row_user != user {
            return Err(format!("{entity} belongs to another user"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn denies_other_tenants_and_users() {
        let org = Uuid::new_v4().to_string();
        let other = Uuid::new_v4().to_string();
        let row = Owner {
            org_id: Some(&org),
            user_id: Some("u1"),
        };
        let caller = |o: Option<&'static str>, u: Option<&'static str>| Owner {
            org_id: o,
            user_id: u,
        };
        assert_eq!(
            assert_tenant_access("execution", row, caller(None, Some("u1"))).unwrap_err(),
            "tenant identity required"
        );
        let leaked: &'static str = Box::leak(other.into_boxed_str());
        assert_eq!(
            assert_tenant_access("execution", row, caller(Some(leaked), Some("u1"))).unwrap_err(),
            "execution belongs to another tenant"
        );
        let same: &'static str = Box::leak(org.clone().into_boxed_str());
        assert_eq!(
            assert_tenant_access("session", row, caller(Some(same), Some("u2"))).unwrap_err(),
            "session belongs to another user"
        );
        assert!(assert_tenant_access("session", row, caller(Some(same), Some("u1"))).is_ok());
        // A row without an org is not a tenant row.
        assert!(assert_tenant_access("execution", Owner::default(), caller(None, None)).is_ok());
        assert!(assert_tenant_access("widget", Owner::default(), caller(None, None)).is_err());
    }
}
