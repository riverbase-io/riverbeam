//! Fail-closed knowledge visibility (Python `knowledge/visibility.py`). A
//! missing identity never broadens access.

use crate::types::{KnowledgeScope, RetrievalFilter, VectorRecord};

/// Python `record_is_visible`. Without filters only unowned global rows are
/// visible; `user`, `agent` and `org` rows need the matching identity.
#[must_use]
pub fn record_is_visible(record: &VectorRecord, filters: Option<&RetrievalFilter>) -> bool {
    let Some(filters) = filters else {
        return matches!(record.scope, None | Some(KnowledgeScope::Global))
            && record.user_id.is_none()
            && record.agent_id.is_none()
            && record.org_id.is_none();
    };
    if filters.scope.is_some_and(|s| record.scope != Some(s)) {
        return false;
    }
    if let Some(scopes) = &filters.scopes {
        if !record.scope.is_some_and(|s| scopes.contains(&s)) {
            return false;
        }
    }
    if !identity_visible(record, filters) {
        return false;
    }
    if let Some(ids) = &filters.document_ids {
        if !record.document_id.as_ref().is_some_and(|d| ids.contains(d)) {
            return false;
        }
    }
    if let Some(types) = &filters.memory_types {
        let kind = record
            .metadata
            .get("type")
            .and_then(serde_json::Value::as_str);
        if !kind.is_some_and(|k| types.iter().any(|t| t == k)) {
            return false;
        }
    }
    filters
        .extra
        .iter()
        .all(|(key, value)| record.metadata.get(key) == Some(value))
}

fn present(value: Option<&String>) -> bool {
    value.is_some_and(|v| !v.is_empty())
}

fn identity_visible(record: &VectorRecord, filters: &RetrievalFilter) -> bool {
    let owned = |rec: Option<&String>, req: Option<&String>| present(req) && rec == req;
    match record.scope {
        Some(KnowledgeScope::User) => {
            return owned(record.user_id.as_ref(), filters.user_id.as_ref())
        }
        Some(KnowledgeScope::Agent) => {
            return owned(record.agent_id.as_ref(), filters.agent_id.as_ref())
        }
        Some(KnowledgeScope::Org) => return owned(record.org_id.as_ref(), filters.org_id.as_ref()),
        _ => {}
    }
    // An owned row outside its scope still needs that owner.
    for (rec, req) in [
        (record.user_id.as_ref(), filters.user_id.as_ref()),
        (record.agent_id.as_ref(), filters.agent_id.as_ref()),
        (record.org_id.as_ref(), filters.org_id.as_ref()),
    ] {
        if present(rec) && (rec != req || !present(req)) {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Namespace;

    fn record(
        scope: Option<KnowledgeScope>,
        user: Option<&str>,
        org: Option<&str>,
    ) -> VectorRecord {
        let mut r = VectorRecord::new("r", Namespace::Knowledge, "t", vec![]);
        r.scope = scope;
        r.user_id = user.map(str::to_owned);
        r.org_id = org.map(str::to_owned);
        r
    }

    #[test]
    fn missing_identity_never_broadens_access() {
        let org_row = record(Some(KnowledgeScope::Org), None, Some("o1"));
        let no_org = RetrievalFilter {
            user_id: Some("u1".into()),
            ..RetrievalFilter::default()
        };
        assert!(!record_is_visible(&org_row, Some(&no_org)));
        assert!(!record_is_visible(&org_row, None));
        let in_org = RetrievalFilter {
            org_id: Some("o1".into()),
            ..RetrievalFilter::default()
        };
        assert!(record_is_visible(&org_row, Some(&in_org)));
        let other_org = RetrievalFilter {
            org_id: Some("o2".into()),
            ..RetrievalFilter::default()
        };
        assert!(!record_is_visible(&org_row, Some(&other_org)));
    }

    #[test]
    fn global_rows_and_owned_global_rows() {
        let global = record(Some(KnowledgeScope::Global), None, None);
        assert!(record_is_visible(&global, None));
        assert!(record_is_visible(
            &global,
            Some(&RetrievalFilter::default())
        ));
        // A global row with an owner is visible to that owner only.
        let owned = record(Some(KnowledgeScope::Global), Some("u1"), None);
        assert!(!record_is_visible(&owned, None));
        assert!(!record_is_visible(
            &owned,
            Some(&RetrievalFilter::default())
        ));
        let me = RetrievalFilter {
            user_id: Some("u1".into()),
            ..RetrievalFilter::default()
        };
        assert!(record_is_visible(&owned, Some(&me)));
    }

    #[test]
    fn extra_and_memory_types_must_match() {
        let mut row = record(None, None, None);
        row.metadata.insert("type".into(), "preference".into());
        row.metadata
            .insert("embedding_revision".into(), "v2".into());
        let mut f = RetrievalFilter {
            memory_types: Some(vec!["preference".into()]),
            ..RetrievalFilter::default()
        };
        f.extra.insert("embedding_revision".into(), "v2".into());
        assert!(record_is_visible(&row, Some(&f)));
        f.extra.insert("embedding_revision".into(), "v1".into());
        assert!(!record_is_visible(&row, Some(&f)));
    }
}
