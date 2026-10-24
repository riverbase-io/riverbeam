//! Identity normalization (port of fluvius-beam `kernel/identity.py`).

use uuid::Uuid;

/// The all-zero UUID Python uses for "no user" in unique keys (`NULL_IDENTITY`).
pub const NULL_IDENTITY: Uuid = Uuid::nil();

/// A preserved identity string, or `None` when absent: empty or whitespace,
/// the `"none"` / `"null"` sentinels (any case) and the all-zero UUID are absent.
/// Valid values are kept without case folding.
#[must_use]
pub fn normalize_identity(value: Option<&str>) -> Option<String> {
    let text = value?.trim();
    if text.is_empty() || text.eq_ignore_ascii_case("none") || text.eq_ignore_ascii_case("null") {
        return None;
    }
    if text == NULL_IDENTITY.to_string() {
        return None;
    }
    Some(text.to_owned())
}

/// A UUID org id, or `None` when missing or not a UUID.
#[must_use]
pub fn coerce_org_id(value: Option<&str>) -> Option<Uuid> {
    value.and_then(|v| Uuid::parse_str(v.trim()).ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_python_normalize_identity() {
        assert_eq!(normalize_identity(None), None);
        for absent in [
            "",
            "  ",
            "None",
            "null",
            "NULL",
            "00000000-0000-0000-0000-000000000000",
        ] {
            assert_eq!(normalize_identity(Some(absent)), None, "{absent:?}");
        }
        assert_eq!(normalize_identity(Some(" User-1 ")), Some("User-1".into()));
    }

    #[test]
    fn coerces_org_ids() {
        let org = Uuid::new_v4();
        assert_eq!(coerce_org_id(Some(&org.to_string())), Some(org));
        assert_eq!(coerce_org_id(Some("not-a-uuid")), None);
        assert_eq!(coerce_org_id(None), None);
    }
}
