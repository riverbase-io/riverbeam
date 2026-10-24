//! Checkpoint and revision compatibility (Python `execution/compatibility.py`,
//! `runtime/versions.py`).

use std::collections::HashSet;

use riverbase_core::RiverbaseResult;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// How a pinned revision or checkpoint may be read by a newer deployment.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompatibilityPolicy {
    #[default]
    Strict,
    Compatible,
    Migratable,
}

/// Whether an execution pinned to `pinned` may resume on revision `current`.
#[must_use]
#[allow(clippy::implicit_hasher)]
pub fn may_resume(
    policy: CompatibilityPolicy,
    pinned: Uuid,
    current: Uuid,
    compatible_with: Option<&HashSet<Uuid>>,
) -> bool {
    if pinned == current {
        return true;
    }
    match policy {
        CompatibilityPolicy::Strict => false,
        CompatibilityPolicy::Compatible => compatible_with.is_some_and(|c| c.contains(&pinned)),
        CompatibilityPolicy::Migratable => true,
    }
}

/// A checkpoint with no versions is readable.
#[must_use]
pub fn same_checkpoint_versions(
    pinned_adapter: Option<&str>,
    deployed_adapter: Option<&str>,
    pinned_library: Option<&str>,
    deployed_library: Option<&str>,
) -> bool {
    if pinned_adapter.is_none() && pinned_library.is_none() {
        return true;
    }
    pinned_adapter == deployed_adapter && pinned_library == deployed_library
}

/// Versions of a stored checkpoint and of the deployed runtime.
#[derive(Debug, Clone, Copy, Default)]
pub struct CheckpointVersions<'a> {
    pub pinned_adapter: Option<&'a str>,
    pub deployed_adapter: Option<&'a str>,
    pub pinned_library: Option<&'a str>,
    pub deployed_library: Option<&'a str>,
}

/// Whether a deployed runtime can read a checkpoint written by another version.
#[must_use]
#[allow(clippy::implicit_hasher)]
pub fn checkpoint_may_read(
    policy: CompatibilityPolicy,
    v: CheckpointVersions<'_>,
    compatible_adapters: Option<&HashSet<String>>,
) -> bool {
    if same_checkpoint_versions(
        v.pinned_adapter,
        v.deployed_adapter,
        v.pinned_library,
        v.deployed_library,
    ) {
        return true;
    }
    match policy {
        CompatibilityPolicy::Strict => false,
        CompatibilityPolicy::Compatible => v
            .pinned_adapter
            .zip(compatible_adapters)
            .is_some_and(|(pinned, ok)| ok.contains(pinned)),
        CompatibilityPolicy::Migratable => true,
    }
}

/// Fail with `BEM-392` (public `BEM-430`,
/// `CheckpointUnreadableError`) unless the checkpoint may be read.
///
/// # Errors
/// `BEM-392` when the checkpoint is unreadable or `can_read` is `Some(false)`.
#[allow(clippy::implicit_hasher)]
pub fn assert_checkpoint_readable(
    policy: CompatibilityPolicy,
    v: CheckpointVersions<'_>,
    compatible_adapters: Option<&HashSet<String>>,
    can_read: Option<bool>,
) -> RiverbaseResult<()> {
    if can_read == Some(false) || !checkpoint_may_read(policy, v, compatible_adapters) {
        return Err(crate::BEM_392.with_data(format!(
            "checkpoint written by adapter {} / library {} cannot be read by deployed adapter {} / library {}",
            py_none(v.pinned_adapter),
            py_none(v.pinned_library),
            py_none(v.deployed_adapter),
            py_none(v.deployed_library),
        )));
    }
    Ok(())
}

/// `None` printed the way Python's f-string prints it.
fn py_none(v: Option<&str>) -> &str {
    v.unwrap_or("None")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn versions<'a>(pa: &'a str, da: &'a str, pl: &'a str, dl: &'a str) -> CheckpointVersions<'a> {
        CheckpointVersions {
            pinned_adapter: Some(pa),
            deployed_adapter: Some(da),
            pinned_library: Some(pl),
            deployed_library: Some(dl),
        }
    }

    #[test]
    fn checkpoint_rules_match_python() {
        let same = versions("1", "1", "a", "a");
        let other = versions("1", "2", "a", "a");
        assert!(checkpoint_may_read(CompatibilityPolicy::Strict, same, None));
        assert!(checkpoint_may_read(
            CompatibilityPolicy::Strict,
            CheckpointVersions::default(),
            None
        ));
        assert!(!checkpoint_may_read(
            CompatibilityPolicy::Strict,
            other,
            None
        ));
        let ok: HashSet<String> = ["1".to_owned()].into();
        assert!(checkpoint_may_read(
            CompatibilityPolicy::Compatible,
            other,
            Some(&ok)
        ));
        assert!(!checkpoint_may_read(
            CompatibilityPolicy::Compatible,
            other,
            None
        ));
        assert!(checkpoint_may_read(
            CompatibilityPolicy::Migratable,
            other,
            None
        ));
        let err =
            assert_checkpoint_readable(CompatibilityPolicy::Strict, other, None, None).unwrap_err();
        assert_eq!(err.errcode.as_str(), "BEM-392");
        assert!(
            assert_checkpoint_readable(CompatibilityPolicy::Strict, same, None, Some(false))
                .is_err()
        );
    }

    #[test]
    fn revision_rules_match_python() {
        let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
        assert!(may_resume(CompatibilityPolicy::Strict, a, a, None));
        assert!(!may_resume(CompatibilityPolicy::Strict, a, b, None));
        assert!(may_resume(
            CompatibilityPolicy::Compatible,
            a,
            b,
            Some(&[a].into())
        ));
        assert!(may_resume(CompatibilityPolicy::Migratable, a, b, None));
    }
}
