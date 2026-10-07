use std::fs;
use std::path::{Path, PathBuf};

use riverbase_core::RiverbaseResult;

/// Path-jailed workspace root. Hosts bind this to a dest checkout.
#[derive(Debug, Clone)]
pub struct SandboxRoot {
    root: PathBuf,
}

impl SandboxRoot {
    /// Create (or open) a sandbox rooted at `root`. The directory is created
    /// if missing and then canonicalized.
    pub fn new(root: impl AsRef<Path>) -> RiverbaseResult<Self> {
        let root = root.as_ref();
        fs::create_dir_all(root).map_err(|e| crate::BEM_310.with_data(e.to_string()))?;
        Ok(Self {
            root: root
                .canonicalize()
                .map_err(|e| crate::BEM_311.with_data(e.to_string()))?,
        })
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.root
    }

    /// Resolve `rel` inside the jail. Rejects absolute paths, NUL, and `..`.
    pub fn resolve(&self, rel: &str) -> RiverbaseResult<PathBuf> {
        if rel.is_empty() {
            return Ok(self.root.clone());
        }
        if rel.starts_with('/') || rel.starts_with('\\') || rel.contains('\0') {
            return Err(crate::BEM_300.with_data(rel.to_string()));
        }
        let components = Path::new(rel).components();
        for c in components {
            match c {
                std::path::Component::Normal(_) | std::path::Component::CurDir => {}
                _ => return Err(crate::BEM_301.with_data(rel.to_string())),
            }
        }
        let candidate = self.root.join(rel);
        if candidate.exists() {
            let canon = candidate
                .canonicalize()
                .map_err(|e| crate::BEM_312.with_data(e.to_string()))?;
            if !canon.starts_with(&self.root) {
                return Err(crate::BEM_302.with_data(rel.to_string()));
            }
            return Ok(canon);
        }
        let parent = candidate.parent().unwrap_or(&self.root);
        if parent.exists() {
            let parent = parent
                .canonicalize()
                .map_err(|e| crate::BEM_313.with_data(e.to_string()))?;
            if !parent.starts_with(&self.root) {
                return Err(crate::BEM_303.with_data(rel.to_string()));
            }
            let name = candidate
                .file_name()
                .ok_or_else(|| crate::BEM_304.with_data(rel.to_string()))?;
            return Ok(parent.join(name));
        }
        // Parent missing: still require the join stays under root by prefix.
        if !candidate.starts_with(&self.root) {
            return Err(crate::BEM_305.with_data(rel.to_string()));
        }
        Ok(candidate)
    }

    pub fn list_tree(&self, rel: &str) -> RiverbaseResult<Vec<String>> {
        let dir = self.resolve(rel)?;
        if !dir.is_dir() {
            return Err(crate::BEM_306.with_data(dir.display().to_string()));
        }
        let mut out = Vec::new();
        collect_tree(&self.root, &dir, &mut out)?;
        out.sort();
        Ok(out)
    }

    pub fn read_file(&self, rel: &str) -> RiverbaseResult<String> {
        let path = self.resolve(rel)?;
        fs::read_to_string(&path).map_err(|e| {
            crate::BEM_307.with_data(format!("{}: {e}", path.display()))
        })
    }

    pub fn write_file(&self, rel: &str, body: &str) -> RiverbaseResult<()> {
        let path = self.resolve(rel)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| crate::BEM_314.with_data(e.to_string()))?;
            let parent = parent
                .canonicalize()
                .map_err(|e| crate::BEM_315.with_data(e.to_string()))?;
            if !parent.starts_with(&self.root) {
                return Err(crate::BEM_308.with_data(rel.to_string()));
            }
        }
        fs::write(&path, body).map_err(|e| crate::BEM_316.with_data(e.to_string()))?;
        Ok(())
    }
}

fn collect_tree(root: &Path, dir: &Path, out: &mut Vec<String>) -> RiverbaseResult<()> {
    for entry in fs::read_dir(dir).map_err(|e| crate::BEM_318.with_data(e.to_string()))? {
        let entry = entry.map_err(|e| crate::BEM_317.with_data(e.to_string()))?;
        let path = entry.path();
        let name = entry.file_name();
        if name == ".git" {
            continue;
        }
        if path.is_dir() {
            collect_tree(root, &path, out)?;
        } else {
            let rel = path
                .strip_prefix(root)
                .map_err(|_| crate::BEM_309.with_data(path.display().to_string()))?;
            out.push(rel.to_string_lossy().replace('\\', "/"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn rejects_parent_escape() {
        let dir = tempdir().unwrap();
        let sb = SandboxRoot::new(dir.path()).unwrap();
        assert!(sb.resolve("../secret").is_err());
        assert!(sb.resolve("foo/../../secret").is_err());
    }

    #[test]
    fn write_and_read_roundtrip() {
        let dir = tempdir().unwrap();
        let sb = SandboxRoot::new(dir.path()).unwrap();
        sb.write_file("docs/a.md", "hello").unwrap();
        assert_eq!(sb.read_file("docs/a.md").unwrap(), "hello");
        let tree = sb.list_tree("").unwrap();
        assert_eq!(tree, vec!["docs/a.md".to_string()]);
    }

    #[test]
    fn rejects_absolute() {
        let dir = tempdir().unwrap();
        let sb = SandboxRoot::new(dir.path()).unwrap();
        assert!(sb.resolve("/etc/passwd").is_err());
    }
}
