use std::path::Path;

use riverbase_core::RiverbaseResult;
use git2::{IndexAddOption, Repository, Signature, StatusOptions};

use crate::jail::SandboxRoot;

/// Git working tree inside a [`SandboxRoot`]. `protected_refs` are never pushed.
#[derive(Debug, Clone)]
pub struct WorkingTree {
    sandbox: SandboxRoot,
    protected_refs: Vec<String>,
}

impl WorkingTree {
    #[must_use]
    pub fn new(sandbox: SandboxRoot, protected_refs: Vec<String>) -> Self {
        Self {
            sandbox,
            protected_refs,
        }
    }

    #[must_use]
    pub fn sandbox(&self) -> &SandboxRoot {
        &self.sandbox
    }

    pub fn init(&self) -> RiverbaseResult<Repository> {
        let repo = Repository::init(self.sandbox.path())
            .map_err(|e| crate::BEM_320.with_data(e.to_string()))?;
        Ok(repo)
    }

    fn open(&self) -> RiverbaseResult<Repository> {
        Repository::open(self.sandbox.path())
            .map_err(|e| crate::BEM_321.with_data(e.to_string()))
    }

    pub fn status(&self) -> RiverbaseResult<Vec<String>> {
        let repo = self.open()?;
        let mut opts = StatusOptions::new();
        opts.include_untracked(true)
            .include_ignored(false)
            .exclude_submodules(true);
        let statuses = repo
            .statuses(Some(&mut opts))
            .map_err(|e| crate::BEM_322.with_data(e.to_string()))?;
        Ok(statuses
            .iter()
            .filter_map(|e| e.path().ok().map(str::to_string))
            .collect())
    }

    pub fn diff(&self) -> RiverbaseResult<String> {
        let repo = self.open()?;
        let mut opts = git2::DiffOptions::new();
        let diff = match repo.head() {
            Ok(head) => {
                let tree = head
                    .peel_to_tree()
                    .map_err(|e| crate::BEM_323.with_data(e.to_string()))?;
                repo.diff_tree_to_workdir_with_index(Some(&tree), Some(&mut opts))
                    .map_err(|e| crate::BEM_323.with_data(e.to_string()))?
            }
            Err(_) => repo
                .diff_tree_to_workdir_with_index(None, Some(&mut opts))
                .map_err(|e| crate::BEM_323.with_data(e.to_string()))?,
        };
        let mut buf = String::new();
        diff.print(git2::DiffFormat::Patch, |_d, _h, line| {
            if let Ok(s) = std::str::from_utf8(line.content()) {
                buf.push_str(s);
            }
            true
        })
        .map_err(|e| crate::BEM_323.with_data(e.to_string()))?;
        Ok(buf)
    }

    pub fn log(&self, limit: usize) -> RiverbaseResult<Vec<String>> {
        let repo = self.open()?;
        let mut revwalk = repo
            .revwalk()
            .map_err(|e| crate::BEM_324.with_data(e.to_string()))?;
        if repo.head().is_ok() {
            revwalk
                .push_head()
                .map_err(|e| crate::BEM_324.with_data(e.to_string()))?;
        } else {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for (i, oid) in revwalk.enumerate() {
            if i >= limit {
                break;
            }
            let commit = repo
                .find_commit(oid.map_err(|e| crate::BEM_324.with_data(e.to_string()))?)
                .map_err(|e| crate::BEM_324.with_data(e.to_string()))?;
            out.push(
                commit
                    .summary()
                    .ok()
                    .flatten()
                    .unwrap_or("")
                    .to_string(),
            );
        }
        Ok(out)
    }

    pub fn add(&self, rel: &str) -> RiverbaseResult<()> {
        let _ = self.sandbox.resolve(rel)?;
        let repo = self.open()?;
        let mut index = repo
            .index()
            .map_err(|e| crate::BEM_325.with_data(e.to_string()))?;
        if rel.is_empty() || rel == "." {
            index
                .add_all(["*"].iter(), IndexAddOption::DEFAULT, None)
                .map_err(|e| crate::BEM_325.with_data(e.to_string()))?;
        } else {
            index
                .add_path(Path::new(rel))
                .map_err(|e| crate::BEM_325.with_data(e.to_string()))?;
        }
        index
            .write()
            .map_err(|e| crate::BEM_325.with_data(e.to_string()))?;
        Ok(())
    }

    pub fn commit(&self, message: &str) -> RiverbaseResult<String> {
        let repo = self.open()?;
        let mut index = repo
            .index()
            .map_err(|e| crate::BEM_326.with_data(e.to_string()))?;
        let tree_id = index
            .write_tree()
            .map_err(|e| crate::BEM_326.with_data(e.to_string()))?;
        let tree = repo
            .find_tree(tree_id)
            .map_err(|e| crate::BEM_326.with_data(e.to_string()))?;
        let sig = Signature::now("beam-agent", "beam@localhost")
            .map_err(|e| crate::BEM_326.with_data(e.to_string()))?;
        let parent = repo.head().ok().and_then(|h| h.peel_to_commit().ok());
        let oid = if let Some(ref p) = parent {
            repo.commit(Some("HEAD"), &sig, &sig, message, &tree, &[p])
                .map_err(|e| crate::BEM_326.with_data(e.to_string()))?
        } else {
            repo.commit(Some("HEAD"), &sig, &sig, message, &tree, &[])
                .map_err(|e| crate::BEM_326.with_data(e.to_string()))?
        };
        Ok(oid.to_string())
    }

    pub fn current_branch(&self) -> RiverbaseResult<String> {
        let repo = self.open()?;
        let head = repo
            .head()
            .map_err(|e| crate::BEM_327.with_data(e.to_string()))?;
        Ok(head.shorthand().unwrap_or("HEAD").to_string())
    }

    fn is_protected(&self, refname: &str) -> bool {
        let short = refname.strip_prefix("refs/heads/").unwrap_or(refname);
        self.protected_refs
            .iter()
            .any(|p| p == short || p == refname)
    }

    /// Push `refname` (current branch if None) to `origin`. Protected refs are denied.
    pub fn push(&self, refname: Option<&str>) -> RiverbaseResult<()> {
        let repo = self.open()?;
        let branch = match refname {
            Some(r) => r.strip_prefix("refs/heads/").unwrap_or(r).to_string(),
            None => self.current_branch()?,
        };
        if self.is_protected(&branch) {
            return Err(crate::BEM_328.with_data(branch));
        }
        let mut remote = repo
            .find_remote("origin")
            .map_err(|_| crate::BEM_329.raise())?;
        let spec = format!("refs/heads/{branch}:refs/heads/{branch}");
        remote
            .push(&[&spec], None)
            .map_err(|e| crate::BEM_330.with_data(e.to_string()))?;
        Ok(())
    }

    pub fn add_origin(&self, url: &str) -> RiverbaseResult<()> {
        let repo = self.open()?;
        match repo.find_remote("origin") {
            Ok(_) => {
                repo.remote_set_url("origin", url)
                    .map_err(|e| crate::BEM_331.with_data(e.to_string()))?;
            }
            Err(_) => {
                repo.remote("origin", url)
                    .map_err(|e| crate::BEM_331.with_data(e.to_string()))?;
            }
        }
        Ok(())
    }

    pub fn checkout_branch(&self, name: &str, create: bool) -> RiverbaseResult<()> {
        if self.is_protected(name) && create {
            // creating a branch named main is fine; pushing it is not
        }
        let repo = self.open()?;
        if create {
            let commit = repo
                .head()
                .map_err(|e| crate::BEM_332.with_data(e.to_string()))?
                .peel_to_commit()
                .map_err(|e| crate::BEM_332.with_data(e.to_string()))?;
            let _ = repo.branch(name, &commit, false);
        }
        let (obj, git_ref) = repo
            .revparse_ext(name)
            .map_err(|e| crate::BEM_332.with_data(e.to_string()))?;
        repo.checkout_tree(&obj, None)
            .map_err(|e| crate::BEM_332.with_data(e.to_string()))?;
        if let Some(r) = git_ref {
            repo.set_head(r.name().unwrap_or(name))
                .map_err(|e| crate::BEM_332.with_data(e.to_string()))?;
        } else {
            repo.set_head(&format!("refs/heads/{name}"))
                .map_err(|e| crate::BEM_332.with_data(e.to_string()))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn tree(protected: &[&str]) -> (tempfile::TempDir, WorkingTree) {
        let dir = tempdir().unwrap();
        let sb = SandboxRoot::new(dir.path()).unwrap();
        let wt = WorkingTree::new(
            sb,
            protected.iter().map(|s| (*s).to_string()).collect(),
        );
        wt.init().unwrap();
        (dir, wt)
    }

    #[test]
    fn commit_and_status() {
        let (_dir, wt) = tree(&["main"]);
        wt.sandbox().write_file("README.md", "hi\n").unwrap();
        assert!(wt.status().unwrap().contains(&"README.md".to_string()));
        wt.add("README.md").unwrap();
        let oid = wt.commit("init").unwrap();
        assert_eq!(oid.len(), 40);
        assert!(wt.log(5).unwrap().contains(&"init".to_string()));
        assert!(wt.status().unwrap().is_empty());
    }

    #[test]
    fn push_protected_denied() {
        let (_dir, wt) = tree(&["main", "master"]);
        wt.sandbox().write_file("a.md", "x\n").unwrap();
        wt.add("a.md").unwrap();
        wt.commit("init").unwrap();
        let err = wt.push(Some("main")).unwrap_err();
        assert_eq!(err.errcode.as_str(), "BEM-328");
    }

    #[test]
    fn push_feature_to_bare_origin() {
        let dir = tempdir().unwrap();
        let work = dir.path().join("work");
        let bare = dir.path().join("bare.git");
        git2::Repository::init_bare(&bare).unwrap();
        let sb = SandboxRoot::new(&work).unwrap();
        let wt = WorkingTree::new(sb, vec!["main".into()]);
        wt.init().unwrap();
        wt.sandbox().write_file("f.md", "ok\n").unwrap();
        wt.add("f.md").unwrap();
        wt.commit("init").unwrap();
        wt.checkout_branch("change-1", true).unwrap();
        wt.add_origin(&format!("file://{}", bare.display())).unwrap();
        wt.push(Some("change-1")).unwrap();
        let bare_repo = git2::Repository::open(&bare).unwrap();
        assert!(bare_repo.find_reference("refs/heads/change-1").is_ok());
    }
}
