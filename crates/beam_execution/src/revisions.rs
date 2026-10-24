//! Agent revisions (Python `execution/{revisions,snapshot,canary,compatibility,activation}.py`).
//!
//! A bind compiles an [`AgentSpec`] into an immutable [`RevisionSnapshot`];
//! [`RevisionBook`] keeps the revisions, the per-agent traffic policy
//! (primary and canary) and the revision each execution is pinned to.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use beam_types::pyjson::{dumps_sorted_compact, dumps_sorted_compact_unicode};
use beam_types::{AgentSpec, CompatibilityPolicy, RevisionStatus};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::records::ExecResult;

fn sha256_hex(text: &str) -> String {
    format!("{:x}", Sha256::digest(text.as_bytes()))
}

/// Python compiler `_digest`: SHA-256 of the sorted compact ASCII JSON.
#[must_use]
pub fn digest(value: &Value) -> String {
    sha256_hex(&dumps_sorted_compact(value))
}

/// The revision hash of a compiled spec.
#[must_use]
pub fn canonical_spec_hash(spec: &AgentSpec) -> String {
    sha256_hex(&dumps_sorted_compact_unicode(&spec.canonical_payload()))
}

/// The parts of Python `compute_revision_hash`.
#[derive(Debug, Clone, Copy, Default)]
pub struct RevisionHashParts<'a> {
    pub build_id: Option<&'a str>,
    pub toolset_revision: Option<&'a str>,
    pub prompt_revision: Option<&'a str>,
    pub model_policy_revision: Option<&'a str>,
    pub security_policy_revision: Option<&'a str>,
}

/// SHA-256 of the `|`-joined identity.
#[must_use]
pub fn compute_revision_hash(
    agent_name: &str,
    version: &str,
    runtime_name: &str,
    parts: RevisionHashParts<'_>,
) -> String {
    let payload = [
        agent_name,
        version,
        runtime_name,
        parts.build_id.unwrap_or_default(),
        parts.toolset_revision.unwrap_or_default(),
        parts.prompt_revision.unwrap_or_default(),
        parts.model_policy_revision.unwrap_or_default(),
        parts.security_policy_revision.unwrap_or_default(),
    ]
    .join("|");
    sha256_hex(&payload)
}

/// A bind's immutable record.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RevisionSnapshot {
    pub id: Uuid,
    pub agent_id: Option<Uuid>,
    pub agent_name: String,
    pub version: String,
    pub revision_hash: String,
    pub runtime_name: String,
    pub runtime_adapter_version: Option<String>,
    pub code_revision: Option<String>,
    pub build_id: Option<String>,
    pub prompt_revision: Option<String>,
    pub toolset_revision: Option<String>,
    pub model_policy_revision: Option<String>,
    pub security_policy_revision: Option<String>,
    pub memory_policy_revision: Option<String>,
    pub knowledge_policy_revision: Option<String>,
    pub input_schema_hash: Option<String>,
    pub output_schema_hash: Option<String>,
    pub runtime_config_snapshot: Value,
    pub framework_config_snapshot: Value,
    pub beam_version: Option<String>,
    pub runtime_library_version: Option<String>,
    pub application_build_id: Option<String>,
}

impl RevisionSnapshot {
    /// The identity hash over the name, version,
    /// runtime, build and policy revisions.
    #[must_use]
    pub fn from_bind(mut self) -> Self {
        self.id = Uuid::new_v4();
        self.revision_hash = compute_revision_hash(
            &self.agent_name,
            &self.version,
            &self.runtime_name,
            RevisionHashParts {
                build_id: self.build_id.as_deref(),
                toolset_revision: self.toolset_revision.as_deref(),
                prompt_revision: self.prompt_revision.as_deref(),
                model_policy_revision: self.model_policy_revision.as_deref(),
                security_policy_revision: self.security_policy_revision.as_deref(),
            },
        );
        if self.application_build_id.is_none() {
            self.application_build_id.clone_from(&self.build_id);
        }
        self
    }
}

/// The beam version a snapshot records.
pub const BEAM_VERSION: &str = env!("CARGO_PKG_VERSION");

/// The runtime a spec is bound to, for a snapshot.
#[derive(Debug, Clone, Default)]
pub struct BoundRuntime {
    pub adapter_version: Option<String>,
    pub library_version: Option<String>,
}

/// Python `compile_agent_spec`'s snapshot (validation is the caller's): the
/// policy revisions are the spec's own or digests of their policy, and the
/// revision hash is [`canonical_spec_hash`].
#[must_use]
pub fn snapshot_from_spec(spec: &AgentSpec, runtime: &BoundRuntime) -> RevisionSnapshot {
    let dump = spec.canonical_payload();
    let section = |key: &str| dump.get(key).cloned().unwrap_or(Value::Null);
    let schema_hash = |schema: Option<&serde_json::Map<String, Value>>| {
        schema
            .filter(|s| !s.is_empty())
            .map(|s| digest(&Value::Object(s.clone())))
    };
    let resolved = serde_json::json!({
        "runtime_name": spec.runtime.name,
        "tools": spec.tools.allowlist,
        "model": spec.model.name,
        "credential_ref": section("model").get("credential_ref").cloned().unwrap_or(Value::Null),
        "knowledge_collections": spec.knowledge.collections,
        "memory_backend": spec.memory.backend,
        "beam_version": BEAM_VERSION,
        "runtime_adapter_version": runtime.adapter_version,
        "runtime_library_version": runtime.library_version,
    });
    let mut snapshot = RevisionSnapshot {
        agent_name: spec.name.clone(),
        version: spec.version.clone(),
        runtime_name: spec.runtime.name.clone(),
        runtime_adapter_version: runtime.adapter_version.clone(),
        toolset_revision: Some(
            spec.tools
                .revision
                .clone()
                .unwrap_or_else(|| digest(&serde_json::json!(spec.tools.allowlist))),
        ),
        prompt_revision: spec.prompt.revision.clone().or_else(|| {
            (!spec.prompt.instructions.is_empty())
                .then(|| digest(&Value::String(spec.prompt.instructions.clone())))
        }),
        model_policy_revision: spec.model.name.clone(),
        security_policy_revision: Some(
            spec.security
                .revision
                .clone()
                .unwrap_or_else(|| digest(&section("security"))),
        ),
        memory_policy_revision: Some(
            spec.memory
                .revision
                .clone()
                .unwrap_or_else(|| digest(&section("memory"))),
        ),
        knowledge_policy_revision: Some(
            spec.knowledge
                .revision
                .clone()
                .unwrap_or_else(|| digest(&section("knowledge"))),
        ),
        input_schema_hash: schema_hash(spec.schemas.input_schema.as_ref()),
        output_schema_hash: schema_hash(spec.schemas.output_schema.as_ref()),
        runtime_config_snapshot: serde_json::json!({ "agent_spec": dump }),
        framework_config_snapshot: serde_json::json!({ "resolved": resolved }),
        beam_version: Some(BEAM_VERSION.to_owned()),
        runtime_library_version: runtime.library_version.clone(),
        ..RevisionSnapshot::default()
    }
    .from_bind();
    snapshot.revision_hash = canonical_spec_hash(spec);
    snapshot
}

/// The bind's [`RevisionSnapshot`] (its fields at
/// the top level of the JSON) plus its lifecycle. The
/// snapshot's fields read through [`std::ops::Deref`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentRevision {
    #[serde(flatten)]
    pub snapshot: RevisionSnapshot,
    pub status: RevisionStatus,
    pub compatibility: CompatibilityPolicy,
    pub compatible_with: BTreeSet<Uuid>,
    pub compatible_adapter_versions: BTreeSet<String>,
    pub created_at: DateTime<Utc>,
    pub activated_at: Option<DateTime<Utc>>,
    pub retired_at: Option<DateTime<Utc>>,
}

impl std::ops::Deref for AgentRevision {
    type Target = RevisionSnapshot;

    fn deref(&self) -> &RevisionSnapshot {
        &self.snapshot
    }
}

impl std::ops::DerefMut for AgentRevision {
    fn deref_mut(&mut self) -> &mut RevisionSnapshot {
        &mut self.snapshot
    }
}

impl AgentRevision {
    fn from_snapshot(snapshot: &RevisionSnapshot, status: RevisionStatus) -> Self {
        let now = Utc::now();
        Self {
            snapshot: snapshot.clone(),
            status,
            compatibility: CompatibilityPolicy::Strict,
            compatible_with: BTreeSet::new(),
            compatible_adapter_versions: BTreeSet::new(),
            created_at: now,
            activated_at: (status == RevisionStatus::Active).then_some(now),
            retired_at: None,
        }
    }

    /// The spec the bind compiled, when there is one.
    #[must_use]
    pub fn spec(&self) -> Option<AgentSpec> {
        let payload = self.runtime_config_snapshot.get("agent_spec")?;
        if payload.is_null() || payload.as_object().is_some_and(serde_json::Map::is_empty) {
            return None;
        }
        serde_json::from_value(payload.clone()).ok()
    }
}

/// How new executions split between an agent's revisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrafficPolicy {
    pub primary_id: Uuid,
    pub canary_id: Option<Uuid>,
    pub percent: i32,
}

/// With an execution id, `uuid.int % 100` buckets
/// it (the same side in both stacks); without one, a coin of `percent`.
#[must_use]
pub fn pick_revision(
    primary: Uuid,
    canary: Option<Uuid>,
    percent: i32,
    execution_id: Option<Uuid>,
) -> Uuid {
    let Some(canary) = canary.filter(|_| percent > 0) else {
        return primary;
    };
    let bucket = match execution_id {
        Some(id) => i32::try_from(id.as_u128() % 100).unwrap_or(0),
        None => i32::try_from(Uuid::new_v4().as_u128() % 100).unwrap_or(0),
    };
    if bucket < percent {
        canary
    } else {
        primary
    }
}

/// The allowed revision transitions.
///
/// # Errors
/// `BEM-492` (public `BEM-425`) for any other move.
pub fn transition(current: RevisionStatus, target: RevisionStatus) -> ExecResult<RevisionStatus> {
    use RevisionStatus::{Active, Deprecated, Draft, Retired, Validating};
    let allowed = matches!(
        (current, target),
        (Draft, Validating | Active)
            | (Validating, Active | Draft)
            | (Active, Deprecated)
            | (Deprecated, Retired)
    );
    if allowed {
        Ok(target)
    } else {
        Err(crate::BEM_492.with_data(format!(
            "illegal revision transition {} → {}",
            current.as_str(),
            target.as_str()
        )))
    }
}

/// Whether an execution pinned to one revision may resume under another.
#[must_use]
pub fn may_resume(
    policy: CompatibilityPolicy,
    pinned: Uuid,
    current: Uuid,
    compatible_with: Option<&BTreeSet<Uuid>>,
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

/// The adapter and library versions that wrote and would read a checkpoint.
#[derive(Debug, Clone, Copy, Default)]
pub struct CheckpointVersions<'a> {
    pub pinned_adapter: Option<&'a str>,
    pub deployed_adapter: Option<&'a str>,
    pub pinned_library: Option<&'a str>,
    pub deployed_library: Option<&'a str>,
}

/// Whether a deployed runtime can read a checkpoint written by another version.
#[must_use]
pub fn checkpoint_may_read(
    policy: CompatibilityPolicy,
    v: CheckpointVersions<'_>,
    compatible_adapter_versions: Option<&BTreeSet<String>>,
) -> bool {
    if v.pinned_adapter.is_none() && v.pinned_library.is_none() {
        return true;
    }
    if v.pinned_adapter == v.deployed_adapter && v.pinned_library == v.deployed_library {
        return true;
    }
    match policy {
        CompatibilityPolicy::Strict => false,
        CompatibilityPolicy::Compatible => matches!(
            (compatible_adapter_versions, v.pinned_adapter),
            (Some(c), Some(pinned)) if c.contains(pinned)
        ),
        CompatibilityPolicy::Migratable => true,
    }
}

/// Fail unless a checkpoint is readable by the deployed runtime.
///
/// # Errors
/// `BEM-493` (public `BEM-430`) with Python's message.
pub fn assert_checkpoint_readable(
    policy: CompatibilityPolicy,
    v: CheckpointVersions<'_>,
    compatible_adapter_versions: Option<&BTreeSet<String>>,
    can_read: Option<bool>,
) -> ExecResult<()> {
    if can_read == Some(false) || !checkpoint_may_read(policy, v, compatible_adapter_versions) {
        let show = |s: Option<&str>| s.map_or_else(|| "None".to_owned(), str::to_owned);
        return Err(crate::BEM_493.with_data(format!(
            "checkpoint written by adapter {} / library {} cannot be read by deployed adapter {} / library {}",
            show(v.pinned_adapter),
            show(v.pinned_library),
            show(v.deployed_adapter),
            show(v.deployed_library)
        )));
    }
    Ok(())
}

fn strict_error() -> riverbase_core::RiverbaseError {
    crate::BEM_491.with_data("execution is pinned to a different agent revision (STRICT)")
}

/// An app hook that moves a runtime ref to a newer revision (`MIGRATABLE`).
pub type MigrateFn = Arc<dyn Fn(&Value) -> Value + Send + Sync>;

/// What a resume runs on: the revision, and the runtime ref (migrated when the
/// revision is `MIGRATABLE`).
#[derive(Debug, Clone)]
pub struct ResumeResolution {
    pub revision: AgentRevision,
    pub runtime_ref: Option<Value>,
}

/// The deployed runtime's view of a pinned checkpoint, for
/// [`RevisionBook::resolve_resume`].
#[derive(Debug, Clone, Default)]
pub struct DeployedRuntime {
    pub adapter_version: Option<String>,
    pub library_version: Option<String>,
    pub can_read_checkpoint: Option<bool>,
    /// The adapter and library versions stored on the execution.
    pub pinned_versions: Option<(Option<String>, Option<String>)>,
}

#[derive(Default)]
struct Book {
    by_id: HashMap<Uuid, AgentRevision>,
    by_agent: HashMap<String, Vec<Uuid>>,
    current: HashMap<String, Uuid>,
    pins: HashMap<Uuid, Uuid>,
    traffic: HashMap<String, TrafficPolicy>,
    migrators: HashMap<String, MigrateFn>,
    /// Ids the store has (loaded or written).
    stored: std::collections::HashSet<Uuid>,
    /// Rows changed here since the last write.
    dirty_rows: std::collections::HashSet<Uuid>,
    /// Agents whose current revision or traffic changed here.
    dirty_agents: std::collections::HashSet<String>,
    /// Agents whose current revision is a local default (see
    /// `set_provisional_current`).
    provisional: std::collections::HashSet<String>,
    /// The store was read at least once.
    loaded: bool,
}

impl Book {
    fn store(&mut self, row: AgentRevision) {
        self.dirty_rows.insert(row.id);
        self.by_agent
            .entry(row.agent_name.clone())
            .or_default()
            .push(row.id);
        self.by_id.insert(row.id, row);
    }

    fn find_hash(&self, agent: &str, hash: &str) -> Option<&AgentRevision> {
        self.by_agent
            .get(agent)?
            .iter()
            .filter_map(|id| self.by_id.get(id))
            .find(|r| r.revision_hash == hash)
    }

    /// Replace the unsaved revision `from` with the stored `row` (same
    /// hash): its contents win, and every reference follows the stored id.
    fn rename(&mut self, from: Uuid, row: AgentRevision) {
        let to = row.id;
        if self.by_id.remove(&from).is_none() {
            return;
        }
        self.dirty_rows.remove(&from);
        self.by_id.insert(to, row);
        let swap = |id: &mut Uuid| {
            if *id == from {
                *id = to;
            }
        };
        self.by_agent.values_mut().flatten().for_each(swap);
        self.current.values_mut().for_each(swap);
        self.pins.values_mut().for_each(swap);
        for t in self.traffic.values_mut() {
            swap(&mut t.primary_id);
            if let Some(c) = t.canary_id.as_mut() {
                swap(c);
            }
        }
    }

    /// A revision to change (marked for the next write).
    fn require(&mut self, id: Uuid) -> ExecResult<&mut AgentRevision> {
        let row = self
            .by_id
            .get_mut(&id)
            .ok_or_else(|| crate::BEM_490.with_data(format!("unknown revision {id}")))?;
        self.dirty_rows.insert(id);
        Ok(row)
    }

    /// A default current revision (a first bind or a fallback pin): stored
    /// traffic replaces it, and it is written only when there is none.
    fn set_provisional_current(&mut self, agent: String, id: Uuid) {
        if !self.dirty_agents.contains(&agent) {
            self.provisional.insert(agent.clone());
        }
        self.current.insert(agent, id);
    }

    fn set_provisional_traffic(&mut self, agent: String, policy: TrafficPolicy) {
        if !self.dirty_agents.contains(&agent) {
            self.provisional.insert(agent.clone());
        }
        self.traffic.insert(agent, policy);
    }

    /// Change an agent's current revision (its traffic is written next).
    fn set_current(&mut self, agent: String, id: Uuid) {
        self.provisional.remove(&agent);
        self.dirty_agents.insert(agent.clone());
        self.current.insert(agent, id);
    }

    /// Change an agent's traffic (written next).
    fn set_traffic(&mut self, agent: String, policy: TrafficPolicy) {
        self.provisional.remove(&agent);
        self.dirty_agents.insert(agent.clone());
        self.traffic.insert(agent, policy);
    }

    fn current(&self, agent: &str) -> Option<&AgentRevision> {
        self.current.get(agent).and_then(|id| self.by_id.get(id))
    }
}

/// Revisions, traffic and execution pins, in process;
/// a [`RevisionStore`] persists and restores them.
#[derive(Default)]
pub struct RevisionBook {
    book: Mutex<Book>,
}

impl std::fmt::Debug for RevisionBook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RevisionBook")
    }
}

impl RevisionBook {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> ExecResult<std::sync::MutexGuard<'_, Book>> {
        self.book
            .lock()
            .map_err(|e| crate::BEM_380.with_data(e.to_string()))
    }

    /// An active revision for an agent bound without
    /// a spec (or reuse the one with the same identity hash).
    ///
    /// # Errors
    /// A poisoned book.
    pub fn pin_revision(
        &self,
        agent_name: &str,
        version: &str,
        runtime_name: &str,
    ) -> ExecResult<AgentRevision> {
        let mut b = self.lock()?;
        let hash = compute_revision_hash(
            agent_name,
            version,
            runtime_name,
            RevisionHashParts::default(),
        );
        if let Some(existing) = b.find_hash(agent_name, &hash).cloned() {
            b.set_provisional_current(agent_name.to_owned(), existing.id);
            return Ok(existing);
        }
        let snapshot = RevisionSnapshot {
            id: Uuid::new_v4(),
            agent_name: agent_name.to_owned(),
            version: version.to_owned(),
            revision_hash: hash,
            runtime_name: runtime_name.to_owned(),
            runtime_config_snapshot: Value::Object(serde_json::Map::new()),
            framework_config_snapshot: Value::Object(serde_json::Map::new()),
            ..RevisionSnapshot::default()
        };
        let row = AgentRevision::from_snapshot(&snapshot, RevisionStatus::Active);
        b.store(row.clone());
        b.set_provisional_current(agent_name.to_owned(), row.id);
        let prior = b.traffic.get(agent_name).copied();
        b.set_provisional_traffic(
            agent_name.to_owned(),
            TrafficPolicy {
                primary_id: row.id,
                canary_id: prior.and_then(|p| p.canary_id),
                percent: prior.map_or(0, |p| p.percent),
            },
        );
        Ok(row)
    }

    /// The first bind of an agent is `active`, later
    /// ones `draft`; the same hash returns the existing revision.
    ///
    /// # Errors
    /// A poisoned book.
    pub fn record_bind(&self, snapshot: &RevisionSnapshot) -> ExecResult<AgentRevision> {
        let mut b = self.lock()?;
        if let Some(existing) = b.find_hash(&snapshot.agent_name, &snapshot.revision_hash) {
            return Ok(existing.clone());
        }
        let first = !b.current.contains_key(&snapshot.agent_name);
        let status = if first {
            RevisionStatus::Active
        } else {
            RevisionStatus::Draft
        };
        let row = AgentRevision::from_snapshot(snapshot, status);
        b.store(row.clone());
        if first {
            b.set_provisional_current(row.agent_name.clone(), row.id);
            b.set_provisional_traffic(
                row.agent_name.clone(),
                TrafficPolicy {
                    primary_id: row.id,
                    canary_id: None,
                    percent: 0,
                },
            );
        }
        Ok(row)
    }

    /// Python `mark_validating`.
    ///
    /// # Errors
    /// `BEM-490`; `BEM-492`.
    pub fn mark_validating(&self, id: Uuid) -> ExecResult<AgentRevision> {
        let mut b = self.lock()?;
        let row = b.require(id)?;
        row.status = transition(row.status, RevisionStatus::Validating)?;
        Ok(row.clone())
    }

    /// A `validating` revision needs a passing eval run;
    /// `as_primary` (or no current revision) makes it the primary.
    ///
    /// # Errors
    /// `BEM-490`; `BEM-492`.
    pub fn activate(
        &self,
        id: Uuid,
        as_primary: bool,
        last_eval_passed: Option<bool>,
    ) -> ExecResult<AgentRevision> {
        let mut b = self.lock()?;
        let row = b.require(id)?;
        if row.status == RevisionStatus::Validating && last_eval_passed != Some(true) {
            return Err(crate::BEM_492.with_data("VALIDATING revision requires a passing eval run"));
        }
        if row.status != RevisionStatus::Active {
            row.status = transition(row.status, RevisionStatus::Active)?;
            row.activated_at = Some(Utc::now());
        }
        let row = row.clone();
        if as_primary || !b.current.contains_key(&row.agent_name) {
            b.set_current(row.agent_name.clone(), row.id);
            let prior = b.traffic.get(&row.agent_name).copied();
            b.set_traffic(
                row.agent_name.clone(),
                TrafficPolicy {
                    primary_id: row.id,
                    canary_id: prior.and_then(|p| p.canary_id),
                    percent: prior.map_or(0, |p| p.percent),
                },
            );
        }
        Ok(row)
    }

    /// Python `deprecate`.
    ///
    /// # Errors
    /// `BEM-490`; `BEM-492`.
    pub fn deprecate(&self, id: Uuid) -> ExecResult<AgentRevision> {
        let mut b = self.lock()?;
        let row = b.require(id)?;
        row.status = transition(row.status, RevisionStatus::Deprecated)?;
        Ok(row.clone())
    }

    /// An active revision is deprecated first; it stops
    /// being the current one and the canary.
    ///
    /// # Errors
    /// `BEM-490`; `BEM-492`.
    pub fn retire(&self, id: Uuid) -> ExecResult<AgentRevision> {
        let mut b = self.lock()?;
        let row = b.require(id)?;
        if row.status == RevisionStatus::Active {
            row.status = transition(row.status, RevisionStatus::Deprecated)?;
        }
        row.status = transition(row.status, RevisionStatus::Retired)?;
        row.retired_at = Some(Utc::now());
        let row = row.clone();
        if b.current.get(&row.agent_name) == Some(&row.id) {
            b.current.remove(&row.agent_name);
            b.dirty_agents.insert(row.agent_name.clone());
        }
        if let Some(traffic) = b.traffic.get(&row.agent_name).copied() {
            if traffic.canary_id == Some(row.id) {
                b.set_traffic(
                    row.agent_name.clone(),
                    TrafficPolicy {
                        primary_id: traffic.primary_id,
                        canary_id: None,
                        percent: 0,
                    },
                );
            }
        }
        Ok(row)
    }

    /// `percent` (clamped to 0..=100) of new executions
    /// go to `revision_id`.
    ///
    /// # Errors
    /// `BEM-490`; `BEM-491` when the revision belongs to another agent or the
    /// agent has no primary.
    pub fn set_canary(
        &self,
        agent_name: &str,
        revision_id: Uuid,
        percent: i32,
    ) -> ExecResult<TrafficPolicy> {
        let mut b = self.lock()?;
        let row = b.require(revision_id)?;
        if row.agent_name != agent_name {
            return Err(crate::BEM_491.with_data("canary revision does not belong to agent"));
        }
        let primary = b
            .current(agent_name)
            .map(|r| r.id)
            .ok_or_else(|| crate::BEM_491.with_data("agent has no primary revision"))?;
        let policy = TrafficPolicy {
            primary_id: primary,
            canary_id: Some(revision_id),
            percent: percent.clamp(0, 100),
        };
        b.set_traffic(agent_name.to_owned(), policy);
        Ok(policy)
    }

    /// Python `set_compatibility`.
    ///
    /// # Errors
    /// `BEM-490`.
    pub fn set_compatibility(
        &self,
        id: Uuid,
        policy: CompatibilityPolicy,
        compatible_with: BTreeSet<Uuid>,
    ) -> ExecResult<AgentRevision> {
        let mut b = self.lock()?;
        let row = b.require(id)?;
        row.compatibility = policy;
        row.compatible_with = compatible_with;
        Ok(row.clone())
    }

    /// Python `set_compatible_adapter_versions`.
    ///
    /// # Errors
    /// `BEM-490`.
    pub fn set_compatible_adapter_versions(
        &self,
        id: Uuid,
        versions: BTreeSet<String>,
    ) -> ExecResult<AgentRevision> {
        let mut b = self.lock()?;
        let row = b.require(id)?;
        row.compatible_adapter_versions = versions;
        Ok(row.clone())
    }

    /// Python `register_migrate`.
    pub fn register_migrate(&self, agent_name: &str, migrate: MigrateFn) {
        if let Ok(mut b) = self.book.lock() {
            b.migrators.insert(agent_name.to_owned(), migrate);
        }
    }

    /// The revision a new execution runs on.
    #[must_use]
    pub fn pick_for_new(&self, agent_name: &str, execution_id: Option<Uuid>) -> Option<Uuid> {
        let b = self.book.lock().ok()?;
        match b.traffic.get(agent_name) {
            Some(t) => Some(pick_revision(
                t.primary_id,
                t.canary_id,
                t.percent,
                execution_id,
            )),
            None => b.current(agent_name).map(|r| r.id),
        }
    }

    /// Python `pin_of`.
    #[must_use]
    pub fn pin_of(&self, execution_id: Uuid) -> Option<Uuid> {
        self.book.lock().ok()?.pins.get(&execution_id).copied()
    }

    /// Pin a new execution; an execution already
    /// pinned elsewhere is refused.
    ///
    /// # Errors
    /// `BEM-491`.
    pub fn bind_execution(&self, execution_id: Uuid, revision_id: Uuid) -> ExecResult<()> {
        let mut b = self.lock()?;
        match b.pins.get(&execution_id) {
            None => {
                b.pins.insert(execution_id, revision_id);
                Ok(())
            }
            Some(pinned) if *pinned == revision_id => Ok(()),
            Some(_) => Err(strict_error()),
        }
    }

    /// Rebuild a durable pin after a restart. An unknown
    /// revision id takes the current revision's identity (or a new active one).
    ///
    /// # Errors
    /// `BEM-491` when the execution is pinned to another revision here.
    pub fn restore_pin(
        &self,
        execution_id: Uuid,
        revision_id: Uuid,
        agent_name: &str,
        runtime_name: &str,
    ) -> ExecResult<AgentRevision> {
        let mut b = self.lock()?;
        if let Some(already) = b.pins.get(&execution_id) {
            if *already != revision_id {
                return Err(strict_error());
            }
        }
        let row = if let Some(row) = b.by_id.get(&revision_id) {
            row.clone()
        } else {
            let row = if let Some(current) = b.current(agent_name).cloned() {
                let mut row = current.clone();
                row.id = revision_id;
                if let Some(traffic) = b.traffic.get(agent_name).copied() {
                    b.set_provisional_traffic(
                        agent_name.to_owned(),
                        TrafficPolicy {
                            primary_id: row.id,
                            canary_id: traffic.canary_id.filter(|c| *c != current.id),
                            percent: traffic.percent,
                        },
                    );
                }
                row
            } else {
                let snapshot = RevisionSnapshot {
                    id: revision_id,
                    agent_name: agent_name.to_owned(),
                    version: "1.0.0".into(),
                    revision_hash: compute_revision_hash(
                        agent_name,
                        "1.0.0",
                        runtime_name,
                        RevisionHashParts::default(),
                    ),
                    runtime_name: runtime_name.to_owned(),
                    runtime_config_snapshot: Value::Object(serde_json::Map::new()),
                    framework_config_snapshot: Value::Object(serde_json::Map::new()),
                    ..RevisionSnapshot::default()
                };
                AgentRevision::from_snapshot(&snapshot, RevisionStatus::Active)
            };
            b.store(row.clone());
            b.set_provisional_current(agent_name.to_owned(), row.id);
            row
        };
        b.pins.insert(execution_id, revision_id);
        Ok(row)
    }

    /// The pinned revision while it is current;
    /// otherwise the current one when the compatibility policy allows it
    /// (`MIGRATABLE` runs the app's migrate hook), else `BEM-491`. The
    /// checkpoint must be readable by the deployed runtime (`BEM-493`).
    ///
    /// # Errors
    /// `BEM-491`, `BEM-493`.
    pub fn resolve_resume(
        &self,
        execution_id: Uuid,
        agent_name: &str,
        runtime_ref: Option<&Value>,
        deployed: &DeployedRuntime,
    ) -> ExecResult<ResumeResolution> {
        let mut b = self.lock()?;
        let pinned_id = *b
            .pins
            .get(&execution_id)
            .ok_or_else(|| crate::BEM_491.with_data("execution has no pinned revision"))?;
        let mut pinned = b
            .by_id
            .get(&pinned_id)
            .cloned()
            .ok_or_else(|| crate::BEM_490.with_data(format!("unknown revision {pinned_id}")))?;
        // The execution's own record of what wrote its checkpoint wins
        // (Python `_restore_sql_pin` overlays it).
        if let Some((adapter, library)) = &deployed.pinned_versions {
            if adapter.is_some() || library.is_some() {
                pinned.runtime_adapter_version.clone_from(adapter);
                pinned.runtime_library_version.clone_from(library);
            }
        }
        let current = b.current(agent_name).cloned();
        let policy = current
            .as_ref()
            .map_or(pinned.compatibility, |c| c.compatibility);
        let compatible_adapters = current
            .as_ref()
            .map_or(&pinned.compatible_adapter_versions, |c| {
                &c.compatible_adapter_versions
            })
            .clone();
        let deployed_adapter = deployed.adapter_version.clone().or_else(|| {
            current
                .as_ref()
                .and_then(|c| c.runtime_adapter_version.clone())
        });
        let deployed_library = deployed.library_version.clone().or_else(|| {
            current
                .as_ref()
                .and_then(|c| c.runtime_library_version.clone())
        });
        let should_check = deployed.can_read_checkpoint.is_some()
            || deployed.adapter_version.is_some()
            || deployed.library_version.is_some()
            || pinned.runtime_adapter_version.is_some()
            || pinned.runtime_library_version.is_some();
        let readable = || {
            if !should_check {
                return Ok(());
            }
            assert_checkpoint_readable(
                policy,
                CheckpointVersions {
                    pinned_adapter: pinned.runtime_adapter_version.as_deref(),
                    deployed_adapter: deployed_adapter.as_deref(),
                    pinned_library: pinned.runtime_library_version.as_deref(),
                    deployed_library: deployed_library.as_deref(),
                },
                Some(&compatible_adapters),
                deployed.can_read_checkpoint,
            )
        };
        let Some(current) = current.filter(|c| c.id != pinned_id) else {
            readable()?;
            return Ok(ResumeResolution {
                revision: pinned,
                runtime_ref: runtime_ref.cloned(),
            });
        };
        if !may_resume(
            policy,
            pinned_id,
            current.id,
            Some(&current.compatible_with),
        ) {
            readable()?;
            return Err(strict_error());
        }
        let runtime_ref = if current.compatibility == CompatibilityPolicy::Migratable {
            let migrate = b.migrators.get(agent_name).cloned().ok_or_else(|| {
                crate::BEM_491.with_data("MIGRATABLE revision has no registered migrate()")
            })?;
            runtime_ref.map(|r| migrate(r))
        } else {
            runtime_ref.cloned()
        };
        readable()?;
        b.pins.insert(execution_id, current.id);
        Ok(ResumeResolution {
            revision: current,
            runtime_ref,
        })
    }

    /// Python `get_revision`.
    #[must_use]
    pub fn get_revision(&self, id: Uuid) -> Option<AgentRevision> {
        self.book.lock().ok()?.by_id.get(&id).cloned()
    }

    /// Python `list_revisions`, in bind order.
    #[must_use]
    pub fn list_revisions(&self, agent_name: &str) -> Vec<AgentRevision> {
        let Ok(b) = self.book.lock() else {
            return Vec::new();
        };
        b.by_agent
            .get(agent_name)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| b.by_id.get(id).cloned())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Python `current_for`.
    #[must_use]
    pub fn current_for(&self, agent_name: &str) -> Option<AgentRevision> {
        self.book.lock().ok()?.current(agent_name).cloned()
    }

    /// The agent's traffic policy.
    #[must_use]
    pub fn traffic_for(&self, agent_name: &str) -> Option<TrafficPolicy> {
        self.book.lock().ok()?.traffic.get(agent_name).copied()
    }

    /// The revision an execution is pinned to.
    #[must_use]
    pub fn snapshot_for_execution(&self, execution_id: Uuid) -> Option<AgentRevision> {
        let b = self.book.lock().ok()?;
        b.pins
            .get(&execution_id)
            .and_then(|id| b.by_id.get(id))
            .cloned()
    }

    /// The rows and agents changed here since the last write (for
    /// persistence): only those are written, so a worker never reverts what
    /// another one stored.
    #[must_use]
    pub fn export(&self) -> (Vec<AgentRevision>, Vec<(String, TrafficPolicy)>) {
        let Ok(b) = self.book.lock() else {
            return (Vec::new(), Vec::new());
        };
        let rows = b
            .dirty_rows
            .iter()
            .filter_map(|id| b.by_id.get(id).cloned())
            .collect();
        let traffic = b
            .dirty_agents
            .iter()
            .chain(b.provisional.iter())
            .filter_map(|name| {
                let id = *b.current.get(name)?;
                let t = b.traffic.get(name).copied().unwrap_or(TrafficPolicy {
                    primary_id: id,
                    canary_id: None,
                    percent: 0,
                });
                Some((
                    name.clone(),
                    TrafficPolicy {
                        primary_id: id,
                        ..t
                    },
                ))
            })
            .collect();
        (rows, traffic)
    }

    /// The exported rows and agents are stored now.
    pub fn mark_persisted(&self, rows: &[AgentRevision], traffic: &[(String, TrafficPolicy)]) {
        let Ok(mut b) = self.book.lock() else { return };
        for row in rows {
            b.dirty_rows.remove(&row.id);
            b.stored.insert(row.id);
        }
        for (name, _) in traffic {
            b.dirty_agents.remove(name);
            b.provisional.remove(name);
        }
    }

    /// Whether anything changed here since the last write.
    #[must_use]
    pub fn is_dirty(&self) -> bool {
        self.book.lock().is_ok_and(|b| {
            !b.dirty_rows.is_empty() || !b.dirty_agents.is_empty() || !b.provisional.is_empty()
        })
    }

    /// Whether the store has been read (a write before that would replace
    /// stored traffic with local defaults).
    #[must_use]
    pub fn is_loaded(&self) -> bool {
        self.book.lock().is_ok_and(|b| b.loaded)
    }

    /// Drop an execution's pin (the stored pin replaces it).
    pub fn forget_pin(&self, execution_id: Uuid) {
        if let Ok(mut b) = self.book.lock() {
            b.pins.remove(&execution_id);
        }
    }

    /// Take the stored revisions and traffic. Stored
    /// rows refresh the ones not changed here; an unsaved bind with a stored
    /// row's hash becomes that row, and one with a new hash a draft when the
    /// agent has stored traffic; pins come back through
    /// [`Self::restore_pin`]. A canary that is not a known revision is dropped.
    pub fn load(&self, rows: Vec<AgentRevision>, traffic: Vec<(String, TrafficPolicy)>) {
        let Ok(mut b) = self.book.lock() else { return };
        for row in rows {
            let id = row.id;
            if b.by_id.contains_key(&id) {
                if !b.dirty_rows.contains(&id) {
                    b.by_id.insert(id, row);
                }
            } else if let Some(local) = b
                .find_hash(&row.agent_name, &row.revision_hash)
                .map(|r| r.id)
                .filter(|local| !b.stored.contains(local))
            {
                b.rename(local, row);
            } else {
                b.store(row);
                b.dirty_rows.remove(&id);
            }
            b.stored.insert(id);
        }
        for (name, mut policy) in traffic {
            if b.dirty_agents.contains(&name) || !b.by_id.contains_key(&policy.primary_id) {
                continue;
            }
            if policy.canary_id.is_some_and(|c| !b.by_id.contains_key(&c)) {
                policy.canary_id = None;
                policy.percent = 0;
            }
            // A bind made before the load was active only because the book
            // was empty: under the stored traffic it is a later bind, a draft
            // until activated (not a second active revision).
            let prior = b.current.get(&name).copied();
            let local = prior.filter(|id| *id != policy.primary_id && !b.stored.contains(id));
            if let (true, Some(local)) = (b.provisional.remove(&name), local) {
                if let Some(row) = b.by_id.get_mut(&local) {
                    if row.status == RevisionStatus::Active {
                        row.status = RevisionStatus::Draft;
                        row.activated_at = None;
                    }
                }
            }
            b.current.insert(name.clone(), policy.primary_id);
            b.traffic.insert(name, policy);
        }
        b.loaded = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(name: &str, version: &str) -> RevisionSnapshot {
        RevisionSnapshot {
            agent_name: name.into(),
            version: version.into(),
            runtime_name: "rig".into(),
            runtime_config_snapshot: Value::Object(serde_json::Map::new()),
            framework_config_snapshot: Value::Object(serde_json::Map::new()),
            ..RevisionSnapshot::default()
        }
        .from_bind()
    }

    /// Plan 021 Phase 8: the snapshot's fields stay at the top level of a
    /// revision's JSON (no `snapshot` key), and the JSON reads back equal.
    #[test]
    fn a_revision_keeps_its_flat_json() {
        let mut snap = snapshot("flat", "1.0.0");
        snap.agent_id = Some(Uuid::new_v4());
        snap.build_id = Some("b1".into());
        snap.input_schema_hash = Some("in".into());
        let mut revision = AgentRevision::from_snapshot(&snap, RevisionStatus::Active);
        revision.compatible_with.insert(Uuid::new_v4());
        let json = serde_json::to_value(&revision).unwrap();
        assert!(json.get("snapshot").is_none(), "{json}");
        assert_eq!(json["agent_name"], "flat");
        assert_eq!(json["build_id"], "b1");
        assert_eq!(json["revision_hash"], snap.revision_hash.as_str());
        assert_eq!(
            json["status"],
            serde_json::to_value(RevisionStatus::Active).unwrap()
        );
        let back: AgentRevision = serde_json::from_value(json).unwrap();
        assert_eq!(back, revision);
        assert_eq!(back.agent_name, "flat", "fields read through the snapshot");
    }

    /// The first bind is active, the second a draft; a
    /// validating revision activates only after a passing eval.
    #[test]
    fn first_bind_is_active_and_validation_gates_activation() {
        let book = RevisionBook::new();
        let first = book.record_bind(&snapshot("a", "1")).unwrap();
        let second = book.record_bind(&snapshot("a", "2")).unwrap();
        assert_eq!(first.status, RevisionStatus::Active);
        assert_eq!(second.status, RevisionStatus::Draft);
        assert_eq!(
            book.record_bind(&snapshot("a", "1")).unwrap().id,
            first.id,
            "the same hash is one revision"
        );
        book.mark_validating(second.id).unwrap();
        let err = book.activate(second.id, true, Some(false)).unwrap_err();
        assert_eq!(err.errcode.as_str(), "BEM-492");
        book.activate(second.id, true, Some(true)).unwrap();
        assert_eq!(book.current_for("a").unwrap().id, second.id);
    }

    /// A pinned resume stays put under STRICT; COMPATIBLE moves
    /// when the pin is listed.
    #[test]
    fn resume_follows_the_compatibility_policy() {
        let book = RevisionBook::new();
        let old = book.record_bind(&snapshot("a", "1")).unwrap();
        let new = book.record_bind(&snapshot("a", "2")).unwrap();
        let execution = Uuid::new_v4();
        book.bind_execution(execution, old.id).unwrap();
        book.activate(new.id, true, None).unwrap();
        let deployed = DeployedRuntime::default();
        let err = book
            .resolve_resume(execution, "a", None, &deployed)
            .unwrap_err();
        assert_eq!(err.errcode.as_str(), "BEM-491");
        book.set_compatibility(
            new.id,
            CompatibilityPolicy::Compatible,
            BTreeSet::from([old.id]),
        )
        .unwrap();
        let moved = book
            .resolve_resume(execution, "a", None, &deployed)
            .unwrap();
        assert_eq!(moved.revision.id, new.id);
        assert_eq!(book.pin_of(execution), Some(new.id));
    }

    /// A restart binds before it loads: a changed agent's bind is a draft
    /// under the stored traffic (not a second active revision), new
    /// executions stay on the stored primary, and activating the bind as
    /// primary moves them.
    #[test]
    fn a_changed_bind_before_the_load_is_a_draft_until_activated() {
        let before = RevisionBook::new();
        let old = before.record_bind(&snapshot("a", "1")).unwrap();
        let (rows, traffic) = before.export();

        let book = RevisionBook::new();
        let changed = book.record_bind(&snapshot("a", "2")).unwrap();
        assert_eq!(
            changed.status,
            RevisionStatus::Active,
            "first in an empty book"
        );
        book.load(rows, traffic);
        let reloaded = book.get_revision(changed.id).unwrap();
        assert_eq!(reloaded.status, RevisionStatus::Draft);
        assert_eq!(reloaded.activated_at, None);
        assert_eq!(book.current_for("a").unwrap().id, old.id);
        assert_eq!(book.pick_for_new("a", Some(Uuid::new_v4())), Some(old.id));
        let (rows, traffic) = book.export();
        assert_eq!(
            rows.iter().map(|r| (r.id, r.status)).collect::<Vec<_>>(),
            vec![(changed.id, RevisionStatus::Draft)]
        );
        assert!(traffic.is_empty(), "the stored traffic is kept");

        book.activate(changed.id, true, None).unwrap();
        assert_eq!(
            book.pick_for_new("a", Some(Uuid::new_v4())),
            Some(changed.id)
        );
        let (_, traffic) = book.export();
        assert_eq!(traffic.first().map(|(_, t)| t.primary_id), Some(changed.id));
    }

    #[test]
    fn canary_traffic_and_retire() {
        let book = RevisionBook::new();
        let primary = book.record_bind(&snapshot("a", "1")).unwrap();
        let canary = book.record_bind(&snapshot("a", "2")).unwrap();
        book.activate(canary.id, false, None).unwrap();
        book.set_canary("a", canary.id, 150).unwrap();
        assert_eq!(book.traffic_for("a").unwrap().percent, 100);
        assert_eq!(
            book.pick_for_new("a", Some(Uuid::new_v4())),
            Some(canary.id)
        );
        book.retire(canary.id).unwrap();
        assert_eq!(
            book.pick_for_new("a", Some(Uuid::new_v4())),
            Some(primary.id)
        );
    }
}
