//! The beam command aggregate (riverbase macro mode, plans 006 and 007).
//!
//! Each command's handler (`crate::commands`) calls one method here, in
//! the submodule for its area: `runs`, `sessions`, `profiles`, `memory`,
//! `knowledge`, `revisions`, `operations`.
//!
//! Riverbase runs each beam command as a typed handler on a [`BeamAggregate`]
//! inside one database transaction, which also holds riverbase's command
//! log and outbox rows. A handler only reads and writes Postgres through
//! [`BeamAggregate::db`] (the same transaction) and, for work a worker must do, inserts
//! an `agent_task` row and emits a [`TASK_TOPIC`] hint
//! ([`BeamAggregate::enqueue`]). It never calls a worker and never waits for
//! one, so no transaction stays open while an agent runs.
//!
//! The command engine runs on the beam datastore
//! ([`beam_store::entity::datastore`]), whose root resources answer
//! riverbase's root lookup with the identifier alone
//! ([`beam_store::entity::RootById`]). The handler loads (and
//! ownership-checks) the real row through [`BeamAggregate::db`] (`beam_store`'s
//! `CommandExec`), and writes rows through their entities on the same
//! transaction.

use std::sync::Arc;

use async_trait::async_trait;
use beam_store::repo::domain::CommandExec;
use beam_store::tasks::{NewTask, TASK_TOPIC};
use beam_store::SchemaName;
use riverbase_core::base::AggregateContext;
use riverbase_core::command::{CommandMessage, CommandMeta};
use riverbase_core::datastore::PgDataStore;
use riverbase_core::logstore::DomainLogStore;
use riverbase_core::{Aggregate, AggregateCore, RiverbaseResult};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::local::{sql, Caller};

mod evaluation;
mod knowledge;
mod memory;
mod operations;
mod profiles;
mod revisions;
mod runs;
mod sessions;
mod tool_rules;
mod usage;

/// The schema of the store the domain was spawned on ([`set_schema`]).
static SCHEMA: std::sync::OnceLock<SchemaName> = std::sync::OnceLock::new();

/// Record the store's schema for the commands' statements (the domain's
/// spawn does). The first one wins: one process serves one beam schema.
pub(crate) fn set_schema(schema: &SchemaName) {
    let _ = SCHEMA.set(schema.clone());
}

/// One beam command's aggregate.
pub struct BeamAggregate {
    core: AggregateCore<PgDataStore>,
}

#[async_trait]
impl Aggregate<PgDataStore> for BeamAggregate {
    async fn open(
        statemgr: Arc<PgDataStore>,
        logstore: DomainLogStore,
        message: &CommandMessage,
        meta: &CommandMeta,
        agg_ctx: &AggregateContext,
    ) -> RiverbaseResult<Self> {
        Ok(Self {
            core: AggregateCore::open(statemgr, logstore, message, meta, agg_ctx.clone()).await?,
        })
    }

    fn core(&self) -> &AggregateCore<PgDataStore> {
        &self.core
    }

    fn core_mut(&mut self) -> &mut AggregateCore<PgDataStore> {
        &mut self.core
    }
}

impl BeamAggregate {
    /// The riverbase context the command runs in.
    #[must_use]
    pub fn context(&self) -> &AggregateContext {
        self.core.context()
    }

    /// The caller (user, org, admin role).
    #[must_use]
    pub fn caller(&self) -> Caller {
        Caller::from_aggregate(self.core.context())
    }

    /// The URL's identifier for object commands; empty for collection ones.
    #[must_use]
    pub fn identifier(&self) -> String {
        self.core
            .aggroot()
            .map(|root| root.identifier.clone())
            .unwrap_or_default()
    }

    /// The riverbase command id, as the task's `src_cmd`.
    fn command_uuid(&self) -> Option<Uuid> {
        Uuid::parse_str(&self.core.cmd_id().0).ok()
    }

    /// The beam domain's statements on this command's transaction, for the
    /// caller's org.
    #[must_use]
    pub fn db(&self) -> CommandExec<'_> {
        CommandExec::new(
            self.core.statemgr(),
            &self.core.context().unit_of_work,
            SCHEMA
                .get()
                .cloned()
                .unwrap_or_else(|| SchemaName::from_env().unwrap_or_default()),
            self.caller().org_id,
        )
    }

    /// Queue `task` (owned by the caller, from this command) and emit its
    /// hint. Both commit with the command; riverbase's outbox publishes the
    /// hint after the commit and retries it.
    ///
    /// # Errors
    /// `BEM-507` when the insert fails; riverbase errors for the hint.
    pub async fn enqueue(&mut self, mut task: NewTask) -> RiverbaseResult<Uuid> {
        let caller = self.caller();
        let parse = |v: Option<&String>| v.and_then(|s| Uuid::parse_str(s.trim()).ok());
        task.user_id = task.user_id.or_else(|| parse(caller.user_id.as_ref()));
        task.org_id = task.org_id.or_else(|| parse(caller.org_id.as_ref()));
        task.src_cmd = self.command_uuid();
        self.db().enqueue_task(&task).await.map_err(sql)?;
        // The task name picks the lane a worker claims it from; a worker
        // that does not run the task ignores the hint.
        self.core
            .emit_message(TASK_TOPIC, json!({ "task_id": task.id, "task": task.task }))
            .await?;
        Ok(task.id)
    }
}

/// The answer of a command that queued `task`.
fn queued(task_id: Uuid, task: &str) -> Value {
    json!({ "task_id": task_id, "task": task, "status": "queued" })
}
