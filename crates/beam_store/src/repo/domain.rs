//! The beam domain's statements (Python `BeamAggregate`'s SQL): the agent
//! behind a command URL, sessions, messages, profiles, runs, tool rules,
//! evaluation datasets, usage and operations.
//!
//! Each statement is written once, in [`DomainSql`], over any
//! [`DomainExec`]: a [`Db`] runs it on the pool (the HTTP app's own routes),
//! a [`CommandExec`] on a riverbase command's transaction. A statement
//! returns its rows as one JSONB column, `row`, which the domain answers
//! with; its operands are typed binds. Text comparisons stay text
//! (`col::text = $n`) and casts stay in the SQL (`$n::uuid`), so a malformed
//! id fails as it always did.

use async_trait::async_trait;
use diesel::pg::Pg;
use diesel::sql_query;
use diesel::sql_types::{BigInt, Bool, Jsonb, Nullable, Text, Uuid as SqlUuid};
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use riverbase_core::datastore::postgres::optional_command_connection;
use riverbase_core::datastore::{CommandUnitOfWork, DataStore, PgDataStore};
use serde_json::Value;
use uuid::Uuid;

use crate::db::{Db, DbResult};
use crate::entity::write;
use crate::repo::rows::JsonRow;
use crate::schema::SchemaName;
use crate::tasks::{NewTask, INVOKE_TASK};

/// One typed operand of a [`Statement`]; `None` binds SQL `NULL`.
#[derive(Debug, Clone)]
enum Bind {
    Text(Option<String>),
    Uuid(Option<Uuid>),
    BigInt(Option<i64>),
    Bool(bool),
    Jsonb(Option<Value>),
}

/// A statement returning one JSONB column `row`, with its operands in order
/// (`$1`, `$2`, …).
#[derive(Debug, Clone)]
pub struct Statement {
    text: String,
    binds: Vec<Bind>,
}

impl Statement {
    /// The SQL text (a test's fake executor can match on it).
    #[must_use]
    pub fn sql(&self) -> &str {
        &self.text
    }

    fn new(text: String) -> Self {
        Self {
            text,
            binds: Vec::new(),
        }
    }

    fn text(mut self, value: impl Into<String>) -> Self {
        self.binds.push(Bind::Text(Some(value.into())));
        self
    }

    fn opt_text(mut self, value: Option<String>) -> Self {
        self.binds.push(Bind::Text(value));
        self
    }

    fn uuid(mut self, value: Uuid) -> Self {
        self.binds.push(Bind::Uuid(Some(value)));
        self
    }

    fn big_int(mut self, value: i64) -> Self {
        self.binds.push(Bind::BigInt(Some(value)));
        self
    }

    fn boolean(mut self, value: bool) -> Self {
        self.binds.push(Bind::Bool(value));
        self
    }

    fn jsonb(mut self, value: Option<Value>) -> Self {
        self.binds.push(Bind::Jsonb(value));
        self
    }
}

/// Run `statement` on `conn`.
async fn load(conn: &mut AsyncPgConnection, statement: Statement) -> DbResult<Vec<Value>> {
    let mut query = sql_query(statement.text).into_boxed::<Pg>();
    for bind in statement.binds {
        query = match bind {
            Bind::Text(v) => query.bind::<Nullable<Text>, _>(v),
            Bind::Uuid(v) => query.bind::<Nullable<SqlUuid>, _>(v),
            Bind::BigInt(v) => query.bind::<Nullable<BigInt>, _>(v),
            Bind::Bool(v) => query.bind::<Bool, _>(v),
            Bind::Jsonb(v) => query.bind::<Nullable<Jsonb>, _>(v),
        };
    }
    let rows: Vec<JsonRow> = query.load(conn).await?;
    Ok(rows.into_iter().map(|r| r.row).collect())
}

/// Something that runs the domain's statements and entity writes.
#[async_trait]
pub trait DomainExec: Send + Sync {
    /// The beam schema.
    fn schema(&self) -> &SchemaName;

    /// The rows `statement` returns.
    ///
    /// # Errors
    /// A failed checkout or statement.
    async fn rows(&self, statement: Statement) -> DbResult<Vec<Value>>;

    /// Set the columns `data` names (keyed by physical column) on row `id`
    /// of `table`, through its entity; a row that does not exist is
    /// inserted.
    ///
    /// # Errors
    /// `BEM-146` for a malformed value; store errors.
    async fn write(&self, table: &str, id: Uuid, data: Value) -> DbResult<()>;

    /// Soft-delete row `id` of `table` through its entity (`_deleted`).
    ///
    /// # Errors
    /// `DAT-078` when there is no such row; store errors.
    async fn soft_delete(&self, table: &str, id: Uuid) -> DbResult<()>;

    /// Insert row `id` of `table` from `data`, unless it exists (then nothing
    /// changes, as with a concurrent insert): Python's
    /// `insert_row_if_absent`.
    ///
    /// # Errors
    /// As [`Self::write`].
    async fn insert(&self, table: &str, id: Uuid, mut data: Value) -> DbResult<()> {
        if let Some(object) = data.as_object_mut() {
            object.insert("_insert".to_owned(), Value::Bool(true));
        }
        self.write(table, id, data).await
    }
}

#[async_trait]
impl DomainExec for Db {
    fn schema(&self) -> &SchemaName {
        Db::schema(self)
    }

    async fn rows(&self, statement: Statement) -> DbResult<Vec<Value>> {
        self.at(crate::BEM_497)
            .scoped(|conn| async move { load(conn, statement).await }.scope_boxed())
            .await
    }

    async fn write(&self, table: &str, id: Uuid, data: Value) -> DbResult<()> {
        let uow = self.unit_of_work().await?;
        let written = write::put(self.datastore(), &uow, table, id, data).await;
        write::finish(&uow, written).await?;
        Ok(())
    }

    async fn soft_delete(&self, table: &str, id: Uuid) -> DbResult<()> {
        let uow = self.unit_of_work().await?;
        let written = match self.datastore().registry().parse(table) {
            Ok(resource) => {
                self.datastore()
                    .invalidate(&uow, resource, &id.to_string())
                    .await
            }
            Err(e) => Err(e),
        };
        write::finish(&uow, written).await?;
        Ok(())
    }
}

/// The domain's statements on a riverbase command's transaction. Each
/// statement takes the transaction's connection and gives it back, so
/// riverbase can write its own rows (messages, outbox) between them. With
/// the optional RLS on, the caller's org is set as `beam.org_id` for the
/// rest of the transaction.
pub struct CommandExec<'a> {
    datastore: &'a PgDataStore,
    uow: &'a CommandUnitOfWork,
    schema: SchemaName,
    org: Option<String>,
}

impl<'a> CommandExec<'a> {
    /// The command's transaction `uow` on `datastore`, for the caller's
    /// `org`.
    #[must_use]
    pub fn new(
        datastore: &'a PgDataStore,
        uow: &'a CommandUnitOfWork,
        schema: SchemaName,
        org: Option<String>,
    ) -> Self {
        Self {
            datastore,
            uow,
            schema,
            org,
        }
    }

    async fn conn(&self) -> DbResult<riverbase_core::datastore::postgres::PgConnectionGuard> {
        Ok(optional_command_connection(Some(self.uow), self.datastore.dbpool()).await?)
    }

    /// Set the caller's org for the rest of the transaction (RLS on only).
    async fn scope(&self, conn: &mut AsyncPgConnection) -> DbResult<()> {
        if crate::org_scope::rls_enabled() {
            sql_query("SELECT set_config('beam.org_id', $1, true)")
                .bind::<Text, _>(self.org.clone().unwrap_or_default())
                .execute(conn)
                .await?;
        }
        Ok(())
    }

    /// Set the scope before a datastore write, which takes the connection
    /// itself.
    async fn scope_transaction(&self) -> DbResult<()> {
        let mut conn = self.conn().await?;
        self.scope(&mut conn).await
    }

    /// Insert `task` on the command's transaction.
    ///
    /// # Errors
    /// `BEM-507` when the insert fails.
    pub async fn enqueue_task(&self, task: &NewTask) -> DbResult<()> {
        let mut conn = self.conn().await?;
        self.scope(&mut conn).await?;
        Ok(crate::tasks::enqueue(&mut conn, &self.schema, task).await?)
    }

    /// Take `user_id`'s legal-hold lock until the command commits, then say
    /// whether the user is held. Every hold change and deletion takes the
    /// same lock first.
    ///
    /// # Errors
    /// `BEM-139` when the lock or read fails.
    pub async fn lock_hold(&self, user_id: &str) -> DbResult<bool> {
        let mut conn = self.conn().await?;
        crate::holds::lock(&mut conn, user_id).await?;
        Ok(crate::holds::is_held(&mut conn, &self.schema, user_id).await?)
    }

    /// Hold `user_id` on the command's transaction, under its lock.
    ///
    /// # Errors
    /// `BEM-139` when the write fails.
    pub async fn place_hold(
        &self,
        user_id: &str,
        org_id: Option<Uuid>,
        reason: Option<&str>,
        placed_by: Option<&str>,
    ) -> DbResult<()> {
        self.lock_hold(user_id).await?;
        let mut conn = self.conn().await?;
        Ok(
            crate::holds::place(&mut conn, &self.schema, user_id, org_id, reason, placed_by)
                .await?,
        )
    }

    /// Release `user_id`'s hold on the command's transaction, under its
    /// lock; whether there was one.
    ///
    /// # Errors
    /// `BEM-139` when the write fails.
    pub async fn release_hold(&self, user_id: &str) -> DbResult<bool> {
        self.lock_hold(user_id).await?;
        let mut conn = self.conn().await?;
        Ok(crate::holds::release(&mut conn, &self.schema, user_id).await?)
    }
}

#[async_trait]
impl DomainExec for CommandExec<'_> {
    fn schema(&self) -> &SchemaName {
        &self.schema
    }

    async fn rows(&self, statement: Statement) -> DbResult<Vec<Value>> {
        let mut conn = self.conn().await?;
        self.scope(&mut conn).await?;
        load(&mut conn, statement).await
    }

    async fn write(&self, table: &str, id: Uuid, data: Value) -> DbResult<()> {
        self.scope_transaction().await?;
        let resource = self.datastore.registry().parse(table)?;
        Ok(self
            .datastore
            .upsert(self.uow, resource, &id.to_string(), data)
            .await?)
    }

    async fn soft_delete(&self, table: &str, id: Uuid) -> DbResult<()> {
        self.scope_transaction().await?;
        let resource = self.datastore.registry().parse(table)?;
        Ok(self
            .datastore
            .invalidate(self.uow, resource, &id.to_string())
            .await?)
    }
}

/// Which column an active usage plan is found by.
#[derive(Debug, Clone, Copy)]
pub enum PlanColumn {
    Id,
    Name,
}

/// What a tool rule row holds; the org and user are UUID text (cast in SQL).
#[derive(Debug, Clone)]
pub struct ToolRuleInsert {
    pub rule_id: Uuid,
    pub scope: String,
    pub org_id: Option<String>,
    pub user_id: Option<String>,
    pub matcher: Value,
    pub mode: String,
    pub locked: bool,
}

/// The domain's statements, over any [`DomainExec`].
#[async_trait]
pub trait DomainSql: DomainExec {
    /// Row `id` of `table` (not soft-deleted), as JSON.
    ///
    /// # Errors
    /// Store errors.
    async fn row(&self, table: &str, id: &str) -> DbResult<Option<Value>> {
        let s = self.schema().clone();
        Ok(self
            .rows(
                Statement::new(format!(
                    "SELECT (to_jsonb(t) - '_txt') AS row FROM {s}.{table} t WHERE t._deleted IS NULL AND t._id::text = $1"
                ))
                .text(id),
            )
            .await?
            .into_iter()
            .next())
    }

    /// The agent a command URL names, by its `_id` or its name, the id
    /// first. No row locks are taken (Python's DAT-904 fix).
    ///
    /// # Errors
    /// Store errors.
    async fn agent_row(&self, identifier: &str) -> DbResult<Option<Value>> {
        let s = self.schema().clone();
        Ok(self
            .rows(
                Statement::new(format!(
                    "SELECT (to_jsonb(t) - '_txt') AS row FROM {s}.agent t WHERE t._deleted IS NULL \
                     AND (t._id::text = $1 OR t.name = $1) \
                     ORDER BY (t._id::text = $1) DESC LIMIT 1"
                ))
                .text(identifier),
            )
            .await?
            .into_iter()
            .next())
    }

    /// Delete the runtime state of `session_id`'s executions (a bulk delete
    /// stays SQL).
    ///
    /// # Errors
    /// Store errors.
    async fn purge_session_runtime_state(&self, session_id: &str) -> DbResult<()> {
        let s = self.schema().clone();
        self.rows(
            Statement::new(format!(
                "DELETE FROM {s}.runtime_state WHERE execution_id::text IN \
                   (SELECT _id::text FROM {s}.agent_execution WHERE session_id::text = $1) \
                 RETURNING jsonb_build_object('execution_id', execution_id) AS row"
            ))
            .text(session_id),
        )
        .await?;
        Ok(())
    }

    /// An edit from transcript sequence `sequence` invalidates the session
    /// summary covering it: its coverage is cut before it.
    ///
    /// # Errors
    /// Store errors.
    async fn invalidate_summary_from(&self, session_id: &str, sequence: i64) -> DbResult<()> {
        let s = self.schema().clone();
        self.rows(
            Statement::new(format!(
                "UPDATE {s}.agent_session_summary SET invalidated = true, \
                 coverage_end = LEAST(coverage_end, $2::int - 1), _updated = now() \
                 WHERE session_id::text = $1 AND coverage_end >= $2::int \
                 RETURNING jsonb_build_object('session_id', session_id) AS row"
            ))
            .text(session_id)
            .big_int(sequence),
        )
        .await?;
        Ok(())
    }

    /// The live profiles with `profile_id`.
    ///
    /// # Errors
    /// Store errors.
    async fn profiles_with_profile_id(&self, profile_id: &str) -> DbResult<Vec<Value>> {
        let s = self.schema().clone();
        self.rows(
            Statement::new(format!(
                "SELECT (to_jsonb(t) - '_txt') AS row FROM {s}.agent_profile t WHERE t._deleted IS NULL AND t.profile_id::text = $1"
            ))
            .text(profile_id),
        )
        .await
    }

    /// The owner of execution `id`: its session's user and org, and its own
    /// org (`row_org_id`).
    ///
    /// # Errors
    /// Store errors.
    async fn execution_owner(&self, id: &str) -> DbResult<Option<Value>> {
        let s = self.schema().clone();
        Ok(self
            .rows(
                Statement::new(format!(
                    "SELECT jsonb_build_object('user_id', s.user_id, 'org_id', s.org_id, 'row_org_id', e.org_id) AS row \
                     FROM {s}.agent_execution e JOIN {s}.agent_session s ON s._id = e.session_id AND s._deleted IS NULL \
                     WHERE e._id::text = $1 AND e._deleted IS NULL"
                ))
                .text(id),
            )
            .await?
            .into_iter()
            .next())
    }

    /// `user_id`'s usage counters in `period`.
    ///
    /// # Errors
    /// Store errors.
    async fn usage_counters(&self, user_id: &str, period: &str) -> DbResult<Vec<Value>> {
        let s = self.schema().clone();
        self.rows(
            Statement::new(format!(
                "SELECT (to_jsonb(t) - '_txt') AS row FROM {s}.agent_usage_counter t \
                 WHERE t.user_id::text = $1 AND t.period = $2"
            ))
            .text(user_id)
            .text(period),
        )
        .await
    }

    /// `user_id`'s active usage profile.
    ///
    /// # Errors
    /// Store errors.
    async fn active_usage_profile(&self, user_id: &str) -> DbResult<Option<Value>> {
        let s = self.schema().clone();
        Ok(self
            .rows(
                Statement::new(format!(
                    "SELECT (to_jsonb(t) - '_txt') AS row FROM {s}.agent_usage_profile t WHERE t._deleted IS NULL \
                     AND t.user_id::text = $1 AND t.active"
                ))
                .text(user_id),
            )
            .await?
            .into_iter()
            .next())
    }

    /// The active usage plan whose `column` is `value`.
    ///
    /// # Errors
    /// Store errors.
    async fn active_plan(&self, column: PlanColumn, value: &str) -> DbResult<Option<Value>> {
        let s = self.schema().clone();
        let column = match column {
            PlanColumn::Id => "_id",
            PlanColumn::Name => "name",
        };
        Ok(self
            .rows(
                Statement::new(format!(
                    "SELECT (to_jsonb(t) - '_txt') AS row FROM {s}.agent_usage_plan t WHERE t._deleted IS NULL \
                     AND t.{column}::text = $1 AND t.status::text ILIKE 'active'"
                ))
                .text(value),
            )
            .await?
            .into_iter()
            .next())
    }

    /// Take an idempotency key's advisory lock (the worker takes the same
    /// one) until the transaction ends.
    ///
    /// # Errors
    /// Store errors.
    async fn lock_idempotency(&self, lock: (String, String)) -> DbResult<()> {
        self.rows(
            Statement::new(
                "SELECT jsonb_build_object('locked', true) AS row FROM \
                 (SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))) l"
                    .to_owned(),
            )
            .text(lock.0)
            .text(lock.1),
        )
        .await?;
        Ok(())
    }

    /// The run an idempotency key already started: `{execution_id,
    /// request_id}`.
    ///
    /// # Errors
    /// Store errors.
    async fn started_run(
        &self,
        key: &str,
        user_id: &str,
        agent_name: &str,
    ) -> DbResult<Option<Value>> {
        let s = self.schema().clone();
        Ok(self
            .rows(
                Statement::new(format!(
                    "SELECT jsonb_build_object('execution_id', i.execution_id, 'request_id', e.metadata_json ->> 'request_id') AS row \
                     FROM {s}.execution_idempotency i LEFT JOIN {s}.agent_execution e ON e._id = i.execution_id \
                     WHERE i.key = $1 AND i.user_id::text = $2 AND i.agent_name = $3 \
                     AND i._deleted IS NULL LIMIT 1"
                ))
                .text(key)
                .text(user_id)
                .text(agent_name),
            )
            .await?
            .into_iter()
            .next())
    }

    /// The run an idempotency key queued but no worker started yet:
    /// `{execution_id, request_id, task_id}`.
    ///
    /// # Errors
    /// Store errors.
    async fn queued_run(
        &self,
        key: &str,
        agent_name: &str,
        user_id: Option<String>,
    ) -> DbResult<Option<Value>> {
        let s = self.schema().clone();
        Ok(self
            .rows(
                Statement::new(format!(
                    "SELECT jsonb_build_object('execution_id', t.execution_id, 'request_id', t.payload ->> 'request_id', 'task_id', t._id) AS row \
                     FROM {s}.agent_task t WHERE t.task = '{INVOKE_TASK}' AND t.status IN ('QUEUED', 'RUNNING') \
                     AND btrim(t.payload ->> 'idempotency_key') = $1 AND t.payload ->> 'agent_name' = $2 \
                     AND COALESCE(t.user_id::text, '') = COALESCE($3, '') AND t._deleted IS NULL LIMIT 1"
                ))
                .text(key)
                .text(agent_name)
                .opt_text(user_id),
            )
            .await?
            .into_iter()
            .next())
    }

    /// The interaction action `action_id` of `session_id` belongs to:
    /// `{interaction_id}`.
    ///
    /// # Errors
    /// Store errors.
    async fn action_interaction(
        &self,
        action_id: Uuid,
        session_id: Uuid,
    ) -> DbResult<Option<Value>> {
        let s = self.schema().clone();
        Ok(self
            .rows(
                Statement::new(format!(
                    "SELECT jsonb_build_object('interaction_id', a.interaction_id) AS row FROM {s}.agent_action a \
                     WHERE a._id::text = $1 AND a.session_id::text = $2 \
                     AND a._deleted IS NULL LIMIT 1"
                ))
                .text(action_id.to_string())
                .text(session_id.to_string()),
            )
            .await?
            .into_iter()
            .next())
    }

    /// The execution behind interaction `interaction_id` of `session_id`:
    /// `{execution_id, status, busy}`, `busy` when another resume of its wait
    /// is queued or running. The run that interrupted (its task still
    /// `RUNNING` for a moment after it finished) started before
    /// `interrupted_at`, so it does not count.
    ///
    /// # Errors
    /// Store errors.
    async fn interaction_execution(
        &self,
        interaction_id: Uuid,
        session_id: Uuid,
    ) -> DbResult<Option<Value>> {
        let s = self.schema().clone();
        Ok(self
            .rows(
                Statement::new(format!(
                    "SELECT jsonb_build_object('execution_id', e._id, 'status', e.status::text, 'busy', EXISTS ( \
                     SELECT 1 FROM {s}.agent_task t WHERE t.execution_id = e._id AND t.task = '{INVOKE_TASK}' \
                     AND t._deleted IS NULL AND (t.status = 'QUEUED' OR (t.status = 'RUNNING' \
                     AND (e.interrupted_at IS NULL OR t.started_at IS NULL OR t.started_at > e.interrupted_at))))) AS row \
                     FROM {s}.agent_interaction i JOIN {s}.agent_execution e ON e._id = i.execution_id \
                     WHERE i._id::text = $1 AND i.session_id::text = $2 \
                     AND i._deleted IS NULL AND e._deleted IS NULL LIMIT 1"
                ))
                .text(interaction_id.to_string())
                .text(session_id.to_string()),
            )
            .await?
            .into_iter()
            .next())
    }

    /// Cancel the queued invoke of `execution_id` when the caller may (an
    /// admin, the task's user, or a task without one): the cancelled task.
    ///
    /// # Errors
    /// Store errors.
    async fn cancel_queued_invoke(
        &self,
        execution_id: &str,
        admin: bool,
        user_id: Option<String>,
    ) -> DbResult<Option<Value>> {
        let s = self.schema().clone();
        Ok(self
            .rows(
                Statement::new(format!(
                    "UPDATE {s}.agent_task t SET status = 'CANCELLED', finished_at = now(), _updated = now() \
                     WHERE t.execution_id::text = $1 AND t.task = '{INVOKE_TASK}' AND t.status = 'QUEUED' \
                     AND t._deleted IS NULL \
                     AND ($2 OR t.user_id IS NULL OR t.user_id::text = $3) \
                     RETURNING (to_jsonb(t) - '_txt') AS row"
                ))
                .text(execution_id)
                .boolean(admin)
                .opt_text(user_id),
            )
            .await?
            .into_iter()
            .next())
    }

    /// Whether an invoke of `execution_id` is still queued.
    ///
    /// # Errors
    /// Store errors.
    async fn invoke_queued(&self, execution_id: &str) -> DbResult<bool> {
        let s = self.schema().clone();
        Ok(!self
            .rows(
                Statement::new(format!(
                    "SELECT jsonb_build_object('_id', t._id) AS row FROM {s}.agent_task t \
                     WHERE t.execution_id::text = $1 AND t.task = '{INVOKE_TASK}' \
                     AND t.status = 'QUEUED' AND t._deleted IS NULL LIMIT 1"
                ))
                .text(execution_id),
            )
            .await?
            .is_empty())
    }

    /// Insert tool rule `rule`; the stored rule
    /// `{rule_id, scope, org_id, user_id, rule}`.
    ///
    /// # Errors
    /// A malformed org or user id; store errors.
    async fn insert_tool_rule(&self, rule: ToolRuleInsert) -> DbResult<Option<Value>> {
        let s = self.schema().clone();
        Ok(self
            .rows(
                Statement::new(format!(
                    "INSERT INTO {s}.agent_tool_rule (_id, scope, org_id, user_id, \"match\", mode, locked) \
                     VALUES ($1, $2, $3::uuid, $4::uuid, $5, $6, $7) \
                     RETURNING jsonb_build_object('rule_id', _id, 'scope', scope, 'org_id', org_id, \
                     'user_id', user_id, 'rule', jsonb_build_object('match', \"match\", 'mode', mode, \
                     'locked', locked)) AS row"
                ))
                .uuid(rule.rule_id)
                .text(rule.scope)
                .opt_text(rule.org_id)
                .opt_text(rule.user_id)
                .jsonb(Some(rule.matcher))
                .text(rule.mode)
                .boolean(rule.locked),
            )
            .await?
            .into_iter()
            .next())
    }

    /// Delete rule `rule_id` with `scope` of `org_id` (and `user_id`);
    /// whether it was there.
    ///
    /// # Errors
    /// A malformed org or user id; store errors.
    async fn delete_tool_rule(
        &self,
        rule_id: Uuid,
        scope: &str,
        org_id: Option<String>,
        user_id: Option<String>,
    ) -> DbResult<bool> {
        let s = self.schema().clone();
        Ok(!self
            .rows(
                Statement::new(format!(
                    "DELETE FROM {s}.agent_tool_rule WHERE _id::text = $1 \
                     AND scope = $2 \
                     AND org_id IS NOT DISTINCT FROM $3::uuid \
                     AND user_id IS NOT DISTINCT FROM $4::uuid \
                     RETURNING jsonb_build_object('_id', _id) AS row"
                ))
                .text(rule_id.to_string())
                .text(scope)
                .opt_text(org_id)
                .opt_text(user_id),
            )
            .await?
            .is_empty())
    }

    /// Delete the tool grants of `session_id` in `org_id` (only
    /// `tool_name`'s when given); how many.
    ///
    /// # Errors
    /// A malformed org id; store errors.
    async fn revoke_tool_grants(
        &self,
        session_id: Uuid,
        org_id: Option<String>,
        tool_name: Option<String>,
    ) -> DbResult<usize> {
        let s = self.schema().clone();
        Ok(self
            .rows(
                Statement::new(format!(
                    "DELETE FROM {s}.agent_tool_grant WHERE session_id::text = $1 \
                     AND org_id IS NOT DISTINCT FROM $2::uuid \
                     AND ($3::text IS NULL OR tool_name = $3) \
                     RETURNING jsonb_build_object('_id', _id) AS row"
                ))
                .text(session_id.to_string())
                .opt_text(org_id)
                .opt_text(tool_name),
            )
            .await?
            .len())
    }

    /// Insert an empty evaluation dataset; `{_id, name, metadata_json}`.
    ///
    /// # Errors
    /// Store errors.
    async fn insert_dataset(
        &self,
        id: Uuid,
        name: &str,
        metadata: Value,
    ) -> DbResult<Option<Value>> {
        let s = self.schema().clone();
        Ok(self
            .rows(
                Statement::new(format!(
                    "INSERT INTO {s}.agent_evaluation_dataset (_id, name, metadata_json) \
                     VALUES ($1, $2, $3) \
                     RETURNING jsonb_build_object('_id', _id, 'name', name, \
                     'metadata_json', metadata_json) AS row"
                ))
                .uuid(id)
                .text(name)
                .jsonb(Some(metadata)),
            )
            .await?
            .into_iter()
            .next())
    }

    /// Insert a case into live dataset `dataset_id`; `None` when there is no
    /// such dataset. A JSON `null` expectation is stored as SQL `NULL`.
    ///
    /// # Errors
    /// Store errors.
    async fn insert_case(
        &self,
        id: Uuid,
        dataset_id: Uuid,
        input: Value,
        expected: Option<Value>,
    ) -> DbResult<Option<Value>> {
        let s = self.schema().clone();
        Ok(self
            .rows(
                Statement::new(format!(
                    "INSERT INTO {s}.agent_evaluation_case (_id, dataset_id, input_json, expected) \
                     SELECT $1, d._id, $3, NULLIF($4, 'null'::jsonb) \
                     FROM {s}.agent_evaluation_dataset d \
                     WHERE d._id::text = $2 AND d._deleted IS NULL \
                     RETURNING jsonb_build_object('_id', _id, 'dataset_id', dataset_id, \
                     'input_json', input_json, 'expected', expected) AS row"
                ))
                .uuid(id)
                .text(dataset_id.to_string())
                .jsonb(Some(input))
                .jsonb(expected),
            )
            .await?
            .into_iter()
            .next())
    }

    /// Give `user_id` plan `plan_id` in one statement: the user's other
    /// active profiles turn inactive and this plan's profile is upserted
    /// active, so the guard reads exactly one active plan. `None` when there
    /// is no such plan.
    ///
    /// # Errors
    /// A malformed org id; store errors.
    async fn assign_usage_plan(
        &self,
        plan_id: &str,
        user_id: Uuid,
        profile_id: Uuid,
        org_id: Option<String>,
    ) -> DbResult<Option<Value>> {
        let s = self.schema().clone();
        Ok(self
            .rows(
                Statement::new(format!(
                    "WITH plan AS (SELECT p._id, p.name FROM {s}.agent_usage_plan p \
                     WHERE p._id::text = $1 AND p._deleted IS NULL), \
                     off AS (UPDATE {s}.agent_usage_profile u SET active = false, _updated = now() \
                     WHERE u.user_id::text = $2 AND u.active \
                     AND u.plan_id <> ALL (SELECT _id FROM plan) AND EXISTS (SELECT 1 FROM plan)), \
                     up AS (INSERT INTO {s}.agent_usage_profile (_id, user_id, plan_id, plan_key, active, org_id) \
                     SELECT $3, $2::uuid, plan._id, plan.name, true, \
                     $4::uuid FROM plan \
                     ON CONFLICT (user_id, plan_id) DO UPDATE SET active = true, \
                     plan_key = EXCLUDED.plan_key, _deleted = NULL, _updated = now() \
                     RETURNING user_id, plan_id, plan_key, active) \
                     SELECT jsonb_build_object('user_id', up.user_id, 'plan_id', up.plan_id, \
                     'plan_key', up.plan_key, 'active', up.active) AS row FROM up"
                ))
                .text(plan_id)
                .text(user_id.to_string())
                .uuid(profile_id)
                .opt_text(org_id),
            )
            .await?
            .into_iter()
            .next())
    }

    /// Delete `user_id`'s tasks that are not running; how many.
    ///
    /// # Errors
    /// Store errors.
    async fn purge_user_tasks(&self, user_id: &str) -> DbResult<usize> {
        let s = self.schema().clone();
        Ok(self
            .rows(
                Statement::new(format!(
                    "DELETE FROM {s}.agent_task t WHERE t.user_id::text = $1 \
                     AND t.status <> 'RUNNING' RETURNING jsonb_build_object('_id', t._id) AS row"
                ))
                .text(user_id),
            )
            .await?
            .len())
    }
}

impl<T: DomainExec + ?Sized> DomainSql for T {}
