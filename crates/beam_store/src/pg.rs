//! Postgres-backed store on `riverbase_core`'s async Diesel pool.
//!
//! The tables are fluvius-beam's (`tests/schema/python_ref_beam.sql`), created
//! by the migrations in [`crate::migrate`]. Connecting never runs DDL: apply
//! migrations first (Python's rule: never migrate on startup).
//!
//! Conventions shared with Python so either stack can read the other's rows:
//! enum columns hold the Python member names ([`crate::DbEnum::db_label`]);
//! inserts leave Fluvius audit columns to their defaults (`_created`, `_etag`);
//! updates set `_updated = now()` and a new random `_etag`; reads skip
//! soft-deleted rows (`_deleted IS NOT NULL`).

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use diesel::expression::SqlLiteral;
use diesel::result::QueryResult;
use diesel::sql_query;
use diesel::sql_types::{Array, BigInt, Jsonb, Text, Timestamptz, Uuid as SqlUuid};
use diesel::{
    ExpressionMethods, OptionalExtension, PgSortExpressionMethods, QueryDsl, Queryable,
    QueryableByName, Selectable, SelectableHelper,
};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{json, Value};
use uuid::Uuid;

use diesel_async::scoped_futures::ScopedFutureExt;
use riverbase_core::datastore::{PgDataStore, PgPool};

use crate::db::Db;

use crate::entity::value::{enum_sql, new_etag, now};
use crate::entity::write;
use crate::rows::{
    ActionRow, ActionStatus, ActionType, AgentRow, DbEnum, InteractionRow, InteractionStatus,
    ManifestRow, MessagePhase, MessageRole, MessageRow, MessageType, ModelRow, PlanRow, RunRow,
    RunStatus, SessionRow, StepRow,
};
use crate::schema::SchemaName;
use crate::store::{CatalogStore, ExecutionStore, MessagePage, StoreResult};

/// SQL that sets the Fluvius update bookkeeping columns.
const TOUCH: &str = "_updated = now(), _etag = gen_random_uuid()";

/// Postgres-backed persistence for the beam catalog and execution hierarchy.
#[derive(Clone)]
pub struct PgBeamStore {
    /// Every checkout goes through the gateway (plan 025).
    db: Db,
}

impl PgBeamStore {
    /// Connect to Postgres; the tables are in `BEAM_SQL_SCHEMA` (default
    /// `riverbeam`). No DDL runs; see [`crate::migrate`].
    ///
    /// # Errors
    /// Returns a store error if the pool cannot be established.
    pub async fn connect(database_url: &str) -> StoreResult<Self> {
        Ok(Self {
            db: Db::connect(database_url).await?,
        })
    }

    /// Build a store over an existing riverbase [`PgPool`] (does not run DDL).
    /// The pool must set the schema's `search_path`
    /// ([`SchemaName::pool_url`]): the entities name tables and enum types
    /// without a schema.
    #[must_use]
    pub fn new(pool: PgPool, schema: SchemaName) -> Self {
        Self {
            db: Db::new(pool, schema),
        }
    }

    /// The database gateway every beam store goes through.
    #[must_use]
    pub fn db(&self) -> &Db {
        &self.db
    }

    /// The beam datastore (plan 013 D3), shared by riverbase's command and
    /// query engines.
    #[must_use]
    pub fn datastore(&self) -> &Arc<PgDataStore> {
        self.db.datastore()
    }

    /// The underlying pool, for tests (plan 025 D7: everything else checks
    /// out through [`Db`]).
    #[cfg(any(test, feature = "testing"))]
    #[must_use]
    pub fn pool(&self) -> &PgPool {
        self.db.pool()
    }

    /// The beam schema this store targets.
    #[must_use]
    pub fn schema(&self) -> &SchemaName {
        self.db.schema()
    }

    /// Mark every `ACTIVE` catalog agent not named in `keep` as `INACTIVE`;
    /// returns how many. For an app that owns its schema: check-in only adds
    /// and updates agents, so one the app stopped registering stays active.
    /// Never call it on a catalog other processes share.
    ///
    /// # Errors
    /// `BEM-114` when the update fails.
    pub async fn deactivate_agents_except(&self, keep: &[String]) -> StoreResult<usize> {
        use crate::entity::agent::{agent as t, AGENTSTATUS};
        let label = |status: crate::rows::AgentStatus| {
            enum_sql::<Text>(
                Some(status.db_label()),
                "status",
                "agentstatus",
                AGENTSTATUS,
            )
        };
        self.db
            .scoped(|conn| {
                async move {
                    let (active, inactive) = (
                        label(crate::rows::AgentStatus::Active)?,
                        label(crate::rows::AgentStatus::Inactive)?,
                    );
                    diesel::update(
                        t::table
                            .filter(t::status.eq(active))
                            .filter(t::name.ne_all(keep))
                            .filter(t::_deleted.is_null()),
                    )
                    .set((
                        t::status.eq(inactive),
                        t::_updated.eq(now()),
                        t::_etag.eq(new_etag()),
                    ))
                    .execute(conn)
                    .await
                    .map_err(|e| crate::BEM_114.with_data(e.to_string()))
                }
                .scope_boxed()
            })
            .await
    }
    /// Whether `dsn` reaches this store's database: the same database name,
    /// server port and server start time (host spellings such as
    /// `localhost` and `127.0.0.1` do not matter). The domain's commands write beam rows and riverbase's
    /// command log in one transaction, so both must live there.
    ///
    /// # Errors
    /// `BEM-113` when either database cannot be reached.
    pub async fn same_database_as(&self, dsn: &str) -> StoreResult<bool> {
        #[derive(QueryableByName)]
        struct Identity {
            #[diesel(sql_type = Text)]
            id: String,
        }
        const IDENTITY: &str =
            "SELECT current_database() || ':' || COALESCE(inet_server_port()::text, '') \
            || '@' || pg_postmaster_start_time()::text AS id";
        let mine: Vec<Identity> = self
            .db
            .cross_org(|conn| {
                async move {
                    sql_query(IDENTITY)
                        .load(conn)
                        .await
                        .map_err(|e| crate::BEM_113.with_data(e.to_string()))
                }
                .scope_boxed()
            })
            .await?;
        let mut other = AsyncPgConnection::establish(dsn)
            .await
            .map_err(|e| crate::BEM_113.with_data(e.to_string()))?;
        let theirs: Vec<Identity> = sql_query(IDENTITY)
            .load(&mut other)
            .await
            .map_err(|e| crate::BEM_113.with_data(e.to_string()))?;
        Ok(matches!(
            (mine.as_slice().first(), theirs.as_slice().first()),
            (Some(a), Some(b)) if a.id == b.id
        ))
    }

    /// Fail unless the schema is migrated to at least [`crate::migrate::latest_version`].
    ///
    /// # Errors
    /// Returns a store error naming the command to run when migrations are missing.
    pub async fn ensure_migrated(&self) -> StoreResult<()> {
        let latest = crate::migrate::latest_version();
        match crate::migrate::applied_version(self.db.pool(), self.db.schema()).await? {
            Some(v) if v >= latest => Ok(()),
            found => Err(crate::BEM_112.with_data(format!(
                "schema {} is at migration {found:?}, need {latest}; run the migrations (showcase-worker migrate)",
                self.db.schema()
            ))),
        }
    }

    /// `CAST($n AS <schema>.<type>)` for an enum parameter.
    fn cast<E: DbEnum>(&self, param: usize) -> String {
        format!("CAST(${param} AS {}.{})", self.db.schema(), E::PG_TYPE)
    }
}

/// Parse an optional caller id into a `UUID`. Non-UUID values are dropped,
/// since the schema types `user_id`, `profile_id` and `org_id` as `UUID`.
fn opt_uuid(value: Option<&str>) -> Option<Uuid> {
    value.and_then(|v| Uuid::parse_str(v).ok())
}

/// The session's next message sequence (`MAX + 1`, from 0), soft-deleted
/// messages included.
async fn next_sequence(conn: &mut AsyncPgConnection, session_id: Uuid) -> QueryResult<i32> {
    use crate::entity::agent_message::agent_message as t;
    let max: Option<i32> = t::table
        .filter(t::session_id.eq(session_id))
        .select(diesel::dsl::max(t::sequence))
        .first(conn)
        .await?;
    Ok(max.map_or(0, |m| m.saturating_add(1)))
}

/// An `actionstatus` value for a typed filter or update (plan 014 D4).
fn action_status(status: ActionStatus) -> StoreResult<SqlLiteral<Text>> {
    enum_sql(
        Some(status.db_label()),
        "status",
        "actionstatus",
        crate::entity::agent_action::ACTIONSTATUS,
    )
}

fn label<E: DbEnum>(value: &str) -> StoreResult<E> {
    E::from_db_label(value)
        .ok_or_else(|| crate::BEM_113.with_data(format!("unknown {} label {value:?}", E::PG_TYPE)))
}

fn json_strings(values: &[String]) -> Value {
    Value::Array(values.iter().cloned().map(Value::String).collect())
}

// The store's rows: typed selects over the generated Diesel
// schemas in `crate::entity`. Enum columns read as text.

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::entity::agent_context_manifest::agent_context_manifest)]
struct ManifestDbRow {
    operation_id: String,
    execution_id: Option<Uuid>,
    operation_kind: String,
    revision_id: Option<String>,
    policy_revision: Option<String>,
    serializer_revision: Option<String>,
    tokenizer_revision: Option<String>,
    input_tokens: i32,
    output_tokens: i32,
    reserve_tokens: i32,
    actual_prompt_tokens: Option<i32>,
    estimate_drift: Option<f64>,
    decisions: Value,
    view: Value,
}

impl From<ManifestDbRow> for ManifestRow {
    fn from(r: ManifestDbRow) -> Self {
        Self {
            operation_id: r.operation_id,
            execution_id: r.execution_id,
            operation_kind: r.operation_kind,
            revision_id: r.revision_id,
            policy_revision: r.policy_revision,
            serializer_revision: r.serializer_revision,
            tokenizer_revision: r.tokenizer_revision,
            input_tokens: r.input_tokens,
            output_tokens: r.output_tokens,
            reserve_tokens: r.reserve_tokens,
            actual_prompt_tokens: r.actual_prompt_tokens,
            estimate_drift: r.estimate_drift,
            decisions: r.decisions,
            view: r.view,
        }
    }
}

/// A manifest's columns, keyed by physical column, for its entity's write.
fn manifest_json(m: &ManifestRow) -> Value {
    json!({
        "operation_id": m.operation_id,
        "execution_id": m.execution_id,
        "operation_kind": m.operation_kind,
        "revision_id": m.revision_id,
        "policy_revision": m.policy_revision,
        "serializer_revision": m.serializer_revision,
        "tokenizer_revision": m.tokenizer_revision,
        "input_tokens": m.input_tokens,
        "output_tokens": m.output_tokens,
        "reserve_tokens": m.reserve_tokens,
        "actual_prompt_tokens": m.actual_prompt_tokens,
        "estimate_drift": m.estimate_drift,
        "decisions": m.decisions,
        "view": m.view,
    })
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::entity::agent_session::agent_session)]
struct SessionDbRow {
    #[diesel(column_name = _id)]
    id: Uuid,
    agent_id: Uuid,
    user_id: Option<Uuid>,
    profile_id: Option<Uuid>,
    org_id: Option<Uuid>,
    name: Option<String>,
    last_active_at: Option<DateTime<Utc>>,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::entity::agent_interaction::agent_interaction)]
struct InteractionDbRow {
    #[diesel(column_name = _id)]
    id: Uuid,
    session_id: Uuid,
    execution_id: Option<Uuid>,
    agent_id: Uuid,
    agent_name: String,
    status: String,
    started_at: DateTime<Utc>,
    ended_at: Option<DateTime<Utc>>,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::entity::agent_run::agent_run)]
struct RunDbRow {
    #[diesel(column_name = _id)]
    id: Uuid,
    session_id: Uuid,
    interaction_id: Uuid,
    execution_id: Option<Uuid>,
    attempt_no: i32,
    agent_id: Uuid,
    agent_name: String,
    status: String,
    started_at: DateTime<Utc>,
    ended_at: Option<DateTime<Utc>>,
    org_id: Option<Uuid>,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::entity::agent_step::agent_step)]
struct StepDbRow {
    #[diesel(column_name = _id)]
    id: Uuid,
    session_id: Uuid,
    interaction_id: Uuid,
    run_id: Uuid,
    agent_id: Uuid,
    status: String,
    lc_step_id: Option<String>,
    name: String,
    input: Value,
    result: Option<Value>,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::entity::agent_message::agent_message)]
struct MessageDbRow {
    #[diesel(column_name = _id)]
    id: Uuid,
    sequence: i32,
    session_id: Uuid,
    execution_id: Option<Uuid>,
    interaction_id: Option<Uuid>,
    run_id: Option<Uuid>,
    agent_id: Uuid,
    step_id: Option<Uuid>,
    role: String,
    #[diesel(column_name = message_type)]
    msg_type: String,
    phase: String,
    content: Value,
    name: Option<String>,
    tool_call_id: Option<String>,
    tool_name: Option<String>,
    tool_args: Option<Value>,
    tool_result: Option<Value>,
    tool_calls: Value,
    metadata_json: Value,
    request_id: Option<Uuid>,
    org_id: Option<Uuid>,
    timestamp: DateTime<Utc>,
}

/// Also read by name: `claim_due_client_tool_actions`' SQL returns
/// [`ACTION_COLUMNS`].
#[derive(Queryable, Selectable, QueryableByName)]
#[diesel(table_name = crate::entity::agent_action::agent_action)]
struct ActionDbRow {
    #[diesel(column_name = _id)]
    id: Uuid,
    session_id: Uuid,
    execution_id: Option<Uuid>,
    interaction_id: Uuid,
    run_id: Uuid,
    step_id: Option<Uuid>,
    message_id: Option<Uuid>,
    agent_id: Uuid,
    action_type: String,
    status: String,
    lc_action_id: Option<String>,
    payload: Value,
    result: Option<Value>,
    resolved_at: Option<DateTime<Utc>>,
    org_id: Option<Uuid>,
    tool_name: Option<String>,
    expires_at: Option<DateTime<Utc>>,
    sweep_after: Option<DateTime<Utc>>,
}

fn uuid_text(value: Option<Uuid>) -> Option<String> {
    value.map(|u| u.to_string())
}

impl From<SessionDbRow> for SessionRow {
    fn from(r: SessionDbRow) -> Self {
        Self {
            id: r.id,
            agent_id: r.agent_id,
            user_id: uuid_text(r.user_id),
            profile_id: uuid_text(r.profile_id),
            org_id: uuid_text(r.org_id),
            name: r.name,
            last_active_at: r.last_active_at,
        }
    }
}

impl TryFrom<InteractionDbRow> for InteractionRow {
    type Error = riverbase_core::RiverbaseError;
    fn try_from(r: InteractionDbRow) -> StoreResult<Self> {
        Ok(Self {
            id: r.id,
            session_id: r.session_id,
            execution_id: r.execution_id,
            agent_id: r.agent_id,
            agent_name: r.agent_name,
            status: label(&r.status)?,
            started_at: r.started_at,
            ended_at: r.ended_at,
        })
    }
}

impl TryFrom<RunDbRow> for RunRow {
    type Error = riverbase_core::RiverbaseError;
    fn try_from(r: RunDbRow) -> StoreResult<Self> {
        Ok(Self {
            id: r.id,
            session_id: r.session_id,
            interaction_id: r.interaction_id,
            execution_id: r.execution_id,
            attempt_no: r.attempt_no,
            agent_id: r.agent_id,
            agent_name: r.agent_name,
            status: label(&r.status)?,
            started_at: r.started_at,
            ended_at: r.ended_at,
            org_id: uuid_text(r.org_id),
        })
    }
}

impl TryFrom<StepDbRow> for StepRow {
    type Error = riverbase_core::RiverbaseError;
    fn try_from(r: StepDbRow) -> StoreResult<Self> {
        Ok(Self {
            id: r.id,
            session_id: r.session_id,
            interaction_id: r.interaction_id,
            run_id: r.run_id,
            agent_id: r.agent_id,
            status: label(&r.status)?,
            lc_step_id: r.lc_step_id,
            name: r.name,
            input: r.input,
            result: r.result,
        })
    }
}

impl TryFrom<MessageDbRow> for MessageRow {
    type Error = riverbase_core::RiverbaseError;
    fn try_from(r: MessageDbRow) -> StoreResult<Self> {
        Ok(Self {
            id: r.id,
            sequence: r.sequence,
            session_id: r.session_id,
            execution_id: r.execution_id,
            interaction_id: r.interaction_id,
            run_id: r.run_id,
            agent_id: r.agent_id,
            step_id: r.step_id,
            role: label::<MessageRole>(&r.role)?,
            msg_type: label::<MessageType>(&r.msg_type)?,
            phase: label::<MessagePhase>(&r.phase)?,
            content: r.content,
            name: r.name,
            tool_call_id: r.tool_call_id,
            tool_name: r.tool_name,
            tool_args: r.tool_args,
            tool_result: r.tool_result,
            tool_calls: r.tool_calls,
            metadata: r.metadata_json,
            request_id: r.request_id,
            org_id: uuid_text(r.org_id),
            timestamp: r.timestamp,
        })
    }
}

impl TryFrom<ActionDbRow> for ActionRow {
    type Error = riverbase_core::RiverbaseError;
    fn try_from(r: ActionDbRow) -> StoreResult<Self> {
        Ok(Self {
            id: r.id,
            session_id: r.session_id,
            execution_id: r.execution_id,
            interaction_id: r.interaction_id,
            run_id: r.run_id,
            step_id: r.step_id,
            message_id: r.message_id,
            agent_id: r.agent_id,
            action_type: label::<ActionType>(&r.action_type)?,
            status: label::<ActionStatus>(&r.status)?,
            lc_action_id: r.lc_action_id,
            payload: r.payload,
            result: r.result,
            resolved_at: r.resolved_at,
            org_id: uuid_text(r.org_id),
            tool_name: r.tool_name,
            expires_at: r.expires_at,
            sweep_after: r.sweep_after,
        })
    }
}

/// The `agent_action` columns [`ActionDbRow`] reads by name.
const ACTION_COLUMNS: &str = "_id, session_id, execution_id, interaction_id, run_id, step_id, \
     message_id, agent_id, type::text AS action_type, status::text AS status, lc_action_id, payload, \
     result, resolved_at, org_id, tool_name, expires_at, sweep_after";

fn collect<D, R>(rows: Vec<D>) -> StoreResult<Vec<R>>
where
    R: TryFrom<D, Error = riverbase_core::RiverbaseError>,
{
    rows.into_iter().map(R::try_from).collect()
}

#[async_trait]
impl CatalogStore for PgBeamStore {
    async fn checkin_agents(&self, agents: &[AgentRow]) -> StoreResult<()> {
        let rows = agents
            .iter()
            .map(|agent| {
                let row = json!({
                    "name": agent.name,
                    "description": agent.description,
                    "supported_modalities": json_strings(&agent.supported_modalities),
                    "output_modalities": json_strings(&agent.output_modalities),
                    "tags": json_strings(&agent.tags),
                    "version": agent.version,
                    "status": agent.status.db_label(),
                });
                (agent.id, row)
            })
            .collect();
        self.checkin("agent", rows)
            .await
            .map_err(|e| crate::BEM_114.with_data(e.to_string()))
    }

    async fn checkin_models(&self, models: &[ModelRow]) -> StoreResult<()> {
        let rows = models
            .iter()
            .map(|model| {
                let row = json!({
                    "name": model.name,
                    "provider": model.provider,
                    "modalities": json_strings(&model.modalities),
                    "capabilities": json_strings(&model.capabilities),
                    "cost_per_1k_tokens": model.cost_per_1k_tokens,
                    "max_tokens": model.max_tokens,
                    "provider_config": model.provider_config,
                    "extra": model.extra,
                    "status": model.status.db_label(),
                });
                (model.id, row)
            })
            .collect();
        self.checkin("model", rows)
            .await
            .map_err(|e| crate::BEM_115.with_data(e.to_string()))
    }

    async fn checkin_plans(&self, plans: &[PlanRow]) -> StoreResult<()> {
        let rows = plans
            .iter()
            .map(|plan| {
                // Enum labels are member names (`TEXT`), as Python's SQLAlchemy writes them.
                let modalities = plan
                    .allowed_modalities
                    .as_ref()
                    .map(|m| m.iter().map(|v| v.to_uppercase()).collect::<Vec<_>>());
                let row = json!({
                    "name": plan.name,
                    "overage_policy": plan.overage_policy,
                    "status": plan.status,
                    "limit_max_requests_per_day": plan.max_requests_per_day,
                    "limit_max_tokens_per_day": plan.max_tokens_per_day,
                    "limit_max_tokens_per_request": plan.max_tokens_per_request,
                    "limit_allowed_agents": plan.allowed_agents,
                    "limit_allowed_modalities": modalities,
                    "limit_allowed_models": plan.allowed_models,
                });
                (plan.id, row)
            })
            .collect();
        self.checkin("agent_usage_plan", rows)
            .await
            .map_err(|e| crate::BEM_115.with_data(e.to_string()))
    }
}

/// One catalog row as `to_jsonb` reads it.
#[derive(QueryableByName)]
struct CatalogJson {
    #[diesel(sql_type = SqlUuid, column_name = id)]
    id: Uuid,
    #[diesel(sql_type = Jsonb, column_name = row)]
    row: Value,
}

/// `a` and `b` are the same JSON, numbers compared by value: `to_jsonb`
/// writes a `float8` 1.0 as `1`.
fn same_json(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x.as_f64() == y.as_f64(),
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(x, y)| same_json(x, y))
        }
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len()
                && x.iter()
                    .all(|(k, v)| y.get(k).is_some_and(|w| same_json(v, w)))
        }
        _ => a == b,
    }
}

impl PgBeamStore {
    /// Write catalog `rows` (`_id`, columns) to `table` through its entity
    /// in one transaction. A row whose columns already hold
    /// these values is left alone, so its `_etag` and `_updated` stay.
    async fn checkin(&self, table: &str, mut rows: Vec<(Uuid, Value)>) -> StoreResult<()> {
        if rows.is_empty() {
            return Ok(());
        }
        // One lock order for every process: two check-ins of the same rows
        // in different orders (plans come from a `HashMap`) would deadlock.
        rows.sort_by_key(|(id, _)| *id);
        let uow = self.db.unit_of_work().await?;
        let written = async {
            let current: Vec<CatalogJson> = {
                let mut conn = uow.postgres_connection().await?;
                let ids: Vec<Uuid> = rows.iter().map(|(id, _)| *id).collect();
                // Unqualified, like the entity's writes: the pool's `search_path`.
                sql_query(format!(
                    "SELECT t._id AS id, to_jsonb(t) AS row FROM {table} t WHERE t._id = ANY($1)"
                ))
                .bind::<Array<SqlUuid>, _>(ids)
                .load(&mut *conn)
                .await?
            };
            for (id, row) in rows {
                let unchanged = current.iter().find(|c| c.id == id).is_some_and(|c| {
                    row.as_object().is_some_and(|columns| {
                        columns
                            .iter()
                            .all(|(k, v)| c.row.get(k).is_some_and(|w| same_json(v, w)))
                    })
                });
                if !unchanged {
                    write::put(self.db.datastore(), &uow, table, id, row).await?;
                }
            }
            Ok(())
        }
        .await;
        write::finish(&uow, written).await
    }
}

/// An `agent_message` row's columns, keyed by physical column.
fn message_json(m: &MessageRow) -> Value {
    json!({
        "sequence": m.sequence,
        "role": m.role.db_label(),
        "type": m.msg_type.db_label(),
        "phase": m.phase.db_label(),
        "content": m.content,
        "name": m.name,
        "tool_call_id": m.tool_call_id,
        "tool_name": m.tool_name,
        "tool_args": m.tool_args,
        "tool_result": m.tool_result,
        "tool_calls": m.tool_calls,
        "metadata_json": m.metadata,
        "timestamp": m.timestamp,
        "session_id": m.session_id,
        "execution_id": m.execution_id,
        "interaction_id": m.interaction_id,
        "run_id": m.run_id,
        "agent_id": m.agent_id,
        "step_id": m.step_id,
        "request_id": m.request_id,
    })
}

/// How a row of a session is written.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Write {
    /// Insert, unless the row exists (`ON CONFLICT DO NOTHING`).
    Insert,
    /// Insert, or set the given columns of an existing row.
    Upsert,
}

/// `data` with the session's owner (plan 008 D1): its user, and `org` when
/// the row has its own, else the session's org.
fn owned(mut data: Value, owner: write::Owner, org: Option<Uuid>, how: Write) -> Value {
    if let Some(object) = data.as_object_mut() {
        object.insert("user_id".to_owned(), json!(owner.user_id));
        object.insert("org_id".to_owned(), json!(org.or(owner.org_id)));
        if how == Write::Insert {
            object.insert("_insert".to_owned(), Value::Bool(true));
        }
    }
    data
}

/// `data` that only inserts (an existing row stays as it is).
fn insert_only(mut data: Value) -> Value {
    if let Some(object) = data.as_object_mut() {
        object.insert("_insert".to_owned(), Value::Bool(true));
    }
    data
}

/// `data` that only updates an existing row (a missing one stays missing).
fn update_only(mut data: Value) -> Value {
    if let Some(object) = data.as_object_mut() {
        object.insert("_update".to_owned(), Value::Bool(true));
    }
    data
}

impl PgBeamStore {
    /// Write `data` to row `id` of `table` through its entity, on a unit of
    /// work of its own.
    async fn put_one(&self, table: &str, id: Uuid, data: Value) -> StoreResult<()> {
        let uow = self.db.unit_of_work().await?;
        let result = write::put(self.db.datastore(), &uow, table, id, data).await;
        write::finish(&uow, result).await
    }

    /// Write row `id` of `table`, a row of session `session_id` that copies
    /// the session's owner ([`owned`]), on a unit of work of its own.
    async fn put_owned(
        &self,
        table: &str,
        id: Uuid,
        session_id: Uuid,
        org: Option<Uuid>,
        data: Value,
        how: Write,
    ) -> StoreResult<()> {
        let uow = self.db.unit_of_work().await?;
        let result = async {
            let owner = write::session_owner(&uow, session_id).await?;
            let data = owned(data, owner, org, how);
            write::put(self.db.datastore(), &uow, table, id, data).await
        }
        .await;
        write::finish(&uow, result).await
    }
}

#[async_trait]
impl ExecutionStore for PgBeamStore {
    fn durable(&self) -> bool {
        true
    }

    async fn ensure_session(&self, session: &SessionRow) -> StoreResult<()> {
        let data = json!({
            "agent_id": session.agent_id,
            "user_id": opt_uuid(session.user_id.as_deref()),
            "profile_id": opt_uuid(session.profile_id.as_deref()),
            "org_id": opt_uuid(session.org_id.as_deref()),
            "name": session.name,
            "_insert": true,
        });
        self.put_one("agent_session", session.id, data)
            .await
            .map_err(|e| crate::BEM_116.with_data(e.to_string()))
    }

    async fn update_session_activity(&self, session_id: Uuid) -> StoreResult<()> {
        let data = update_only(json!({ "last_active_at": Utc::now() }));
        self.put_one("agent_session", session_id, data)
            .await
            .map_err(|e| crate::BEM_117.with_data(e.to_string()))
    }

    async fn create_interaction(&self, row: &InteractionRow) -> StoreResult<()> {
        let data = json!({
            "session_id": row.session_id,
            "execution_id": row.execution_id,
            "agent_id": row.agent_id,
            "agent_name": row.agent_name,
            "status": row.status.db_label(),
            "started_at": row.started_at,
            "ended_at": row.ended_at,
        });
        self.put_owned(
            "agent_interaction",
            row.id,
            row.session_id,
            None,
            data,
            Write::Insert,
        )
        .await
        .map_err(|e| crate::BEM_118.with_data(e.to_string()))
    }

    async fn complete_interaction(
        &self,
        interaction_id: Uuid,
        status: InteractionStatus,
    ) -> StoreResult<()> {
        let data = update_only(json!({ "status": status.db_label(), "ended_at": Utc::now() }));
        self.put_one("agent_interaction", interaction_id, data)
            .await
            .map_err(|e| crate::BEM_119.with_data(e.to_string()))
    }

    async fn create_run(&self, row: &RunRow) -> StoreResult<()> {
        let data = json!({
            "session_id": row.session_id,
            "interaction_id": row.interaction_id,
            "execution_id": row.execution_id,
            "attempt_no": row.attempt_no,
            "agent_id": row.agent_id,
            "agent_name": row.agent_name,
            "status": row.status.db_label(),
            "started_at": row.started_at,
            "ended_at": row.ended_at,
        });
        let org = opt_uuid(row.org_id.as_deref());
        self.put_owned(
            "agent_run",
            row.id,
            row.session_id,
            org,
            data,
            Write::Insert,
        )
        .await
        .map_err(|e| crate::BEM_120.with_data(e.to_string()))
    }

    async fn set_run_trace_id(&self, run_id: Uuid, trace_id: &str) -> StoreResult<()> {
        let data = update_only(json!({ "trace_id": trace_id }));
        self.put_one("agent_run", run_id, data)
            .await
            .map_err(|e| crate::BEM_120.with_data(e.to_string()))
    }

    async fn run_trace_id(&self, run_id: Uuid) -> StoreResult<Option<String>> {
        use crate::entity::agent_run::agent_run as t;
        self.db
            .scoped(|conn| {
                async move {
                    let trace: Option<Option<String>> = t::table
                        .filter(t::_id.eq(run_id))
                        .select(t::trace_id)
                        .first(conn)
                        .await
                        .optional()
                        .map_err(|e| crate::BEM_129.with_data(e.to_string()))?;
                    Ok(trace.flatten())
                }
                .scope_boxed()
            })
            .await
    }
    async fn set_run_runtime_ref(&self, run_id: Uuid, runtime_ref: &Value) -> StoreResult<()> {
        let data = update_only(json!({ "runtime_ref_snapshot": runtime_ref }));
        self.put_one("agent_run", run_id, data)
            .await
            .map_err(|e| crate::BEM_120.with_data(e.to_string()))
    }

    async fn complete_run(&self, run_id: Uuid, status: RunStatus) -> StoreResult<()> {
        let data = update_only(json!({ "status": status.db_label(), "ended_at": Utc::now() }));
        self.put_one("agent_run", run_id, data)
            .await
            .map_err(|e| crate::BEM_121.with_data(e.to_string()))
    }

    async fn list_execution_runs(&self, execution_id: Uuid) -> StoreResult<Vec<RunRow>> {
        use crate::entity::agent_run::agent_run as t;
        self.db
            .scoped(|conn| {
                async move {
                    let rows: Vec<RunDbRow> = t::table
                        .filter(t::execution_id.eq(execution_id))
                        .filter(t::_deleted.is_null())
                        .order((t::attempt_no.asc(), t::started_at.asc()))
                        .select(RunDbRow::as_select())
                        .load(conn)
                        .await
                        .map_err(|e| crate::BEM_129.with_data(e.to_string()))?;
                    collect::<_, RunRow>(rows)
                }
                .scope_boxed()
            })
            .await
    }
    async fn save_step(&self, step: &StepRow) -> StoreResult<()> {
        // A step is saved when it starts and again when it ends.
        let data = json!({
            "session_id": step.session_id,
            "interaction_id": step.interaction_id,
            "run_id": step.run_id,
            "agent_id": step.agent_id,
            "status": step.status.db_label(),
            "lc_step_id": step.lc_step_id,
            "name": step.name,
            "input": step.input,
            "result": step.result,
        });
        self.put_owned(
            "agent_step",
            step.id,
            step.session_id,
            None,
            data,
            Write::Upsert,
        )
        .await
        .map_err(|e| crate::BEM_122.with_data(e.to_string()))
    }

    async fn save_message(&self, m: MessageRow) -> StoreResult<()> {
        let org = opt_uuid(m.org_id.as_deref());
        self.put_owned(
            "agent_message",
            m.id,
            m.session_id,
            org,
            message_json(&m),
            Write::Insert,
        )
        .await
        .map_err(|e| crate::BEM_122.with_data(e.to_string()))
    }

    async fn append_messages(&self, session_id: Uuid, rows: Vec<MessageRow>) -> StoreResult<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let uow = self.db.unit_of_work().await?;
        let result = async {
            // One transaction (Python saves a turn in one), and writers to one
            // session take turns, so sequences stay dense and unique.
            let next = {
                let mut conn = uow.postgres_connection().await?;
                sql_query(
                    "SELECT pg_advisory_xact_lock(hashtext('beam:transcript'), hashtext($1))",
                )
                .bind::<Text, _>(session_id.to_string())
                .execute(&mut *conn)
                .await?;
                next_sequence(&mut conn, session_id).await?
            };
            let owner = write::session_owner(&uow, session_id).await?;
            for (offset, mut row) in (0_i32..).zip(rows) {
                row.sequence = next.saturating_add(offset);
                let org = opt_uuid(row.org_id.as_deref());
                let data = owned(message_json(&row), owner, org, Write::Insert);
                write::put(self.db.datastore(), &uow, "agent_message", row.id, data).await?;
            }
            Ok(())
        }
        .await;
        write::finish(&uow, result)
            .await
            .map_err(|e| crate::BEM_122.with_data(e.to_string()))
    }

    async fn next_sequence(&self, session_id: Uuid) -> StoreResult<i32> {
        self.db
            .scoped(|conn| {
                async move {
                    next_sequence(conn, session_id)
                        .await
                        .map_err(|e| crate::BEM_123.with_data(e.to_string()))
                }
                .scope_boxed()
            })
            .await
    }
    async fn create_action(&self, a: ActionRow) -> StoreResult<()> {
        let data = json!({
            "session_id": a.session_id,
            "execution_id": a.execution_id,
            "interaction_id": a.interaction_id,
            "run_id": a.run_id,
            "step_id": a.step_id,
            "message_id": a.message_id,
            "agent_id": a.agent_id,
            "type": a.action_type.db_label(),
            "status": a.status.db_label(),
            "lc_action_id": a.lc_action_id,
            "payload": a.payload,
            "result": a.result,
            "resolved_at": a.resolved_at,
            "tool_name": a.tool_name,
            "expires_at": a.expires_at,
            "sweep_after": a.sweep_after,
        });
        let org = opt_uuid(a.org_id.as_deref());
        self.put_owned("agent_action", a.id, a.session_id, org, data, Write::Insert)
            .await
            .map_err(|e| crate::BEM_124.with_data(e.to_string()))
    }

    async fn get_action(&self, action_id: Uuid) -> StoreResult<ActionRow> {
        use crate::entity::agent_action::agent_action as t;
        self.db
            .scoped(|conn| {
                async move {
                    let row: Option<ActionDbRow> = t::table
                        .filter(t::_id.eq(action_id))
                        .filter(t::_deleted.is_null())
                        .select(ActionDbRow::as_select())
                        .first(conn)
                        .await
                        .optional()
                        .map_err(|e| crate::BEM_125.with_data(e.to_string()))?;
                    row.map(ActionRow::try_from)
                        .transpose()?
                        .ok_or_else(|| crate::BEM_027.with_data(action_id.to_string()))
                }
                .scope_boxed()
            })
            .await
    }
    async fn expire_pending_actions(&self, execution_id: Uuid) -> StoreResult<u64> {
        self.db
            .scoped(|conn| {
                async move {
                    write::expire_pending_actions(conn, execution_id)
                        .await
                        .map_err(|e| crate::BEM_126.with_data(e.to_string()))
                }
                .scope_boxed()
            })
            .await
    }
    async fn resolve_action(&self, action_id: Uuid, result: Value) -> StoreResult<()> {
        let uow = self.db.unit_of_work().await?;
        let written = async {
            if !write::exists(&uow, write::Table::AgentAction, action_id).await? {
                return Ok(false);
            }
            let data = update_only(json!({
                "status": ActionStatus::Resolved.db_label(),
                "result": result,
                "resolved_at": Utc::now(),
            }));
            write::put(self.db.datastore(), &uow, "agent_action", action_id, data).await?;
            Ok(true)
        }
        .await;
        let found = write::finish(&uow, written)
            .await
            .map_err(|e| crate::BEM_126.with_data(e.to_string()))?;
        if !found {
            return Err(crate::BEM_028.with_data(action_id.to_string()));
        }
        Ok(())
    }

    async fn claim_due_client_tool_actions(
        &self,
        now: DateTime<Utc>,
        until: DateTime<Utc>,
        limit: i64,
    ) -> StoreResult<Vec<ActionRow>> {
        let s = self.db.schema();
        self.db
            .scoped(|conn| {
                async move {
                    // One statement: rows another sweep holds are skipped, not waited on.
                    let mut rows: Vec<ActionRow> = collect::<_, ActionRow>(
                        sql_query(format!(
                            "UPDATE {s}.agent_action SET sweep_after = $5, {TOUCH} WHERE _id IN ( \
                         SELECT _id FROM {s}.agent_action \
                         WHERE type = {kind} AND status = {pending} AND expires_at <= $1 \
                         AND (sweep_after IS NULL OR sweep_after <= $1) AND _deleted IS NULL \
                         ORDER BY expires_at LIMIT $4 FOR UPDATE SKIP LOCKED) \
                         RETURNING {ACTION_COLUMNS}",
                            kind = self.cast::<ActionType>(2),
                            pending = self.cast::<ActionStatus>(3),
                        ))
                        .bind::<Timestamptz, _>(now)
                        .bind::<Text, _>(ActionType::ClientTool.db_label())
                        .bind::<Text, _>(ActionStatus::Pending.db_label())
                        .bind::<BigInt, _>(limit)
                        .bind::<Timestamptz, _>(until)
                        .load::<ActionDbRow>(conn)
                        .await
                        .map_err(|e| crate::BEM_126.with_data(e.to_string()))?,
                    )?;
                    rows.sort_by_key(|a| a.expires_at);
                    Ok(rows)
                }
                .scope_boxed()
            })
            .await
    }

    async fn defer_client_tool_actions(
        &self,
        action_ids: &[Uuid],
        until: DateTime<Utc>,
    ) -> StoreResult<()> {
        use crate::entity::agent_action::agent_action as t;
        if action_ids.is_empty() {
            return Ok(());
        }
        self.db
            .scoped(|conn| {
                async move {
                    diesel::update(
                        t::table
                            .filter(t::_id.eq_any(action_ids))
                            .filter(t::_deleted.is_null()),
                    )
                    .set((
                        t::sweep_after.eq(until),
                        t::_updated.eq(now()),
                        t::_etag.eq(new_etag()),
                    ))
                    .execute(conn)
                    .await
                    .map_err(|e| crate::BEM_126.with_data(e.to_string()))?;
                    Ok(())
                }
                .scope_boxed()
            })
            .await
    }
    async fn list_pending_actions(&self, interaction_id: Uuid) -> StoreResult<Vec<ActionRow>> {
        use crate::entity::agent_action::agent_action as t;
        let pending = action_status(ActionStatus::Pending)?;
        self.db
            .scoped(|conn| {
                async move {
                    let rows: Vec<ActionDbRow> = t::table
                        .filter(t::interaction_id.eq(interaction_id))
                        .filter(t::status.eq(pending))
                        .filter(t::_deleted.is_null())
                        .select(ActionDbRow::as_select())
                        .load(conn)
                        .await
                        .map_err(|e| crate::BEM_125.with_data(e.to_string()))?;
                    collect(rows)
                }
                .scope_boxed()
            })
            .await
    }
    async fn get_session(&self, session_id: Uuid) -> StoreResult<Option<SessionRow>> {
        use crate::entity::agent_session::agent_session as t;
        self.db
            .scoped(|conn| {
                async move {
                    let row: Option<SessionDbRow> = t::table
                        .filter(t::_id.eq(session_id))
                        .filter(t::_deleted.is_null())
                        .select(SessionDbRow::as_select())
                        .first(conn)
                        .await
                        .optional()
                        .map_err(|e| crate::BEM_127.with_data(e.to_string()))?;
                    Ok(row.map(SessionRow::from))
                }
                .scope_boxed()
            })
            .await
    }
    async fn get_interaction(&self, interaction_id: Uuid) -> StoreResult<Option<InteractionRow>> {
        use crate::entity::agent_interaction::agent_interaction as t;
        self.db
            .scoped(|conn| {
                async move {
                    let row: Option<InteractionDbRow> = t::table
                        .filter(t::_id.eq(interaction_id))
                        .filter(t::_deleted.is_null())
                        .select(InteractionDbRow::as_select())
                        .first(conn)
                        .await
                        .optional()
                        .map_err(|e| crate::BEM_128.with_data(e.to_string()))?;
                    row.map(InteractionRow::try_from).transpose()
                }
                .scope_boxed()
            })
            .await
    }
    async fn get_run(&self, run_id: Uuid) -> StoreResult<Option<RunRow>> {
        use crate::entity::agent_run::agent_run as t;
        self.db
            .scoped(|conn| {
                async move {
                    let row: Option<RunDbRow> = t::table
                        .filter(t::_id.eq(run_id))
                        .filter(t::_deleted.is_null())
                        .select(RunDbRow::as_select())
                        .first(conn)
                        .await
                        .optional()
                        .map_err(|e| crate::BEM_129.with_data(e.to_string()))?;
                    row.map(RunRow::try_from).transpose()
                }
                .scope_boxed()
            })
            .await
    }
    async fn list_steps(&self, run_id: Uuid) -> StoreResult<Vec<StepRow>> {
        use crate::entity::agent_step::agent_step as t;
        self.db
            .scoped(|conn| {
                async move {
                    let rows: Vec<StepDbRow> = t::table
                        .filter(t::run_id.eq(run_id))
                        .filter(t::_deleted.is_null())
                        .order((t::_created.asc(), t::_id.asc()))
                        .select(StepDbRow::as_select())
                        .load(conn)
                        .await
                        .map_err(|e| crate::BEM_130.with_data(e.to_string()))?;
                    collect(rows)
                }
                .scope_boxed()
            })
            .await
    }
    async fn list_messages(&self, session_id: Uuid) -> StoreResult<Vec<MessageRow>> {
        use crate::entity::agent_message::agent_message as t;
        self.db
            .scoped(|conn| {
                async move {
                    let rows: Vec<MessageDbRow> = t::table
                        .filter(t::session_id.eq(session_id))
                        .filter(t::_deleted.is_null())
                        .order(t::sequence.asc())
                        .select(MessageDbRow::as_select())
                        .load(conn)
                        .await
                        .map_err(|e| crate::BEM_130.with_data(e.to_string()))?;
                    collect(rows)
                }
                .scope_boxed()
            })
            .await
    }
    async fn list_messages_page(
        &self,
        session_id: Uuid,
        page: MessagePage,
    ) -> StoreResult<Vec<MessageRow>> {
        use crate::entity::agent_message::agent_message as t;
        // `sequence` is an int4; a bound past its range clamps to it.
        let seq = |v: i64| i32::try_from(v).unwrap_or(if v < 0 { i32::MIN } else { i32::MAX });
        let mut query = t::table
            .filter(t::session_id.eq(session_id))
            .filter(t::_deleted.is_null())
            .into_boxed();
        if let Some(after) = page.after {
            query = query.filter(t::sequence.gt(seq(after)));
        }
        if let Some(before) = page.before {
            query = query.filter(t::sequence.lt(seq(before)));
        }
        if let Some(through) = page.through {
            query = query.filter(t::sequence.le(seq(through)));
        }
        // The latest `limit` rows, then in ascending order.
        query = query.order(t::sequence.desc());
        if let Some(limit) = page.limit.filter(|l| *l > 0) {
            query = query.limit(i64::try_from(limit).unwrap_or(i64::MAX));
        }
        self.db
            .scoped(|conn| {
                async move {
                    let mut rows: Vec<MessageDbRow> = query
                        .select(MessageDbRow::as_select())
                        .load(conn)
                        .await
                        .map_err(|e| crate::BEM_130.with_data(e.to_string()))?;
                    rows.reverse();
                    collect(rows)
                }
                .scope_boxed()
            })
            .await
    }
    async fn count_messages(&self, session_id: Uuid) -> StoreResult<usize> {
        use crate::entity::agent_message::agent_message as t;
        self.db
            .scoped(|conn| {
                async move {
                    let n: i64 = t::table
                        .filter(t::session_id.eq(session_id))
                        .filter(t::_deleted.is_null())
                        .count()
                        .get_result(conn)
                        .await
                        .map_err(|e| crate::BEM_130.with_data(e.to_string()))?;
                    Ok(usize::try_from(n).unwrap_or(0))
                }
                .scope_boxed()
            })
            .await
    }
    async fn delete_messages(&self, session_id: Uuid) -> StoreResult<u64> {
        use crate::entity::agent_message::agent_message as t;
        self.db
            .scoped(|conn| {
                async move {
                    let n = diesel::delete(t::table.filter(t::session_id.eq(session_id)))
                        .execute(conn)
                        .await
                        .map_err(|e| crate::BEM_133.with_data(e.to_string()))?;
                    Ok(n as u64)
                }
                .scope_boxed()
            })
            .await
    }
    async fn put_manifest(&self, m: &ManifestRow) -> StoreResult<()> {
        use crate::entity::agent_context_manifest::{
            agent_context_manifest as t, upsert_agent_context_manifest,
        };
        self.db
            .scoped(|conn| {
                async move {
                    // Python: update the row with this operation id, else insert one.
                    let stored: Option<Uuid> = t::table
                        .filter(t::operation_id.eq(&m.operation_id))
                        .filter(t::_deleted.is_null())
                        .order(t::_created.asc())
                        .select(t::_id)
                        .first(conn)
                        .await
                        .optional()
                        .map_err(|e| crate::BEM_135.with_data(e.to_string()))?;
                    let (id, data) = if let Some(id) = stored {
                        (id, update_only(manifest_json(m)))
                    } else {
                        // A new manifest carries its execution's owner, which the
                        // query resource scopes by.
                        let mut data = manifest_json(m);
                        if let Some(execution) = m.execution_id {
                            use crate::entity::agent_execution::agent_execution as e;
                            let owner: Option<(Option<Uuid>, Option<Uuid>)> = e::table
                                .filter(e::_id.eq(execution))
                                .select((e::user_id, e::org_id))
                                .first(conn)
                                .await
                                .optional()
                                .map_err(|err| crate::BEM_135.with_data(err.to_string()))?;
                            if let (Some((user, org)), Some(object)) = (owner, data.as_object_mut())
                            {
                                object.insert("user_id".into(), json!(user));
                                object.insert("org_id".into(), json!(org));
                            }
                        }
                        (Uuid::new_v4(), insert_only(data))
                    };
                    upsert_agent_context_manifest(conn, id, &data)
                        .await
                        .map_err(|e| crate::BEM_135.with_data(e.to_string()))
                }
                .scope_boxed()
            })
            .await
    }
    async fn get_manifest(&self, operation_id: &str) -> StoreResult<Option<ManifestRow>> {
        use crate::entity::agent_context_manifest::agent_context_manifest as t;
        self.db
            .scoped(|conn| {
                async move {
                    let row: Option<ManifestDbRow> = t::table
                        .filter(t::operation_id.eq(operation_id))
                        .filter(t::_deleted.is_null())
                        .order(t::_created.asc())
                        .select(ManifestDbRow::as_select())
                        .first(conn)
                        .await
                        .optional()
                        .map_err(|e| crate::BEM_136.with_data(e.to_string()))?;
                    Ok(row.map(ManifestRow::from))
                }
                .scope_boxed()
            })
            .await
    }
    async fn list_manifests(&self, execution_id: Uuid) -> StoreResult<Vec<ManifestRow>> {
        use crate::entity::agent_context_manifest::agent_context_manifest as t;
        self.db
            .scoped(|conn| {
                async move {
                    let rows: Vec<ManifestDbRow> = t::table
                        .filter(t::execution_id.eq(execution_id))
                        .filter(t::_deleted.is_null())
                        .order((t::_created.asc(), t::_id.asc()))
                        .select(ManifestDbRow::as_select())
                        .load(conn)
                        .await
                        .map_err(|e| crate::BEM_136.with_data(e.to_string()))?;
                    Ok(rows.into_iter().map(ManifestRow::from).collect())
                }
                .scope_boxed()
            })
            .await
    }
    async fn list_user_sessions(&self, user_id: &str) -> StoreResult<Vec<Uuid>> {
        use crate::entity::agent_session::agent_session as t;
        // `user_id` is a uuid column; anything else matches no session.
        let Some(user) = opt_uuid(Some(user_id.trim())) else {
            return Ok(Vec::new());
        };
        self.db
            .scoped(|conn| {
                async move {
                    t::table
                        .filter(t::user_id.eq(user))
                        .order(t::last_active_at.desc().nulls_last())
                        .select(t::_id)
                        .load(conn)
                        .await
                        .map_err(|e| crate::BEM_134.with_data(e.to_string()))
                }
                .scope_boxed()
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures;
    use crate::rows::StepStatus;
    use crate::store::CatalogStore;

    /// A store on a migrated, test-only schema; `None` when no database is configured.
    ///
    /// Set `BEAM_DATABASE_URL` (or `DATABASE_URL`), e.g. the parity Postgres from
    /// `docker-compose.parity.yml`: `postgres://postgres@127.0.0.1:55432/postgres`.
    /// Each test opens its own pool: every `#[tokio::test]` has its own runtime,
    /// and pooled connections die with the runtime that opened them. Concurrent
    /// `migrate` calls serialize on the advisory lock and skip applied versions.
    async fn store() -> Option<PgBeamStore> {
        crate::testdb::store("beam_store_unit").await
    }

    #[tokio::test]
    async fn migrated_store_reports_version() {
        let Some(store) = store().await else { return };
        store.ensure_migrated().await.unwrap();
    }

    /// A typed bulk update: `ne_all` over the names and an enum
    /// value through its cast; the agents it keeps are untouched.
    #[tokio::test]
    async fn deactivate_keeps_named_agents() {
        // A database of its own: the deactivation touches every agent, and
        // an agent another test checks in between the read below and the
        // update would be deactivated too.
        let Some(store) = crate::testdb::store("beam_store_deactivate").await else {
            return;
        };
        let (keep, drop) = (fixtures::Ids::new(), fixtures::Ids::new());
        store
            .checkin_agents(&[fixtures::agent(&keep), fixtures::agent(&drop)])
            .await
            .unwrap();
        let status = |name: String| {
            let store = store.clone();
            async move {
                use crate::entity::agent::agent as t;
                let mut conn = store.pool().get().await.unwrap();
                t::table
                    .filter(t::name.eq(name))
                    .select(t::status)
                    .first::<String>(&mut conn)
                    .await
                    .unwrap()
            }
        };
        // Keep every other active agent (earlier runs' rows in this database),
        // so only `drop` changes.
        let active: Vec<String> = {
            use crate::entity::agent::agent as t;
            let mut conn = store.pool().get().await.unwrap();
            t::table
                .filter(
                    t::status.eq(enum_sql::<Text>(
                        Some("ACTIVE"),
                        "status",
                        "agentstatus",
                        crate::entity::agent::AGENTSTATUS,
                    )
                    .unwrap()),
                )
                .select(t::name)
                .load(&mut conn)
                .await
                .unwrap()
        };
        let others: Vec<String> = active
            .into_iter()
            .filter(|n| *n != drop.agent_name)
            .collect();
        let n = store.deactivate_agents_except(&others).await.unwrap();
        assert_eq!(n, 1);
        assert_eq!(status(keep.agent_name.clone()).await, "ACTIVE");
        assert_eq!(status(drop.agent_name.clone()).await, "INACTIVE");
    }

    #[tokio::test]
    async fn manifest_upsert_get_and_list() {
        let Some(store) = store().await else { return };
        let execution = Uuid::new_v4();
        let operation = Uuid::new_v4().to_string();
        let mut row = ManifestRow {
            operation_id: operation.clone(),
            execution_id: Some(execution),
            operation_kind: "invoke".into(),
            revision_id: None,
            policy_revision: Some("p1".into()),
            serializer_revision: Some("beam-envelope-v1".into()),
            tokenizer_revision: Some("estimated_conservative".into()),
            input_tokens: 10,
            output_tokens: 4,
            reserve_tokens: 2,
            actual_prompt_tokens: None,
            estimate_drift: None,
            // Python's `ManifestDecision` and `inspect_manifest` shapes, so the
            // cross-read script can load the row.
            decisions: serde_json::json!([{
                "item_id": "current:x", "source": "current", "action": "selected",
                "reason": "required", "tokens": 3, "content_hash": "0123456789abcdef",
                "trust": "user",
            }]),
            view: serde_json::json!({"schema_version": "1", "input_tokens": 10, "output_tokens": 4}),
        };
        store.put_manifest(&row).await.unwrap();
        row.actual_prompt_tokens = Some(12);
        row.estimate_drift = Some(0.2);
        store.put_manifest(&row).await.unwrap();
        assert_eq!(
            store.get_manifest(&operation).await.unwrap(),
            Some(row.clone())
        );
        assert_eq!(store.list_manifests(execution).await.unwrap(), vec![row]);
        assert!(store.get_manifest("missing").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn message_pages_counts_and_deletes() {
        let Some(store) = store().await else { return };
        let ids = fixtures::Ids::new();
        store
            .checkin_agents(&[fixtures::agent(&ids)])
            .await
            .unwrap();
        store
            .ensure_session(&fixtures::session(&ids))
            .await
            .unwrap();
        store.update_session_activity(ids.session).await.unwrap();
        store
            .create_interaction(&fixtures::interaction(&ids))
            .await
            .unwrap();
        store.create_run(&fixtures::run(&ids)).await.unwrap();
        for sequence in 0..5 {
            store
                .save_message(fixtures::message(&ids, sequence))
                .await
                .unwrap();
        }
        let seqs = |rows: Vec<MessageRow>| rows.into_iter().map(|r| r.sequence).collect::<Vec<_>>();
        let page = |limit, after, before, through| MessagePage {
            limit,
            before,
            after,
            through,
        };
        let all = store
            .list_messages_page(ids.session, MessagePage::default())
            .await
            .unwrap();
        assert_eq!(seqs(all), vec![0, 1, 2, 3, 4]);
        let latest = store
            .list_messages_page(ids.session, page(Some(2), None, None, None))
            .await
            .unwrap();
        assert_eq!(seqs(latest), vec![3, 4]);
        let window = store
            .list_messages_page(ids.session, page(Some(2), Some(0), Some(4), Some(2)))
            .await
            .unwrap();
        assert_eq!(seqs(window), vec![1, 2]);
        assert_eq!(store.count_messages(ids.session).await.unwrap(), 5);
        let sessions = store
            .list_user_sessions(&ids.user.to_string())
            .await
            .unwrap();
        assert_eq!(sessions, vec![ids.session]);
        assert!(store
            .list_user_sessions("not-a-uuid")
            .await
            .unwrap()
            .is_empty());
        assert_eq!(store.delete_messages(ids.session).await.unwrap(), 5);
        assert_eq!(store.count_messages(ids.session).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn execution_hierarchy_roundtrip() {
        let Some(store) = store().await else {
            eprintln!("skipping: no BEAM_DATABASE_URL/DATABASE_URL set");
            return;
        };
        let ids = fixtures::Ids::new();
        store
            .checkin_agents(&[fixtures::agent(&ids)])
            .await
            .unwrap();
        store
            .checkin_agents(&[fixtures::agent(&ids)])
            .await
            .unwrap(); // upsert
        store.checkin_models(&[fixtures::model()]).await.unwrap();

        let session = fixtures::session(&ids);
        store.ensure_session(&session).await.unwrap();
        store.ensure_session(&session).await.unwrap(); // idempotent
        store.update_session_activity(ids.session).await.unwrap();
        let fetched = store.get_session(ids.session).await.unwrap().unwrap();
        assert_eq!(fetched.user_id, session.user_id);
        assert_eq!(fetched.org_id, session.org_id);
        assert!(fetched.last_active_at.is_some());

        store
            .create_interaction(&fixtures::interaction(&ids))
            .await
            .unwrap();
        store.create_run(&fixtures::run(&ids)).await.unwrap();
        let mut step = fixtures::step(&ids);
        store.save_step(&step).await.unwrap();
        step.status = StepStatus::Completed;
        step.result = Some(serde_json::json!({"ok": true}));
        store.save_step(&step).await.unwrap();
        assert_eq!(store.list_steps(ids.run).await.unwrap(), vec![step]);

        assert_eq!(store.next_sequence(ids.session).await.unwrap(), 0);
        let message = fixtures::message(&ids, 0);
        store.save_message(message.clone()).await.unwrap();
        assert_eq!(store.next_sequence(ids.session).await.unwrap(), 1);
        let messages = store.list_messages(ids.session).await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].role, message.role);
        assert_eq!(messages[0].phase, message.phase);
        assert_eq!(messages[0].content, message.content);

        store
            .complete_run(ids.run, RunStatus::Completed)
            .await
            .unwrap();
        store
            .complete_interaction(ids.interaction, InteractionStatus::Completed)
            .await
            .unwrap();
        assert_eq!(
            store.get_run(ids.run).await.unwrap().unwrap().status,
            RunStatus::Completed
        );
        assert_eq!(
            store
                .get_interaction(ids.interaction)
                .await
                .unwrap()
                .unwrap()
                .status,
            InteractionStatus::Completed
        );
    }

    /// Reads the execution tree `scripts/cross_read.py --write` inserted through
    /// Python's ORM into `ref_beam`. Runs only when `BEAM_CROSS_DATABASE_URL`
    /// names that database.
    #[tokio::test]
    async fn python_written_rows_are_readable() {
        let Ok(url) = std::env::var("BEAM_CROSS_DATABASE_URL") else {
            return;
        };
        // The typed reads name tables without a schema. Python's is `ref_beam`.
        let schema = crate::testdb::schema();
        let pool = riverbase_core::datastore::establish_dbpool(&schema.pool_url(&url))
            .await
            .expect("connect");
        let store = PgBeamStore::new(pool, schema);
        let id =
            |n: u8| Uuid::parse_str(&format!("c0000000-0000-4000-8000-00000000000{n}")).unwrap();
        let session = store.get_session(id(1)).await.unwrap().expect("session");
        assert_eq!(
            session.org_id.as_deref(),
            Some("c0000000-0000-4000-8000-000000000007")
        );
        assert_eq!(
            store
                .get_interaction(id(2))
                .await
                .unwrap()
                .expect("interaction")
                .status,
            InteractionStatus::InProgress
        );
        assert_eq!(
            store.get_run(id(3)).await.unwrap().expect("run").status,
            RunStatus::Interrupted
        );
        let steps = store.list_steps(id(3)).await.unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].status, StepStatus::Completed);
        let messages = store.list_messages(id(1)).await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(
            (messages[0].role, messages[0].phase),
            (MessageRole::Human, MessagePhase::Final)
        );
        let action = store.get_action(id(6)).await.unwrap();
        assert_eq!(
            (action.action_type, action.status),
            (ActionType::Choice, ActionStatus::Pending)
        );
        assert_eq!(store.next_sequence(id(1)).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn due_client_tool_actions_follow_expiry_and_sweep_after() {
        let Some(store) = store().await else { return };
        fixtures::check_due_client_tools(&store).await;
    }

    #[tokio::test]
    async fn action_lifecycle() {
        let Some(store) = store().await else { return };
        let ids = fixtures::Ids::new();
        store
            .checkin_agents(&[fixtures::agent(&ids)])
            .await
            .unwrap();
        store
            .ensure_session(&fixtures::session(&ids))
            .await
            .unwrap();
        store
            .create_interaction(&fixtures::interaction(&ids))
            .await
            .unwrap();
        store.create_run(&fixtures::run(&ids)).await.unwrap();
        let action = fixtures::action(&ids);
        store.create_action(action.clone()).await.unwrap();
        let fetched = store.get_action(action.id).await.unwrap();
        assert_eq!(fetched.status, ActionStatus::Pending);
        assert_eq!(fetched.action_type, ActionType::Confirmation);
        store
            .resolve_action(action.id, serde_json::json!({"approved": true}))
            .await
            .unwrap();
        let resolved = store.get_action(action.id).await.unwrap();
        assert_eq!(resolved.status, ActionStatus::Resolved);
        assert_eq!(resolved.result, Some(serde_json::json!({"approved": true})));
        assert!(resolved.resolved_at.is_some());
        let missing = store
            .resolve_action(Uuid::new_v4(), Value::Null)
            .await
            .unwrap_err();
        assert_eq!(missing.errcode.as_str(), "BEM-028");
        let unknown = store.get_action(Uuid::new_v4()).await.unwrap_err();
        assert_eq!(unknown.errcode.as_str(), "BEM-027");
    }
}
