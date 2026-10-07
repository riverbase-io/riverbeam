//! Postgres-backed [`BeamStore`] on top of `riverbase_core`'s async Diesel pool.
//!
//! This is the production persistence backend (the in-memory store is retained
//! only as a hermetic unit-test fixture for the invoke pipeline). It targets the
//! `ref_beam` schema from [`migrations/0001_beam_schema.sql`](../../../migrations/0001_beam_schema.sql),
//! reusing [`riverbase_core::datastore::establish_dbpool`] so beam shares the same
//! connection pool, migration, and config conventions as every other riverbase
//! domain.
//!
//! [`PgBeamStore::ensure_schema`] bootstraps the **non-vector** execution tables
//! (`model`, `agent`, `agent_session`, `agent_interaction`, `agent_run`,
//! `agent_message`, `agent_action`) so the store is usable against a stock
//! Postgres without the `pgvector` extension. The canonical full schema
//! (knowledge/memory pgvector tables, full foreign keys) remains
//! `migrations/0001_beam_schema.sql`, applied at application startup.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use diesel::sql_query;
use diesel::sql_types::{Integer, Jsonb, Nullable, Text, Timestamptz, Uuid as SqlUuid};
use diesel::QueryableByName;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde_json::Value;
use uuid::Uuid;

use riverbase_core::datastore::{establish_dbpool, PgPool};

use crate::rows::{
    ActionRow, ActionStatus, AgentRow, InteractionRow, InteractionStatus, MessageRow, ModelRow,
    RunRow, RunStatus, SessionRow,
};
use crate::store::{BeamStore, StoreResult};

/// Bootstrap DDL for the execution tables this store reads/writes.
///
/// Foreign keys are intentionally omitted here (kept in the canonical migration)
/// so the store tolerates the pipeline's optional-interaction flows without
/// requiring a `pgvector`-enabled database.
const BEAM_CORE_DDL: &str = r"
CREATE SCHEMA IF NOT EXISTS ref_beam;

CREATE TABLE IF NOT EXISTS ref_beam.model (
    _id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    provider TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    metadata_json JSONB NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS ref_beam.agent (
    _id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    metadata_json JSONB NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS ref_beam.agent_session (
    _id UUID PRIMARY KEY,
    agent_id UUID NOT NULL,
    user_id UUID,
    profile_id UUID,
    name TEXT,
    last_active_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS ref_beam.agent_interaction (
    _id UUID PRIMARY KEY,
    session_id UUID NOT NULL,
    agent_id UUID NOT NULL,
    agent_name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'in_progress',
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS ref_beam.agent_run (
    _id UUID PRIMARY KEY,
    session_id UUID NOT NULL,
    interaction_id UUID NOT NULL,
    agent_id UUID NOT NULL,
    agent_name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS ref_beam.agent_message (
    _id UUID PRIMARY KEY,
    sequence INTEGER NOT NULL,
    role TEXT NOT NULL,
    type TEXT NOT NULL,
    content JSONB NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    session_id UUID NOT NULL,
    interaction_id UUID,
    run_id UUID,
    agent_id UUID NOT NULL,
    request_id UUID
);
CREATE INDEX IF NOT EXISTS ix_agent_message_session ON ref_beam.agent_message(session_id);

CREATE TABLE IF NOT EXISTS ref_beam.agent_action (
    _id UUID PRIMARY KEY,
    session_id UUID NOT NULL,
    interaction_id UUID NOT NULL,
    run_id UUID NOT NULL,
    agent_id UUID NOT NULL,
    type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    payload JSONB NOT NULL DEFAULT '{}',
    result JSONB,
    resolved_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS ix_agent_action_run ON ref_beam.agent_action(run_id);
";

/// Postgres-backed persistence façade for the beam execution hierarchy.
#[derive(Clone)]
pub struct PgBeamStore {
    pool: PgPool,
}

impl PgBeamStore {
    /// Connect to Postgres (running riverbase framework migrations) and bootstrap
    /// the beam execution tables.
    ///
    /// # Errors
    /// Returns a store error if the pool cannot be established or the
    /// bootstrap DDL fails.
    pub async fn connect(database_url: &str) -> StoreResult<Self> {
        let pool = establish_dbpool(database_url)
            .await
            .map_err(|e| crate::BEM_110.with_data(e.to_string()))?;
        let store = Self::new(pool);
        store.ensure_schema().await?;
        Ok(store)
    }

    /// Build a store over an existing riverbase [`PgPool`] (does not run DDL).
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Expose the underlying pool for callers that share it (e.g. knowledge/memory).
    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Create the non-vector execution tables if they do not yet exist.
    ///
    /// # Errors
    /// Returns a store error if the DDL cannot be applied.
    pub async fn ensure_schema(&self) -> StoreResult<()> {
        use diesel_async::SimpleAsyncConnection;
        let mut guard = self.pool.get().await.map_err(|e| crate::BEM_111.with_data(e.to_string()))?;
        let conn: &mut AsyncPgConnection = &mut guard;
        conn.batch_execute(BEAM_CORE_DDL)
            .await
            .map_err(|e| crate::BEM_112.with_data(e.to_string()))?;
        Ok(())
    }

    async fn conn(
        &self,
    ) -> StoreResult<impl std::ops::DerefMut<Target = AsyncPgConnection> + Send + '_> {
        self.pool.get().await.map_err(|e| crate::BEM_113.with_data(e.to_string()))
    }
}

/// Parse an optional caller id into a `UUID`. Non-UUID values are dropped, since
/// the `ref_beam` schema (matching the Python parity target) types `user_id` /
/// `profile_id` as `UUID`.
fn opt_uuid(value: Option<&str>) -> Option<Uuid> {
    value.and_then(|v| Uuid::parse_str(v).ok())
}

fn interaction_status_from_db(value: &str) -> InteractionStatus {
    match value {
        "completed" => InteractionStatus::Completed,
        "failed" => InteractionStatus::Failed,
        _ => InteractionStatus::InProgress,
    }
}

fn run_status_from_db(value: &str) -> RunStatus {
    match value {
        "completed" => RunStatus::Completed,
        "failed" => RunStatus::Failed,
        "interrupted" => RunStatus::Interrupted,
        _ => RunStatus::Running,
    }
}

fn action_status_from_db(value: &str) -> ActionStatus {
    match value {
        "resolved" => ActionStatus::Resolved,
        "cancelled" => ActionStatus::Cancelled,
        _ => ActionStatus::Pending,
    }
}

#[derive(QueryableByName)]
struct SeqRow {
    #[diesel(sql_type = Integer, column_name = seq)]
    seq: i32,
}

#[derive(QueryableByName)]
struct SessionDbRow {
    #[diesel(sql_type = SqlUuid, column_name = _id)]
    id: Uuid,
    #[diesel(sql_type = SqlUuid)]
    agent_id: Uuid,
    #[diesel(sql_type = Nullable<SqlUuid>)]
    user_id: Option<Uuid>,
    #[diesel(sql_type = Nullable<SqlUuid>)]
    profile_id: Option<Uuid>,
    #[diesel(sql_type = Nullable<Text>)]
    name: Option<String>,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    last_active_at: Option<DateTime<Utc>>,
}

#[derive(QueryableByName)]
struct InteractionDbRow {
    #[diesel(sql_type = SqlUuid, column_name = _id)]
    id: Uuid,
    #[diesel(sql_type = SqlUuid)]
    session_id: Uuid,
    #[diesel(sql_type = SqlUuid)]
    agent_id: Uuid,
    #[diesel(sql_type = Text)]
    agent_name: String,
    #[diesel(sql_type = Text)]
    status: String,
    #[diesel(sql_type = Timestamptz)]
    started_at: DateTime<Utc>,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    ended_at: Option<DateTime<Utc>>,
}

#[derive(QueryableByName)]
struct RunDbRow {
    #[diesel(sql_type = SqlUuid, column_name = _id)]
    id: Uuid,
    #[diesel(sql_type = SqlUuid)]
    session_id: Uuid,
    #[diesel(sql_type = SqlUuid)]
    interaction_id: Uuid,
    #[diesel(sql_type = SqlUuid)]
    agent_id: Uuid,
    #[diesel(sql_type = Text)]
    agent_name: String,
    #[diesel(sql_type = Text)]
    status: String,
    #[diesel(sql_type = Timestamptz)]
    started_at: DateTime<Utc>,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    ended_at: Option<DateTime<Utc>>,
}

#[derive(QueryableByName)]
struct MessageDbRow {
    #[diesel(sql_type = SqlUuid, column_name = _id)]
    id: Uuid,
    #[diesel(sql_type = Integer)]
    sequence: i32,
    #[diesel(sql_type = SqlUuid)]
    session_id: Uuid,
    #[diesel(sql_type = Nullable<SqlUuid>)]
    interaction_id: Option<Uuid>,
    #[diesel(sql_type = Nullable<SqlUuid>)]
    run_id: Option<Uuid>,
    #[diesel(sql_type = SqlUuid)]
    agent_id: Uuid,
    #[diesel(sql_type = Text)]
    role: String,
    #[diesel(sql_type = Text, column_name = type_)]
    msg_type: String,
    #[diesel(sql_type = Jsonb)]
    content: Value,
    #[diesel(sql_type = Nullable<SqlUuid>)]
    request_id: Option<Uuid>,
    #[diesel(sql_type = Timestamptz)]
    timestamp: DateTime<Utc>,
}

#[derive(QueryableByName)]
struct ActionDbRow {
    #[diesel(sql_type = SqlUuid, column_name = _id)]
    id: Uuid,
    #[diesel(sql_type = SqlUuid)]
    session_id: Uuid,
    #[diesel(sql_type = SqlUuid)]
    interaction_id: Uuid,
    #[diesel(sql_type = SqlUuid)]
    run_id: Uuid,
    #[diesel(sql_type = SqlUuid)]
    agent_id: Uuid,
    #[diesel(sql_type = Text, column_name = type_)]
    action_type: String,
    #[diesel(sql_type = Text)]
    status: String,
    #[diesel(sql_type = Jsonb)]
    payload: Value,
    #[diesel(sql_type = Nullable<Jsonb>)]
    result: Option<Value>,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    resolved_at: Option<DateTime<Utc>>,
}

impl From<SessionDbRow> for SessionRow {
    fn from(r: SessionDbRow) -> Self {
        Self {
            id: r.id,
            agent_id: r.agent_id,
            user_id: r.user_id.map(|u| u.to_string()),
            profile_id: r.profile_id.map(|u| u.to_string()),
            name: r.name,
            last_active_at: r.last_active_at,
        }
    }
}

impl From<InteractionDbRow> for InteractionRow {
    fn from(r: InteractionDbRow) -> Self {
        Self {
            id: r.id,
            session_id: r.session_id,
            agent_id: r.agent_id,
            agent_name: r.agent_name,
            status: interaction_status_from_db(&r.status),
            started_at: r.started_at,
            ended_at: r.ended_at,
        }
    }
}

impl From<RunDbRow> for RunRow {
    fn from(r: RunDbRow) -> Self {
        Self {
            id: r.id,
            session_id: r.session_id,
            interaction_id: r.interaction_id,
            agent_id: r.agent_id,
            agent_name: r.agent_name,
            status: run_status_from_db(&r.status),
            started_at: r.started_at,
            ended_at: r.ended_at,
        }
    }
}

impl From<MessageDbRow> for MessageRow {
    fn from(r: MessageDbRow) -> Self {
        Self {
            id: r.id,
            sequence: r.sequence,
            session_id: r.session_id,
            interaction_id: r.interaction_id,
            run_id: r.run_id,
            agent_id: r.agent_id,
            role: r.role,
            msg_type: r.msg_type,
            content: r.content,
            request_id: r.request_id,
            timestamp: r.timestamp,
        }
    }
}

impl From<ActionDbRow> for ActionRow {
    fn from(r: ActionDbRow) -> Self {
        Self {
            id: r.id,
            session_id: r.session_id,
            interaction_id: r.interaction_id,
            run_id: r.run_id,
            agent_id: r.agent_id,
            action_type: r.action_type,
            status: action_status_from_db(&r.status),
            payload: r.payload,
            result: r.result,
            resolved_at: r.resolved_at,
        }
    }
}

#[async_trait]
impl BeamStore for PgBeamStore {
    async fn checkin_agents(&self, agents: &[AgentRow]) -> StoreResult<()> {
        let mut guard = self.conn().await?;
        let conn: &mut AsyncPgConnection = &mut guard;
        for agent in agents {
            sql_query(
                "INSERT INTO ref_beam.agent (_id, name, status) VALUES ($1, $2, $3) \
                 ON CONFLICT (_id) DO UPDATE SET name = EXCLUDED.name, status = EXCLUDED.status",
            )
            .bind::<SqlUuid, _>(agent.id)
            .bind::<Text, _>(&agent.name)
            .bind::<Text, _>(&agent.status)
            .execute(conn)
            .await
            .map_err(|e| crate::BEM_114.with_data(e.to_string()))?;
        }
        Ok(())
    }

    async fn checkin_models(&self, models: &[ModelRow]) -> StoreResult<()> {
        let mut guard = self.conn().await?;
        let conn: &mut AsyncPgConnection = &mut guard;
        for model in models {
            sql_query(
                "INSERT INTO ref_beam.model (_id, name, provider, status) VALUES ($1, $2, $3, $4) \
                 ON CONFLICT (_id) DO UPDATE SET name = EXCLUDED.name, \
                 provider = EXCLUDED.provider, status = EXCLUDED.status",
            )
            .bind::<SqlUuid, _>(model.id)
            .bind::<Text, _>(&model.name)
            .bind::<Text, _>(&model.provider)
            .bind::<Text, _>(&model.status)
            .execute(conn)
            .await
            .map_err(|e| crate::BEM_115.with_data(e.to_string()))?;
        }
        Ok(())
    }

    async fn ensure_session(
        &self,
        session_id: Uuid,
        agent_id: Uuid,
        user_id: Option<&str>,
        profile_id: Option<&str>,
        name: Option<&str>,
    ) -> StoreResult<()> {
        let mut guard = self.conn().await?;
        let conn: &mut AsyncPgConnection = &mut guard;
        sql_query(
            "INSERT INTO ref_beam.agent_session (_id, agent_id, user_id, profile_id, name) \
             VALUES ($1, $2, $3, $4, $5) ON CONFLICT (_id) DO NOTHING",
        )
        .bind::<SqlUuid, _>(session_id)
        .bind::<SqlUuid, _>(agent_id)
        .bind::<Nullable<SqlUuid>, _>(opt_uuid(user_id))
        .bind::<Nullable<SqlUuid>, _>(opt_uuid(profile_id))
        .bind::<Nullable<Text>, _>(name.map(str::to_string))
        .execute(conn)
        .await
        .map_err(|e| crate::BEM_116.with_data(e.to_string()))?;
        Ok(())
    }

    async fn update_session_activity(&self, session_id: Uuid) -> StoreResult<()> {
        let mut guard = self.conn().await?;
        let conn: &mut AsyncPgConnection = &mut guard;
        sql_query("UPDATE ref_beam.agent_session SET last_active_at = now() WHERE _id = $1")
            .bind::<SqlUuid, _>(session_id)
            .execute(conn)
            .await
            .map_err(|e| crate::BEM_117.with_data(e.to_string()))?;
        Ok(())
    }

    async fn create_interaction(
        &self,
        interaction_id: Uuid,
        session_id: Uuid,
        agent_id: Uuid,
        agent_name: &str,
    ) -> StoreResult<()> {
        let mut guard = self.conn().await?;
        let conn: &mut AsyncPgConnection = &mut guard;
        sql_query(
            "INSERT INTO ref_beam.agent_interaction \
             (_id, session_id, agent_id, agent_name, status, started_at) \
             VALUES ($1, $2, $3, $4, 'in_progress', now()) ON CONFLICT (_id) DO NOTHING",
        )
        .bind::<SqlUuid, _>(interaction_id)
        .bind::<SqlUuid, _>(session_id)
        .bind::<SqlUuid, _>(agent_id)
        .bind::<Text, _>(agent_name)
        .execute(conn)
        .await
        .map_err(|e| crate::BEM_118.with_data(e.to_string()))?;
        Ok(())
    }

    async fn complete_interaction(
        &self,
        interaction_id: Uuid,
        status: InteractionStatus,
    ) -> StoreResult<()> {
        let mut guard = self.conn().await?;
        let conn: &mut AsyncPgConnection = &mut guard;
        sql_query(
            "UPDATE ref_beam.agent_interaction SET status = $2, ended_at = now() WHERE _id = $1",
        )
        .bind::<SqlUuid, _>(interaction_id)
        .bind::<Text, _>(status.as_str())
        .execute(conn)
        .await
        .map_err(|e| crate::BEM_119.with_data(e.to_string()))?;
        Ok(())
    }

    async fn create_run(
        &self,
        run_id: Uuid,
        session_id: Uuid,
        interaction_id: Uuid,
        agent_id: Uuid,
        agent_name: &str,
    ) -> StoreResult<()> {
        let mut guard = self.conn().await?;
        let conn: &mut AsyncPgConnection = &mut guard;
        sql_query(
            "INSERT INTO ref_beam.agent_run \
             (_id, session_id, interaction_id, agent_id, agent_name, status, started_at) \
             VALUES ($1, $2, $3, $4, $5, 'running', now()) ON CONFLICT (_id) DO NOTHING",
        )
        .bind::<SqlUuid, _>(run_id)
        .bind::<SqlUuid, _>(session_id)
        .bind::<SqlUuid, _>(interaction_id)
        .bind::<SqlUuid, _>(agent_id)
        .bind::<Text, _>(agent_name)
        .execute(conn)
        .await
        .map_err(|e| crate::BEM_120.with_data(e.to_string()))?;
        Ok(())
    }

    async fn complete_run(&self, run_id: Uuid, status: RunStatus) -> StoreResult<()> {
        let mut guard = self.conn().await?;
        let conn: &mut AsyncPgConnection = &mut guard;
        sql_query("UPDATE ref_beam.agent_run SET status = $2, ended_at = now() WHERE _id = $1")
            .bind::<SqlUuid, _>(run_id)
            .bind::<Text, _>(status.as_str())
            .execute(conn)
            .await
            .map_err(|e| crate::BEM_121.with_data(e.to_string()))?;
        Ok(())
    }

    async fn save_message(&self, message: MessageRow) -> StoreResult<()> {
        let mut guard = self.conn().await?;
        let conn: &mut AsyncPgConnection = &mut guard;
        sql_query(
            "INSERT INTO ref_beam.agent_message \
             (_id, sequence, role, type, content, timestamp, session_id, \
              interaction_id, run_id, agent_id, request_id) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) \
             ON CONFLICT (_id) DO NOTHING",
        )
        .bind::<SqlUuid, _>(message.id)
        .bind::<Integer, _>(message.sequence)
        .bind::<Text, _>(&message.role)
        .bind::<Text, _>(&message.msg_type)
        .bind::<Jsonb, _>(&message.content)
        .bind::<Timestamptz, _>(message.timestamp)
        .bind::<SqlUuid, _>(message.session_id)
        .bind::<Nullable<SqlUuid>, _>(message.interaction_id)
        .bind::<Nullable<SqlUuid>, _>(message.run_id)
        .bind::<SqlUuid, _>(message.agent_id)
        .bind::<Nullable<SqlUuid>, _>(message.request_id)
        .execute(conn)
        .await
        .map_err(|e| crate::BEM_122.with_data(e.to_string()))?;
        Ok(())
    }

    async fn next_sequence(&self, session_id: Uuid) -> StoreResult<i32> {
        let mut guard = self.conn().await?;
        let conn: &mut AsyncPgConnection = &mut guard;
        let row: SeqRow = sql_query(
            "SELECT COUNT(*)::int AS seq FROM ref_beam.agent_message WHERE session_id = $1",
        )
        .bind::<SqlUuid, _>(session_id)
        .get_result(conn)
        .await
        .map_err(|e| crate::BEM_123.with_data(e.to_string()))?;
        Ok(row.seq)
    }

    async fn create_action(&self, action: ActionRow) -> StoreResult<()> {
        let mut guard = self.conn().await?;
        let conn: &mut AsyncPgConnection = &mut guard;
        sql_query(
            "INSERT INTO ref_beam.agent_action \
             (_id, session_id, interaction_id, run_id, agent_id, type, status, \
              payload, result, resolved_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
             ON CONFLICT (_id) DO NOTHING",
        )
        .bind::<SqlUuid, _>(action.id)
        .bind::<SqlUuid, _>(action.session_id)
        .bind::<SqlUuid, _>(action.interaction_id)
        .bind::<SqlUuid, _>(action.run_id)
        .bind::<SqlUuid, _>(action.agent_id)
        .bind::<Text, _>(&action.action_type)
        .bind::<Text, _>(action.status.as_str())
        .bind::<Jsonb, _>(&action.payload)
        .bind::<Nullable<Jsonb>, _>(action.result.clone())
        .bind::<Nullable<Timestamptz>, _>(action.resolved_at)
        .execute(conn)
        .await
        .map_err(|e| crate::BEM_124.with_data(e.to_string()))?;
        Ok(())
    }

    async fn get_action(&self, action_id: Uuid) -> StoreResult<ActionRow> {
        let mut guard = self.conn().await?;
        let conn: &mut AsyncPgConnection = &mut guard;
        let rows: Vec<ActionDbRow> = sql_query(
            "SELECT _id, session_id, interaction_id, run_id, agent_id, type AS type_, \
             status, payload, result, resolved_at \
             FROM ref_beam.agent_action WHERE _id = $1",
        )
        .bind::<SqlUuid, _>(action_id)
        .load(conn)
        .await
        .map_err(|e| crate::BEM_125.with_data(e.to_string()))?;
        rows.into_iter()
            .next()
            .map(ActionRow::from)
            .ok_or_else(|| crate::BEM_027.with_data(action_id.to_string()))
    }

    async fn resolve_action(&self, action_id: Uuid, result: Value) -> StoreResult<()> {
        let mut guard = self.conn().await?;
        let conn: &mut AsyncPgConnection = &mut guard;
        let affected = sql_query(
            "UPDATE ref_beam.agent_action \
             SET status = 'resolved', result = $2, resolved_at = now() WHERE _id = $1",
        )
        .bind::<SqlUuid, _>(action_id)
        .bind::<Jsonb, _>(result)
        .execute(conn)
        .await
        .map_err(|e| crate::BEM_126.with_data(e.to_string()))?;
        if affected == 0 {
            return Err(crate::BEM_028.with_data(action_id.to_string()));
        }
        Ok(())
    }

    async fn get_session(&self, session_id: Uuid) -> StoreResult<Option<SessionRow>> {
        let mut guard = self.conn().await?;
        let conn: &mut AsyncPgConnection = &mut guard;
        let rows: Vec<SessionDbRow> = sql_query(
            "SELECT _id, agent_id, user_id, profile_id, name, last_active_at \
             FROM ref_beam.agent_session WHERE _id = $1",
        )
        .bind::<SqlUuid, _>(session_id)
        .load(conn)
        .await
        .map_err(|e| crate::BEM_127.with_data(e.to_string()))?;
        Ok(rows.into_iter().next().map(SessionRow::from))
    }

    async fn get_interaction(&self, interaction_id: Uuid) -> StoreResult<Option<InteractionRow>> {
        let mut guard = self.conn().await?;
        let conn: &mut AsyncPgConnection = &mut guard;
        let rows: Vec<InteractionDbRow> = sql_query(
            "SELECT _id, session_id, agent_id, agent_name, status, started_at, ended_at \
             FROM ref_beam.agent_interaction WHERE _id = $1",
        )
        .bind::<SqlUuid, _>(interaction_id)
        .load(conn)
        .await
        .map_err(|e| crate::BEM_128.with_data(e.to_string()))?;
        Ok(rows.into_iter().next().map(InteractionRow::from))
    }

    async fn get_run(&self, run_id: Uuid) -> StoreResult<Option<RunRow>> {
        let mut guard = self.conn().await?;
        let conn: &mut AsyncPgConnection = &mut guard;
        let rows: Vec<RunDbRow> = sql_query(
            "SELECT _id, session_id, interaction_id, agent_id, agent_name, status, \
             started_at, ended_at FROM ref_beam.agent_run WHERE _id = $1",
        )
        .bind::<SqlUuid, _>(run_id)
        .load(conn)
        .await
        .map_err(|e| crate::BEM_129.with_data(e.to_string()))?;
        Ok(rows.into_iter().next().map(RunRow::from))
    }

    async fn list_messages(&self, session_id: Uuid) -> StoreResult<Vec<MessageRow>> {
        let mut guard = self.conn().await?;
        let conn: &mut AsyncPgConnection = &mut guard;
        let rows: Vec<MessageDbRow> = sql_query(
            "SELECT _id, sequence, session_id, interaction_id, run_id, agent_id, role, \
             type AS type_, content, request_id, timestamp \
             FROM ref_beam.agent_message WHERE session_id = $1 ORDER BY sequence ASC",
        )
        .bind::<SqlUuid, _>(session_id)
        .load(conn)
        .await
        .map_err(|e| crate::BEM_130.with_data(e.to_string()))?;
        Ok(rows.into_iter().map(MessageRow::from).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Connect against a local Postgres if one is configured; otherwise skip.
    ///
    /// Set `BEAM_DATABASE_URL` (or `DATABASE_URL`) to e.g.
    /// `postgres://<user>@127.0.0.1:5432/beam_test` to exercise these tests.
    /// The pool/migrations are established once and shared so parallel tests do
    /// not race on the one-time framework migration.
    async fn store() -> Option<PgBeamStore> {
        static SHARED: tokio::sync::OnceCell<Option<PgBeamStore>> = tokio::sync::OnceCell::const_new();
        SHARED
            .get_or_init(|| async {
                let url = std::env::var("BEAM_DATABASE_URL")
                    .or_else(|_| std::env::var("DATABASE_URL"))
                    .ok()?;
                Some(
                    PgBeamStore::connect(&url)
                        .await
                        .expect("connect + bootstrap schema"),
                )
            })
            .await
            .clone()
    }

    #[tokio::test]
    async fn execution_hierarchy_roundtrip() {
        let Some(store) = store().await else {
            eprintln!("skipping: no BEAM_DATABASE_URL/DATABASE_URL set");
            return;
        };

        let agent_id = Uuid::new_v4();
        let session = Uuid::new_v4();
        let interaction = Uuid::new_v4();
        let run = Uuid::new_v4();
        let user = Uuid::new_v4();

        store
            .checkin_agents(&[AgentRow {
                id: agent_id,
                name: format!("demo-{agent_id}"),
                status: "active".into(),
            }])
            .await
            .unwrap();
        store
            .checkin_models(&[ModelRow {
                id: Uuid::new_v4(),
                name: "gpt-test".into(),
                provider: "openai".into(),
                status: "active".into(),
            }])
            .await
            .unwrap();

        store
            .ensure_session(
                session,
                agent_id,
                Some(&user.to_string()),
                None,
                Some("sess"),
            )
            .await
            .unwrap();
        store.update_session_activity(session).await.unwrap();

        let fetched = store.get_session(session).await.unwrap().unwrap();
        assert_eq!(fetched.id, session);
        assert_eq!(fetched.user_id.as_deref(), Some(user.to_string().as_str()));
        assert!(fetched.last_active_at.is_some());

        store
            .create_interaction(interaction, session, agent_id, "demo")
            .await
            .unwrap();
        store
            .create_run(run, session, interaction, agent_id, "demo")
            .await
            .unwrap();

        let seq = store.next_sequence(session).await.unwrap();
        assert_eq!(seq, 0);
        store
            .save_message(MessageRow {
                id: Uuid::new_v4(),
                sequence: seq,
                session_id: session,
                interaction_id: Some(interaction),
                run_id: Some(run),
                agent_id,
                role: "user".into(),
                msg_type: "text".into(),
                content: json!({"text": "hello"}),
                request_id: None,
                timestamp: Utc::now(),
            })
            .await
            .unwrap();
        assert_eq!(store.next_sequence(session).await.unwrap(), 1);
        let messages = store.list_messages(session).await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].content, json!({"text": "hello"}));

        store.complete_run(run, RunStatus::Completed).await.unwrap();
        store
            .complete_interaction(interaction, InteractionStatus::Completed)
            .await
            .unwrap();
        assert_eq!(
            store.get_run(run).await.unwrap().unwrap().status,
            RunStatus::Completed
        );
        assert_eq!(
            store
                .get_interaction(interaction)
                .await
                .unwrap()
                .unwrap()
                .status,
            InteractionStatus::Completed
        );
    }

    #[tokio::test]
    async fn action_lifecycle() {
        let Some(store) = store().await else {
            eprintln!("skipping: no BEAM_DATABASE_URL/DATABASE_URL set");
            return;
        };

        let agent_id = Uuid::new_v4();
        let session = Uuid::new_v4();
        let interaction = Uuid::new_v4();
        let run = Uuid::new_v4();
        let action_id = Uuid::new_v4();

        store
            .checkin_agents(&[AgentRow {
                id: agent_id,
                name: format!("a-{agent_id}"),
                status: "active".into(),
            }])
            .await
            .unwrap();
        store
            .ensure_session(session, agent_id, None, None, None)
            .await
            .unwrap();
        store
            .create_interaction(interaction, session, agent_id, "demo")
            .await
            .unwrap();
        store
            .create_run(run, session, interaction, agent_id, "demo")
            .await
            .unwrap();

        store
            .create_action(ActionRow {
                id: action_id,
                session_id: session,
                interaction_id: interaction,
                run_id: run,
                agent_id,
                action_type: "tool_approval".into(),
                status: ActionStatus::Pending,
                payload: json!({"tool": "search"}),
                result: None,
                resolved_at: None,
            })
            .await
            .unwrap();

        let pending = store.get_action(action_id).await.unwrap();
        assert_eq!(pending.status, ActionStatus::Pending);
        assert_eq!(pending.payload, json!({"tool": "search"}));

        store
            .resolve_action(action_id, json!({"approved": true}))
            .await
            .unwrap();
        let resolved = store.get_action(action_id).await.unwrap();
        assert_eq!(resolved.status, ActionStatus::Resolved);
        assert_eq!(resolved.result, Some(json!({"approved": true})));
        assert!(resolved.resolved_at.is_some());

        let missing = store.resolve_action(Uuid::new_v4(), json!({})).await;
        assert_eq!(missing.unwrap_err().errcode.as_str(), "BEM-027");
    }
}
