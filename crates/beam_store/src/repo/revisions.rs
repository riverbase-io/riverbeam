//! Agent revisions, `db.revisions()` (Python `RevisionBook.persist_all` /
//! `reconstruct`): `agent_revision` rows, written through their entity,
//! and each agent's `active_revision_id`.

use chrono::{DateTime, Utc};
use diesel::sql_query;
use diesel::sql_types::{Jsonb, Nullable, Text, Timestamptz, Uuid as SqlUuid};
use diesel::QueryableByName;
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::RunQueryDsl;
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::db::{Db, DbResult};
use crate::entity::write;
use crate::repo::rows::Found;

/// One revision to write: the columns a stored row may change (status,
/// timestamps, compatibility), and the rest a new row is inserted with,
/// both keyed by physical column.
#[derive(Debug, Clone)]
pub struct RevisionWrite {
    pub id: Uuid,
    pub agent_name: String,
    pub revision_hash: String,
    pub changes: Map<String, Value>,
    pub columns: Map<String, Value>,
}

/// One stored revision; `status` and `compatibility_policy` are labels.
#[derive(Debug, Clone, QueryableByName)]
pub struct RevisionRecord {
    #[diesel(sql_type = SqlUuid, column_name = _id)]
    pub id: Uuid,
    #[diesel(sql_type = Nullable<SqlUuid>)]
    pub agent_id: Option<Uuid>,
    #[diesel(sql_type = Text)]
    pub agent_name: String,
    #[diesel(sql_type = Text)]
    pub version: String,
    #[diesel(sql_type = Text)]
    pub revision_hash: String,
    #[diesel(sql_type = Text)]
    pub runtime_name: String,
    #[diesel(sql_type = Nullable<Text>)]
    pub runtime_adapter_version: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    pub code_revision: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    pub build_id: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    pub prompt_revision: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    pub toolset_revision: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    pub model_policy_revision: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    pub security_policy_revision: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    pub memory_policy_revision: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    pub knowledge_policy_revision: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    pub input_schema_hash: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    pub output_schema_hash: Option<String>,
    #[diesel(sql_type = Nullable<Jsonb>)]
    pub runtime_config_snapshot: Option<Value>,
    #[diesel(sql_type = Nullable<Jsonb>)]
    pub framework_config_snapshot: Option<Value>,
    #[diesel(sql_type = Nullable<Text>)]
    pub beam_version: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    pub runtime_library_version: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    pub application_build_id: Option<String>,
    #[diesel(sql_type = Text)]
    pub status: String,
    #[diesel(sql_type = Text)]
    pub compatibility_policy: String,
    #[diesel(sql_type = Nullable<Jsonb>)]
    pub compatible_with: Option<Value>,
    #[diesel(sql_type = Timestamptz)]
    pub created_at: DateTime<Utc>,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    pub activated_at: Option<DateTime<Utc>>,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    pub retired_at: Option<DateTime<Utc>>,
}

#[derive(QueryableByName)]
struct TrafficRow {
    #[diesel(sql_type = Text)]
    name: String,
    #[diesel(sql_type = SqlUuid)]
    active_revision_id: Uuid,
}

/// `data` with the write flag `flag` (`_insert` or `_update`).
fn with(mut data: Map<String, Value>, flag: &str) -> Value {
    data.insert(flag.to_owned(), Value::Bool(true));
    Value::Object(data)
}

/// The revisions on a [`Db`].
pub struct Revisions<'d> {
    db: &'d Db,
}

impl Db {
    /// Agent revisions and each agent's active one.
    #[must_use]
    pub fn revisions(&self) -> Revisions<'_> {
        Revisions { db: self }
    }
}

impl Revisions<'_> {
    /// Write `rows` and set each agent's active revision (`active`, by agent
    /// name), in one unit of work. A row whose hash the agent already has
    /// under another id is skipped, even its updates; a stored row changes
    /// only its `changes`; a new one is inserted whole.
    ///
    /// # Errors
    /// A failed unit of work, read or write (rolled back).
    pub async fn persist(&self, rows: &[RevisionWrite], active: &[(String, Uuid)]) -> DbResult<()> {
        let s = self.db.schema();
        let uow = self.db.unit_of_work().await?;
        let written = async {
            for r in rows {
                let taken: Found = {
                    let mut conn = uow.postgres_connection().await?;
                    sql_query(format!(
                        "SELECT EXISTS (SELECT 1 FROM {s}.agent_revision \
                         WHERE agent_name = $1 AND revision_hash = $2 AND _id <> $3) AS found"
                    ))
                    .bind::<Text, _>(&r.agent_name)
                    .bind::<Text, _>(&r.revision_hash)
                    .bind::<SqlUuid, _>(r.id)
                    .get_result(&mut *conn)
                    .await?
                };
                if taken.found {
                    continue;
                }
                let data = if write::exists(&uow, write::Table::AgentRevision, r.id).await? {
                    with(r.changes.clone(), "_update")
                } else {
                    let mut row = r.changes.clone();
                    row.extend(r.columns.clone());
                    with(row, "_insert")
                };
                write::put(self.db.datastore(), &uow, "agent_revision", r.id, data).await?;
            }
            // Active revisions are set by agent name: a bulk update stays SQL.
            let mut conn = uow.postgres_connection().await?;
            for (name, primary) in active {
                sql_query(format!(
                    "UPDATE {s}.agent SET active_revision_id = $2 WHERE name = $1"
                ))
                .bind::<Text, _>(name)
                .bind::<SqlUuid, _>(*primary)
                .execute(&mut *conn)
                .await?;
            }
            Ok(())
        }
        .await;
        write::finish(&uow, written).await?;
        Ok(())
    }

    /// Every stored revision, oldest first, and each agent's active one.
    ///
    /// # Errors
    /// A failed checkout or read.
    pub async fn load(&self) -> DbResult<(Vec<RevisionRecord>, Vec<(String, Uuid)>)> {
        let s = self.db.schema();
        self.db
            .scoped(|conn| {
                async move {
                    let rows: Vec<RevisionRecord> = sql_query(format!(
                        "SELECT _id, agent_id, agent_name, version, revision_hash, runtime_name, runtime_adapter_version, \
                         code_revision, build_id, prompt_revision, toolset_revision, model_policy_revision, \
                         security_policy_revision, memory_policy_revision, knowledge_policy_revision, input_schema_hash, \
                         output_schema_hash, runtime_config_snapshot, framework_config_snapshot, beam_version, \
                         runtime_library_version, application_build_id, status::text AS status, \
                         compatibility_policy::text AS compatibility_policy, compatible_with, created_at, activated_at, retired_at \
                         FROM {s}.agent_revision WHERE _deleted IS NULL ORDER BY created_at, _id"
                    ))
                    .load(&mut *conn)
                    .await?;
                    let active: Vec<TrafficRow> = sql_query(format!(
                        "SELECT name, active_revision_id FROM {s}.agent WHERE active_revision_id IS NOT NULL"
                    ))
                    .load(conn)
                    .await?;
                    Ok((
                        rows,
                        active
                            .into_iter()
                            .map(|t| (t.name, t.active_revision_id))
                            .collect(),
                    ))
                }
                .scope_boxed()
            })
            .await
    }
}
