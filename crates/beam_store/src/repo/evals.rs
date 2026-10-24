//! Evaluation datasets, runs and feedback, `db.evals()`: datasets and cases
//! in `agent_evaluation_dataset`/`_case`, runs and results in
//! `agent_evaluation_run`/`_result`, feedback in `agent_feedback`. Rows saved
//! together get `_created` a microsecond apart, so they read back in order.

use chrono::{DateTime, Utc};
use diesel::sql_query;
use diesel::sql_types::{Integer, Nullable, Text, Uuid as SqlUuid};
use diesel::{ExpressionMethods, OptionalExtension, QueryDsl, QueryableByName};
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde_json::{json, Map, Value};
use uuid::Uuid;

use crate::db::{Db, DbError, DbResult};
use crate::entity::write;
use crate::repo::rows::JsonRow;

// The dataset tables have no entity (plan 022 D2): the columns this store
// uses. `_etag` keeps its database default.
diesel::table! {
    agent_evaluation_dataset (_id) {
        _id -> diesel::sql_types::Uuid,
        name -> diesel::sql_types::Text,
        metadata_json -> diesel::sql_types::Nullable<diesel::sql_types::Jsonb>,
        _updated -> diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>,
        _deleted -> diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>,
    }
}

diesel::table! {
    agent_evaluation_case (_id) {
        _id -> diesel::sql_types::Uuid,
        dataset_id -> diesel::sql_types::Uuid,
        input_json -> diesel::sql_types::Jsonb,
        expected -> diesel::sql_types::Nullable<diesel::sql_types::Jsonb>,
        _created -> diesel::sql_types::Timestamptz,
        _updated -> diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>,
        _deleted -> diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>,
    }
}

/// A dataset and its cases, in order.
#[derive(Debug, Clone)]
pub struct DatasetRecord {
    pub id: Uuid,
    pub name: String,
    pub cases: Vec<CaseRecord>,
}

/// One case: its agent input and expected outcome, as stored JSON.
#[derive(Debug, Clone)]
pub struct CaseRecord {
    pub id: Uuid,
    pub input: Value,
    pub expected: Option<Value>,
}

/// A run and its results, in order.
#[derive(Debug, Clone)]
pub struct RunRecord {
    pub id: Uuid,
    pub dataset_id: Uuid,
    pub agent_revision_id: Uuid,
    pub passed: bool,
    pub results: Vec<ResultRecord>,
}

/// One case's result.
#[derive(Debug, Clone)]
pub struct ResultRecord {
    pub case_id: Uuid,
    pub passed: bool,
    pub score: Option<f64>,
    pub details: Map<String, Value>,
}

/// One feedback row.
#[derive(Debug, Clone)]
pub struct FeedbackRecord {
    pub id: Uuid,
    pub execution_id: Uuid,
    pub agent_revision_id: Option<Uuid>,
    pub rating: Option<i32>,
    pub thumb: Option<String>,
    pub comment: Option<String>,
}

/// `now` plus `index` microseconds: rows saved together keep their order in
/// `_created`.
fn ordered(now: DateTime<Utc>, index: usize) -> DateTime<Utc> {
    now + chrono::Duration::microseconds(i64::try_from(index).unwrap_or(i64::MAX))
}

#[derive(QueryableByName)]
struct FeedbackRow {
    #[diesel(sql_type = SqlUuid, column_name = _id)]
    id: Uuid,
    #[diesel(sql_type = SqlUuid)]
    execution_id: Uuid,
    #[diesel(sql_type = Nullable<SqlUuid>)]
    agent_revision_id: Option<Uuid>,
    #[diesel(sql_type = Nullable<Integer>)]
    rating: Option<i32>,
    #[diesel(sql_type = Nullable<Text>)]
    thumb: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    comment: Option<String>,
}

/// The eval store on a [`Db`].
pub struct Evals<'d> {
    db: &'d Db,
}

impl Db {
    /// Evaluation datasets, runs and feedback.
    #[must_use]
    pub fn evals(&self) -> Evals<'_> {
        Evals { db: self }
    }
}

impl Evals<'_> {
    /// The dataset row, then its cases: replaced in one transaction, the
    /// cases no longer in it deleted.
    ///
    /// # Errors
    /// A failed checkout or write (nothing changes then).
    pub async fn save_dataset(&self, dataset: DatasetRecord) -> DbResult<()> {
        use agent_evaluation_case as c;
        use agent_evaluation_dataset as d;
        let DatasetRecord { id, name, cases } = dataset;
        self.db
            .transaction::<_, DbError, _>(|tx| {
                Box::pin(async move {
                    let now = Utc::now();
                    diesel::insert_into(d::table)
                        .values((d::_id.eq(id), d::name.eq(name.clone())))
                        .on_conflict(d::_id)
                        .do_update()
                        .set((
                            d::name.eq(name),
                            d::_updated.eq(Some(now)),
                            d::_deleted.eq(None::<DateTime<Utc>>),
                        ))
                        .execute(tx)
                        .await?;
                    let keep: Vec<Uuid> = cases.iter().map(|case| case.id).collect();
                    diesel::delete(
                        c::table
                            .filter(c::dataset_id.eq(id))
                            .filter(c::_id.ne_all(keep)),
                    )
                    .execute(tx)
                    .await?;
                    for (index, case) in cases.into_iter().enumerate() {
                        let created = ordered(now, index);
                        diesel::insert_into(c::table)
                            .values((
                                c::_id.eq(case.id),
                                c::dataset_id.eq(id),
                                c::input_json.eq(case.input.clone()),
                                c::expected.eq(case.expected.clone()),
                                c::_created.eq(created),
                            ))
                            .on_conflict(c::_id)
                            .do_update()
                            .set((
                                c::input_json.eq(case.input),
                                c::expected.eq(case.expected),
                                c::_created.eq(created),
                                c::_updated.eq(Some(now)),
                                c::_deleted.eq(None::<DateTime<Utc>>),
                            ))
                            .execute(tx)
                            .await?;
                    }
                    Ok(())
                })
            })
            .await
    }

    /// Dataset `id` with its cases in the order they were saved.
    ///
    /// # Errors
    /// A failed checkout or read.
    pub async fn get_dataset(&self, id: Uuid) -> DbResult<Option<DatasetRecord>> {
        use agent_evaluation_case as c;
        use agent_evaluation_dataset as d;
        self.db
            .scoped(|conn| {
                async move {
                    let name: Option<String> = d::table
                        .filter(d::_id.eq(id))
                        .filter(d::_deleted.is_null())
                        .select(d::name)
                        .first(conn)
                        .await
                        .optional()?;
                    let Some(name) = name else {
                        return Ok(None);
                    };
                    let rows: Vec<(Uuid, Value, Option<Value>)> = c::table
                        .filter(c::dataset_id.eq(id))
                        .filter(c::_deleted.is_null())
                        .order((c::_created.asc(), c::_id.asc()))
                        .select((c::_id, c::input_json, c::expected))
                        .load(conn)
                        .await?;
                    let cases = rows
                        .into_iter()
                        .map(|(case_id, input, expected)| CaseRecord {
                            id: case_id,
                            input,
                            expected,
                        })
                        .collect();
                    Ok(Some(DatasetRecord { id, name, cases }))
                }
                .scope_boxed()
            })
            .await
    }

    /// The run, then its results, in one transaction; a run already saved
    /// stays as it was (insert-only, as the runner saves each run once).
    ///
    /// # Errors
    /// A failed checkout or write (nothing changes then).
    pub async fn save_run(&self, run: RunRecord) -> DbResult<()> {
        use crate::entity::agent_evaluation_result::agent_evaluation_result as res;
        use crate::entity::agent_evaluation_run::agent_evaluation_run as r;
        self.db
            .transaction::<_, DbError, _>(|tx| {
                Box::pin(async move {
                    let now = Utc::now();
                    let inserted = diesel::insert_into(r::table)
                        .values((
                            r::_id.eq(run.id),
                            r::dataset_id.eq(run.dataset_id),
                            r::agent_revision_id.eq(run.agent_revision_id),
                            r::passed.eq(run.passed),
                            r::created_at.eq(now),
                        ))
                        .on_conflict(r::_id)
                        .do_nothing()
                        .execute(tx)
                        .await?;
                    if inserted == 0 {
                        return Ok(());
                    }
                    for (index, result) in run.results.into_iter().enumerate() {
                        diesel::insert_into(res::table)
                            .values((
                                res::_id.eq(Uuid::new_v4()),
                                res::run_id.eq(run.id),
                                res::case_id.eq(result.case_id),
                                res::passed.eq(result.passed),
                                res::score.eq(result.score),
                                res::details.eq(Some(Value::Object(result.details))),
                                res::_created.eq(ordered(now, index)),
                            ))
                            .execute(tx)
                            .await?;
                    }
                    Ok(())
                })
            })
            .await
    }

    /// Run `id` with its results in the order they were saved.
    ///
    /// # Errors
    /// A failed checkout or read.
    pub async fn get_run(&self, id: Uuid) -> DbResult<Option<RunRecord>> {
        self.db
            .scoped(|conn| async move { load_run(conn, id).await }.scope_boxed())
            .await
    }

    /// The latest run of revision `revision_id`.
    ///
    /// # Errors
    /// A failed checkout or read.
    pub async fn last_run_for_revision(&self, revision_id: Uuid) -> DbResult<Option<RunRecord>> {
        use crate::entity::agent_evaluation_run::agent_evaluation_run as r;
        self.db
            .scoped(|conn| {
                async move {
                    let id: Option<Uuid> = r::table
                        .filter(r::agent_revision_id.eq(revision_id))
                        .filter(r::_deleted.is_null())
                        .order((r::created_at.desc(), r::_id.desc()))
                        .select(r::_id)
                        .first(conn)
                        .await
                        .optional()?;
                    match id {
                        Some(id) => load_run(conn, id).await,
                        None => Ok(None),
                    }
                }
                .scope_boxed()
            })
            .await
    }

    /// Store feedback through its entity, with the owner of the execution's
    /// session, the execution's org first (none when the execution is not
    /// stored), and the time.
    ///
    /// # Errors
    /// A failed unit of work, read or write (rolled back).
    pub async fn save_feedback(&self, f: &FeedbackRecord) -> DbResult<()> {
        let s = self.db.schema();
        let uow = self.db.unit_of_work().await?;
        let written = async {
            let context: JsonRow = {
                let mut conn = uow.postgres_connection().await?;
                sql_query(format!(
                    "SELECT jsonb_build_object('user_id', s.user_id, \
                     'org_id', COALESCE(e.org_id, s.org_id), 'created_at', now()) AS row \
                     FROM (SELECT 1) one \
                     LEFT JOIN {s}.agent_execution e ON e._id = $1 \
                     LEFT JOIN {s}.agent_session s ON s._id = e.session_id"
                ))
                .bind::<SqlUuid, _>(f.execution_id)
                .get_result(&mut *conn)
                .await?
            };
            let mut data = json!({
                "execution_id": f.execution_id,
                "agent_revision_id": f.agent_revision_id,
                "rating": f.rating,
                "thumb": f.thumb,
                "comment": f.comment,
                "_insert": true,
            });
            if let (Some(data), Value::Object(context)) = (data.as_object_mut(), context.row) {
                data.extend(context);
            }
            write::put(self.db.datastore(), &uow, "agent_feedback", f.id, data).await
        }
        .await;
        write::finish(&uow, written).await?;
        Ok(())
    }

    /// The feedback on execution `execution_id`, oldest first.
    ///
    /// # Errors
    /// A failed checkout or read.
    pub async fn list_feedback(&self, execution_id: Uuid) -> DbResult<Vec<FeedbackRecord>> {
        let s = self.db.schema();
        self.db
            .scoped(|conn| {
                async move {
                    let rows: Vec<FeedbackRow> = sql_query(format!(
                        "SELECT _id, execution_id, agent_revision_id, rating, thumb, comment FROM {s}.agent_feedback \
                         WHERE execution_id = $1 AND _deleted IS NULL ORDER BY created_at, _id"
                    ))
                    .bind::<SqlUuid, _>(execution_id)
                    .load(conn)
                    .await?;
                    Ok(rows
                        .into_iter()
                        .map(|r| FeedbackRecord {
                            id: r.id,
                            execution_id: r.execution_id,
                            agent_revision_id: r.agent_revision_id,
                            rating: r.rating,
                            thumb: r.thumb,
                            comment: r.comment,
                        })
                        .collect())
                }
                .scope_boxed()
            })
            .await
    }
}

/// Run `id` with its results in the order they were saved.
async fn load_run(conn: &mut AsyncPgConnection, id: Uuid) -> DbResult<Option<RunRecord>> {
    use crate::entity::agent_evaluation_result::agent_evaluation_result as res;
    use crate::entity::agent_evaluation_run::agent_evaluation_run as run;
    let head: Option<(Uuid, Uuid, bool)> = run::table
        .filter(run::_id.eq(id))
        .filter(run::_deleted.is_null())
        .select((run::dataset_id, run::agent_revision_id, run::passed))
        .first(conn)
        .await
        .optional()?;
    let Some((dataset_id, agent_revision_id, passed)) = head else {
        return Ok(None);
    };
    let rows: Vec<(Uuid, bool, Option<f64>, Option<Value>)> = res::table
        .filter(res::run_id.eq(id))
        .filter(res::_deleted.is_null())
        .order((res::_created.asc(), res::_id.asc()))
        .select((res::case_id, res::passed, res::score, res::details))
        .load(conn)
        .await?;
    Ok(Some(RunRecord {
        id,
        dataset_id,
        agent_revision_id,
        passed,
        results: rows
            .into_iter()
            .map(|(case_id, passed, score, details)| ResultRecord {
                case_id,
                passed,
                score,
                details: details
                    .and_then(|d| d.as_object().cloned())
                    .unwrap_or_default(),
            })
            .collect(),
    }))
}
