//! Durable session summaries, `ref_beam.agent_session_summary` (rc8
//! `transcript/stores/sql.py`, plan 023 B3): one row per session, the summary
//! as a JSON payload, its coverage (the last transcript sequence it covers)
//! in `coverage_end`. `beam_memory` reads and writes the payload.
//!
//! [`PgSessionSummaries::cas_put`] is rc8's compare-and-set: under the row's
//! lock, the update must name the coverage end it read, and coverage never
//! moves back; a lost insert race returns `false`.

use diesel::sql_query;
use diesel::sql_types::{Bool, Integer, Jsonb, Uuid as SqlUuid};
use diesel::QueryableByName;
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::RunQueryDsl;
use serde_json::Value;
use uuid::Uuid;

use crate::store::StoreResult;

fn db(e: impl std::fmt::Display) -> riverbase_core::RiverbaseError {
    crate::BEM_139.with_data(e.to_string())
}

/// A stored summary: its coverage end, whether an edit invalidated it, and
/// its payload.
#[derive(Debug, Clone, PartialEq)]
pub struct SummaryRow {
    pub coverage_end: i64,
    pub invalidated: bool,
    pub payload: Value,
}

#[derive(QueryableByName)]
struct Row {
    #[diesel(sql_type = Integer)]
    coverage_end: i32,
    #[diesel(sql_type = Bool)]
    invalidated: bool,
    #[diesel(sql_type = Jsonb)]
    payload: Value,
}

/// The session summaries of a beam database.
#[derive(Clone)]
pub struct PgSessionSummaries {
    db: crate::Db,
}

fn coverage(end: i64) -> i32 {
    i32::try_from(end).unwrap_or(i32::MAX)
}

impl PgSessionSummaries {
    #[must_use]
    pub fn for_store(store: &crate::PgBeamStore) -> Self {
        Self {
            db: store.db().clone(),
        }
    }

    /// The session's summary.
    ///
    /// # Errors
    /// `BEM-139` when the read fails.
    pub async fn get(&self, session_id: Uuid) -> StoreResult<Option<SummaryRow>> {
        self.db
            .at(crate::BEM_139)
            .scoped(|conn| {
                async move {
                    let rows: Vec<Row> = sql_query(format!(
                        "SELECT coverage_end, invalidated, payload FROM {}.agent_session_summary \
                     WHERE session_id = $1 AND _deleted IS NULL",
                        self.db.schema()
                    ))
                    .bind::<SqlUuid, _>(session_id)
                    .load(&mut *conn)
                    .await
                    .map_err(db)?;
                    Ok(rows.into_iter().next().map(|r| SummaryRow {
                        coverage_end: i64::from(r.coverage_end),
                        invalidated: r.invalidated,
                        payload: r.payload,
                    }))
                }
                .scope_boxed()
            })
            .await
    }

    /// Store `payload` covering through `coverage_end` unless a concurrent
    /// writer moved the coverage: the current end must equal `expected_end`
    /// (when given), and the new end must not be behind it. The org is the
    /// session's. Returns whether it was stored.
    ///
    /// # Errors
    /// `BEM-139` when a statement fails.
    pub async fn cas_put(
        &self,
        session_id: Uuid,
        coverage_end: i64,
        payload: &Value,
        expected_end: Option<i64>,
    ) -> StoreResult<bool> {
        let schema = self.db.schema().clone();
        let payload = payload.clone();
        self.db
            .at(crate::BEM_139)
            .transaction::<_, riverbase_core::RiverbaseError, _>(|tx| {
                Box::pin(async move {
                    let current: Vec<Row> = sql_query(format!(
                    "SELECT coverage_end, invalidated, payload FROM {schema}.agent_session_summary \
                     WHERE session_id = $1 FOR UPDATE"
                ))
                    .bind::<SqlUuid, _>(session_id)
                    .load(tx)
                    .await
                    .map_err(db)?;
                    if let Some(row) = <[Row]>::first(&current) {
                        let end = i64::from(row.coverage_end);
                        if expected_end.is_some_and(|e| e != end) || coverage_end < end {
                            return Ok(false);
                        }
                        sql_query(format!(
                            "UPDATE {schema}.agent_session_summary SET coverage_end = $2, \
                         invalidated = false, payload = $3, _deleted = NULL, _updated = now() \
                         WHERE session_id = $1"
                        ))
                        .bind::<SqlUuid, _>(session_id)
                        .bind::<Integer, _>(coverage(coverage_end))
                        .bind::<Jsonb, _>(payload)
                        .execute(tx)
                        .await
                        .map_err(db)?;
                        return Ok(true);
                    }
                    if expected_end.is_some() {
                        return Ok(false);
                    }
                    // A concurrent first insert wins; this one returns false.
                    let inserted = sql_query(format!(
                        "INSERT INTO {schema}.agent_session_summary \
                     (_id, session_id, coverage_end, invalidated, payload, org_id) \
                     SELECT $1, $2, $3, false, $4, \
                     (SELECT s.org_id FROM {schema}.agent_session s WHERE s._id = $2) \
                     ON CONFLICT (session_id) DO NOTHING"
                    ))
                    .bind::<SqlUuid, _>(Uuid::new_v4())
                    .bind::<SqlUuid, _>(session_id)
                    .bind::<Integer, _>(coverage(coverage_end))
                    .bind::<Jsonb, _>(payload)
                    .execute(tx)
                    .await
                    .map_err(db)?;
                    Ok(inserted > 0)
                })
            })
            .await
    }

    /// An edit from transcript sequence `from` invalidates a summary that
    /// covers it (its coverage is cut before it); without `from` the summary
    /// is deleted.
    ///
    /// # Errors
    /// `BEM-139` when the write fails.
    pub async fn invalidate(&self, session_id: Uuid, from: Option<i64>) -> StoreResult<()> {
        self.db
            .at(crate::BEM_139)
            .scoped(|conn| {
                async move {
                    match from {
                        None => {
                            sql_query(format!(
                                "DELETE FROM {}.agent_session_summary WHERE session_id = $1",
                                self.db.schema()
                            ))
                            .bind::<SqlUuid, _>(session_id)
                            .execute(&mut *conn)
                            .await
                            .map_err(db)?;
                        }
                        Some(from) => {
                            sql_query(format!(
                                "UPDATE {}.agent_session_summary SET invalidated = true, \
                             coverage_end = LEAST(coverage_end, $2 - 1), _updated = now() \
                             WHERE session_id = $1 AND coverage_end >= $2",
                                self.db.schema()
                            ))
                            .bind::<SqlUuid, _>(session_id)
                            .bind::<Integer, _>(coverage(from))
                            .execute(&mut *conn)
                            .await
                            .map_err(db)?;
                        }
                    }
                    Ok(())
                }
                .scope_boxed()
            })
            .await
    }
}
