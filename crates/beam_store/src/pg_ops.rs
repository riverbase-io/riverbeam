//! [`OpsStore`] and [`RetentionStore`] on Postgres, with Python's SQL
//! (`store/ops/outbox.py`, `dlq.py`, `orchestration/tasks/retention.py`).

use async_trait::async_trait;
use chrono::{Duration, Utc};
use diesel::sql_query;
use diesel::sql_types::{BigInt, Double, Integer, Jsonb, Nullable, Text, Uuid as SqlUuid};
use diesel::{
    ExpressionMethods, OptionalExtension, QueryDsl, Queryable, QueryableByName, Selectable,
    SelectableHelper,
};
use diesel_async::RunQueryDsl;
use serde_json::{json, Value};
use uuid::Uuid;

use diesel_async::scoped_futures::ScopedFutureExt;

use crate::db::Db;
use crate::repo::rows::JsonRow;

use crate::entity::agent_execution_event::upsert_agent_execution_event;
use crate::entity::agent_outbox_event::upsert_agent_outbox_event;
use crate::entity::value::now;
use crate::ops::{
    backoff, format_lease, DeadLetter, DeletionCounts, ExecutionEventRow, NewDeadLetter, OpsLock,
    OpsStore, OutboxEvent, OutboxSnapshot, RetentionStore, OUTBOX_DEAD_RETENTION_DAYS,
    OUTBOX_LEASE_SECONDS, OUTBOX_MAX_ATTEMPTS, OUTBOX_PUBLISHED_RETENTION_DAYS, RECONCILE_LOCK_KEY,
};
use crate::store::StoreResult;
use crate::PgBeamStore;

/// [`OpsStore`] over the beam schema's ops tables.
#[derive(Clone)]
pub struct PgOpsStore {
    db: Db,
}

impl PgOpsStore {
    /// The ops tables of `store`'s database and schema.
    #[must_use]
    pub fn for_store(store: &PgBeamStore) -> Self {
        Self {
            db: store.db().clone(),
        }
    }
}

fn outbox_err(e: impl std::fmt::Display) -> riverbase_core::RiverbaseError {
    crate::BEM_138.with_data(e.to_string())
}

fn ops_err(e: impl std::fmt::Display) -> riverbase_core::RiverbaseError {
    crate::BEM_139.with_data(e.to_string())
}

#[derive(QueryableByName)]
struct SequenceRow {
    #[diesel(sql_type = Integer)]
    event_sequence: i32,
}

#[derive(QueryableByName)]
struct ClaimedRow {
    #[diesel(sql_type = SqlUuid, column_name = _id)]
    id: Uuid,
    #[diesel(sql_type = Nullable<SqlUuid>)]
    event_id: Option<Uuid>,
    #[diesel(sql_type = SqlUuid)]
    execution_id: Uuid,
    #[diesel(sql_type = Integer)]
    sequence: i32,
    #[diesel(sql_type = Text)]
    event_type: String,
    #[diesel(sql_type = Jsonb)]
    payload: Value,
    #[diesel(sql_type = Integer)]
    attempts: i32,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::entity::agent_execution_event::agent_execution_event)]
struct EventRow {
    #[diesel(column_name = _id)]
    id: Uuid,
    sequence: i32,
    event_type: String,
    payload: Value,
}

#[derive(QueryableByName)]
struct SnapshotRow {
    #[diesel(sql_type = BigInt)]
    pending: i64,
    #[diesel(sql_type = BigInt)]
    dead: i64,
    #[diesel(sql_type = Nullable<Double>)]
    oldest: Option<f64>,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::entity::agent_dead_letter::agent_dead_letter)]
struct LetterRow {
    #[diesel(column_name = _id)]
    id: Uuid,
    task_name: String,
    payload: Value,
    error: Option<String>,
    attempts: i32,
    status: String,
    execution_id: Option<Uuid>,
    error_code: Option<String>,
}

impl From<LetterRow> for DeadLetter {
    fn from(row: LetterRow) -> Self {
        Self {
            id: row.id,
            task_name: row.task_name,
            payload: row.payload,
            error: row.error,
            attempts: row.attempts,
            status: row.status,
            execution_id: row.execution_id,
            error_code: row.error_code,
        }
    }
}

#[derive(QueryableByName)]
struct Flag {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    locked: bool,
}

/// `data` with the write flag `_insert`: insert only.
fn insert_only(mut data: Value) -> Value {
    if let Some(object) = data.as_object_mut() {
        object.insert("_insert".to_owned(), Value::Bool(true));
    }
    data
}

/// `v` as an `int4` sequence bound: past its range, its edge.
fn sequence_bound(v: i64) -> i32 {
    i32::try_from(v).unwrap_or(if v < 0 { i32::MIN } else { i32::MAX })
}

#[async_trait]
impl OpsStore for PgOpsStore {
    async fn append_event(
        &self,
        execution_id: Uuid,
        org_id: Option<Uuid>,
        payload: &Value,
        event_type: &str,
    ) -> StoreResult<OutboxEvent> {
        let s = self.db.schema().clone();
        let (event_id, outbox_id) = (Uuid::new_v4(), Uuid::new_v4());
        let (payload, event_type) = (payload.clone(), event_type.to_owned());
        let sequence = self
            .db
            .at(crate::BEM_139)
            .transaction::<_, riverbase_core::RiverbaseError, _>(|tx| {
                let (payload, event_type) = (payload.clone(), event_type.clone());
                Box::pin(async move {
                    // An increment stays SQL.
                    let rows: Vec<SequenceRow> = sql_query(format!(
                        "UPDATE {s}.agent_execution SET event_sequence = event_sequence + 1 \
                         WHERE _id = $1 RETURNING event_sequence"
                    ))
                    .bind::<SqlUuid, _>(execution_id)
                    .load(tx)
                    .await
                    .map_err(outbox_err)?;
                    let Some(row) = rows.into_iter().next() else {
                        return Err(crate::BEM_138.with_data(format!(
                            "execution {execution_id} has no event_sequence row"
                        )));
                    };
                    let sequence = row.event_sequence;
                    // The two rows through their entities (plan 014 D3), on
                    // this transaction.
                    let event = insert_only(json!({
                        "execution_id": execution_id,
                        "org_id": org_id,
                        "sequence": sequence,
                        "event_type": event_type,
                        "payload": payload,
                    }));
                    upsert_agent_execution_event(tx, event_id, &event)
                        .await
                        .map_err(outbox_err)?;
                    // The delivery row points at the event; its payload stays
                    // empty, as Python writes it.
                    let delivery = insert_only(json!({
                        "event_id": event_id,
                        "execution_id": execution_id,
                        "org_id": org_id,
                        "sequence": sequence,
                        "event_type": event_type,
                        "destination": "redis",
                        "payload": {},
                        "sent": false,
                        "attempts": 0,
                    }));
                    upsert_agent_outbox_event(tx, outbox_id, &delivery)
                        .await
                        .map_err(outbox_err)?;
                    Ok(sequence)
                })
            })
            .await?;
        Ok(OutboxEvent {
            id: event_id,
            outbox_id,
            execution_id,
            sequence: i64::from(sequence),
            payload,
            event_type,
            attempts: 0,
        })
    }

    async fn mark_sent(&self, outbox_id: Uuid) -> StoreResult<()> {
        use crate::entity::agent_outbox_event::agent_outbox_event as t;
        self.db
            .at(crate::BEM_139)
            .scoped(|conn| {
                async move {
                    diesel::update(t::table.filter(t::_id.eq(outbox_id)))
                        .set((
                            t::sent.eq(true),
                            t::published_at.eq(now()),
                            t::publisher_lease.eq(None::<String>),
                        ))
                        .execute(&mut *conn)
                        .await
                        .map_err(outbox_err)?;
                    Ok(())
                }
                .scope_boxed()
            })
            .await
    }

    async fn mark_failed(&self, outbox_id: Uuid, error: &str) -> StoreResult<()> {
        use crate::entity::agent_outbox_event::agent_outbox_event as t;
        self.db
            .at(crate::BEM_139)
            .scoped(|conn| {
                async move {
                    let attempts: Option<i32> = t::table
                        .filter(t::_id.eq(outbox_id))
                        .select(t::attempts)
                        .first(&mut *conn)
                        .await
                        .optional()
                        .map_err(outbox_err)?;
                    let attempts = attempts.unwrap_or(0);
                    let next_at =
                        (attempts < OUTBOX_MAX_ATTEMPTS).then(|| Utc::now() + backoff(attempts));
                    let error: String = error.chars().take(500).collect();
                    diesel::update(t::table.filter(t::_id.eq(outbox_id)))
                        .set((
                            t::publisher_lease.eq(None::<String>),
                            t::last_error.eq(error),
                            t::next_attempt_at.eq(next_at),
                        ))
                        .execute(&mut *conn)
                        .await
                        .map_err(outbox_err)?;
                    Ok(())
                }
                .scope_boxed()
            })
            .await
    }

    async fn claim_batch(&self, worker_id: &str, limit: i64) -> StoreResult<Vec<OutboxEvent>> {
        self.db
            .at(crate::BEM_139)
            .scoped(|conn| {
                async move {
                    let s = self.db.schema();
                    let lease = format_lease(
                        worker_id,
                        Utc::now() + Duration::seconds(OUTBOX_LEASE_SECONDS),
                    );
                    // One statement, committed before publishing.
                    // `RETURNING` keeps no order, so the claimed rows are sorted after it:
                    // the relay publishes each execution's frames in sequence.
                    let claimed: Vec<ClaimedRow> = sql_query(format!(
                        "WITH picked AS ( \
                           SELECT _id FROM {s}.agent_outbox_event \
                           WHERE sent = false AND COALESCE(attempts, 0) < $3 \
                             AND (next_attempt_at IS NULL OR next_attempt_at <= NOW()) \
                             AND (publisher_lease IS NULL OR split_part(publisher_lease, ' ', 1)::timestamptz <= NOW()) \
                           ORDER BY _created, sequence LIMIT $2 FOR UPDATE SKIP LOCKED), \
                         claimed AS ( \
                           UPDATE {s}.agent_outbox_event AS outbox \
                           SET publisher_lease = $1, attempts = COALESCE(outbox.attempts, 0) + 1 \
                           FROM picked WHERE outbox._id = picked._id \
                           RETURNING outbox._id, outbox.event_id, outbox.execution_id, outbox.sequence, \
                                     outbox.event_type, outbox.payload, outbox.attempts, outbox._created) \
                         SELECT _id, event_id, execution_id, sequence, event_type, payload, attempts \
                         FROM claimed ORDER BY _created, sequence"
                    ))
                    .bind::<Text, _>(&lease)
                    .bind::<BigInt, _>(limit)
                    .bind::<Integer, _>(OUTBOX_MAX_ATTEMPTS)
                    .load(&mut *conn)
                    .await
                    .map_err(outbox_err)?;
                    let mut events = Vec::with_capacity(claimed.len());
                    for row in claimed {
                        // The frame lives on the event row.
                        let mut payload = row.payload;
                        if let (Some(event_id), true) = (
                            row.event_id,
                            payload.as_object().is_none_or(serde_json::Map::is_empty),
                        ) {
                            use crate::entity::agent_execution_event::agent_execution_event as e;
                            let stored: Option<Value> = e::table
                                .filter(e::_id.eq(event_id))
                                .select(e::payload)
                                .first(&mut *conn)
                                .await
                                .optional()
                                .map_err(outbox_err)?;
                            payload = stored.unwrap_or_else(|| Value::Object(serde_json::Map::new()));
                        }
                        events.push(OutboxEvent {
                            id: row.event_id.unwrap_or(row.id),
                            outbox_id: row.id,
                            execution_id: row.execution_id,
                            sequence: i64::from(row.sequence),
                            payload,
                            event_type: row.event_type,
                            attempts: row.attempts,
                        });
                }
                Ok(events)
                }
                .scope_boxed()
            })
            .await
    }

    async fn release_expired_leases(&self) -> StoreResult<u64> {
        self.db
            .at(crate::BEM_139)
            .scoped(|conn| {
                async move {
                    let s = self.db.schema();
                    let released = sql_query(format!(
                        "UPDATE {s}.agent_outbox_event SET publisher_lease = NULL \
                     WHERE sent = false AND publisher_lease IS NOT NULL \
                       AND split_part(publisher_lease, ' ', 1)::timestamptz <= NOW()"
                    ))
                    .execute(&mut *conn)
                    .await
                    .map_err(outbox_err)?;
                    Ok(released as u64)
                }
                .scope_boxed()
            })
            .await
    }

    async fn snapshot(&self) -> StoreResult<OutboxSnapshot> {
        self.db
            .at(crate::BEM_139)
            .scoped(|conn| {
                async move {
                    let s = self.db.schema();
                    let rows: Vec<SnapshotRow> = sql_query(format!(
                        "SELECT \
                           COUNT(*) FILTER (WHERE sent = false AND COALESCE(attempts, 0) < $1) AS pending, \
                           COUNT(*) FILTER (WHERE sent = false AND COALESCE(attempts, 0) >= $1) AS dead, \
                           EXTRACT(EPOCH FROM (NOW() - MIN(_created) FILTER ( \
                             WHERE sent = false AND COALESCE(attempts, 0) < $1)))::float8 AS oldest \
                         FROM {s}.agent_outbox_event"
                    ))
                    .bind::<Integer, _>(OUTBOX_MAX_ATTEMPTS)
                    .load(&mut *conn)
                    .await
                    .map_err(outbox_err)?;
                    let row = rows.into_iter().next();
                    Ok(OutboxSnapshot {
                        pending: row
                            .as_ref()
                            .map_or(0, |r| u64::try_from(r.pending).unwrap_or(0)),
                        oldest_age_seconds: row.as_ref().and_then(|r| r.oldest).unwrap_or(0.0),
                        dead: row
                            .as_ref()
                            .map_or(0, |r| u64::try_from(r.dead).unwrap_or(0)),
                    })
                }
                .scope_boxed()
            })
            .await
    }

    async fn purge(&self) -> StoreResult<(u64, u64)> {
        use crate::entity::agent_outbox_event::agent_outbox_event as t;
        self.db
            .at(crate::BEM_139)
            .scoped(|conn| {
                async move {
                    let published_before =
                        Utc::now() - Duration::days(OUTBOX_PUBLISHED_RETENTION_DAYS);
                    let published = diesel::delete(
                        t::table
                            .filter(t::sent.eq(true))
                            .filter(t::published_at.is_not_null())
                            .filter(t::published_at.lt(published_before)),
                    )
                    .execute(&mut *conn)
                    .await
                    .map_err(outbox_err)?;
                    let dead_before = Utc::now() - Duration::days(OUTBOX_DEAD_RETENTION_DAYS);
                    let dead = diesel::delete(
                        t::table
                            .filter(t::sent.eq(false))
                            .filter(t::attempts.ge(OUTBOX_MAX_ATTEMPTS))
                            .filter(t::_created.lt(dead_before)),
                    )
                    .execute(&mut *conn)
                    .await
                    .map_err(outbox_err)?;
                    Ok((published as u64, dead as u64))
                }
                .scope_boxed()
            })
            .await
    }

    async fn events_after(
        &self,
        execution_id: Uuid,
        after: i64,
        limit: i64,
    ) -> StoreResult<Vec<ExecutionEventRow>> {
        use crate::entity::agent_execution_event::agent_execution_event as t;
        self.db
            .at(crate::BEM_139)
            .scoped(|conn| {
                async move {
                    // Python `REPLAY_SQL`.
                    let rows: Vec<EventRow> = t::table
                        .filter(t::execution_id.eq(execution_id))
                        .filter(t::sequence.gt(sequence_bound(after)))
                        .order(t::sequence.asc())
                        .limit(limit)
                        .select(EventRow::as_select())
                        .load(&mut *conn)
                        .await
                        .map_err(outbox_err)?;
                    Ok(rows
                        .into_iter()
                        .map(|r| ExecutionEventRow {
                            id: r.id,
                            sequence: i64::from(r.sequence),
                            event_type: r.event_type,
                            payload: r.payload,
                        })
                        .collect())
                }
                .scope_boxed()
            })
            .await
    }

    async fn event_sequence(&self, event_id: Uuid) -> StoreResult<Option<i64>> {
        use crate::entity::agent_execution_event::agent_execution_event as t;
        self.db
            .at(crate::BEM_139)
            .scoped(|conn| {
                async move {
                    let sequence: Option<i32> = t::table
                        .filter(t::_id.eq(event_id))
                        .select(t::sequence)
                        .first(&mut *conn)
                        .await
                        .optional()
                        .map_err(outbox_err)?;
                    Ok(sequence.map(i64::from))
                }
                .scope_boxed()
            })
            .await
    }

    async fn enqueue_dead_letter(&self, letter: NewDeadLetter) -> StoreResult<DeadLetter> {
        use crate::entity::agent_dead_letter::{agent_dead_letter as t, upsert_agent_dead_letter};
        self.db
            .at(crate::BEM_139)
            .scoped(|conn| {
                async move {
                    let id = Uuid::new_v4();
                    let data = insert_only(json!({
                        "task_name": letter.task_name,
                        "payload": letter.payload,
                        "error": letter.error,
                        "attempts": 0,
                        "status": "pending",
                        "execution_id": letter.execution_id,
                        "org_id": letter.org_id,
                        "error_code": letter.error_code,
                    }));
                    upsert_agent_dead_letter(&mut *conn, id, &data)
                        .await
                        .map_err(ops_err)?;
                    let row: Option<LetterRow> = t::table
                        .filter(t::_id.eq(id))
                        .select(LetterRow::as_select())
                        .first(&mut *conn)
                        .await
                        .optional()
                        .map_err(ops_err)?;
                    row.map(DeadLetter::from)
                        .ok_or_else(|| ops_err("insert returned no row"))
                }
                .scope_boxed()
            })
            .await
    }

    async fn list_dead_letters(&self, status: &str) -> StoreResult<Vec<DeadLetter>> {
        use crate::entity::agent_dead_letter::agent_dead_letter as t;
        self.db
            .at(crate::BEM_139)
            .scoped(|conn| {
                async move {
                    let rows: Vec<LetterRow> = t::table
                        .filter(t::status.eq(status))
                        .filter(t::_deleted.is_null())
                        .order(t::_created.asc())
                        .select(LetterRow::as_select())
                        .load(&mut *conn)
                        .await
                        .map_err(ops_err)?;
                    Ok(rows.into_iter().map(DeadLetter::from).collect())
                }
                .scope_boxed()
            })
            .await
    }

    async fn retry_dead_letter(&self, id: Uuid) -> StoreResult<DeadLetter> {
        use crate::entity::agent_dead_letter::agent_dead_letter as t;
        self.db
            .at(crate::BEM_139)
            .scoped(|conn| {
                async move {
                    let row: Option<LetterRow> = diesel::update(t::table.filter(t::_id.eq(id)))
                        .set((
                            t::attempts.eq(t::attempts + 1),
                            t::status.eq("pending"),
                            t::_updated.eq(now()),
                        ))
                        .returning(LetterRow::as_returning())
                        .get_result(&mut *conn)
                        .await
                        .optional()
                        .map_err(ops_err)?;
                    row.map(DeadLetter::from)
                        .ok_or_else(|| crate::BEM_488.with_data(id.to_string()))
                }
                .scope_boxed()
            })
            .await
    }

    async fn discard_dead_letter(&self, id: Uuid) -> StoreResult<DeadLetter> {
        use crate::entity::agent_dead_letter::agent_dead_letter as t;
        self.db
            .at(crate::BEM_139)
            .scoped(|conn| {
                async move {
                    let row: Option<LetterRow> = diesel::update(t::table.filter(t::_id.eq(id)))
                        .set((t::status.eq("discarded"), t::_updated.eq(now())))
                        .returning(LetterRow::as_returning())
                        .get_result(&mut *conn)
                        .await
                        .optional()
                        .map_err(ops_err)?;
                    row.map(DeadLetter::from)
                        .ok_or_else(|| crate::BEM_488.with_data(id.to_string()))
                }
                .scope_boxed()
            })
            .await
    }

    async fn ping(&self) -> StoreResult<()> {
        self.db
            .at(crate::BEM_139)
            .scoped(|conn| {
                async move {
                    sql_query("SELECT 1")
                        .execute(&mut *conn)
                        .await
                        .map_err(ops_err)?;
                    Ok(())
                }
                .scope_boxed()
            })
            .await
    }

    async fn schema_version(&self) -> StoreResult<Option<i32>> {
        crate::migrate::applied_version(self.db.pool(), self.db.schema()).await
    }

    async fn try_reconcile_lock(&self) -> StoreResult<Option<OpsLock>> {
        // A session lock on a connection taken out of the pool: released
        // explicitly, or by closing the connection when the guard drops.
        let mut conn = self.db.detached(crate::BEM_139).await?;
        let rows: Vec<Flag> = sql_query("SELECT pg_try_advisory_lock(hashtext($1)) AS locked")
            .bind::<Text, _>(RECONCILE_LOCK_KEY)
            .load(&mut conn)
            .await
            .map_err(ops_err)?;
        Ok(rows
            .into_iter()
            .next()
            .is_some_and(|r| r.locked)
            .then(|| OpsLock::Pg(Box::new(conn))))
    }
}

/// Deletes in foreign-key order: children of the user's sessions and
/// executions first, then executions, sessions and the user's own rows.
/// `$1` is the user id as text; `S` and `E` are the user's sessions and their
/// executions.
const DELETE_USER_DATA: &[(&str, &str)] = &[
    ("agent_action", "session_id::text IN (S)"),
    ("agent_message", "session_id::text IN (S)"),
    ("agent_suggestion", "session_id::text IN (S)"),
    // rc8's context and governance tables (migration 0010).
    ("agent_session_summary", "session_id::text IN (S)"),
    ("agent_session_note", "session_id::text IN (S)"),
    ("agent_tool_artifact", "session_id::text IN (S)"),
    ("agent_tool_grant", "session_id::text IN (S) OR user_id::text = $1"),
    ("agent_tool_rule", "user_id::text = $1"),
    ("agent_feedback", "execution_id::text IN (E)"),
    ("agent_artifact", "session_id::text IN (S) OR execution_id::text IN (E)"),
    ("agent_context_manifest", "execution_id::text IN (E)"),
    ("tool_call_idempotency", "execution_id::text IN (E)"),
    ("runtime_state", "execution_id::text IN (E)"),
    ("agent_execution_event", "execution_id::text IN (E)"),
    ("agent_outbox_event", "execution_id::text IN (E)"),
    ("agent_dead_letter", "execution_id::text IN (E)"),
    (
        "agent_security_rule_narration",
        "decision_id::text IN (SELECT _id::text FROM {s}.agent_security_decision WHERE session_id::text IN (S))",
    ),
    ("agent_security_decision", "session_id::text IN (S)"),
    ("execution_concurrency_slot", "execution_id::text IN (E)"),
    ("execution_idempotency", "execution_id::text IN (E)"),
    ("agent_step", "session_id::text IN (S)"),
    ("agent_run", "session_id::text IN (S)"),
    ("agent_interaction", "session_id::text IN (S)"),
    ("agent_execution", "session_id::text IN (S)"),
    ("agent_event", "user_id::text = $1 OR session_id::text IN (S)"),
    ("agent_session", "user_id::text = $1"),
    ("agent_memory", "user_id::text = $1"),
    (
        "agent_knowledge_chunk",
        "user_id::text = $1 OR document_id::text IN (SELECT _id::text FROM {s}.agent_knowledge_document WHERE user_id::text = $1)",
    ),
    ("agent_knowledge_document", "user_id::text = $1"),
    ("beam_vector_json", "user_id::text = $1"),
];

#[async_trait]
impl RetentionStore for PgBeamStore {
    async fn delete_user_data(&self, user_id: &str) -> StoreResult<DeletionCounts> {
        let s = self.schema().clone();
        let user = user_id.to_owned();
        self.db()
            .at(crate::BEM_139)
            .transaction::<_, riverbase_core::RiverbaseError, _>(|tx| {
            Box::pin(async move {
                // A legal hold refuses the deletion, checked under the hold
                // lock before any row goes: a hold placed by any
                // process either commits first and is seen, or waits.
                crate::holds::lock(tx, &user).await?;
                if crate::holds::is_held(tx, &s, &user).await? {
                    return Err(crate::BEM_489.with_data(json!({ "user_id": user })));
                }
                // Ids compare as text: some tables keep them as varchar.
                let sessions = format!("SELECT _id::text FROM {s}.agent_session WHERE user_id::text = $1");
                let executions =
                    format!("SELECT _id::text FROM {s}.agent_execution WHERE session_id::text IN ({sessions})");
                let mut counts = DeletionCounts::new();
                for (table, filter) in DELETE_USER_DATA {
                    let filter = filter
                        .replace("{s}", &s.to_string())
                        .replace("(S)", &format!("({sessions})"))
                        .replace("(E)", &format!("({executions})"));
                    let deleted = sql_query(format!("DELETE FROM {s}.{table} WHERE {filter}"))
                        .bind::<Text, _>(&user)
                        .execute(tx)
                        .await
                        .map_err(ops_err)?;
                    counts.insert((*table).to_owned(), deleted as u64);
                }
                Ok(counts)
            })
        })
        .await
    }

    async fn export_session(&self, session_id: Uuid) -> StoreResult<Value> {
        let s = self.schema();
        self.db()
            .at(crate::BEM_139)
            .scoped(|conn| {
                async move {
        let load = |sql: String| sql_query(sql).bind::<SqlUuid, _>(session_id);
        let session: Vec<JsonRow> = load(format!(
            "SELECT (to_jsonb(t) - '_txt') AS row FROM {s}.agent_session t WHERE _id = $1"
        ))
        .load(&mut *conn)
        .await
        .map_err(ops_err)?;
        let messages: Vec<JsonRow> = load(format!(
            "SELECT (to_jsonb(t) - '_txt') AS row FROM {s}.agent_message t WHERE session_id = $1 ORDER BY sequence"
        ))
        .load(&mut *conn)
        .await
        .map_err(ops_err)?;
        let executions: Vec<JsonRow> = load(format!(
            "SELECT to_jsonb(t._id::text) AS row FROM {s}.agent_execution t WHERE session_id = $1 ORDER BY created_at"
        ))
        .load(&mut *conn)
        .await
        .map_err(ops_err)?;
        Ok(serde_json::json!({
            "session_id": session_id.to_string(),
            "session": session.into_iter().next().map_or(Value::Null, |r| r.row),
            "messages": messages.into_iter().map(|r| r.row).collect::<Vec<_>>(),
            "executions": executions.into_iter().map(|r| r.row).collect::<Vec<_>>(),
        }))
                }
                .scope_boxed()
            })
            .await
    }
}
