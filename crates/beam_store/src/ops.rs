//! Stream outbox, dead letters, legal holds and the reconcile lock (Python
//! `store/ops/`: `outbox.py`, `dlq.py`, `holds.py`).
//!
//! [`OpsStore`] has an in-memory implementation ([`InMemoryOps`]) and a
//! Postgres one ([`crate::PgOpsStore`]) over `agent_execution_event`,
//! `agent_outbox_event` and `agent_dead_letter`.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

use crate::store::StoreResult;

/// Rows one relay pass claims.
pub const OUTBOX_CLAIM_BATCH: i64 = 100;
/// After this many claims a row is dead.
pub const OUTBOX_MAX_ATTEMPTS: i32 = 8;
/// How long a claimed row is leased.
pub const OUTBOX_LEASE_SECONDS: i64 = 30;
/// Days a published row is kept.
pub const OUTBOX_PUBLISHED_RETENTION_DAYS: i64 = 7;
/// Days a dead row is kept.
pub const OUTBOX_DEAD_RETENTION_DAYS: i64 = 30;
/// The reconcile lock's key, hashed into a Postgres advisory lock both stacks take.
pub const RECONCILE_LOCK_KEY: &str = "beam:reconcile:lock";

/// Expiry first, so SQL can compare its first word.
#[must_use]
pub fn format_lease(worker_id: &str, expires_at: DateTime<Utc>) -> String {
    format!(
        "{} {worker_id}",
        expires_at.to_rfc3339_opts(chrono::SecondsFormat::Micros, false)
    )
}

fn lease_expiry(lease: Option<&str>) -> Option<DateTime<Utc>> {
    let raw = lease?.split(' ').next()?;
    DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|t| t.with_timezone(&Utc))
}

/// `min(60, 2**max(attempts, 1))` seconds.
#[must_use]
pub fn backoff(attempts: i32) -> Duration {
    let exponent = u32::try_from(attempts.max(1)).unwrap_or(1).min(6);
    Duration::seconds(i64::from(2_u32.pow(exponent)).min(60))
}

/// A durable frame and its delivery row: `id` is the
/// `agent_execution_event` id (the replay cursor), `outbox_id` the delivery row.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct OutboxEvent {
    pub id: Uuid,
    pub outbox_id: Uuid,
    pub execution_id: Uuid,
    pub sequence: i64,
    pub payload: Value,
    pub event_type: String,
    pub attempts: i32,
}

/// The outbox gauges.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize)]
pub struct OutboxSnapshot {
    pub pending: u64,
    pub oldest_age_seconds: f64,
    pub dead: u64,
}

/// One `agent_execution_event` row, for replay.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionEventRow {
    pub id: Uuid,
    pub sequence: i64,
    pub event_type: String,
    pub payload: Value,
}

/// A failed task kept for retry or discard.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DeadLetter {
    pub id: Uuid,
    pub task_name: String,
    pub payload: Value,
    pub error: Option<String>,
    pub attempts: i32,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
}

/// A dead letter to enqueue.
#[derive(Debug, Clone, Default)]
pub struct NewDeadLetter {
    pub task_name: String,
    pub payload: Value,
    pub error: Option<String>,
    pub execution_id: Option<Uuid>,
    pub org_id: Option<Uuid>,
    pub error_code: Option<String>,
}

/// Held while reconcile runs; released with [`OpsLock::release`]. Dropping it
/// unreleased also frees the lock (the Postgres connection is closed).
pub enum OpsLock {
    Memory(tokio::sync::OwnedMutexGuard<()>),
    Pg(Box<diesel_async::AsyncPgConnection>),
}

impl OpsLock {
    /// Release the lock.
    pub async fn release(self) {
        if let Self::Pg(mut conn) = self {
            use diesel_async::RunQueryDsl;
            let unlock = diesel::sql_query("SELECT pg_advisory_unlock(hashtext($1))")
                .bind::<diesel::sql_types::Text, _>(RECONCILE_LOCK_KEY);
            if let Err(e) = unlock.execute(&mut *conn).await {
                tracing::warn!(error = %e, "reconcile lock release failed; closing its connection");
            }
        }
    }
}

/// The stream outbox, dead letters and the reconcile lock.
#[async_trait]
pub trait OpsStore: Send + Sync {
    /// In one transaction, the execution's next
    /// sequence, the `agent_execution_event` row and an unsent outbox row.
    /// `payload` is stored as given (already privacy-filtered).
    async fn append_event(
        &self,
        execution_id: Uuid,
        org_id: Option<Uuid>,
        payload: &Value,
        event_type: &str,
    ) -> StoreResult<OutboxEvent>;

    /// Python `mark_sent`.
    async fn mark_sent(&self, outbox_id: Uuid) -> StoreResult<()>;

    /// Drop the lease, keep the error, back off (a row
    /// past [`OUTBOX_MAX_ATTEMPTS`] is not retried).
    async fn mark_failed(&self, outbox_id: Uuid, error: &str) -> StoreResult<()>;

    /// Lease up to `limit` due rows to `worker_id` and
    /// count the attempt. The caller publishes after this returns.
    async fn claim_batch(&self, worker_id: &str, limit: i64) -> StoreResult<Vec<OutboxEvent>>;

    /// Python `release_expired_leases`.
    async fn release_expired_leases(&self) -> StoreResult<u64>;

    /// Python `snapshot`.
    async fn snapshot(&self) -> StoreResult<OutboxSnapshot>;

    /// Sent rows after 7 days, dead rows after 30.
    /// Returns `(published, dead)`.
    async fn purge(&self) -> StoreResult<(u64, u64)>;

    /// Durable events after `after`, in sequence order (replay).
    async fn events_after(
        &self,
        execution_id: Uuid,
        after: i64,
        limit: i64,
    ) -> StoreResult<Vec<ExecutionEventRow>>;

    /// The sequence of a stored event (a non-numeric `Last-Event-ID`).
    async fn event_sequence(&self, event_id: Uuid) -> StoreResult<Option<i64>>;

    /// Enqueue a dead letter.
    async fn enqueue_dead_letter(&self, letter: NewDeadLetter) -> StoreResult<DeadLetter>;

    /// Dead letters with `status`, oldest first.
    async fn list_dead_letters(&self, status: &str) -> StoreResult<Vec<DeadLetter>>;

    /// One more attempt, status `pending`. `BEM-488` when unknown.
    async fn retry_dead_letter(&self, id: Uuid) -> StoreResult<DeadLetter>;

    /// Status `discarded`. `BEM-488` when unknown.
    async fn discard_dead_letter(&self, id: Uuid) -> StoreResult<DeadLetter>;

    /// Try to take the reconcile lock; `None` when another reconciler holds it.
    async fn try_reconcile_lock(&self) -> StoreResult<Option<OpsLock>>;

    /// Readiness: the database answers (no-op in memory).
    async fn ping(&self) -> StoreResult<()> {
        Ok(())
    }

    /// Readiness: the applied beam migration, when there is a database.
    async fn schema_version(&self) -> StoreResult<Option<i32>> {
        Ok(None)
    }
}

#[derive(Debug, Clone)]
struct OutboxRow {
    event: OutboxEvent,
    sent: bool,
    next_attempt_at: Option<DateTime<Utc>>,
    lease: Option<String>,
    published_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    last_error: Option<String>,
}

#[derive(Default)]
struct OpsTables {
    outbox: Vec<OutboxRow>,
    sequences: HashMap<Uuid, i64>,
    letters: Vec<DeadLetter>,
}

/// In-process [`OpsStore`].
pub struct InMemoryOps {
    tables: Mutex<OpsTables>,
    lock: Arc<tokio::sync::Mutex<()>>,
}

impl Default for InMemoryOps {
    fn default() -> Self {
        Self {
            tables: Mutex::new(OpsTables::default()),
            lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }
}

impl InMemoryOps {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn tables(&self) -> StoreResult<std::sync::MutexGuard<'_, OpsTables>> {
        self.tables
            .lock()
            .map_err(|e| crate::BEM_139.with_data(e.to_string()))
    }
}

fn find_letter(letters: &mut [DeadLetter], id: Uuid) -> StoreResult<&mut DeadLetter> {
    letters
        .iter_mut()
        .find(|l| l.id == id)
        .ok_or_else(|| crate::BEM_488.with_data(id.to_string()))
}

#[async_trait]
impl OpsStore for InMemoryOps {
    async fn append_event(
        &self,
        execution_id: Uuid,
        _org_id: Option<Uuid>,
        payload: &Value,
        event_type: &str,
    ) -> StoreResult<OutboxEvent> {
        let mut t = self.tables()?;
        let sequence = t.sequences.entry(execution_id).or_insert(0);
        *sequence += 1;
        let id = Uuid::new_v4();
        let event = OutboxEvent {
            id,
            outbox_id: id,
            execution_id,
            sequence: *sequence,
            payload: payload.clone(),
            event_type: event_type.to_owned(),
            attempts: 0,
        };
        t.outbox.push(OutboxRow {
            event: event.clone(),
            sent: false,
            next_attempt_at: None,
            lease: None,
            published_at: None,
            created_at: Utc::now(),
            last_error: None,
        });
        Ok(event)
    }

    async fn mark_sent(&self, outbox_id: Uuid) -> StoreResult<()> {
        let mut t = self.tables()?;
        if let Some(row) = t.outbox.iter_mut().find(|r| r.event.outbox_id == outbox_id) {
            row.sent = true;
            row.published_at = Some(Utc::now());
            row.lease = None;
        }
        Ok(())
    }

    async fn mark_failed(&self, outbox_id: Uuid, error: &str) -> StoreResult<()> {
        let mut t = self.tables()?;
        if let Some(row) = t.outbox.iter_mut().find(|r| r.event.outbox_id == outbox_id) {
            row.lease = None;
            row.last_error = Some(error.chars().take(500).collect());
            row.next_attempt_at = (row.event.attempts < OUTBOX_MAX_ATTEMPTS)
                .then(|| Utc::now() + backoff(row.event.attempts));
        }
        Ok(())
    }

    async fn claim_batch(&self, worker_id: &str, limit: i64) -> StoreResult<Vec<OutboxEvent>> {
        let now = Utc::now();
        let lease = format_lease(worker_id, now + Duration::seconds(OUTBOX_LEASE_SECONDS));
        let mut t = self.tables()?;
        let mut claimed = Vec::new();
        for row in &mut t.outbox {
            if usize::try_from(limit).is_ok_and(|l| claimed.len() >= l) {
                break;
            }
            if row.sent || row.event.attempts >= OUTBOX_MAX_ATTEMPTS {
                continue;
            }
            if row.next_attempt_at.is_some_and(|at| at > now) {
                continue;
            }
            if lease_expiry(row.lease.as_deref()).is_some_and(|e| e > now) {
                continue;
            }
            row.event.attempts += 1;
            row.lease = Some(lease.clone());
            claimed.push(row.event.clone());
        }
        Ok(claimed)
    }

    async fn release_expired_leases(&self) -> StoreResult<u64> {
        let now = Utc::now();
        let mut t = self.tables()?;
        let mut released = 0;
        for row in &mut t.outbox {
            if row.sent
                || row.lease.is_none()
                || lease_expiry(row.lease.as_deref()).is_some_and(|e| e > now)
            {
                continue;
            }
            row.lease = None;
            released += 1;
        }
        Ok(released)
    }

    #[allow(clippy::cast_precision_loss)] // Ages in ms, far below 2^52.
    async fn snapshot(&self) -> StoreResult<OutboxSnapshot> {
        let now = Utc::now();
        let t = self.tables()?;
        let pending: Vec<&OutboxRow> = t
            .outbox
            .iter()
            .filter(|r| !r.sent && r.event.attempts < OUTBOX_MAX_ATTEMPTS)
            .collect();
        let dead = t
            .outbox
            .iter()
            .filter(|r| !r.sent && r.event.attempts >= OUTBOX_MAX_ATTEMPTS)
            .count();
        let oldest = pending.iter().map(|r| r.created_at).min();
        Ok(OutboxSnapshot {
            pending: pending.len() as u64,
            oldest_age_seconds: oldest
                .map_or(0.0, |o| (now - o).num_milliseconds().max(0) as f64 / 1000.0),
            dead: dead as u64,
        })
    }

    async fn purge(&self) -> StoreResult<(u64, u64)> {
        let now = Utc::now();
        let published_before = now - Duration::days(OUTBOX_PUBLISHED_RETENTION_DAYS);
        let dead_before = now - Duration::days(OUTBOX_DEAD_RETENTION_DAYS);
        let mut t = self.tables()?;
        let (mut published, mut dead) = (0, 0);
        t.outbox.retain(|r| {
            if r.sent && r.published_at.is_some_and(|p| p <= published_before) {
                published += 1;
                return false;
            }
            if !r.sent && r.event.attempts >= OUTBOX_MAX_ATTEMPTS && r.created_at <= dead_before {
                dead += 1;
                return false;
            }
            true
        });
        Ok((published, dead))
    }

    async fn events_after(
        &self,
        execution_id: Uuid,
        after: i64,
        limit: i64,
    ) -> StoreResult<Vec<ExecutionEventRow>> {
        let t = self.tables()?;
        let mut rows: Vec<ExecutionEventRow> = t
            .outbox
            .iter()
            .filter(|r| r.event.execution_id == execution_id && r.event.sequence > after)
            .map(|r| ExecutionEventRow {
                id: r.event.id,
                sequence: r.event.sequence,
                event_type: r.event.event_type.clone(),
                payload: r.event.payload.clone(),
            })
            .collect();
        rows.sort_by_key(|r| r.sequence);
        rows.truncate(usize::try_from(limit).unwrap_or(0));
        Ok(rows)
    }

    async fn event_sequence(&self, event_id: Uuid) -> StoreResult<Option<i64>> {
        let t = self.tables()?;
        Ok(t.outbox
            .iter()
            .find(|r| r.event.id == event_id)
            .map(|r| r.event.sequence))
    }

    async fn enqueue_dead_letter(&self, letter: NewDeadLetter) -> StoreResult<DeadLetter> {
        let row = DeadLetter {
            id: Uuid::new_v4(),
            task_name: letter.task_name,
            payload: letter.payload,
            error: letter.error,
            attempts: 0,
            status: "pending".into(),
            execution_id: letter.execution_id,
            error_code: letter.error_code,
        };
        self.tables()?.letters.push(row.clone());
        Ok(row)
    }

    async fn list_dead_letters(&self, status: &str) -> StoreResult<Vec<DeadLetter>> {
        Ok(self
            .tables()?
            .letters
            .iter()
            .filter(|l| l.status == status)
            .cloned()
            .collect())
    }

    async fn retry_dead_letter(&self, id: Uuid) -> StoreResult<DeadLetter> {
        let mut t = self.tables()?;
        let letter = find_letter(&mut t.letters, id)?;
        letter.attempts += 1;
        letter.status = "pending".into();
        Ok(letter.clone())
    }

    async fn discard_dead_letter(&self, id: Uuid) -> StoreResult<DeadLetter> {
        let mut t = self.tables()?;
        let letter = find_letter(&mut t.letters, id)?;
        letter.status = "discarded".into();
        Ok(letter.clone())
    }

    async fn try_reconcile_lock(&self) -> StoreResult<Option<OpsLock>> {
        Ok(Arc::clone(&self.lock)
            .try_lock_owned()
            .ok()
            .map(OpsLock::Memory))
    }
}

/// Users whose data may not be deleted. Postgres holds
/// ([`PgLegalHolds`]) are seen by every process; [`LegalHolds`] is the
/// in-memory book for beams without a database.
#[async_trait]
pub trait LegalHoldStore: Send + Sync {
    /// Hold `user_id`; placing it again replaces the reason, org and placer.
    async fn place(
        &self,
        user_id: &str,
        org_id: Option<Uuid>,
        reason: Option<&str>,
        placed_by: Option<&str>,
    ) -> StoreResult<()>;

    /// Release `user_id`'s hold. Returns whether there was one.
    async fn release(&self, user_id: &str) -> StoreResult<bool>;

    /// Release `user_id`'s hold only if `placed_by` placed it. Returns
    /// whether it went.
    async fn release_placed_by(&self, user_id: &str, placed_by: &str) -> StoreResult<bool>;

    /// Whether `user_id` is held. An error must refuse the deletion.
    async fn is_held(&self, user_id: &str) -> StoreResult<bool>;
}

/// Users whose data may not be deleted, in process
/// (beams without a database).
#[derive(Debug, Default)]
pub struct LegalHolds {
    /// Held users ([`crate::holds::key`]) and who placed each hold.
    held: Mutex<HashMap<String, Option<String>>>,
}

impl LegalHolds {
    fn book(&self) -> StoreResult<std::sync::MutexGuard<'_, HashMap<String, Option<String>>>> {
        self.held
            .lock()
            .map_err(|_| crate::BEM_139.with_data("legal hold book poisoned"))
    }
}

#[async_trait]
impl LegalHoldStore for LegalHolds {
    async fn place(
        &self,
        user_id: &str,
        _org_id: Option<Uuid>,
        _reason: Option<&str>,
        placed_by: Option<&str>,
    ) -> StoreResult<()> {
        self.book()?
            .insert(crate::holds::key(user_id), placed_by.map(str::to_owned));
        Ok(())
    }

    async fn release(&self, user_id: &str) -> StoreResult<bool> {
        Ok(self.book()?.remove(&crate::holds::key(user_id)).is_some())
    }

    async fn release_placed_by(&self, user_id: &str, placed_by: &str) -> StoreResult<bool> {
        let mut book = self.book()?;
        let key = crate::holds::key(user_id);
        if book.get(&key).and_then(Option::as_deref) == Some(placed_by) {
            book.remove(&key);
            return Ok(true);
        }
        Ok(false)
    }

    /// Fails closed: a poisoned book is an error.
    async fn is_held(&self, user_id: &str) -> StoreResult<bool> {
        Ok(self.book()?.contains_key(&crate::holds::key(user_id)))
    }
}

/// [`LegalHoldStore`] over `agent_legal_hold` (migration 0009).
#[derive(Clone)]
pub struct PgLegalHolds {
    db: crate::Db,
}

impl PgLegalHolds {
    /// The holds of `store`'s database and schema.
    #[must_use]
    pub fn for_store(store: &crate::PgBeamStore) -> Self {
        Self {
            db: store.db().clone(),
        }
    }
}

#[async_trait]
impl LegalHoldStore for PgLegalHolds {
    async fn place(
        &self,
        user_id: &str,
        org_id: Option<Uuid>,
        reason: Option<&str>,
        placed_by: Option<&str>,
    ) -> StoreResult<()> {
        let schema = self.db.schema().clone();
        let (user, reason, placed_by) = (
            user_id.to_owned(),
            reason.map(str::to_owned),
            placed_by.map(str::to_owned),
        );
        self.db
            .at(crate::BEM_139)
            .transaction::<_, riverbase_core::RiverbaseError, _>(|tx| {
                Box::pin(async move {
                    crate::holds::lock(tx, &user).await?;
                    crate::holds::place(
                        tx,
                        &schema,
                        &user,
                        org_id,
                        reason.as_deref(),
                        placed_by.as_deref(),
                    )
                    .await
                })
            })
            .await
    }

    async fn release(&self, user_id: &str) -> StoreResult<bool> {
        let schema = self.db.schema().clone();
        let user = user_id.to_owned();
        self.db
            .at(crate::BEM_139)
            .transaction::<_, riverbase_core::RiverbaseError, _>(|tx| {
                Box::pin(async move {
                    crate::holds::lock(tx, &user).await?;
                    crate::holds::release(tx, &schema, &user).await
                })
            })
            .await
    }

    async fn release_placed_by(&self, user_id: &str, placed_by: &str) -> StoreResult<bool> {
        let schema = self.db.schema().clone();
        let (user, placed_by) = (user_id.to_owned(), placed_by.to_owned());
        self.db
            .at(crate::BEM_139)
            .transaction::<_, riverbase_core::RiverbaseError, _>(|tx| {
                Box::pin(async move {
                    crate::holds::lock(tx, &user).await?;
                    crate::holds::release_placed_by(tx, &schema, &user, &placed_by).await
                })
            })
            .await
    }

    async fn is_held(&self, user_id: &str) -> StoreResult<bool> {
        // The hold table has no RLS: every org's holds are read.
        self.db
            .at(crate::BEM_139)
            .cross_org(|conn| {
                Box::pin(
                    async move { crate::holds::is_held(conn, self.db.schema(), user_id).await },
                )
            })
            .await
    }
}

impl LegalHolds {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

/// Rows removed per table by [`RetentionStore::delete_user_data`].
pub type DeletionCounts = BTreeMap<String, u64>;

/// Data retention.
#[async_trait]
pub trait RetentionStore: Send + Sync {
    /// Delete everything a user's sessions and executions hold, the user's
    /// audit events, memories and knowledge documents, in one transaction.
    /// Usage and cost records stay (billing). Returns rows per table.
    async fn delete_user_data(&self, user_id: &str) -> StoreResult<DeletionCounts>;

    /// The session row, its messages and its execution ids as JSON (Python
    /// `export_session`, before `redact_secrets`). `None` session when unknown.
    async fn export_session(&self, session_id: Uuid) -> StoreResult<Value>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn sequences_are_per_execution_and_claims_lease_rows() {
        let ops = InMemoryOps::new();
        let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
        let first = ops
            .append_event(a, None, &json!({"type": "started"}), "started")
            .await
            .unwrap();
        ops.append_event(b, None, &json!({}), "done").await.unwrap();
        let second = ops.append_event(a, None, &json!({}), "done").await.unwrap();
        assert_eq!((first.sequence, second.sequence), (1, 2));

        let claimed = ops.claim_batch("w1", 10).await.unwrap();
        assert_eq!(claimed.len(), 3);
        assert!(
            ops.claim_batch("w2", 10).await.unwrap().is_empty(),
            "leased rows are skipped"
        );
        ops.mark_sent(first.outbox_id).await.unwrap();
        ops.mark_failed(second.outbox_id, "nats down")
            .await
            .unwrap();
        let snapshot = ops.snapshot().await.unwrap();
        assert_eq!(snapshot.pending, 2);
        assert!(
            ops.claim_batch("w2", 10).await.unwrap().is_empty(),
            "a failed row backs off"
        );

        let replay = ops.events_after(a, 1, 10).await.unwrap();
        assert_eq!(replay.iter().map(|e| e.sequence).collect::<Vec<_>>(), [2]);
        assert_eq!(ops.event_sequence(second.id).await.unwrap(), Some(2));
    }

    #[tokio::test]
    async fn dead_letters_retry_and_discard() {
        let ops = InMemoryOps::new();
        let letter = ops
            .enqueue_dead_letter(NewDeadLetter {
                task_name: "invoke_agent".into(),
                payload: json!({}),
                ..NewDeadLetter::default()
            })
            .await
            .unwrap();
        assert_eq!(ops.retry_dead_letter(letter.id).await.unwrap().attempts, 1);
        assert_eq!(
            ops.discard_dead_letter(letter.id).await.unwrap().status,
            "discarded"
        );
        assert!(ops.list_dead_letters("pending").await.unwrap().is_empty());
        let err = ops.retry_dead_letter(Uuid::new_v4()).await.unwrap_err();
        assert_eq!(err.errcode.as_str(), "BEM-488");
    }

    #[tokio::test]
    async fn one_reconciler_at_a_time() {
        let ops = InMemoryOps::new();
        let held = ops.try_reconcile_lock().await.unwrap().expect("free");
        assert!(ops.try_reconcile_lock().await.unwrap().is_none());
        held.release().await;
        assert!(ops.try_reconcile_lock().await.unwrap().is_some());
    }

    #[test]
    fn leases_and_backoff_follow_python() {
        let at = DateTime::parse_from_rfc3339("2026-10-18T12:00:00.5+00:00")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(format_lease("w", at), "2026-10-18T12:00:00.500000+00:00 w");
        assert_eq!(lease_expiry(Some(&format_lease("w", at))), Some(at));
        assert_eq!(backoff(0), Duration::seconds(2));
        assert_eq!(backoff(3), Duration::seconds(8));
        assert_eq!(backoff(9), Duration::seconds(60));
    }
}
