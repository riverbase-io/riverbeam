//! Usage, `db.usage()`: counters, usage and cost events, plan profiles and
//! token reservations on fluvius-beam's tables (Python
//! `usage/stores/sql.py`). Rows key users by UUID, as Python's schema does.
//! Reservations are serialized per user under Python's advisory lock key, so
//! both stacks see each other's holds.

use diesel::sql_query;
use diesel::sql_types::{BigInt, Double, Integer, Nullable, Text, Uuid as SqlUuid};
use diesel::{ExpressionMethods, OptionalExtension, QueryDsl, QueryableByName};
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::RunQueryDsl;
use serde_json::Value;
use uuid::Uuid;

use crate::db::{Db, DbError, DbResult};
use crate::entity::agent_cost_event::upsert_agent_cost_event;
use crate::entity::agent_usage_event::upsert_agent_usage_event;
use crate::repo::rows::Counted;

const TOUCH: &str = "_updated = now(), _etag = gen_random_uuid()";

#[derive(QueryableByName)]
struct ValueRow {
    #[diesel(sql_type = Integer)]
    value: i32,
}

#[derive(QueryableByName)]
struct FieldRow {
    #[diesel(sql_type = Text)]
    field: String,
    #[diesel(sql_type = Integer)]
    value: i32,
}

#[derive(QueryableByName)]
struct HeldRow {
    #[diesel(sql_type = BigInt)]
    held: i64,
}

/// A token reservation to take.
#[derive(Debug, Clone)]
pub struct NewReservation {
    pub id: Uuid,
    pub user_id: Uuid,
    /// The user id as the caller gave it: Python's lock key hashes it.
    pub lock_key: String,
    pub org_id: Option<Uuid>,
    pub tokens: i32,
    pub execution_id: Option<Uuid>,
    pub ttl_seconds: f64,
    pub owner: Option<String>,
    /// Refuse (`BEM-362`) when today's usage plus the live holds plus this
    /// one would pass the limit.
    pub daily_limit: Option<u64>,
    pub usage_tokens: u64,
}

/// Usage on a [`Db`].
pub struct Usage<'d> {
    db: &'d Db,
}

impl Db {
    /// Usage counters, events, plan profiles and reservations.
    #[must_use]
    pub fn usage(&self) -> Usage<'_> {
        Usage { db: self }
    }
}

impl Usage<'_> {
    /// Add `amount` to `field` of `user_id` in `period`; the new value. One
    /// statement on Python's unique (user, period, field): race-free against
    /// Rust and Python writers alike.
    ///
    /// # Errors
    /// A failed checkout or write.
    pub async fn increment(
        &self,
        user_id: Uuid,
        period: &str,
        field: &str,
        amount: i32,
    ) -> DbResult<Option<i32>> {
        let s = self.db.schema();
        self.db
            .scoped(|conn| {
                async move {
                    let rows: Vec<ValueRow> = sql_query(format!(
                        "INSERT INTO {s}.agent_usage_counter (_id, user_id, period, field, value) \
                         VALUES ($1, $2, $3, $4, $5) \
                         ON CONFLICT (user_id, period, field) \
                         DO UPDATE SET value = {s}.agent_usage_counter.value + EXCLUDED.value, {TOUCH} \
                         RETURNING value"
                    ))
                    .bind::<SqlUuid, _>(Uuid::new_v4())
                    .bind::<SqlUuid, _>(user_id)
                    .bind::<Text, _>(period)
                    .bind::<Text, _>(field)
                    .bind::<Integer, _>(amount)
                    .load(conn)
                    .await?;
                    Ok(rows.into_iter().next().map(|r| r.value))
                }
                .scope_boxed()
            })
            .await
    }

    /// Every counter of `user_id` in `period`.
    ///
    /// # Errors
    /// A failed checkout or read.
    pub async fn counters(&self, user_id: Uuid, period: &str) -> DbResult<Vec<(String, i32)>> {
        let s = self.db.schema();
        self.db
            .scoped(|conn| {
                async move {
                    let rows: Vec<FieldRow> = sql_query(format!(
                        "SELECT field, value FROM {s}.agent_usage_counter WHERE user_id = $1 AND period = $2"
                    ))
                    .bind::<SqlUuid, _>(user_id)
                    .bind::<Text, _>(period)
                    .load(conn)
                    .await?;
                    Ok(rows.into_iter().map(|r| (r.field, r.value)).collect())
                }
                .scope_boxed()
            })
            .await
    }

    /// Delete the counters of `user_id` in `period`.
    ///
    /// # Errors
    /// A failed checkout or delete.
    pub async fn reset(&self, user_id: Uuid, period: &str) -> DbResult<()> {
        let s = self.db.schema();
        self.db
            .scoped(|conn| {
                async move {
                    sql_query(format!(
                        "DELETE FROM {s}.agent_usage_counter WHERE user_id = $1 AND period = $2"
                    ))
                    .bind::<SqlUuid, _>(user_id)
                    .bind::<Text, _>(period)
                    .execute(conn)
                    .await?;
                    Ok(())
                }
                .scope_boxed()
            })
            .await
    }

    /// Insert a usage event through its entity (`data` keyed by column).
    ///
    /// # Errors
    /// A failed checkout or write.
    pub async fn append_usage_event(&self, data: &Value) -> DbResult<()> {
        self.db
            .scoped(|conn| {
                async move {
                    upsert_agent_usage_event(conn, Uuid::new_v4(), data)
                        .await
                        .map_err(|e| DbError::Query(e.to_string()))
                }
                .scope_boxed()
            })
            .await
    }

    /// Insert a cost event through its entity (`data` keyed by column).
    ///
    /// # Errors
    /// A failed checkout or write.
    pub async fn append_cost_event(&self, data: &Value) -> DbResult<()> {
        self.db
            .scoped(|conn| {
                async move {
                    upsert_agent_cost_event(conn, Uuid::new_v4(), data)
                        .await
                        .map_err(|e| DbError::Query(e.to_string()))
                }
                .scope_boxed()
            })
            .await
    }

    /// The plan key of `user_id`'s active profile (an unordered `LIMIT 1`).
    ///
    /// # Errors
    /// A failed checkout or read.
    pub async fn active_plan(&self, user_id: Uuid) -> DbResult<Option<String>> {
        use crate::entity::agent_usage_profile::agent_usage_profile as p;
        self.db
            .scoped(|conn| {
                async move {
                    Ok(p::table
                        .filter(p::user_id.eq(user_id))
                        .filter(p::active.eq(true))
                        .filter(p::_deleted.is_null())
                        .select(p::plan_key)
                        .first::<String>(conn)
                        .await
                        .optional()?)
                }
                .scope_boxed()
            })
            .await
    }

    /// Give `user_id` an active profile on `plan_key` unless one on that
    /// plan exists (Python keys the plan row as `UUID_GENF(plan_key)`).
    ///
    /// # Errors
    /// A failed checkout or write.
    pub async fn ensure_profile(
        &self,
        user_id: Uuid,
        plan_key: &str,
        plan_id: Uuid,
    ) -> DbResult<()> {
        let s = self.db.schema();
        self.db
            .scoped(|conn| {
                async move {
                    sql_query(format!(
                        "INSERT INTO {s}.agent_usage_profile (_id, user_id, plan_key, active, plan_id) \
                         VALUES ($1, $2, $3, true, $4) ON CONFLICT (user_id, plan_id) DO NOTHING"
                    ))
                    .bind::<SqlUuid, _>(Uuid::new_v4())
                    .bind::<SqlUuid, _>(user_id)
                    .bind::<Text, _>(plan_key)
                    .bind::<SqlUuid, _>(plan_id)
                    .execute(conn)
                    .await?;
                    Ok(())
                }
                .scope_boxed()
            })
            .await
    }

    /// Make `plan_key` `user_id`'s only active plan: the other profiles
    /// turn inactive, and the profile on `plan_id` is made (or turned)
    /// active.
    ///
    /// # Errors
    /// A failed checkout or write.
    pub async fn set_active_plan(
        &self,
        user_id: Uuid,
        plan_key: &str,
        plan_id: Uuid,
    ) -> DbResult<()> {
        let s = self.db.schema();
        self.db
            .scoped(|conn| {
                async move {
                    sql_query(format!(
                        "UPDATE {s}.agent_usage_profile SET active = (plan_key = $2) WHERE user_id = $1"
                    ))
                    .bind::<SqlUuid, _>(user_id)
                    .bind::<Text, _>(plan_key)
                    .execute(conn)
                    .await?;
                    sql_query(format!(
                        "INSERT INTO {s}.agent_usage_profile (_id, user_id, plan_key, active, plan_id) \
                         VALUES ($1, $2, $3, true, $4) ON CONFLICT (user_id, plan_id) DO UPDATE SET active = true"
                    ))
                    .bind::<SqlUuid, _>(Uuid::new_v4())
                    .bind::<SqlUuid, _>(user_id)
                    .bind::<Text, _>(plan_key)
                    .bind::<SqlUuid, _>(plan_id)
                    .execute(conn)
                    .await?;
                    Ok(())
                }
                .scope_boxed()
            })
            .await
    }

    /// Take reservation `r` in one transaction under Python's quota lock:
    /// expire the user's stale holds, sum the live ones, refuse when over the
    /// daily limit (`BEM-362`, rolled back), then insert the hold.
    ///
    /// # Errors
    /// `BEM-362` when refused; a failed checkout or statement.
    pub async fn reserve(&self, r: NewReservation) -> DbResult<Uuid> {
        let s = self.db.schema();
        self.db
            .transaction::<_, DbError, _>(|tx| {
                Box::pin(async move {
                    sql_query("SELECT pg_advisory_xact_lock(hashtext('beam:quota'), hashtext($1))")
                        .bind::<Text, _>(&r.lock_key)
                        .execute(tx)
                        .await?;
                    // Python expires stale holds before summing the live ones.
                    sql_query(format!(
                        "UPDATE {s}.agent_usage_reservation SET status = 'expired', {TOUCH} \
                         WHERE user_id = $1 AND status = 'reserved' AND expires_at IS NOT NULL \
                         AND expires_at <= now()"
                    ))
                    .bind::<SqlUuid, _>(r.user_id)
                    .execute(tx)
                    .await?;
                    let held: Vec<HeldRow> = sql_query(format!(
                        "SELECT COALESCE(sum(tokens), 0)::bigint AS held FROM {s}.agent_usage_reservation \
                         WHERE user_id = $1 AND status = 'reserved'"
                    ))
                    .bind::<SqlUuid, _>(r.user_id)
                    .load(tx)
                    .await?;
                    let held = held
                        .into_iter()
                        .next()
                        .map_or(0, |h| u64::try_from(h.held).unwrap_or(0));
                    let tokens = u64::try_from(r.tokens).unwrap_or(0);
                    if let Some(limit) = r.daily_limit {
                        if r.usage_tokens + held + tokens > limit {
                            return Err(DbError::Beam(
                                crate::BEM_362.with_data("daily token reservation denied"),
                            ));
                        }
                    }
                    sql_query(format!(
                        "INSERT INTO {s}.agent_usage_reservation \
                         (_id, user_id, org_id, tokens, status, execution_id, expires_at, created_at, owner) \
                         VALUES ($1, $2, $3, $4, 'reserved', $5, now() + make_interval(secs => $6), now(), $7)"
                    ))
                    .bind::<SqlUuid, _>(r.id)
                    .bind::<SqlUuid, _>(r.user_id)
                    .bind::<Nullable<SqlUuid>, _>(r.org_id)
                    .bind::<Integer, _>(r.tokens)
                    .bind::<Nullable<SqlUuid>, _>(r.execution_id)
                    .bind::<Double, _>(r.ttl_seconds)
                    .bind::<Nullable<Text>, _>(r.owner.as_deref())
                    .execute(tx)
                    .await?;
                    Ok(r.id)
                })
            })
            .await
    }

    /// Close a held reservation as `status` (with `settled` tokens); a second
    /// settle or release changes nothing.
    ///
    /// # Errors
    /// A failed checkout or update.
    pub async fn finish_reservation(
        &self,
        reservation_id: Uuid,
        status: &str,
        settled: Option<i32>,
    ) -> DbResult<()> {
        let s = self.db.schema();
        self.db
            .scoped(|conn| {
                async move {
                    sql_query(format!(
                        "UPDATE {s}.agent_usage_reservation SET status = $2, \
                         settled_tokens = COALESCE($3, settled_tokens), {TOUCH} \
                         WHERE _id = $1 AND status = 'reserved'"
                    ))
                    .bind::<SqlUuid, _>(reservation_id)
                    .bind::<Text, _>(status)
                    .bind::<Nullable<Integer>, _>(settled)
                    .execute(conn)
                    .await?;
                    Ok(())
                }
                .scope_boxed()
            })
            .await
    }

    /// Expire every abandoned hold; how many.
    ///
    /// # Errors
    /// A failed checkout or update.
    pub async fn expire_abandoned(&self) -> DbResult<u64> {
        let s = self.db.schema();
        self.db
            .scoped(|conn| {
                async move {
                    let rows: Vec<Counted> = sql_query(format!(
                        "WITH expired AS (UPDATE {s}.agent_usage_reservation SET status = 'expired', {TOUCH} \
                         WHERE status = 'reserved' AND expires_at IS NOT NULL AND expires_at <= now() RETURNING 1) \
                         SELECT count(*) AS n FROM expired"
                    ))
                    .load(conn)
                    .await?;
                    Ok(rows
                        .into_iter()
                        .next()
                        .map_or(0, |c| u64::try_from(c.n).unwrap_or(0)))
                }
                .scope_boxed()
            })
            .await
    }
}
