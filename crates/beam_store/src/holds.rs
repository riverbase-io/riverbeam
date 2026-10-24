//! Durable legal holds, `ref_beam.agent_legal_hold`.
//!
//! One row per held user. Every place, release and delete transaction takes
//! [`lock`] first, so placing a hold and deleting the user's data run one
//! after the other: a hold committed first refuses the deletion, and a
//! deletion committed first has already finished.
//!
//! Every function takes the connection to run on, so a command can check a
//! hold on its own transaction and a store on its delete transaction.

use diesel::sql_query;
use diesel::sql_types::{Nullable, Text, Uuid as SqlUuid};
use diesel::QueryableByName;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use uuid::Uuid;

use crate::schema::SchemaName;
use crate::store::StoreResult;

/// The advisory-lock namespace for holds (`hashtext` of it, then the user).
const LOCK_KEY: &str = "beam:legal_hold";

/// The key a hold is stored and locked under: a UUID in its canonical
/// lower-case form (`ABC…` and `abc…` are one user), else the trimmed text.
/// Every function here keys by it, so a hold placed with one spelling stops
/// a deletion asked with another.
#[must_use]
pub(crate) fn key(user_id: &str) -> String {
    let trimmed = user_id.trim();
    Uuid::parse_str(trimmed).map_or_else(|_| trimmed.to_owned(), |id| id.to_string())
}

fn db(e: impl std::fmt::Display) -> riverbase_core::RiverbaseError {
    crate::BEM_139.with_data(e.to_string())
}

#[derive(QueryableByName)]
struct Held {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    held: bool,
}

/// Serialize hold changes and deletions for `user_id` until the end of the
/// current transaction.
///
/// # Errors
/// `BEM-139` when the lock statement fails.
pub(crate) async fn lock(conn: &mut AsyncPgConnection, user_id: &str) -> StoreResult<()> {
    sql_query("SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))")
        .bind::<Text, _>(LOCK_KEY)
        .bind::<Text, _>(key(user_id))
        .execute(conn)
        .await
        .map_err(db)?;
    Ok(())
}

/// Hold `user_id`. Placing an existing hold replaces its reason, placer and org.
///
/// # Errors
/// `BEM-139` when the write fails.
pub(crate) async fn place(
    conn: &mut AsyncPgConnection,
    schema: &SchemaName,
    user_id: &str,
    org_id: Option<Uuid>,
    reason: Option<&str>,
    placed_by: Option<&str>,
) -> StoreResult<()> {
    sql_query(format!(
        "INSERT INTO {schema}.agent_legal_hold (_id, user_id, org_id, reason, placed_by) \
         VALUES ($1, $2, $3, $4, $5) \
         ON CONFLICT (user_id) DO UPDATE SET org_id = EXCLUDED.org_id, reason = EXCLUDED.reason, \
         placed_by = EXCLUDED.placed_by, _updated = now()"
    ))
    .bind::<SqlUuid, _>(Uuid::new_v4())
    .bind::<Text, _>(key(user_id))
    .bind::<Nullable<SqlUuid>, _>(org_id)
    .bind::<Nullable<Text>, _>(reason)
    .bind::<Nullable<Text>, _>(placed_by)
    .execute(conn)
    .await
    .map_err(db)?;
    Ok(())
}

/// Release `user_id`'s hold. Returns whether there was one.
///
/// # Errors
/// `BEM-139` when the delete fails.
pub(crate) async fn release(
    conn: &mut AsyncPgConnection,
    schema: &SchemaName,
    user_id: &str,
) -> StoreResult<bool> {
    let deleted = sql_query(format!(
        "DELETE FROM {schema}.agent_legal_hold WHERE user_id = $1"
    ))
    .bind::<Text, _>(key(user_id))
    .execute(conn)
    .await
    .map_err(db)?;
    Ok(deleted > 0)
}

/// Release `user_id`'s hold only if `placed_by` placed it (a mirror such as
/// loan desk must not release an admin's hold). Returns whether it went.
///
/// # Errors
/// `BEM-139` when the delete fails.
pub(crate) async fn release_placed_by(
    conn: &mut AsyncPgConnection,
    schema: &SchemaName,
    user_id: &str,
    placed_by: &str,
) -> StoreResult<bool> {
    let deleted = sql_query(format!(
        "DELETE FROM {schema}.agent_legal_hold WHERE user_id = $1 AND placed_by = $2"
    ))
    .bind::<Text, _>(key(user_id))
    .bind::<Text, _>(placed_by)
    .execute(conn)
    .await
    .map_err(db)?;
    Ok(deleted > 0)
}

/// Whether `user_id` is held.
///
/// # Errors
/// `BEM-139` when the read fails; callers must then refuse the deletion.
pub(crate) async fn is_held(
    conn: &mut AsyncPgConnection,
    schema: &SchemaName,
    user_id: &str,
) -> StoreResult<bool> {
    let rows: Vec<Held> = sql_query(format!(
        "SELECT EXISTS (SELECT 1 FROM {schema}.agent_legal_hold WHERE user_id = $1) AS held"
    ))
    .bind::<Text, _>(key(user_id))
    .load(conn)
    .await
    .map_err(db)?;
    Ok(rows.into_iter().next().is_some_and(|r| r.held))
}
