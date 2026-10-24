//! Throwaway databases for Postgres tests.
//!
//! The tests always use the `ref_beam` schema ([`schema`]), so they isolate by
//! database: `beam_test_<label>` on the server `BEAM_DATABASE_URL` (or
//! `DATABASE_URL`) names. Each test binary passes its own label. The database
//! is created on first use, migrated (riverbase's framework migrations, then
//! beam's), and kept for the next run, as the per-test schemas were.
//!
//! Every helper returns `None` when no database URL is set, so the tests skip.

use diesel::sql_query;
use diesel::sql_types::{Integer, Text};
use diesel::QueryableByName;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use riverbase_core::datastore::{establish_dbpool, PgPool};

use crate::{PgBeamStore, SchemaName};

#[derive(QueryableByName)]
struct Found {
    #[diesel(sql_type = Integer)]
    #[allow(dead_code)]
    found: i32,
}

/// The server URL tests run against: `BEAM_DATABASE_URL`, else `DATABASE_URL`.
#[must_use]
pub fn base_url() -> Option<String> {
    std::env::var("BEAM_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .ok()
        .filter(|u| !u.trim().is_empty())
}

/// `url` with its database replaced by `database` (the query string is kept).
#[must_use]
pub fn with_database(url: &str, database: &str) -> String {
    let (base, query) = match url.split_once('?') {
        Some((base, query)) => (base, Some(query)),
        None => (url, None),
    };
    let host = base.find("://").map_or(0, |i| i + 3);
    let path = base[host..].find('/').map_or(base.len(), |i| host + i);
    let mut out = format!("{}/{database}", &base[..path]);
    if let Some(query) = query {
        out.push('?');
        out.push_str(query);
    }
    out
}

/// `beam_test_<label>`, with anything but ASCII letters and digits as `_`.
#[must_use]
pub fn database_name(label: &str) -> String {
    let clean: String = label
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect();
    format!("beam_test_{clean}")
}

async fn create_database(base: &str, name: &str) {
    let mut conn = AsyncPgConnection::establish(base)
        .await
        .expect("connect to the test database server");
    let exists: Vec<Found> = sql_query("SELECT 1 AS found FROM pg_database WHERE datname = $1")
        .bind::<Text, _>(name)
        .load(&mut conn)
        .await
        .expect("look up the test database");
    if !exists.is_empty() {
        return;
    }
    if let Err(e) = sql_query(format!("CREATE DATABASE \"{name}\""))
        .execute(&mut conn)
        .await
    {
        // Another test in this binary created it first.
        let text = e.to_string();
        assert!(
            text.contains("already exists") || text.contains("duplicate key"),
            "create database {name}: {text}"
        );
    }
}

/// The URL of the test database for `label`, created if missing.
pub async fn database_url(label: &str) -> Option<String> {
    let base = base_url()?;
    let name = database_name(label);
    create_database(&base, &name).await;
    Some(with_database(&base, &name))
}

/// The test databases' beam schema, `ref_beam`, not the default `riverbeam`:
/// the tests' SQL and `crates/beam_store/sql/rls.sql` name it.
///
/// # Panics
/// Never: `ref_beam` is a valid schema name.
#[must_use]
pub fn schema() -> SchemaName {
    SchemaName::new("ref_beam").expect("ref_beam is a valid schema name")
}

/// A pool on the test database for `label`, with riverbase's framework
/// migrations and beam's migrations applied.
///
/// # Panics
/// When the server is unreachable or a migration fails.
pub async fn pool(label: &str) -> Option<PgPool> {
    let url = database_url(label).await?;
    let schema = schema();
    let pool = establish_dbpool(&schema.pool_url(&url))
        .await
        .expect("connect");
    crate::migrate::migrate(&url, &schema)
        .await
        .expect("migrate");
    Some(pool)
}

/// A [`PgBeamStore`] on the test database for `label`.
///
/// # Panics
/// When the server is unreachable or a migration fails.
pub async fn store(label: &str) -> Option<PgBeamStore> {
    Some(PgBeamStore::new(pool(label).await?, schema()))
}

/// The non-superuser role the row-level-security tests connect as
/// (superusers bypass RLS).
pub const RLS_ROLE: &str = "beam_rls_test";

/// The test database for `label` with `crates/beam_store/sql/rls.sql` applied:
/// a store on it as the superuser (for seeding, which bypasses RLS) and a
/// pool connecting as [`RLS_ROLE`], which may use every table but is held to
/// the policies. Callers turn RLS on with
/// [`crate::org_scope::set_rls_enabled`] (process-wide: use a test binary of
/// its own).
///
/// # Panics
/// When the server is unreachable, a migration fails, or the superuser in
/// the URL is not `postgres`.
pub async fn rls_pools(label: &str) -> Option<(PgBeamStore, PgPool)> {
    rls_pools_sized(label, 4).await
}

/// [`rls_pools`] with a role pool of `size` connections (one, to make every
/// checkout reuse the same connection).
///
/// # Panics
/// As [`rls_pools`].
pub async fn rls_pools_sized(label: &str, size: usize) -> Option<(PgBeamStore, PgPool)> {
    use diesel_async::pooled_connection::deadpool::Pool;
    use diesel_async::pooled_connection::AsyncDieselConnectionManager;

    let url = database_url(label).await?;
    let store = store(label).await?;
    let mut conn = AsyncPgConnection::establish(&url)
        .await
        .expect("admin connection");
    // The role may use every schema the beam and riverbase write (riverbase
    // keeps its command log and outbox beside the beam's tables).
    let setup = [
        format!(
            "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{RLS_ROLE}') THEN \
             CREATE ROLE {RLS_ROLE} LOGIN NOSUPERUSER; END IF; END $$"
        ),
        format!(
            "DO $$ DECLARE s text; BEGIN FOR s IN SELECT nspname FROM pg_namespace \
             WHERE nspname NOT LIKE 'pg_%' AND nspname <> 'information_schema' LOOP \
             EXECUTE format('GRANT USAGE ON SCHEMA %I TO {RLS_ROLE}', s); \
             EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA %I TO {RLS_ROLE}', s); \
             EXECUTE format('GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA %I TO {RLS_ROLE}', s); \
             END LOOP; END $$"
        ),
    ];
    let policies = include_str!("../sql/rls.sql")
        .lines()
        .filter(|line| !line.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");
    let policies = policies
        .split(';')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned);
    for statement in setup.into_iter().chain(policies) {
        sql_query(&statement)
            .execute(&mut conn)
            .await
            .unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
    // The same database as the role. A plain pool: the role cannot run
    // riverbase's startup migrations (applied above as the superuser).
    let role_url = url.replacen("://postgres@", &format!("://{RLS_ROLE}@"), 1);
    let manager =
        AsyncDieselConnectionManager::<AsyncPgConnection>::new(schema().pool_url(&role_url));
    let pool = Pool::builder(manager)
        .max_size(size)
        .build()
        .expect("role pool");
    Some((store, pool))
}

#[cfg(test)]
mod tests {
    use super::{database_name, with_database};

    #[test]
    fn replaces_only_the_database() {
        assert_eq!(
            with_database(
                "postgres://postgres@127.0.0.1:55432/postgres",
                "beam_test_x"
            ),
            "postgres://postgres@127.0.0.1:55432/beam_test_x"
        );
        assert_eq!(
            with_database("postgres://u:p@h/db?sslmode=disable", "t"),
            "postgres://u:p@h/t?sslmode=disable"
        );
        assert_eq!(
            with_database("postgres://h:5432", "t"),
            "postgres://h:5432/t"
        );
        assert_eq!(database_name("beam-domain.pg"), "beam_test_beam_domain_pg");
    }
}
