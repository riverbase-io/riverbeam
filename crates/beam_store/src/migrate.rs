//! Versioned SQL migrations for the beam schema (plans 027, 028).
//!
//! Diesel migrations in `crates/beam_store/migrations/`, run by riverbase's
//! `run_embedded_migrations`: it creates the schema, puts it first on
//! `search_path` and applies each pending `up.sql` in a transaction, so the SQL
//! names no schema. Diesel's ledger is `<schema>.__diesel_schema_migrations`.
//!
//! Like fluvius-beam, riverbeam never migrates on startup. Operators run
//! [`migrate`] (the server's `showcase-worker migrate`), and readiness checks
//! [`applied_version`].
//!
//! `0012_baseline` creates the schema on an empty database (plan 028): every
//! type, table, constraint and index migrations 0002 through 0012 built, as
//! plain DDL (`tests/schema/riverbeam_baseline.sql` records it). Versions are
//! four-digit integers: the next migration is `0013_<name>`.
//!
//! The pre-diesel runner kept its ledger in `riverbeam_meta.schema_migrations`.
//! [`migrate`] bridges a schema it took through 0012: diesel's ledger records
//! the baseline as applied instead of running it again.

use diesel::migration::MigrationSource;
use diesel::pg::{Pg, PgConnection};
use diesel::sql_query;
use diesel::sql_types::{Bool, Integer, Text};
use diesel::QueryableByName;
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use riverbase_core::datastore::postgres::prepare_migration_search_path;
use riverbase_core::datastore::{run_embedded_migrations, PgPool};

use crate::schema::SchemaName;
use crate::store::StoreResult;

/// fluvius-beam rc8's `BEAM_SCHEMA_VERSION`: the schema a database migrated
/// through [`RC8_PARITY_MIGRATION`] matches.
pub const PYTHON_SCHEMA_VERSION: i32 = 26;

/// The schema versions rc8 accepts (`store/schema/version.py`).
pub const PYTHON_SCHEMA_COMPATIBLE: [i32; 2] = [25, 26];

/// The migration that brings a database to rc8's shape (0010, in the baseline).
pub const RC8_PARITY_MIGRATION: i32 = 10;

/// The baseline's version: everything the pre-diesel runner applied.
pub const BASELINE_VERSION: i32 = 12;

/// The beam schema's migrations, in `crates/beam_store/migrations/`.
pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

/// fluvius-beam's schema version for a database at migration `applied`:
/// rc8's 26 from [`RC8_PARITY_MIGRATION`] on, else the rc1 baseline's 22.
#[must_use]
pub fn python_schema_version(applied: i32) -> i32 {
    if applied >= RC8_PARITY_MIGRATION {
        PYTHON_SCHEMA_VERSION
    } else {
        22
    }
}

/// Diesel's ledger table, in the beam schema.
const LEDGER: &str = "__diesel_schema_migrations";

/// Session advisory lock key for migrations (`hashtext('riverbeam.migrate')` is not stable across versions, so a constant).
const MIGRATE_LOCK_KEY: i64 = 0x7269_7665_7262_6d01;

/// The embedded migrations' versions, ascending (`0012_baseline` is 12).
fn versions() -> Vec<i32> {
    let migrations = MigrationSource::<Pg>::migrations(&MIGRATIONS).expect("embedded migrations");
    let mut versions: Vec<i32> = migrations
        .iter()
        .map(|m| {
            let version = m.name().version().to_string();
            version
                .parse()
                .unwrap_or_else(|_| panic!("migration version {version:?} is not a number"))
        })
        .collect();
    versions.sort_unstable();
    versions
}

/// Latest migration version.
#[must_use]
pub fn latest_version() -> i32 {
    versions().last().copied().unwrap_or(0)
}

#[derive(QueryableByName)]
struct Found {
    #[diesel(sql_type = Bool)]
    found: bool,
}

#[derive(QueryableByName)]
struct LedgerRow {
    #[diesel(sql_type = Text)]
    version: String,
}

#[derive(QueryableByName)]
struct LegacyRow {
    #[diesel(sql_type = Integer)]
    version: i32,
}

/// The highest migration version `schema` has with every migration up to it
/// applied (plan 020 D10), from diesel's ledger, or `None` when the schema was
/// never migrated (no ledger, or an empty one). A gap answers the version
/// before the gap, so readiness reports the schema behind.
///
/// # Errors
/// Returns a store error if the database cannot be queried.
pub async fn applied_version(pool: &PgPool, schema: &SchemaName) -> StoreResult<Option<i32>> {
    use diesel_async::{AsyncPgConnection, RunQueryDsl};

    let mut guard = pool
        .get()
        .await
        .map_err(|e| crate::BEM_111.with_data(e.to_string()))?;
    let conn: &mut AsyncPgConnection = &mut guard;
    let ledger: Vec<Found> = sql_query("SELECT to_regclass($1) IS NOT NULL AS found")
        .bind::<Text, _>(format!("{schema}.{LEDGER}"))
        .load(conn)
        .await
        .map_err(|e| crate::BEM_112.with_data(e.to_string()))?;
    if !ledger.iter().any(|row| row.found) {
        return Ok(None);
    }
    let rows: Vec<LedgerRow> = sql_query(format!("SELECT version FROM {schema}.{LEDGER}"))
        .load(conn)
        .await
        .map_err(|e| crate::BEM_112.with_data(e.to_string()))?;
    let recorded: Vec<i32> = rows.iter().filter_map(|r| r.version.parse().ok()).collect();
    Ok(contiguous_version(&versions(), &recorded))
}

/// The highest of `versions` (ascending) with every one up to it in
/// `recorded`; `Some(0)` when the first one is missing, `None` when nothing is
/// recorded.
fn contiguous_version(versions: &[i32], recorded: &[i32]) -> Option<i32> {
    if recorded.is_empty() {
        return None;
    }
    let mut reached = 0;
    for version in versions {
        if !recorded.contains(version) {
            break;
        }
        reached = *version;
    }
    Some(reached)
}

/// Apply every pending migration to `schema` on the database at
/// `database_url`, through riverbase's `run_embedded_migrations`, under a
/// session advisory lock so concurrent runners serialize. Returns the versions
/// applied by this call.
///
/// Diesel migrations are synchronous: this runs on a blocking thread.
///
/// # Errors
/// Returns a store error if the database cannot be reached or a migration
/// fails; that migration is rolled back and later ones are not attempted.
pub async fn migrate(database_url: &str, schema: &SchemaName) -> StoreResult<Vec<i32>> {
    let url = database_url.to_owned();
    let schema = schema.clone();
    tokio::task::spawn_blocking(move || migrate_blocking(&url, &schema))
        .await
        .map_err(|e| crate::BEM_112.with_data(e.to_string()))?
}

fn migrate_blocking(url: &str, schema: &SchemaName) -> StoreResult<Vec<i32>> {
    use diesel::{Connection, RunQueryDsl};

    let mut conn =
        PgConnection::establish(url).map_err(|e| crate::BEM_111.with_data(e.to_string()))?;
    sql_query(format!("SELECT pg_advisory_lock({MIGRATE_LOCK_KEY})"))
        .execute(&mut conn)
        .map_err(|e| crate::BEM_112.with_data(e.to_string()))?;
    let result = apply_pending(&mut conn, url, schema);
    let unlock =
        sql_query(format!("SELECT pg_advisory_unlock({MIGRATE_LOCK_KEY})")).execute(&mut conn);
    let applied = result?;
    unlock.map_err(|e| crate::BEM_112.with_data(e.to_string()))?;
    Ok(applied)
}

/// Under the lock: bridge, read what is pending, then run it.
fn apply_pending(conn: &mut PgConnection, url: &str, schema: &SchemaName) -> StoreResult<Vec<i32>> {
    prepare_migration_search_path(conn, schema.as_str())?;
    bridge(conn, schema)?;
    let pending: Vec<i32> = conn
        .pending_migrations(MIGRATIONS)
        .map_err(|e| crate::BEM_112.with_data(e.to_string()))?
        .iter()
        .filter_map(|m| m.name().version().to_string().parse().ok())
        .collect();
    if !pending.is_empty() {
        run_embedded_migrations(url, MIGRATIONS, schema.as_str())
            .map_err(|e| crate::BEM_112.with_data(e.to_string()))?;
    }
    Ok(pending)
}

/// Record the baseline in diesel's ledger when the pre-diesel runner took
/// `schema` through [`BASELINE_VERSION`] with no migration missing and the
/// schema's tables still exist. Anything less runs the baseline, which
/// creates the schema only on an empty one: a schema the old runner left
/// short of 0012 fails to migrate and has to be recreated.
fn bridge(conn: &mut PgConnection, schema: &SchemaName) -> StoreResult<()> {
    use diesel::RunQueryDsl;

    let legacy: Vec<Found> = sql_query(
        "SELECT to_regclass('riverbeam_meta.schema_migrations') IS NOT NULL \
         AND to_regclass(quote_ident($1) || '.agent') IS NOT NULL AS found",
    )
    .bind::<Text, _>(schema.as_str())
    .load(conn)
    .map_err(|e| crate::BEM_112.with_data(e.to_string()))?;
    if !legacy.iter().any(|row| row.found) {
        return Ok(());
    }
    let rows: Vec<LegacyRow> =
        sql_query("SELECT version FROM riverbeam_meta.schema_migrations WHERE schema_name = $1")
            .bind::<Text, _>(schema.as_str())
            .load(conn)
            .map_err(|e| crate::BEM_112.with_data(e.to_string()))?;
    let recorded: Vec<i32> = rows.into_iter().map(|r| r.version).collect();
    if !(2..=BASELINE_VERSION).all(|v| recorded.contains(&v)) {
        return Ok(());
    }
    // Diesel's own ledger DDL (`setup_migration_table.sql`), in the schema.
    sql_query(format!(
        "CREATE TABLE IF NOT EXISTS {schema}.{LEDGER} (\
         version VARCHAR(50) PRIMARY KEY NOT NULL, \
         run_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    ))
    .execute(conn)
    .map_err(|e| crate::BEM_112.with_data(e.to_string()))?;
    let bridged = sql_query(format!(
        "INSERT INTO {schema}.{LEDGER} (version) VALUES ($1) ON CONFLICT DO NOTHING"
    ))
    .bind::<Text, _>(format!("{BASELINE_VERSION:04}"))
    .execute(conn)
    .map_err(|e| crate::BEM_112.with_data(e.to_string()))?;
    if bridged > 0 {
        tracing::info!(%schema, version = BASELINE_VERSION, "recorded the baseline for a schema the pre-diesel runner migrated");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::*;

    /// The migration directories on disk, ascending.
    fn migration_dirs() -> Vec<PathBuf> {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
        let mut dirs: Vec<PathBuf> = std::fs::read_dir(&root)
            .expect("migrations directory")
            .map(|e| e.expect("entry").path())
            .filter(|p| p.is_dir())
            .collect();
        dirs.sort();
        dirs
    }

    fn up_sql(dir: &Path) -> String {
        std::fs::read_to_string(dir.join("up.sql")).expect("up.sql")
    }

    /// Four-digit versions, unique and ascending, and the embedded list is
    /// the directory's (so `0013_x` sorts after the baseline; a diesel CLI
    /// timestamp would not).
    #[test]
    fn versions_are_four_digit_and_embedded() {
        let mut on_disk = Vec::new();
        for dir in migration_dirs() {
            let name = dir.file_name().unwrap().to_string_lossy().into_owned();
            let (version, rest) = name.split_once('_').expect("<version>_<name>");
            assert!(
                version.len() == 4
                    && version.bytes().all(|b| b.is_ascii_digit())
                    && !rest.is_empty(),
                "{name}: want a four-digit version, `NNNN_<name>`"
            );
            assert!(dir.join("up.sql").is_file(), "{name}: no up.sql");
            on_disk.push(version.parse::<i32>().unwrap());
        }
        let mut sorted = on_disk.clone();
        sorted.dedup();
        assert_eq!(on_disk, sorted, "versions must be unique");
        assert_eq!(versions(), on_disk);
        assert_eq!(versions().first(), Some(&BASELINE_VERSION));
    }

    /// Every DDL statement after the baseline is behind a catalog check, so a
    /// run on a live database takes no table locks. The baseline creates an
    /// empty schema: plain DDL.
    #[test]
    fn migration_ddl_is_catalog_guarded() {
        let after_baseline = migration_dirs()
            .into_iter()
            .filter(|dir| !dir.ends_with("0012_baseline"));
        for dir in after_baseline {
            for line in up_sql(&dir).lines() {
                let line = line.trim_start();
                for bare in [
                    "ALTER TABLE",
                    "CREATE INDEX",
                    "CREATE UNIQUE INDEX",
                    "CREATE TABLE",
                    "ALTER TYPE",
                ] {
                    assert!(
                        !line.starts_with(bare),
                        "{}: unguarded DDL {line:?}",
                        dir.display()
                    );
                }
            }
        }
    }

    /// Whether `line` has `ref_beam` as a whole identifier.
    fn names_ref_beam(line: &str) -> bool {
        let is_ident = |c: char| c.is_ascii_alphanumeric() || c == '_';
        line.match_indices("ref_beam").any(|(at, name)| {
            !line[..at].chars().next_back().is_some_and(is_ident)
                && !line[at + name.len()..].chars().next().is_some_and(is_ident)
        })
    }

    #[test]
    fn names_ref_beam_matches_whole_identifiers_only() {
        for line in [
            "CREATE TABLE ref_beam.agent (",
            "WHERE n.nspname = 'ref_beam'",
            "SET search_path TO ref_beam",
            "SELECT 1 FROM \"ref_beam\".agent",
        ] {
            assert!(names_ref_beam(line), "{line}");
        }
        for line in [
            "CREATE INDEX ix_ref_beam_agent_action_tool_name ON agent_action",
            "tests/schema/python_ref_beam.sql",
            "ref_beam_ckpt",
        ] {
            assert!(!names_ref_beam(line), "{line}");
        }
    }

    /// The SQL names no schema: `search_path` picks it. (rc8's index names,
    /// `ix_ref_beam_…`, are names, not the schema.)
    #[test]
    fn migrations_name_no_schema() {
        for dir in migration_dirs() {
            for line in up_sql(&dir).lines() {
                assert!(
                    !names_ref_beam(line),
                    "{}: names the schema: {line:?}",
                    dir.display()
                );
            }
        }
    }

    /// Plan 020 D10: a gap stops the reported version before it.
    #[test]
    fn a_missing_migration_stops_the_applied_version() {
        let versions = [12, 13, 14];
        assert_eq!(contiguous_version(&versions, &[12, 13, 14]), Some(14));
        assert_eq!(contiguous_version(&versions, &[12, 14]), Some(12));
        assert_eq!(
            contiguous_version(&versions, &[13, 14]),
            Some(0),
            "first missing"
        );
        assert_eq!(contiguous_version(&versions, &[12, 13, 14, 99]), Some(14));
        assert_eq!(contiguous_version(&versions, &[]), None);
        let embedded = super::versions();
        assert_eq!(
            contiguous_version(&embedded, &embedded),
            Some(latest_version())
        );
    }

    #[test]
    fn rc8_parity_reports_schema_26() {
        assert_eq!(python_schema_version(RC8_PARITY_MIGRATION - 1), 22);
        assert_eq!(python_schema_version(RC8_PARITY_MIGRATION), 26);
        assert!(PYTHON_SCHEMA_COMPATIBLE.contains(&python_schema_version(latest_version())));
    }
}
