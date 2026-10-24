//! Plans 027 and 028: the beam schema's diesel migrations on Postgres. One
//! test, in order, since its cases share the pre-diesel ledger
//! (`riverbeam_meta`): a fresh schema; one the pre-diesel runner took through
//! 0012 (bridged, not migrated again); one whose ledger misses a version before
//! 0012 (refused: the baseline only creates an empty schema), and one whose
//! tables are gone (the baseline runs). Needs `BEAM_DATABASE_URL`; skips
//! without a database.

use beam_store::migrate::{applied_version, latest_version, migrate, BASELINE_VERSION};
use beam_store::SchemaName;
use diesel::sql_query;
use diesel::sql_types::Text;
use diesel::QueryableByName;
use diesel_async::{AsyncConnection, AsyncPgConnection, SimpleAsyncConnection};
use riverbase_core::datastore::establish_dbpool;

/// The pre-diesel runner's ledger.
const LEGACY_LEDGER: &str = "CREATE SCHEMA IF NOT EXISTS riverbeam_meta;
CREATE TABLE IF NOT EXISTS riverbeam_meta.schema_migrations (
    schema_name TEXT NOT NULL,
    version INTEGER NOT NULL,
    name TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (schema_name, version)
);";

#[derive(QueryableByName)]
struct Version {
    #[diesel(sql_type = Text)]
    version: String,
}

async fn exec(conn: &mut AsyncPgConnection, sql: &str) {
    conn.batch_execute(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// Drop `schema` and forget it in the pre-diesel ledger.
async fn reset(conn: &mut AsyncPgConnection, schema: &SchemaName) {
    exec(conn, &format!("DROP SCHEMA IF EXISTS {schema} CASCADE")).await;
    exec(
        conn,
        &format!("DELETE FROM riverbeam_meta.schema_migrations WHERE schema_name = '{schema}'"),
    )
    .await;
}

/// Record `versions` for `schema` in the pre-diesel ledger, as its runner did.
async fn record_legacy(
    conn: &mut AsyncPgConnection,
    schema: &SchemaName,
    versions: impl IntoIterator<Item = i32>,
) {
    for version in versions {
        exec(
            conn,
            &format!(
                "INSERT INTO riverbeam_meta.schema_migrations (schema_name, version, name) \
                 VALUES ('{schema}', {version}, 'pre-diesel')"
            ),
        )
        .await;
    }
}

/// A schema the pre-diesel runner left at `legacy`: migrated, then diesel's
/// ledger dropped and the old ledger written.
async fn pre_diesel(
    conn: &mut AsyncPgConnection,
    url: &str,
    schema: &SchemaName,
    legacy: impl IntoIterator<Item = i32>,
) {
    reset(conn, schema).await;
    migrate(url, schema).await.expect("build the schema");
    exec(
        conn,
        &format!("DROP TABLE {schema}.__diesel_schema_migrations"),
    )
    .await;
    record_legacy(conn, schema, legacy).await;
}

async fn ledger(conn: &mut AsyncPgConnection, schema: &SchemaName) -> Vec<String> {
    // Here only: in scope, `RunQueryDsl::first` would shadow `<[T]>::first`.
    use diesel_async::RunQueryDsl;

    sql_query(format!(
        "SELECT version FROM {schema}.__diesel_schema_migrations ORDER BY version"
    ))
    .load::<Version>(conn)
    .await
    .expect("diesel's ledger")
    .into_iter()
    .map(|row| row.version)
    .collect()
}

#[tokio::test]
async fn migrate_fresh_bridged_and_short_schemas() {
    let Some(url) = beam_store::testdb::database_url("beam_migrate").await else {
        return;
    };
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    exec(&mut conn, LEGACY_LEDGER).await;
    let latest = latest_version();
    let schema = |name: &str| SchemaName::new(name).expect("schema name");

    // Fresh: every migration, then nothing; readiness sees the latest.
    let fresh = schema("beam_mig_fresh");
    reset(&mut conn, &fresh).await;
    let pool = establish_dbpool(&fresh.pool_url(&url)).await.expect("pool");
    assert_eq!(applied_version(&pool, &fresh).await.unwrap(), None);
    let applied = migrate(&url, &fresh).await.expect("migrate");
    assert_eq!(applied.first(), Some(&BASELINE_VERSION));
    assert_eq!(applied.last(), Some(&latest));
    assert_eq!(
        migrate(&url, &fresh).await.expect("again"),
        Vec::<i32>::new()
    );
    assert_eq!(applied_version(&pool, &fresh).await.unwrap(), Some(latest));

    // Pre-diesel through 0012: readiness reads nothing until migrate runs
    // (D4), and migrate records the baseline instead of running it.
    let bridged = schema("beam_mig_bridged");
    pre_diesel(&mut conn, &url, &bridged, 2..=BASELINE_VERSION).await;
    let pool = establish_dbpool(&bridged.pool_url(&url))
        .await
        .expect("pool");
    assert_eq!(applied_version(&pool, &bridged).await.unwrap(), None);
    let applied = migrate(&url, &bridged).await.expect("migrate");
    assert!(
        !applied.contains(&BASELINE_VERSION),
        "bridged, not run: {applied:?}"
    );
    assert_eq!(
        ledger(&mut conn, &bridged)
            .await
            .first()
            .map(String::as_str),
        Some("0012")
    );
    assert_eq!(
        applied_version(&pool, &bridged).await.unwrap(),
        Some(latest)
    );

    // Pre-diesel through 0012 but missing 0010: no bridge (every version up to
    // the baseline must be there), and the baseline only creates a schema on
    // an empty one, so migrate fails rather than touch the tables.
    let short = schema("beam_mig_short");
    let missing_10 = (2..=BASELINE_VERSION).filter(|v| *v != 10);
    pre_diesel(&mut conn, &url, &short, missing_10).await;
    assert!(
        migrate(&url, &short).await.is_err(),
        "the baseline ran over existing tables"
    );
    assert!(
        ledger(&mut conn, &short).await.is_empty(),
        "nothing recorded"
    );

    // A full pre-diesel ledger for a schema whose tables are gone: no bridge.
    let gone = schema("beam_mig_gone");
    reset(&mut conn, &gone).await;
    record_legacy(&mut conn, &gone, 2..=BASELINE_VERSION).await;
    let applied = migrate(&url, &gone).await.expect("migrate");
    assert_eq!(applied.first(), Some(&BASELINE_VERSION));

    for s in [&fresh, &bridged, &short, &gone] {
        reset(&mut conn, s).await;
    }
}
