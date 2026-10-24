//! `AUDIT_SINKS=["stdout","file","sql"]`
//! writes the same event to all three. Needs `--features postgres` and
//! `BEAM_DATABASE_URL`; skips otherwise. Rows are left for
//! `scripts/cross_read.py` to load with Python.
#![cfg(feature = "postgres")]

use std::sync::Arc;

use beam_audit::{build_audit_sinks, AuditAction, AuditEvent, AuditLogger, AuditSink, SqlSink};
use diesel::sql_types::{BigInt, Text};
use diesel_async::RunQueryDsl;

#[derive(diesel::QueryableByName)]
struct Row {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

#[tokio::test]
async fn sql_sink_roundtrip() {
    let Some(pool) = beam_store::testdb::pool("beam_audit").await else {
        return;
    };
    let store = beam_store::PgBeamStore::new(pool.clone(), beam_store::testdb::schema());
    let sql: Arc<dyn AuditSink> = Arc::new(SqlSink::for_store(&store));
    let file = std::env::temp_dir().join(format!("beam-audit-{}.jsonl", uuid::Uuid::new_v4()));
    let names = ["stdout".to_owned(), "file".to_owned(), "sql".to_owned()];
    let sinks = build_audit_sinks(&names, file.to_str().expect("path"), Some(&sql)).expect("sinks");
    assert_eq!(sinks.len(), 3);
    let logger = AuditLogger::new(sinks);

    // A unique agent name finds this event in both stores.
    let agent = format!("audit-{}", uuid::Uuid::new_v4().simple());
    let mut event = AuditEvent::new(AuditAction::SecurityEvent)
        .agent_name(Some(agent.clone()))
        .user_id(Some(uuid::Uuid::new_v4().to_string()))
        .status("blocked");
    event.metadata =
        serde_json::json!({"stage": "before_input", "outcome_action": "deny", "api_key": "k"});
    event.modalities_used = vec!["text".into()];
    logger.log(event).await;
    logger
        .log(AuditEvent::new(AuditAction::MemoryWritten))
        .await;
    logger.flush().await;

    let lines = std::fs::read_to_string(&file).expect("jsonl");
    let _ = std::fs::remove_file(&file);
    let written: Vec<serde_json::Value> = lines
        .lines()
        .map(|l| serde_json::from_str(l).expect("json line"))
        .filter(|v: &serde_json::Value| v["agent_name"] == agent.as_str())
        .collect();
    assert_eq!(written.len(), 1, "the file sink has the event");
    assert_eq!(written[0]["metadata"]["api_key"], "[redacted]");

    let mut conn = pool.get().await.expect("conn");
    let rows: Vec<Row> = diesel::sql_query(
        "SELECT count(*) AS n FROM ref_beam.agent_event \
         WHERE agent_name = $1 AND status = 'blocked' AND metadata_json->>'api_key' = '[redacted]'",
    )
    .bind::<Text, _>(&agent)
    .load(&mut conn)
    .await
    .expect("query");
    assert_eq!(rows[0].n, 1, "the SQL sink has the event");
}
