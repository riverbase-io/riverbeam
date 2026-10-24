//! The ops tables on Postgres: outbox sequences, claims
//! and replay, dead letters, the reconcile lock, and user-data deletion.
//! Needs `BEAM_DATABASE_URL`; skips without it.

use beam_store::ops::NewDeadLetter;
use beam_store::{
    AgentRow, AgentStatus, CatalogStore, ExecutionStore, LegalHoldStore, OpsStore, PgBeamStore,
    PgLegalHolds, PgOpsStore, RetentionStore, SessionRow,
};
use diesel::sql_types::Uuid as SqlUuid;
use diesel_async::RunQueryDsl;
use serde_json::json;
use uuid::Uuid;

async fn store() -> Option<PgBeamStore> {
    beam_store::testdb::store("beam_store_ops").await
}

/// An agent, a session for `user` and one execution in it.
async fn execution(store: &PgBeamStore, user: Option<Uuid>) -> (Uuid, Uuid) {
    let name = format!("ops-{}", Uuid::new_v4().simple());
    let agent = beam_store::agent_id_for(&name);
    store
        .checkin_agents(&[AgentRow {
            id: agent,
            name,
            description: None,
            supported_modalities: vec!["text".into()],
            output_modalities: vec!["text".into()],
            tags: vec![],
            version: None,
            status: AgentStatus::Active,
        }])
        .await
        .expect("agent");
    let session = Uuid::new_v4();
    store
        .ensure_session(&SessionRow {
            id: session,
            agent_id: agent,
            user_id: user.map(|u| u.to_string()),
            profile_id: None,
            org_id: None,
            name: None,
            last_active_at: None,
        })
        .await
        .expect("session");
    let execution = Uuid::new_v4();
    let mut conn = store.pool().get().await.expect("conn");
    diesel::sql_query(
        "INSERT INTO ref_beam.agent_execution (_id, session_id, agent_id, status, created_at) \
         VALUES ($1, $2, $3, 'RUNNING', now())",
    )
    .bind::<SqlUuid, _>(execution)
    .bind::<SqlUuid, _>(session)
    .bind::<SqlUuid, _>(agent)
    .execute(&mut conn)
    .await
    .expect("execution");
    (session, execution)
}

#[tokio::test]
async fn outbox_sequences_claims_and_replay() {
    let Some(store) = store().await else { return };
    let ops = PgOpsStore::for_store(&store);
    let (_, execution) = execution(&store, None).await;
    let started = ops
        .append_event(execution, None, &json!({"type": "started"}), "started")
        .await
        .expect("append");
    let done = ops
        .append_event(
            execution,
            None,
            &json!({"type": "done", "result": {}}),
            "done",
        )
        .await
        .expect("append");
    assert_eq!((started.sequence, done.sequence), (1, 2));
    let unknown = ops
        .append_event(Uuid::new_v4(), None, &json!({}), "done")
        .await
        .unwrap_err();
    assert_eq!(unknown.errcode.as_str(), "BEM-138");

    // The claim reads the frame from the event row; a second worker gets none.
    let worker = format!("w-{}", Uuid::new_v4().simple());
    let claimed: Vec<_> = ops
        .claim_batch(&worker, 1000)
        .await
        .expect("claim")
        .into_iter()
        .filter(|e| e.execution_id == execution)
        .collect();
    assert_eq!(claimed.len(), 2);
    assert_eq!(claimed[1].payload["type"], "done");
    assert_eq!(claimed[1].id, done.id);
    assert_eq!(claimed[0].attempts, 1);
    let again = ops.claim_batch("other", 1000).await.expect("claim");
    assert!(
        again.iter().all(|e| e.execution_id != execution),
        "leased rows are skipped"
    );
    ops.mark_sent(started.outbox_id).await.expect("sent");
    ops.mark_failed(done.outbox_id, "nats down")
        .await
        .expect("failed");

    let replay = ops.events_after(execution, 1, 500).await.expect("replay");
    assert_eq!(replay.iter().map(|e| e.sequence).collect::<Vec<_>>(), [2]);
    assert_eq!(ops.event_sequence(done.id).await.expect("lookup"), Some(2));
    assert!(ops.snapshot().await.expect("snapshot").pending >= 1);
    // Fresh rows are not purged (sent after 7 days, dead after 30).
    ops.purge().await.expect("purge");
    assert_eq!(
        ops.events_after(execution, 0, 500)
            .await
            .expect("replay")
            .len(),
        2
    );
    // Other tests' rows share the table, so only check that it runs.
    ops.release_expired_leases().await.expect("release");
}

#[tokio::test]
async fn dead_letters_persist_their_status() {
    let Some(store) = store().await else { return };
    let ops = PgOpsStore::for_store(&store);
    let letter = ops
        .enqueue_dead_letter(NewDeadLetter {
            task_name: "invoke_agent".into(),
            payload: json!({"execution_id": "x"}),
            error: Some("boom".into()),
            ..NewDeadLetter::default()
        })
        .await
        .expect("enqueue");
    assert!(ops
        .list_dead_letters("pending")
        .await
        .expect("list")
        .iter()
        .any(|l| l.id == letter.id));
    assert_eq!(
        ops.retry_dead_letter(letter.id)
            .await
            .expect("retry")
            .attempts,
        1
    );
    assert_eq!(
        ops.discard_dead_letter(letter.id)
            .await
            .expect("discard")
            .status,
        "discarded"
    );
    assert!(ops
        .list_dead_letters("pending")
        .await
        .expect("list")
        .iter()
        .all(|l| l.id != letter.id));
    let unknown = ops.discard_dead_letter(Uuid::new_v4()).await.unwrap_err();
    assert_eq!(unknown.errcode.as_str(), "BEM-488");
}

/// One reconciler at a time across connections.
#[tokio::test]
async fn the_reconcile_lock_is_exclusive() {
    let Some(store) = store().await else { return };
    let (a, b) = (PgOpsStore::for_store(&store), PgOpsStore::for_store(&store));
    let held = a.try_reconcile_lock().await.expect("lock").expect("free");
    assert!(
        b.try_reconcile_lock().await.expect("lock").is_none(),
        "a second reconciler waits"
    );
    held.release().await;
    let again = b
        .try_reconcile_lock()
        .await
        .expect("lock")
        .expect("released");
    // Dropping a guard unreleased closes its connection, which frees the lock.
    drop(again);
    assert!(a.try_reconcile_lock().await.expect("lock").is_some());
}

#[tokio::test]
async fn deleting_a_user_keeps_other_users_rows() {
    let Some(store) = store().await else { return };
    let ops = PgOpsStore::for_store(&store);
    let (user, other) = (Uuid::new_v4(), Uuid::new_v4());
    let (session, exec) = execution(&store, Some(user)).await;
    let (other_session, other_exec) = execution(&store, Some(other)).await;
    for execution in [exec, other_exec] {
        ops.append_event(execution, None, &json!({"type": "done"}), "done")
            .await
            .expect("event");
    }
    let exported = store.export_session(session).await.expect("export");
    assert_eq!(exported["session"]["user_id"], user.to_string());
    assert_eq!(exported["executions"], json!([exec.to_string()]));

    let counts = store
        .delete_user_data(&user.to_string())
        .await
        .expect("delete");
    assert_eq!(counts["agent_session"], 1);
    assert_eq!(counts["agent_execution"], 1);
    assert_eq!(counts["agent_execution_event"], 1);
    assert!(store.get_session(session).await.expect("read").is_none());
    assert!(store
        .get_session(other_session)
        .await
        .expect("read")
        .is_some());
    assert_eq!(
        ops.events_after(other_exec, 0, 10)
            .await
            .expect("replay")
            .len(),
        1
    );
}

/// A durable hold refuses deletion from any store on the database;
/// after release the user's rows go.
#[tokio::test]
async fn a_held_user_is_not_deleted_until_released() {
    let Some(store) = store().await else { return };
    let holds = PgLegalHolds::for_store(&store);
    let user = Uuid::new_v4();
    let (session, _) = execution(&store, Some(user)).await;
    holds
        .place(&user.to_string(), None, Some("litigation"), Some("admin"))
        .await
        .expect("place");
    let err = store
        .delete_user_data(&user.to_string())
        .await
        .expect_err("held");
    assert_eq!(err.errcode.as_str(), "BEM-489");
    assert!(store.get_session(session).await.expect("read").is_some());
    assert!(holds.release(&user.to_string()).await.expect("release"));
    assert!(!holds.release(&user.to_string()).await.expect("again"));
    let counts = store
        .delete_user_data(&user.to_string())
        .await
        .expect("delete");
    assert_eq!(counts["agent_session"], 1);
}

/// Plan 019 review: a hold keys a UUID in any case, and a mirror releases
/// only the holds it placed.
#[tokio::test]
async fn holds_match_any_uuid_case_and_keep_others_holds() {
    let Some(store) = store().await else { return };
    let holds = PgLegalHolds::for_store(&store);
    let user = Uuid::new_v4();
    let (session, _) = execution(&store, Some(user)).await;
    let upper = user.to_string().to_uppercase();
    holds
        .place(&upper, None, Some("litigation"), Some("admin"))
        .await
        .expect("place");
    assert!(holds.is_held(&user.to_string()).await.expect("read"));
    let err = store
        .delete_user_data(&user.to_string())
        .await
        .expect_err("held under another spelling");
    assert_eq!(err.errcode.as_str(), "BEM-489");
    assert!(store.get_session(session).await.expect("read").is_some());
    // Loan desk's mirror cannot release the admin's hold.
    assert!(!holds
        .release_placed_by(&user.to_string(), "loan_desk")
        .await
        .expect("release"));
    assert!(holds.is_held(&upper).await.expect("still held"));
    assert!(holds
        .release_placed_by(&upper, "admin")
        .await
        .expect("release"));
}

/// The purges compute their cutoffs in Rust: a row published
/// eight days ago goes, one published yesterday stays.
#[tokio::test]
async fn outbox_purge_uses_the_cutoff() {
    #[derive(diesel::QueryableByName)]
    struct Left {
        #[diesel(sql_type = SqlUuid, column_name = _id)]
        id: Uuid,
    }
    let Some(store) = store().await else { return };
    let ops = PgOpsStore::for_store(&store);
    let (_, execution) = execution(&store, None).await;
    let old = ops
        .append_event(execution, None, &json!({"type": "old"}), "old")
        .await
        .expect("append");
    let recent = ops
        .append_event(execution, None, &json!({"type": "recent"}), "recent")
        .await
        .expect("append");
    ops.mark_sent(old.outbox_id).await.expect("sent");
    ops.mark_sent(recent.outbox_id).await.expect("sent");
    let mut conn = store.pool().get().await.expect("conn");
    for (id, days) in [(old.outbox_id, 8), (recent.outbox_id, 1)] {
        diesel::sql_query(
            "UPDATE ref_beam.agent_outbox_event SET published_at = now() - make_interval(days => $2) \
             WHERE _id = $1",
        )
        .bind::<SqlUuid, _>(id)
        .bind::<diesel::sql_types::Integer, _>(days)
        .execute(&mut conn)
        .await
        .expect("age the row");
    }
    ops.purge().await.expect("purge");
    let left: Vec<Left> =
        diesel::sql_query("SELECT _id FROM ref_beam.agent_outbox_event WHERE execution_id = $1")
            .bind::<SqlUuid, _>(execution)
            .load(&mut conn)
            .await
            .expect("rows");
    let left: Vec<Uuid> = left.into_iter().map(|r| r.id).collect();
    assert_eq!(left, vec![recent.outbox_id]);
}

/// Plan 023 B3: session summaries compare-and-set under the row's lock. Of
/// two first writes one wins; a stale expected end loses; coverage never
/// moves back; an edit cuts the coverage before it.
#[tokio::test]
async fn session_summaries_compare_and_set() {
    let Some(store) = store().await else { return };
    let summaries = beam_store::session_summary::PgSessionSummaries::for_store(&store);
    let (session, _) = execution(&store, None).await;
    let payload = |end: i64| json!({"session_id": session.to_string(), "coverage_end": end});
    let (five, six) = (payload(5), payload(6));
    let (a, b) = tokio::join!(
        summaries.cas_put(session, 5, &five, None),
        summaries.cas_put(session, 6, &six, None)
    );
    assert!(a.unwrap() ^ b.unwrap(), "one first write wins");
    let current = summaries
        .get(session)
        .await
        .unwrap()
        .expect("a row")
        .coverage_end;
    assert!(!summaries
        .cas_put(session, 9, &payload(9), Some(current + 1))
        .await
        .unwrap());
    assert!(!summaries
        .cas_put(session, current - 1, &payload(current - 1), Some(current))
        .await
        .unwrap());
    assert!(summaries
        .cas_put(session, 9, &payload(9), Some(current))
        .await
        .unwrap());
    summaries.invalidate(session, Some(7)).await.unwrap();
    let row = summaries.get(session).await.unwrap().expect("a row");
    assert!(row.invalidated);
    assert_eq!(row.coverage_end, 6);
}
