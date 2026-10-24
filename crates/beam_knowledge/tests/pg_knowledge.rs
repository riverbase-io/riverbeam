//! Knowledge on Postgres: pgvector and the JSON store,
//! document rows, visibility, long-term memory. Needs `--features postgres`
//! and `BEAM_DATABASE_URL` on a database with pgvector; skips otherwise.
#![cfg(feature = "postgres")]

use std::sync::Arc;

use beam_knowledge::DocumentRecords;
use beam_knowledge::{
    DocumentStatus, IngestRequest, KnowledgeManager, KnowledgeScope, MockEmbedder,
    PgDocumentRecords, PgVectorStore, RecallRequest, RememberRequest, RetrievalFilter,
    SqlJsonVectorStore, VectorStore,
};
use beam_store::{CatalogStore, SchemaName};
use riverbase_core::datastore::PgPool;
use uuid::Uuid;

async fn pool() -> Option<(PgPool, SchemaName)> {
    let pool = beam_store::testdb::pool("beam_knowledge").await?;
    Some((pool, beam_store::testdb::schema()))
}

fn manager(store: Arc<dyn VectorStore>, pool: &PgPool, schema: &SchemaName) -> KnowledgeManager {
    KnowledgeManager::new(Arc::new(MockEmbedder::new(1536)), store).with_records(Arc::new(
        PgDocumentRecords::from_db(beam_store::Db::new(pool.clone(), schema.clone())),
    ))
}

/// Ingest an org-scoped document, then check dedupe, visibility and delete.
async fn exercise(knowledge: &KnowledgeManager) {
    let (org, other_org, user) = (
        Uuid::new_v4().to_string(),
        Uuid::new_v4().to_string(),
        Uuid::new_v4().to_string(),
    );
    let marker = Uuid::new_v4().simple().to_string();
    let body = format!("The {marker} expense policy allows taxis after 9pm.");
    let org_doc = knowledge
        .ingest(IngestRequest {
            scope: KnowledgeScope::Org,
            org_id: Some(org.clone()),
            source_label: Some("handbook.md".into()),
            ..IngestRequest::text(body.clone(), "policy")
        })
        .await
        .expect("ingest");
    assert_eq!(
        (org_doc.status, org_doc.chunk_count),
        (DocumentStatus::Indexed, 1)
    );
    // Same body, scope and owners: skipped through the document row.
    let again = knowledge
        .ingest(IngestRequest {
            scope: KnowledgeScope::Org,
            org_id: Some(org.clone()),
            ..IngestRequest::text(body, "policy")
        })
        .await
        .expect("ingest");
    assert!(again.skipped);
    assert_eq!(again.document_id, org_doc.document_id);

    let query = format!("{marker} expense policy");
    let ours = |hits: &[beam_knowledge::Hit]| {
        hits.iter()
            .any(|h| h.document_id.as_deref() == Some(org_doc.document_id.as_str()))
    };
    let in_org = RetrievalFilter {
        org_id: Some(org.clone()),
        ..RetrievalFilter::default()
    };
    let hits = knowledge
        .retrieve(&query, 50, in_org.clone())
        .await
        .expect("retrieve");
    assert!(ours(&hits));
    let hit = hits
        .iter()
        .find(|h| h.document_id.as_deref() == Some(org_doc.document_id.as_str()))
        .unwrap();
    assert_eq!(hit.source.as_deref(), Some("handbook.md"));
    // No org, or another org: the org row is invisible (fail closed).
    let no_org = RetrievalFilter {
        user_id: Some(user),
        ..RetrievalFilter::default()
    };
    let other = RetrievalFilter {
        org_id: Some(other_org),
        ..RetrievalFilter::default()
    };
    for filters in [no_org, other] {
        assert!(!ours(
            &knowledge
                .retrieve(&query, 50, filters)
                .await
                .expect("retrieve")
        ));
    }
    assert_eq!(
        knowledge
            .delete_document(&org_doc.document_id)
            .await
            .expect("delete"),
        1
    );
    assert!(!ours(
        &knowledge
            .retrieve(&query, 50, in_org)
            .await
            .expect("retrieve")
    ));
    // Left behind for `scripts/cross_read.py` to load with Python.
    knowledge
        .ingest(IngestRequest {
            source_label: Some("kept.md".into()),
            ..IngestRequest::text(
                format!("Kept {marker} document for the cross-read."),
                "kept",
            )
        })
        .await
        .expect("ingest");
}

async fn memory(knowledge: &KnowledgeManager) {
    let user = Uuid::new_v4().to_string();
    let id = knowledge
        .remember(RememberRequest::new(
            user.clone(),
            "user prefers metric units",
        ))
        .await
        .expect("remember");
    let store = knowledge.memory_store();
    store
        .edit(&id, Some("user prefers imperial units"), None)
        .await
        .expect("edit");
    let record = store.get(&id).await.expect("get").expect("row");
    assert_eq!(record.text, "user prefers imperial units");
    assert_eq!(record.metadata["version"], 2);
    let recall = RecallRequest::new("imperial units", user.clone());
    assert_eq!(knowledge.recall(&recall).await.expect("recall").len(), 1);
    store
        .expire(&id, chrono::Utc::now() - chrono::Duration::seconds(5))
        .await
        .expect("expire");
    assert!(knowledge.recall(&recall).await.expect("recall").is_empty());
    let stranger = RecallRequest::new("imperial units", Uuid::new_v4().to_string());
    assert!(knowledge
        .recall(&stranger)
        .await
        .expect("recall")
        .is_empty());
    assert_eq!(knowledge.forget(&id).await.expect("forget"), 1);
    // Left behind for `scripts/cross_read.py`.
    knowledge
        .remember(RememberRequest::new(user, "kept memory for the cross-read"))
        .await
        .expect("remember");
}

#[tokio::test]
async fn pgvector_parity() {
    let Some((pool, schema)) = pool().await else {
        return;
    };
    assert!(beam_store::Db::new(pool.clone(), schema.clone())
        .knowledge_ready()
        .await
        .expect("check"));
    let knowledge = manager(
        Arc::new(PgVectorStore::from_db(beam_store::Db::new(
            pool.clone(),
            schema.clone(),
        ))),
        &pool,
        &schema,
    );
    exercise(&knowledge).await;
    memory(&knowledge).await;
}

#[tokio::test]
async fn sqljson_store() {
    let Some((pool, schema)) = pool().await else {
        return;
    };
    let knowledge = manager(
        Arc::new(SqlJsonVectorStore::from_db(beam_store::Db::new(
            pool.clone(),
            schema.clone(),
        ))),
        &pool,
        &schema,
    );
    exercise(&knowledge).await;
    memory(&knowledge).await;
}

/// A user id that is not a UUID never widens recall to every user's memories.
#[tokio::test]
async fn recall_with_a_non_uuid_user_sees_nothing() {
    let Some((pool, schema)) = pool().await else {
        return;
    };
    for store in [
        Arc::new(PgVectorStore::from_db(beam_store::Db::new(
            pool.clone(),
            schema.clone(),
        ))) as Arc<dyn VectorStore>,
        Arc::new(SqlJsonVectorStore::from_db(beam_store::Db::new(
            pool.clone(),
            schema.clone(),
        ))),
    ] {
        let knowledge = manager(store, &pool, &schema);
        let owner = Uuid::new_v4().to_string();
        let marker = Uuid::new_v4().simple().to_string();
        let id = knowledge
            .remember(RememberRequest::new(
                owner,
                format!("secret {marker} preference"),
            ))
            .await
            .expect("remember");
        for stranger in ["alice", "", " "] {
            let hits = knowledge
                .recall(&RecallRequest::new(format!("secret {marker}"), stranger))
                .await
                .expect("recall");
            assert!(
                hits.iter().all(|h| h.id != id),
                "{stranger:?} saw another user's memory"
            );
        }
        knowledge.forget(&id).await.expect("forget");
    }
}

/// A new agent in the catalog, for rows whose `agent_id` references one: its id.
async fn catalog_agent(pool: &PgPool, schema: &SchemaName) -> String {
    let name = format!("knowledge-{}", Uuid::new_v4().simple());
    let metadata: beam_types::AgentMetadata =
        serde_json::from_value(serde_json::json!({"name": name})).expect("metadata");
    beam_store::PgBeamStore::new(pool.clone(), schema.clone())
        .checkin_agents(&[beam_store::AgentRow::active(&metadata)])
        .await
        .expect("agent");
    beam_store::agent_id_for(&name).to_string()
}

/// A memory saved without an agent (the `remember-memory` command) is the
/// user's, so an agent-scoped recall sees it; another agent's memory stays
/// hidden. Same rule as `record_is_visible` and the in-memory store.
#[tokio::test]
async fn agent_recall_sees_memories_without_an_agent() {
    let Some((pool, schema)) = pool().await else {
        return;
    };
    for store in [
        Arc::new(PgVectorStore::from_db(beam_store::Db::new(
            pool.clone(),
            schema.clone(),
        ))) as Arc<dyn VectorStore>,
        Arc::new(SqlJsonVectorStore::from_db(beam_store::Db::new(
            pool.clone(),
            schema.clone(),
        ))),
    ] {
        let knowledge = manager(store, &pool, &schema);
        // `agent_memory.agent_id` references a catalog agent.
        let (user, agent, other_agent) = (
            Uuid::new_v4().to_string(),
            catalog_agent(&pool, &schema).await,
            catalog_agent(&pool, &schema).await,
        );
        let marker = Uuid::new_v4().simple().to_string();
        let remember = |agent_id: Option<&str>, text: &str| RememberRequest {
            agent_id: agent_id.map(str::to_owned),
            ..RememberRequest::new(user.clone(), format!("{marker} {text}"))
        };
        let shared = knowledge
            .remember(remember(None, "prefers window seats"))
            .await
            .expect("remember");
        let own = knowledge
            .remember(remember(Some(&agent), "prefers aisle seats"))
            .await
            .expect("remember");
        let foreign = knowledge
            .remember(remember(Some(&other_agent), "prefers no seat"))
            .await
            .expect("remember");
        let hits = knowledge
            .recall(&RecallRequest {
                agent_id: Some(agent),
                ..RecallRequest::new(format!("{marker} seats"), user)
            })
            .await
            .expect("recall");
        let ids: Vec<&str> = hits.iter().map(|h| h.id.as_str()).collect();
        assert!(
            ids.contains(&shared.as_str()),
            "missed the agent-less memory: {ids:?}"
        );
        assert!(
            ids.contains(&own.as_str()),
            "missed the agent's memory: {ids:?}"
        );
        assert!(
            !ids.contains(&foreign.as_str()),
            "saw another agent's memory: {ids:?}"
        );
        for id in [shared, own, foreign] {
            knowledge.forget(&id).await.expect("forget");
        }
    }
}

/// The same text ingested by two orgs is two documents (dedupe is per org).
#[tokio::test]
async fn dedupe_is_per_org() {
    let Some((pool, schema)) = pool().await else {
        return;
    };
    let knowledge = manager(
        Arc::new(PgVectorStore::from_db(beam_store::Db::new(
            pool.clone(),
            schema.clone(),
        ))),
        &pool,
        &schema,
    );
    let body = format!("Org-private {} rate card.", Uuid::new_v4().simple());
    let ingest = |org: String| IngestRequest {
        scope: KnowledgeScope::Org,
        org_id: Some(org),
        ..IngestRequest::text(body.clone(), "rates")
    };
    let a = knowledge
        .ingest(ingest(Uuid::new_v4().to_string()))
        .await
        .expect("ingest a");
    let b = knowledge
        .ingest(ingest(Uuid::new_v4().to_string()))
        .await
        .expect("ingest b");
    assert!(!b.skipped, "org B must not reuse org A's document");
    assert_ne!(a.document_id, b.document_id);
    knowledge
        .delete_document(&a.document_id)
        .await
        .expect("delete");
    knowledge
        .delete_document(&b.document_id)
        .await
        .expect("delete");
}

/// Python's visibility matrix (`tests/knowledge_compat/fixtures.json`) through
/// each SQL store: pgvector applies it in SQL, the JSON store in memory.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn visibility_matrix_in_sql_stores() {
    use beam_knowledge::{Namespace, VectorRecord};
    use serde_json::Value;
    let Some((pool, schema)) = pool().await else {
        return;
    };
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/knowledge_compat/fixtures.json");
    let fx: Value =
        serde_json::from_str(&std::fs::read_to_string(path).expect("fixture")).expect("json");
    let cases = fx["visibility"].as_array().expect("cases");
    for (pgvector, store) in [
        (
            true,
            Arc::new(PgVectorStore::from_db(beam_store::Db::new(
                pool.clone(),
                schema.clone(),
            ))) as Arc<dyn VectorStore>,
        ),
        (
            false,
            Arc::new(SqlJsonVectorStore::from_db(beam_store::Db::new(
                pool.clone(),
                schema.clone(),
            ))),
        ),
    ] {
        // Fresh UUIDs per run stand in for the fixture's ids.
        let mut ids: std::collections::HashMap<&str, String> = ["u1", "u2", "o1"]
            .into_iter()
            .map(|k| (k, Uuid::new_v4().to_string()))
            .collect();
        // `agent_id` references a catalog agent.
        let agent_name = format!("matrix-{}", Uuid::new_v4().simple());
        let metadata: beam_types::AgentMetadata =
            serde_json::from_value(serde_json::json!({"name": agent_name})).expect("metadata");
        beam_store::PgBeamStore::new(pool.clone(), schema.clone())
            .checkin_agents(&[beam_store::AgentRow::active(&metadata)])
            .await
            .expect("agent");
        ids.insert("a1", beam_store::agent_id_for(&agent_name).to_string());
        let map = |v: &Value| v.as_str().map(|s| ids[s].clone());
        let document = Uuid::new_v4().to_string();
        // Chunks reference their document row.
        PgDocumentRecords::from_db(beam_store::Db::new(pool.clone(), schema.clone()))
            .upsert_document(&beam_knowledge::DocumentRow {
                id: document.clone(),
                title: "matrix".into(),
                scope: KnowledgeScope::Global,
                doc_type: "text".into(),
                agent_id: None,
                user_id: None,
                org_id: None,
                source: None,
                body: None,
                media_id: None,
                metadata: serde_json::Map::new(),
                status: beam_knowledge::DocumentStatus::Indexed,
                content_hash: None,
                chunk_count: 0,
                embedder_name: None,
            })
            .await
            .expect("document");
        let mut records: Vec<(String, Value)> = Vec::new();
        for case in cases {
            let rec = &case["record"];
            if records.iter().any(|(_, r)| r == rec) {
                continue;
            }
            let id = Uuid::new_v4().to_string();
            let mut record =
                VectorRecord::new(id.clone(), Namespace::Knowledge, "matrix", vec![0.5; 1536]);
            record.scope = rec["scope"].as_str().and_then(KnowledgeScope::parse);
            record.user_id = map(&rec["user_id"]);
            record.org_id = map(&rec["org_id"]);
            record.agent_id = map(&rec["agent_id"]);
            record.document_id = Some(document.clone());
            record.chunk_index = Some(records.len());
            store.upsert(vec![record]).await.expect("upsert");
            records.push((id, rec.clone()));
        }
        for case in cases {
            let Some(filter) = case["filter"].as_object() else {
                continue; // The SQL stores are always queried with a filter.
            };
            let mut filter: RetrievalFilter =
                serde_json::from_value(Value::Object(filter.clone())).expect("filter");
            filter.user_id = filter.user_id.as_deref().map(|s| ids[s].clone());
            filter.org_id = filter.org_id.as_deref().map(|s| ids[s].clone());
            filter.agent_id = filter.agent_id.as_deref().map(|s| ids[s].clone());
            filter.document_ids = Some(vec![document.clone()]);
            let hits = store
                .query(&[0.5; 1536], 1000, Some(&filter), Namespace::Knowledge)
                .await
                .expect("query");
            let id = &records
                .iter()
                .find(|(_, r)| *r == case["record"])
                .expect("record")
                .0;
            let python = case["visible"].as_bool().expect("bool");
            let expected = if pgvector {
                let sql = pg_rule(&case["record"], &case["filter"]);
                assert!(
                    !sql || python,
                    "pgvector must never be broader: {}",
                    case["record"]
                );
                sql
            } else {
                python
            };
            assert_eq!(
                hits.iter().any(|h| &h.id == id),
                expected,
                "{} under {}",
                case["record"],
                case["filter"]
            );
        }
        store
            .delete(None, Some(&document), Namespace::Knowledge)
            .await
            .expect("cleanup");
        PgDocumentRecords::from_db(beam_store::Db::new(pool.clone(), schema.clone()))
            .delete_document(&document)
            .await
            .expect("cleanup");
    }
}

/// Python's `_apply_knowledge_filters` SQL (the pgvector rule): every owner
/// column is checked whatever the scope, and `scope != X` is NULL for a row
/// without a scope. Stricter than `record_is_visible`, never broader.
fn pg_rule(rec: &serde_json::Value, filter: &serde_json::Value) -> bool {
    let scope = rec["scope"].as_str();
    if let Some(want) = filter["scope"].as_str() {
        if scope != Some(want) {
            return false;
        }
    }
    if let Some(scopes) = filter["scopes"].as_array() {
        if !scopes.iter().any(|s| s.as_str() == scope) {
            return false;
        }
    }
    [
        ("agent_id", "agent"),
        ("user_id", "user"),
        ("org_id", "org"),
    ]
    .iter()
    .all(|(col, owned)| {
        let owner = rec[*col].as_str();
        let other_scope = scope.is_some_and(|s| s != *owned);
        match filter[*col].as_str() {
            None => owner.is_none() && other_scope,
            Some(id) => owner == Some(id) || (owner.is_none() && other_scope),
        }
    })
}

/// Plan 015: the factory's pool sets the schema's `search_path`, which the
/// document entity needs. The URL here has no `options=`.
#[tokio::test]
async fn a_factory_pool_writes_documents() {
    use beam_knowledge::{build_knowledge, EmbedderKind, KnowledgeBuildRequest, StoreKind};
    // Migrated through `pool`, then opened by its plain URL.
    if pool().await.is_none() {
        return;
    }
    let Some(url) = beam_store::testdb::database_url("beam_knowledge").await else {
        return;
    };
    // The factory opens the schema `BEAM_SQL_SCHEMA` names (default
    // `riverbeam`), not the test schema `pool` migrated.
    let factory_schema = SchemaName::from_env().expect("BEAM_SQL_SCHEMA");
    beam_store::migrate::migrate(&url, &factory_schema)
        .await
        .expect("migrate the factory's schema");
    let knowledge = build_knowledge(KnowledgeBuildRequest {
        embedder: EmbedderKind::Mock,
        store: StoreKind::PgVector,
        database_url: Some(url),
        ..KnowledgeBuildRequest::from_env(1536)
    })
    .await
    .expect("build");
    let doc = knowledge
        .ingest(IngestRequest {
            scope: KnowledgeScope::Org,
            org_id: Some(Uuid::new_v4().to_string()),
            ..IngestRequest::text(format!("Factory {} memo.", Uuid::new_v4().simple()), "memo")
        })
        .await
        .expect("ingest through the factory's pool");
    assert_eq!(doc.status, DocumentStatus::Indexed);
    knowledge
        .delete_document(&doc.document_id)
        .await
        .expect("delete");
}

/// Plan 015: a document left `INDEXING` past the cutoff fails, whether it
/// was updated since its insert or not (`_updated` is NULL until the first
/// update); a fresh one stays.
#[tokio::test]
async fn stuck_indexing_fails_after_the_cutoff() {
    use beam_store::entity::agent_knowledge_document::agent_knowledge_document as t;
    use diesel::{ExpressionMethods, QueryDsl};
    use diesel_async::RunQueryDsl;
    let Some((pool, schema)) = pool().await else {
        return;
    };
    let records = PgDocumentRecords::from_db(beam_store::Db::new(pool.clone(), schema));
    let row = |hash: &str| beam_knowledge::DocumentRow {
        id: Uuid::new_v4().to_string(),
        title: "stuck".into(),
        scope: KnowledgeScope::Global,
        doc_type: "text".into(),
        agent_id: None,
        user_id: None,
        org_id: None,
        source: None,
        body: Some("body".into()),
        media_id: None,
        metadata: serde_json::Map::new(),
        status: DocumentStatus::Indexing,
        content_hash: Some(hash.to_owned()),
        chunk_count: 0,
        embedder_name: None,
    };
    let hashes: Vec<String> = (0..3).map(|_| Uuid::new_v4().to_string()).collect();
    let (updated, never_updated, fresh) = (row(&hashes[0]), row(&hashes[1]), row(&hashes[2]));
    for doc in [&updated, &never_updated, &fresh] {
        records.upsert_document(doc).await.expect("insert");
    }
    let two_hours_ago = chrono::Utc::now() - chrono::Duration::hours(2);
    let id = |doc: &beam_knowledge::DocumentRow| Uuid::parse_str(&doc.id).unwrap();
    {
        let mut conn = pool.get().await.unwrap();
        diesel::update(t::table.filter(t::_id.eq(id(&updated))))
            .set((t::_created.eq(two_hours_ago), t::_updated.eq(two_hours_ago)))
            .execute(&mut conn)
            .await
            .unwrap();
        diesel::update(t::table.filter(t::_id.eq(id(&never_updated))))
            .set((
                t::_created.eq(two_hours_ago),
                t::_updated.eq(None::<chrono::DateTime<chrono::Utc>>),
            ))
            .execute(&mut conn)
            .await
            .unwrap();
    }
    assert!(records.fail_stuck_indexing(60).await.unwrap() >= 2);
    for (doc, hash, want) in [
        (&updated, &hashes[0], DocumentStatus::Failed),
        (&never_updated, &hashes[1], DocumentStatus::Failed),
        (&fresh, &hashes[2], DocumentStatus::Indexing),
    ] {
        let found = records.find(&doc.id, hash).await.unwrap().unwrap();
        assert_eq!(found.status, want, "{}", doc.id);
        records.delete_document(&doc.id).await.unwrap();
    }
}

/// Plan 022: of two documents with one fingerprint, the INDEXED one is
/// found, even when its id sorts after the other's.
#[tokio::test]
async fn find_by_fingerprint_prefers_an_indexed_row() {
    let Some((pool, schema)) = pool().await else {
        return;
    };
    let records = PgDocumentRecords::from_db(beam_store::Db::new(pool, schema));
    let hash = Uuid::new_v4().to_string();
    let suffix = Uuid::new_v4().simple().to_string();
    let doc = |id: String, status: DocumentStatus| beam_knowledge::DocumentRow {
        id,
        title: "twin".into(),
        scope: KnowledgeScope::Global,
        doc_type: "text".into(),
        agent_id: None,
        user_id: None,
        org_id: None,
        source: None,
        body: Some("body".into()),
        media_id: None,
        metadata: serde_json::Map::new(),
        status,
        content_hash: Some(hash.clone()),
        chunk_count: 0,
        embedder_name: None,
    };
    // The pending row's id sorts first; the indexed row must still win.
    let pending = doc(
        format!("00000000-0000-4000-8000-{}", &suffix[..12]),
        DocumentStatus::Pending,
    );
    let indexed = doc(
        format!("ffffffff-ffff-4fff-bfff-{}", &suffix[..12]),
        DocumentStatus::Indexed,
    );
    records.upsert_document(&pending).await.expect("pending");
    records.upsert_document(&indexed).await.expect("indexed");
    let found = records
        .find_by_fingerprint(KnowledgeScope::Global, None, None, None, &hash)
        .await
        .unwrap()
        .expect("a row");
    assert_eq!(found.id, indexed.id);
    for d in [&pending, &indexed] {
        records.delete_document(&d.id).await.unwrap();
    }
}
