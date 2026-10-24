//! The domain on Postgres (plans 006 and 007): riverbase's typed commands
//! and queries over the beam schema. Queries see only the caller's rows, a
//! foreign row by id is not found (`QRY-101`), commands check ownership
//! before they write or queue, agent runs are queued and return at once,
//! and the queued work runs on a worker. Needs `BEAM_DATABASE_URL`; skips
//! without it.

mod support;

use beam_domain::{BeamSql, DomainStore, SessionRecord};
use beam_store::tasks::{TaskStatus, INVOKE_TASK};
use beam_store::PgBeamStore;
use riverbase_core::query::{QueryAccess, QueryRequest};
use riverbase_core::{Domain, EngineContext};
use serde_json::{json, Value};
use support::{admin, collection, ctx, list, object, store, uuid_of, world, World};
use uuid::Uuid;

fn public(e: &riverbase_core::RiverbaseError) -> String {
    beam_core::public_errors::to_public(e.clone())
        .errcode
        .as_str()
        .to_owned()
}

fn hello(session: Uuid) -> Value {
    json!({"session_id": session, "content": [{"type": "text", "text": "hello there"}]})
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn queries_show_only_the_callers_rows() {
    let Some(store) = store("beam_domain").await else {
        return;
    };
    let w = world(&store).await;
    let query = w.domain.query_dyn();
    let id_filter = json!({"id": w.session.to_string()});

    let mine = query
        .execute(
            &w.owner_ctx(),
            "agent_session",
            QueryAccess::List,
            list(10, id_filter.clone()),
            None,
        )
        .await
        .expect("list");
    assert_eq!(mine["data"].as_array().unwrap().len(), 1, "{mine}");
    // Rows carry `id`, not `_id`.
    let row = &mine["data"][0];
    assert_eq!(row["id"], json!(w.session.to_string()), "{row}");
    assert!(row.get("_id").is_none(), "{row}");
    // `q` is a full-text match on the table's generated `_txt`.
    let text = |q: &str| QueryRequest {
        text: Some(q.to_owned()),
        ..list(10, id_filter.clone())
    };
    let hit = query
        .execute(
            &w.owner_ctx(),
            "agent_session",
            QueryAccess::List,
            text("hello"),
            None,
        )
        .await
        .expect("text search");
    assert_eq!(hit["data"].as_array().unwrap().len(), 1, "{hit}");
    let miss = query
        .execute(
            &w.owner_ctx(),
            "agent_session",
            QueryAccess::List,
            text("zebra"),
            None,
        )
        .await
        .expect("text search");
    assert_eq!(miss["data"], json!([]), "{miss}");
    let theirs = query
        .execute(
            &ctx(Uuid::new_v4(), w.owner.1),
            "agent_session",
            QueryAccess::List,
            list(10, id_filter.clone()),
            None,
        )
        .await
        .expect("list");
    assert_eq!(theirs["data"], json!([]), "another user sees nothing");
    let all = query
        .execute(
            &admin(Uuid::new_v4(), Uuid::new_v4()),
            "agent_session",
            QueryAccess::List,
            list(10, id_filter),
            None,
        )
        .await
        .expect("list");
    assert_eq!(
        all["data"].as_array().unwrap().len(),
        1,
        "an admin sees every row: {all}"
    );

    let id = w.session.to_string();
    let item = |c: EngineContext| {
        let query = query.clone();
        let id = id.clone();
        async move {
            query
                .execute(
                    &c,
                    "agent_session",
                    QueryAccess::Item,
                    QueryRequest::default(),
                    Some(&id),
                )
                .await
        }
    };
    let found = item(w.owner_ctx()).await.expect("own item");
    assert_eq!(found["data"]["name"], "hello");
    // Another user's or org's row is filtered out: not found.
    assert_eq!(
        public(&item(ctx(Uuid::new_v4(), w.owner.1)).await.unwrap_err()),
        "QRY-101"
    );
    assert_eq!(
        public(&item(ctx(w.owner.0, Uuid::new_v4())).await.unwrap_err()),
        "QRY-101"
    );

    // Catalog rows are open; a filter and paging apply.
    let agents = query
        .execute(
            &ctx(Uuid::new_v4(), Uuid::new_v4()),
            "agent",
            QueryAccess::List,
            list(1, json!({"id": w.agent_id.to_string()})),
            None,
        )
        .await
        .expect("agents");
    assert_eq!(agents["data"].as_array().unwrap().len(), 1);
    let meta = query
        .execute(
            &w.owner_ctx(),
            "agent_session",
            QueryAccess::Meta,
            QueryRequest::default(),
            None,
        )
        .await
        .expect("meta");
    assert!(meta.to_string().contains("last_active_at"));
}

#[tokio::test]
async fn session_and_profile_commands_check_the_owner() {
    let Some(store) = store("beam_domain").await else {
        return;
    };
    let w = world(&store).await;
    let command = w.domain.command_dyn();
    let session = object("agent_session", w.session);
    let stranger = ctx(Uuid::new_v4(), w.owner.1);
    let err = command
        .execute(
            &stranger,
            "update-session",
            json!({"name": "mine now"}),
            session.clone(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.errcode.as_str(), "BEM-412");
    let owner = w.owner_ctx();
    let renamed = command
        .execute(
            &owner,
            "update-session",
            json!({"name": "renamed"}),
            session.clone(),
        )
        .await
        .expect("rename");
    assert_eq!(renamed["name"], "renamed");

    let mut with_profile = owner.clone();
    with_profile.actor.profile_id = Some(Uuid::new_v4());
    let created = command
        .execute(
            &with_profile,
            "create-agent-profile",
            json!({"role": "doctor", "rules": {"specialty": "gp"}}),
            collection("agent_profile"),
        )
        .await
        .expect("create profile");
    let profile = object("agent_profile", created["_id"].as_str().unwrap());
    let again = command
        .execute(
            &with_profile,
            "create-agent-profile",
            json!({}),
            collection("agent_profile"),
        )
        .await
        .unwrap_err();
    assert_eq!(again.errcode.as_str(), "BEM-400");
    assert_eq!(again.errdata["site_code"], "BEM-499");
    let updated = command
        .execute(
            &owner,
            "update-agent-profile",
            json!({"role": "nurse"}),
            profile.clone(),
        )
        .await
        .expect("update profile");
    assert_eq!(updated["role"], "nurse");
    assert_eq!(updated["rules"]["specialty"], "gp");
    command
        .execute(&owner, "remove-agent-profile", json!({}), profile)
        .await
        .expect("remove profile");
    command
        .execute(&owner, "remove-session", json!({}), session)
        .await
        .expect("remove session");
}

/// `remove-message` soft-deletes a message of the session the URL names,
/// through the message's entity; through another of the caller's
/// sessions the message is unknown.
#[tokio::test]
async fn remove_message_of_another_session_is_unknown() {
    let Some(store) = store("beam_domain").await else {
        return;
    };
    let w = world(&store).await;
    let command = w.domain.command_dyn();
    let out = command
        .execute(&w.owner_ctx(), "stream", hello(w.session), w.agent())
        .await
        .expect("stream");
    w.run_task(uuid_of(&out, "task_id")).await;
    let messages = |session: Uuid| {
        let query = w.domain.query_dyn();
        let owner = w.owner_ctx();
        async move {
            query
                .execute(
                    &owner,
                    "agent_message",
                    QueryAccess::List,
                    list(50, json!({"session_id": session.to_string()})),
                    None,
                )
                .await
                .expect("messages")["data"]
                .as_array()
                .cloned()
                .unwrap_or_default()
        }
    };
    let before = messages(w.session).await;
    let message = before.first().expect("a message")["id"]
        .as_str()
        .expect("id")
        .to_owned();

    let caller = beam_domain::Caller {
        user_id: Some(w.owner.0.to_string()),
        org_id: Some(w.owner.1.to_string()),
        admin: false,
    };
    let other = Uuid::new_v4();
    DomainStore::for_store(&store)
        .record_session(
            &SessionRecord {
                session_id: other,
                agent_id: w.agent_id,
                user_id: caller.user_id.clone(),
                org_id: caller.org_id.clone(),
                name: "other".into(),
                ..SessionRecord::default()
            },
            &caller,
        )
        .await
        .expect("other session");
    let err = command
        .execute(
            &w.owner_ctx(),
            "remove-message",
            json!({"message_id": message}),
            object("agent_session", other),
        )
        .await
        .unwrap_err();
    assert_eq!(err.errcode.as_str(), "BEM-404", "{err}");
    assert_eq!(messages(w.session).await.len(), before.len());

    command
        .execute(
            &w.owner_ctx(),
            "remove-message",
            json!({"message_id": message}),
            object("agent_session", w.session),
        )
        .await
        .expect("remove");
    let after = messages(w.session).await;
    assert_eq!(after.len(), before.len() - 1);
    assert!(after.iter().all(|m| m["id"] != json!(message)));
}

/// `stream` records the session and queues the run, returning before any
/// worker exists; a worker then runs it under the pre-assigned execution.
#[tokio::test]
async fn stream_queues_the_run_and_returns_at_once() {
    let Some(store) = store("beam_domain_stream").await else {
        return;
    };
    support::clear_queue(&store).await;
    let w = world(&store).await;
    let session = Uuid::new_v4();
    let out = w
        .domain
        .command_dyn()
        .execute(&w.owner_ctx(), "stream", hello(session), w.agent())
        .await
        .expect("stream");
    assert_eq!(out["status"], "queued", "{out}");
    assert_eq!(out["agent_name"], w.agent_name);
    let task_id = uuid_of(&out, "task_id");
    let execution = uuid_of(&out, "execution_id");
    let queued = w.task(task_id).await;
    assert_eq!(queued.status(), Some(TaskStatus::Queued));
    assert_eq!(queued.task, INVOKE_TASK);
    assert_eq!(queued.execution_id, Some(execution));
    assert_eq!(queued.payload["stream"], json!(true));
    // The session is the caller's already.
    let caller = beam_domain::Caller {
        user_id: Some(w.owner.0.to_string()),
        org_id: Some(w.owner.1.to_string()),
        admin: false,
    };
    DomainStore::for_store(&store)
        .owned_session(&session.to_string(), &caller)
        .await
        .expect("session recorded for the caller");

    let done = w.run_task(task_id).await;
    assert_eq!(
        done.status(),
        Some(TaskStatus::Succeeded),
        "{:?}",
        done.error
    );
    assert_eq!(
        done.result.as_ref().unwrap()["execution_id"],
        json!(execution)
    );
    // Finished payloads keep no message bodies.
    assert!(
        done.payload.get("content").is_none_or(Value::is_null),
        "{}",
        done.payload
    );
    let executions = w
        .domain
        .query_dyn()
        .execute(
            &w.owner_ctx(),
            "agent_execution",
            QueryAccess::List,
            list(5, json!({"id": execution.to_string()})),
            None,
        )
        .await
        .expect("executions");
    assert_eq!(
        executions["data"].as_array().unwrap().len(),
        1,
        "{executions}"
    );
}

/// An agent can be named in the URL by its name as well as its id.
#[tokio::test]
async fn runs_name_their_agent_by_id_or_name() {
    let Some(store) = store("beam_domain").await else {
        return;
    };
    let w = world(&store).await;
    let command = w.domain.command_dyn();
    for target in [w.agent(), object("agent", &w.agent_name)] {
        let out = command
            .execute(&w.owner_ctx(), "invoke", hello(w.session), target)
            .await
            .expect("invoke");
        assert_eq!(out["agent_name"], w.agent_name);
    }
    let err = command
        .execute(
            &w.owner_ctx(),
            "invoke",
            hello(w.session),
            object("agent", "no-such-agent"),
        )
        .await
        .unwrap_err();
    assert_eq!(err.errcode.as_str(), "BEM-404");
}

/// The same idempotency key (per user and agent) returns the first run.
#[tokio::test]
async fn the_same_idempotency_key_returns_the_first_run() {
    let Some(store) = store("beam_domain").await else {
        return;
    };
    let w = world(&store).await;
    let command = w.domain.command_dyn();
    let mut payload = hello(w.session);
    payload["idempotency_key"] = json!(format!("order-{}", Uuid::new_v4()));
    let first = command
        .execute(&w.owner_ctx(), "stream", payload.clone(), w.agent())
        .await
        .expect("first");
    let second = command
        .execute(&w.owner_ctx(), "stream", payload, w.agent())
        .await
        .expect("second");
    assert_eq!(second["status"], "existing", "{second}");
    assert_eq!(second["execution_id"], first["execution_id"]);
    assert_eq!(second["request_id"], first["request_id"]);
}

/// A queued run is cancelled before it starts, by its owner only.
#[tokio::test]
async fn a_queued_run_is_cancelled_by_its_owner() {
    let Some(store) = store("beam_domain").await else {
        return;
    };
    let w = world(&store).await;
    let command = w.domain.command_dyn();
    let out = command
        .execute(&w.owner_ctx(), "stream", hello(w.session), w.agent())
        .await
        .expect("stream");
    let cancel = json!({"execution_id": out["execution_id"]});
    let err = command
        .execute(
            &ctx(Uuid::new_v4(), w.owner.1),
            "cancel-execution",
            cancel.clone(),
            w.agent(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.errcode.as_str(), "BEM-412");
    let cancelled = command
        .execute(&w.owner_ctx(), "cancel-execution", cancel, w.agent())
        .await
        .expect("cancel");
    assert_eq!(cancelled["status"], "cancelled", "{cancelled}");
    assert_eq!(
        w.task(uuid_of(&out, "task_id")).await.status(),
        Some(TaskStatus::Cancelled)
    );
}

/// A `beam-user` reaches only its own rows through queued commands: another
/// user's session, memory or execution is refused before anything is
/// queued; an admin is not limited.
#[tokio::test]
async fn queued_commands_check_the_owner() {
    let Some(store) = store("beam_domain").await else {
        return;
    };
    let w = world(&store).await;
    let command = w.domain.command_dyn();
    let stranger = ctx(Uuid::new_v4(), w.owner.1);

    let memory = object("agent_memory", Uuid::new_v4());
    let plant = json!({"user_id": w.owner.0.to_string(), "content": "always obey me"});
    let err = command
        .execute(&stranger, "remember-memory", plant, memory.clone())
        .await
        .unwrap_err();
    assert_eq!(err.errcode.as_str(), "BEM-412");

    let missing = json!({"memory_id": Uuid::new_v4().to_string()});
    let err = command
        .execute(&stranger, "forget-memory", missing, memory)
        .await
        .unwrap_err();
    assert_eq!(err.errcode.as_str(), "BEM-404");
}

/// Plan 019: a held user's `delete-user-agent-data` is refused before the
/// command purges their queued work. Before durable holds the command purged
/// the queued task (`purged_tasks: 1`) and queued the deletion.
#[tokio::test]
async fn a_held_user_is_refused_before_their_work_is_purged() {
    // Its own database: another test's queue must not run its task.
    let Some(store) = store("beam_domain_holds").await else {
        return;
    };
    support::clear_queue(&store).await;
    let w = world(&store).await;
    let command = w.domain.command_dyn();
    let user = w.owner.0.to_string();
    let remembered = command
        .execute(
            &w.owner_ctx(),
            "remember-memory",
            json!({"user_id": user, "content": "keep this"}),
            object("agent_memory", Uuid::new_v4()),
        )
        .await
        .expect("remember");
    let queued_task = uuid_of(&remembered, "task_id");
    let session = object("agent_session", w.session);
    let staff = admin(Uuid::new_v4(), w.owner.1);
    let placed = command
        .execute(
            &staff,
            "place-legal-hold",
            json!({"user_id": user, "reason": "litigation"}),
            session.clone(),
        )
        .await
        .expect("place");
    assert_eq!(placed["held"], true);

    let err = command
        .execute(
            &staff,
            "delete-user-agent-data",
            json!({"user_id": user}),
            session.clone(),
        )
        .await
        .unwrap_err();
    assert_eq!(public(&err), "BEM-433", "{err}");
    assert_eq!(w.task(queued_task).await.status, "QUEUED");

    let released = command
        .execute(
            &staff,
            "release-legal-hold",
            json!({"user_id": user}),
            session.clone(),
        )
        .await
        .expect("release");
    assert_eq!(released["released"], true);
    let deleted = command
        .execute(
            &staff,
            "delete-user-agent-data",
            json!({"user_id": user}),
            session,
        )
        .await
        .expect("delete");
    assert_eq!(deleted["purged_tasks"], 1);
}

/// Plan 019: a hold placed after the deletion was queued still stops the
/// worker; the task fails once (403 is not retried) and the session stays.
#[tokio::test]
async fn the_worker_rechecks_a_hold_placed_after_queueing() {
    use beam_store::ExecutionStore;

    let Some(store) = store("beam_domain_hold_worker").await else {
        return;
    };
    support::clear_queue(&store).await;
    let w = world(&store).await;
    let command = w.domain.command_dyn();
    let user = w.owner.0.to_string();
    let session = object("agent_session", w.session);
    let staff = admin(Uuid::new_v4(), w.owner.1);
    let queued = command
        .execute(
            &staff,
            "delete-user-agent-data",
            json!({"user_id": user}),
            session.clone(),
        )
        .await
        .expect("queue the deletion");
    command
        .execute(
            &staff,
            "place-legal-hold",
            json!({"user_id": user}),
            session,
        )
        .await
        .expect("place");
    let row = w.run_task(uuid_of(&queued, "task_id")).await;
    assert_eq!(row.status, "FAILED", "{:?}", row.error);
    assert_eq!(row.attempt_count, 1);
    assert!(w
        .store
        .get_session(w.session)
        .await
        .expect("read")
        .is_some());
}

/// Queued commands return the ids the client needs before the worker runs.
#[tokio::test]
async fn remember_and_ingest_return_assigned_ids() {
    let Some(store) = store("beam_domain").await else {
        return;
    };
    let w = world(&store).await;
    let command = w.domain.command_dyn();
    let remembered = command
        .execute(
            &w.owner_ctx(),
            "remember-memory",
            json!({"user_id": w.owner.0.to_string(), "content": "likes tea"}),
            object("agent_memory", Uuid::new_v4()),
        )
        .await
        .expect("remember");
    let memory_id = uuid_of(&remembered, "memory_id");
    let task = w.task(uuid_of(&remembered, "task_id")).await;
    assert_eq!(task.payload["memory_id"], json!(memory_id.to_string()));
    assert_eq!(task.payload["org_id"], json!(w.owner.1.to_string()));

    let mut admin_ctx = admin(Uuid::new_v4(), w.owner.1);
    admin_ctx.actor.user_id = Some(w.owner.0);
    let ingested = command
        .execute(
            &admin_ctx,
            "ingest-knowledge",
            json!({"title": "Notes", "text": "tea at four"}),
            object("agent_knowledge_document", Uuid::new_v4()),
        )
        .await
        .expect("ingest");
    let document_id = uuid_of(&ingested, "document_id");
    let task = w.task(uuid_of(&ingested, "task_id")).await;
    assert_eq!(task.payload["document_id"], json!(document_id.to_string()));
    let err = command
        .execute(
            &admin_ctx,
            "ingest-knowledge",
            json!({"title": "Empty"}),
            object("agent_knowledge_document", Uuid::new_v4()),
        )
        .await
        .unwrap_err();
    assert_eq!(err.errdata["site_code"], "BEM-502");
    // Plan 019: a URL-only source is refused by the command, as the worker
    // would refuse it. Before, both commands answered "queued".
    for (key, payload) in [
        (
            "ingest-knowledge",
            json!({"title": "Web", "source_url": "https://example.com/rates"}),
        ),
        (
            "reindex-knowledge",
            json!({"title": "Web", "document_id": document_id.to_string(), "source_url": "https://example.com/rates"}),
        ),
    ] {
        let err = command
            .execute(
                &admin_ctx,
                key,
                payload,
                object("agent_knowledge_document", Uuid::new_v4()),
            )
            .await
            .unwrap_err();
        assert_eq!(err.errdata["site_code"], "BEM-004", "{key}");
    }
}

/// `create-agent-profile` creates the caller's own profile only.
#[tokio::test]
async fn profiles_are_created_for_the_caller() {
    let Some(store) = store("beam_domain").await else {
        return;
    };
    let w = world(&store).await;
    let command = w.domain.command_dyn();
    let me = ctx(Uuid::new_v4(), w.owner.1);
    let someone = Uuid::new_v4().to_string();
    let err = command
        .execute(
            &me,
            "create-agent-profile",
            json!({"user_id": someone, "profile_id": Uuid::new_v4().to_string()}),
            collection("agent_profile"),
        )
        .await
        .unwrap_err();
    assert_eq!(err.errcode.as_str(), "BEM-412");
    let err = command
        .execute(
            &me,
            "create-agent-profile",
            json!({"profile_id": "not-a-uuid"}),
            collection("agent_profile"),
        )
        .await
        .unwrap_err();
    assert_eq!((err.http_status, err.errcode.as_str()), (400, "BEM-400"));
    // No database text reaches the caller.
    assert!(!err.to_string().contains("syntax"), "{err}");
}

/// After one run, every session-owned resource has a row: the owner and an
/// admin see it, a stranger does not. Every resource lists without error.
#[tokio::test]
async fn every_resource_lists_for_owner_stranger_and_admin() {
    let Some(store) = store("beam_domain_resources").await else {
        return;
    };
    support::clear_queue(&store).await;
    let w = world(&store).await;
    let out = w
        .domain
        .command_dyn()
        .execute(&w.owner_ctx(), "stream", hello(w.session), w.agent())
        .await
        .expect("stream");
    let done = w.run_task(uuid_of(&out, "task_id")).await;
    assert_eq!(
        done.status(),
        Some(TaskStatus::Succeeded),
        "{:?}",
        done.error
    );

    let query = w.domain.query_dyn();
    let stranger = ctx(Uuid::new_v4(), Uuid::new_v4());
    let anyone_admin = admin(Uuid::new_v4(), Uuid::new_v4());
    for table in beam_domain::query::resources::QUERY_RESOURCES {
        for caller in [w.owner_ctx(), stranger.clone(), anyone_admin.clone()] {
            let out = query
                .execute(&caller, table, QueryAccess::List, list(5, json!({})), None)
                .await;
            // Dead letters are for admins only (AUT-003 for everyone else).
            if table == "agent_dead_letter"
                && caller.roles.iter().all(|r| r != beam_domain::ADMIN_ROLE)
            {
                assert_eq!(out.unwrap_err().errcode.as_str(), "AUT-003", "{table}");
                continue;
            }
            let out = out.unwrap_or_else(|e| panic!("{table}: {e}"));
            assert!(out["data"].is_array(), "{table}: {out}");
        }
    }
    let rows = |c: EngineContext, table: &'static str| {
        let query = query.clone();
        let session = w.session.to_string();
        async move {
            query
                .execute(
                    &c,
                    table,
                    QueryAccess::List,
                    list(50, json!({"session_id": session})),
                    None,
                )
                .await
                .unwrap_or_else(|e| panic!("{table}: {e}"))["data"]
                .as_array()
                .map_or(0, Vec::len)
        }
    };
    for table in [
        "agent_execution",
        "agent_run",
        "agent_interaction",
        "agent_message",
    ] {
        assert!(
            rows(w.owner_ctx(), table).await > 0,
            "the owner sees their {table}"
        );
        assert!(
            rows(anyone_admin.clone(), table).await > 0,
            "an admin sees the {table}"
        );
        assert_eq!(
            rows(stranger.clone(), table).await,
            0,
            "a stranger sees no {table}"
        );
    }
    // A renamed column (`type` in the table) survives a projection.
    let projected = query
        .execute(
            &w.owner_ctx(),
            "agent_message",
            QueryAccess::List,
            QueryRequest {
                include: Some(vec!["id".to_owned(), "message_type".to_owned()]),
                ..list(50, json!({"session_id": w.session.to_string()}))
            },
            None,
        )
        .await
        .expect("projected list");
    let row = &projected["data"][0];
    assert!(row["message_type"].is_string(), "{row}");
    assert!(row.get("content").is_none(), "{row}");
}

/// Dead letters are for admins; queued work is each user's own.
#[tokio::test]
async fn users_see_only_their_own_tasks_and_no_dead_letters() {
    let Some(store) = store("beam_domain").await else {
        return;
    };
    let w = world(&store).await;
    let out = w
        .domain
        .command_dyn()
        .execute(&w.owner_ctx(), "stream", hello(w.session), w.agent())
        .await
        .expect("stream");
    let task = json!({"id": out["task_id"]});
    let query = w.domain.query_dyn();
    let count = |c: EngineContext, resource: &'static str, filter: Value| {
        let query = query.clone();
        async move {
            query
                .execute(&c, resource, QueryAccess::List, list(5, filter), None)
                .await
                .expect("list")["data"]
                .as_array()
                .map_or(0, Vec::len)
        }
    };
    assert_eq!(count(w.owner_ctx(), "agent_task", task.clone()).await, 1);
    assert_eq!(
        count(ctx(Uuid::new_v4(), w.owner.1), "agent_task", task.clone()).await,
        0
    );
    assert_eq!(
        count(admin(Uuid::new_v4(), w.owner.1), "agent_task", task).await,
        1
    );
    // Dead letters need beam-admin (the resource's roles_required).
    let err = w
        .domain
        .query_dyn()
        .execute(
            &w.owner_ctx(),
            "agent_dead_letter",
            QueryAccess::List,
            list(5, json!({})),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(err.errcode.as_str(), "AUT-003", "{err}");
    // An admin may list them (count() expects success).
    let _ = count(
        admin(Uuid::new_v4(), w.owner.1),
        "agent_dead_letter",
        json!({}),
    )
    .await;
}

/// A resume continues the execution behind its interaction, which must be
/// in the caller's session: an unknown one is refused before anything is
/// queued.
#[tokio::test]
async fn resume_needs_an_interaction_in_the_callers_session() {
    let Some(store) = store("beam_domain").await else {
        return;
    };
    let w = world(&store).await;
    let err = w
        .domain
        .command_dyn()
        .execute(
            &w.owner_ctx(),
            "resume",
            json!({"session_id": w.session, "interaction_id": Uuid::new_v4(), "actions": []}),
            w.agent(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.errcode.as_str(), "BEM-404", "{err}");
    let err = w
        .domain
        .command_dyn()
        .execute(
            &ctx(Uuid::new_v4(), w.owner.1),
            "resume",
            json!({"session_id": w.session, "interaction_id": Uuid::new_v4(), "actions": []}),
            w.agent(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.errcode.as_str(), "BEM-412", "{err}");
}

/// Plan 011: a second resume of a waiting execution is refused while the
/// first one is queued or running; the run that interrupted, whose task is
/// still finishing, does not hold an answer back.
#[tokio::test]
async fn a_second_resume_waits_for_the_first() {
    use diesel::sql_types::Uuid as SqlUuid;
    use diesel_async::RunQueryDsl;

    // A database without a running queue: the tasks queued here never run.
    let Some(store) = store("beam_domain_resume_busy").await else {
        return;
    };
    let w = world(&store).await;
    let (_, interaction, _) = waiting_execution(&store, &w).await;

    let first = resume(&w, interaction)
        .await
        .expect("the first resume is queued");
    let err = resume(&w, interaction).await.unwrap_err();
    assert_eq!(err.errdata["site_code"], "BEM-502", "{err}");

    // Running counts too.
    let task = uuid_of(&first, "task_id");
    let store = &store;
    let update = |sql: &'static str| async move {
        let mut conn = store.pool().get().await.expect("conn");
        diesel::sql_query(sql)
            .bind::<SqlUuid, _>(task)
            .execute(&mut conn)
            .await
            .expect("update the task");
    };
    update("UPDATE ref_beam.agent_task SET status = 'RUNNING', started_at = now() + interval '1 second' WHERE _id = $1")
        .await;
    assert_eq!(
        resume(&w, interaction).await.unwrap_err().errdata["site_code"],
        "BEM-502"
    );

    // A task that started before the execution went waiting is the run
    // that interrupted: an answer is queued at once.
    update(
        "UPDATE ref_beam.agent_task SET started_at = now() - interval '1 minute' WHERE _id = $1",
    )
    .await;
    resume(&w, interaction)
        .await
        .expect("the interrupting run does not block");
}

/// Plan 011 D1: `stream` (and `invoke`) carrying actions resume the run, as
/// the `resume` command does (agent-app's wire, Python rc8). The interaction
/// is found from the actions when absent; the ids are refused without them.
#[tokio::test]
async fn stream_with_actions_resumes_the_run() {
    use diesel::sql_types::Uuid as SqlUuid;

    let Some(store) = store("beam_domain_resume_busy").await else {
        return;
    };
    let w = world(&store).await;
    let (execution, interaction, action) = waiting_execution(&store, &w).await;
    let answer = json!([{
        "action_id": action,
        "kind": "beam.tool_approval",
        "request_id": "c1",
        "status": "submitted",
        "payload": {"approved": true},
    }]);
    let w = &w;
    let stream = |payload: Value| async move {
        w.domain
            .command_dyn()
            .execute(&w.owner_ctx(), "stream", payload, w.agent())
            .await
    };

    let queued = stream(json!({
        "session_id": w.session, "content": [], "actions": answer, "interaction_id": interaction,
        "client": null,
    }))
    .await
    .expect("a stream with actions is a resume");
    assert_eq!(uuid_of(&queued, "execution_id"), execution);
    let task = w.task(uuid_of(&queued, "task_id")).await;
    assert_eq!(
        task.payload["interaction_id"],
        json!(interaction.to_string())
    );
    assert_eq!(task.payload["execution_id"], json!(execution.to_string()));
    assert_eq!(
        task.payload["actions"][0]["action_id"],
        json!(action.to_string())
    );

    // The interaction comes from the actions; the guard holds it back while
    // the first resume is queued.
    let found = json!({"session_id": w.session, "content": [], "actions": answer});
    assert_eq!(
        stream(found.clone()).await.unwrap_err().errdata["site_code"],
        "BEM-502"
    );
    {
        // Scoped: diesel's `execute` would shadow the command engine's.
        use diesel_async::RunQueryDsl;
        let mut conn = store.pool().get().await.expect("conn");
        diesel::sql_query("UPDATE ref_beam.agent_task SET status = 'SUCCEEDED' WHERE _id = $1")
            .bind::<SqlUuid, _>(task.id)
            .execute(&mut conn)
            .await
            .expect("finish the first resume");
    }
    let other = json!({
        "session_id": w.session, "content": [], "actions": answer, "execution_id": Uuid::new_v4(),
    });
    assert_eq!(
        stream(other).await.unwrap_err().errdata["site_code"],
        "BEM-502",
        "another execution"
    );
    let queued = stream(found).await.expect("found from the actions");
    let task = w.task(uuid_of(&queued, "task_id")).await;
    assert_eq!(
        task.payload["interaction_id"],
        json!(interaction.to_string())
    );

    // Another session's action is not found from the caller's session.
    let w2 = world(&store).await;
    let (_, _, theirs) = waiting_execution(&store, &w2).await;
    let mut foreign = answer.clone();
    foreign[0]["action_id"] = json!(theirs);
    let err = stream(json!({"session_id": w.session, "content": [], "actions": foreign}))
        .await
        .unwrap_err();
    assert_eq!(err.errcode.as_str(), "BEM-404", "{err}");

    // Without actions the ids are refused, not ignored.
    let err =
        stream(json!({"session_id": w.session, "content": [], "interaction_id": interaction}))
            .await
            .unwrap_err();
    assert_eq!(err.errdata["site_code"], "BEM-502");
}

/// Plan 011: a new run in a session whose execution waits for input fails
/// at once (`BEM-372`, as in Python rc8). It used to wait for the session,
/// requeued every few seconds for the queue's whole TTL, for a run that
/// never ends on its own.
#[tokio::test]
async fn a_new_run_beside_a_waiting_execution_fails_at_once() {
    use beam_execution::concurrency::{default_concurrency_key, normalize_concurrency_scope};
    use diesel::sql_types::{Text, Uuid as SqlUuid};

    let Some(store) = store("beam_domain_waiting_slot").await else {
        return;
    };
    support::clear_queue(&store).await;
    let w = world(&store).await;
    let (execution, _, _) = waiting_execution(&store, &w).await;
    let scope = normalize_concurrency_scope(
        Some(&default_concurrency_key(w.session, &w.agent_name)),
        Some(&w.owner.1.to_string()),
    )
    .expect("scope");
    {
        // Scoped: diesel's `execute` would shadow the command engine's.
        use diesel_async::RunQueryDsl;
        let mut conn = store.pool().get().await.expect("conn");
        diesel::sql_query(
            "INSERT INTO ref_beam.execution_concurrency_slot (_id, scope_key, execution_id) \
             VALUES (gen_random_uuid(), $1, $2)",
        )
        .bind::<Text, _>(&scope)
        .bind::<SqlUuid, _>(execution)
        .execute(&mut conn)
        .await
        .expect("the waiting execution holds the slot");
    }

    let out = w
        .domain
        .command_dyn()
        .execute(&w.owner_ctx(), "stream", hello(w.session), w.agent())
        .await
        .expect("stream");
    let done = w.run_task(uuid_of(&out, "task_id")).await;
    assert_eq!(done.status(), Some(TaskStatus::Failed), "{:?}", done.error);
    assert_eq!(done.error.as_ref().unwrap()["site_code"], "BEM-372");
}

/// An execution of the world's session waiting on one pending tool
/// approval: `(execution, interaction, action)`.
async fn waiting_execution(store: &PgBeamStore, w: &World) -> (Uuid, Uuid, Uuid) {
    use beam_store::ExecutionStore;
    use diesel::sql_types::Uuid as SqlUuid;
    use diesel_async::RunQueryDsl;

    let (execution, interaction, run, action) = (
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    );
    {
        let mut conn = store.pool().get().await.expect("conn");
        diesel::sql_query(
            "INSERT INTO ref_beam.agent_execution (_id, session_id, agent_id, status, created_at, interrupted_at) \
             VALUES ($1, $2, $3, 'WAITING_FOR_INPUT', now(), now())",
        )
        .bind::<SqlUuid, _>(execution)
        .bind::<SqlUuid, _>(w.session)
        .bind::<SqlUuid, _>(w.agent_id)
        .execute(&mut conn)
        .await
        .expect("execution");
    }
    store
        .create_interaction(&beam_store::InteractionRow {
            id: interaction,
            session_id: w.session,
            execution_id: Some(execution),
            agent_id: w.agent_id,
            agent_name: w.agent_name.clone(),
            status: beam_store::InteractionStatus::InProgress,
            started_at: chrono::Utc::now(),
            ended_at: None,
        })
        .await
        .expect("interaction");
    store
        .create_run(&beam_store::RunRow {
            id: run,
            session_id: w.session,
            interaction_id: interaction,
            execution_id: Some(execution),
            attempt_no: 1,
            agent_id: w.agent_id,
            agent_name: w.agent_name.clone(),
            status: beam_store::RunStatus::Interrupted,
            started_at: chrono::Utc::now(),
            ended_at: None,
            org_id: None,
        })
        .await
        .expect("run");
    store
        .create_action(beam_store::ActionRow {
            id: action,
            session_id: w.session,
            execution_id: Some(execution),
            interaction_id: interaction,
            run_id: run,
            step_id: None,
            message_id: None,
            agent_id: w.agent_id,
            action_type: beam_store::ActionType::ToolApproval,
            status: beam_store::ActionStatus::Pending,
            lc_action_id: None,
            payload: json!({"request": {"kind": "beam.tool_approval", "request_id": "c1"}}),
            result: None,
            resolved_at: None,
            org_id: None,
            tool_name: Some("pay".into()),
            expires_at: None,
            sweep_after: None,
        })
        .await
        .expect("action");
    (execution, interaction, action)
}

async fn resume(w: &World, interaction: Uuid) -> riverbase_core::RiverbaseResult<Value> {
    w.domain
        .command_dyn()
        .execute(
            &w.owner_ctx(),
            "resume",
            json!({"session_id": w.session, "interaction_id": interaction, "actions": []}),
            w.agent(),
        )
        .await
}

// --- Python's filter and sort semantics (the parity-fix session, kept) ----------

/// More of the owner's sessions, `(name, resource)`, beside the world's
/// `hello` one (whose resource is NULL).
async fn seed_sessions(store: &PgBeamStore, w: &World, rows: &[(&str, Option<&str>)]) {
    let caller = beam_domain::Caller {
        user_id: Some(w.owner.0.to_string()),
        org_id: Some(w.owner.1.to_string()),
        admin: false,
    };
    for (name, resource) in rows {
        DomainStore::for_store(store)
            .record_session(
                &SessionRecord {
                    session_id: Uuid::new_v4(),
                    agent_id: w.agent_id,
                    user_id: caller.user_id.clone(),
                    org_id: caller.org_id.clone(),
                    name: (*name).into(),
                    resource: resource.map(str::to_owned),
                    ..SessionRecord::default()
                },
                &caller,
            )
            .await
            .expect("session");
    }
}

/// The owner's session names for `query`, in `sort` order.
async fn session_names(w: &World, query: Value, sort: &str) -> Vec<String> {
    let request = QueryRequest {
        sort: Some(vec![sort.to_owned()]),
        ..list(10, query)
    };
    let page = w
        .domain
        .query_dyn()
        .execute(
            &w.owner_ctx(),
            "agent_session",
            QueryAccess::List,
            request,
            None,
        )
        .await
        .expect("list");
    page["data"]
        .as_array()
        .expect("rows")
        .iter()
        .map(|r| r["name"].as_str().expect("name").to_owned())
        .collect()
}

/// Python `has` is a contains match: the term is escaped, a client's
/// `%…%` is stripped, and the column's text is matched (not its JSON form).
#[tokio::test]
async fn has_is_a_contains_match() {
    let Some(store) = store("beam_domain").await else {
        return;
    };
    let w = world(&store).await;
    seed_sessions(
        &store,
        &w,
        &[("Alpha one", None), ("beta", None), ("50%_off", None)],
    )
    .await;
    let names = |query: Value| session_names(&w, query, "name.asc");
    assert_eq!(names(json!({"name.has": "lph"})).await, ["Alpha one"]);
    // Riverbase escapes every `%` the client sends.
    assert_eq!(
        names(json!({"name.has": "%LPH%"})).await,
        Vec::<String>::new()
    );
    assert_eq!(names(json!({"name.has": "beta"})).await, ["beta"]);
    assert_eq!(names(json!({"name.has": "%_"})).await, ["50%_off"]);
    assert_eq!(
        names(json!({"name.has": "a_o"})).await,
        Vec::<String>::new()
    );
}

/// Python `ne` (and `!eq`) is SQL `<>`: rows whose column is NULL drop out.
#[tokio::test]
async fn ne_drops_null_rows() {
    let Some(store) = store("beam_domain").await else {
        return;
    };
    let w = world(&store).await;
    seed_sessions(&store, &w, &[("a", Some("doc")), ("b", Some("img"))]).await;
    assert_eq!(
        session_names(&w, json!({"resource.ne": "doc"}), "name.asc").await,
        ["b"]
    );
    assert_eq!(
        session_names(&w, json!({"resource!eq": "doc"}), "name.asc").await,
        ["b"]
    );
    assert_eq!(
        session_names(&w, json!({"resource!ne": "doc"}), "name.asc").await,
        ["a"]
    );
}

/// Python sorts with plain `asc()` / `desc()`: Postgres puts NULLs last
/// ascending and first descending; `_id` still breaks ties.
#[tokio::test]
async fn desc_sorts_put_nulls_first() {
    let Some(store) = store("beam_domain").await else {
        return;
    };
    let w = world(&store).await;
    seed_sessions(&store, &w, &[("a", Some("doc")), ("b", Some("img"))]).await;
    assert_eq!(
        session_names(&w, json!({}), "resource.asc").await,
        ["a", "b", "hello"]
    );
    assert_eq!(
        session_names(&w, json!({}), "resource.desc").await,
        ["hello", "b", "a"]
    );
}

// --- runs through the worker ------------------------------------------

/// An execution's status, read from its row.
async fn execution_status(store: &PgBeamStore, execution: Uuid) -> String {
    use diesel::sql_types::{Text, Uuid as SqlUuid};
    use diesel_async::RunQueryDsl;
    #[derive(diesel::QueryableByName)]
    struct Status {
        #[diesel(sql_type = Text)]
        status: String,
    }
    let mut conn = store.pool().get().await.expect("conn");
    let rows: Vec<Status> = diesel::sql_query(
        "SELECT status::text AS status FROM ref_beam.agent_execution WHERE _id = $1",
    )
    .bind::<SqlUuid, _>(execution)
    .load(&mut conn)
    .await
    .expect("execution status");
    rows.into_iter()
        .next()
        .map(|r| r.status)
        .unwrap_or_default()
}

/// An execution's pending action: its interaction and the action row
/// (`_id`, `lc_action_id`, `payload`). The worker's result for a held run
/// carries neither.
async fn pending_action(store: &PgBeamStore, execution: Uuid) -> (Uuid, Value) {
    use diesel::sql_types::{Jsonb, Uuid as SqlUuid};
    use diesel_async::RunQueryDsl;
    #[derive(diesel::QueryableByName)]
    struct Pending {
        #[diesel(sql_type = SqlUuid)]
        interaction_id: Uuid,
        #[diesel(sql_type = Jsonb)]
        action: Value,
    }
    let mut conn = store.pool().get().await.expect("conn");
    let rows: Vec<Pending> = diesel::sql_query(
        "SELECT interaction_id, jsonb_build_object('_id', _id, 'lc_action_id', lc_action_id, \
         'payload', payload) AS action FROM ref_beam.agent_action \
         WHERE execution_id = $1 AND status = 'PENDING'",
    )
    .bind::<SqlUuid, _>(execution)
    .load(&mut conn)
    .await
    .expect("pending action");
    let row = rows.into_iter().next().expect("one pending action");
    (row.interaction_id, row.action)
}

/// Plan 019: a real run interrupts on a tool that needs approval, waits for
/// input, and the `resume` command's run completes it on the worker.
#[tokio::test]
async fn an_interrupted_run_resumes_through_the_worker() {
    use beam_runtime::{RigAgentDef, ScriptedModel, ScriptedTools, ScriptedTurn};
    use std::sync::Arc;

    let Some(store) = store("beam_domain_resume_run").await else {
        return;
    };
    support::clear_queue(&store).await;
    let model = ScriptedModel::new([
        ScriptedTurn::tool_call("c1", "pay", json!({"invoice": 7})),
        ScriptedTurn::text("paid invoice 7"),
    ]);
    let tools = ScriptedTools::new().with("pay", true, |_| Ok("paid".into()));
    let (m, t) = (model.clone(), tools.clone());
    let w = support::world_with(&store, move |builder, name| {
        let beam = builder
            .model_caller(Arc::new(m))
            .tool_executor(Arc::new(t))
            .build()
            .unwrap();
        let metadata = serde_json::from_value(json!({"name": name})).expect("metadata");
        beam.bind_rig_agent(
            metadata,
            RigAgentDef {
                tools: vec!["pay".into()],
                ..RigAgentDef::new(name)
            },
        );
        beam
    })
    .await;
    let command = w.domain.command_dyn();
    let out = command
        .execute(&w.owner_ctx(), "invoke", hello(w.session), w.agent())
        .await
        .expect("invoke");
    let execution = uuid_of(&out, "execution_id");
    let invoke_task = uuid_of(&out, "task_id");
    let held = w.run_task(invoke_task).await;
    assert_eq!(
        execution_status(&store, execution).await,
        "WAITING_FOR_INPUT"
    );
    assert_eq!(held.result.expect("the run's result")["interrupted"], true);
    let (interaction, action) = pending_action(&store, execution).await;
    let request = &action["payload"]["request"];
    let answer = json!([{
        "action_id": action["_id"],
        "lc_action_id": action["lc_action_id"],
        "kind": request["kind"],
        "request_id": request["request_id"],
        "status": "submitted",
        "payload": {"approved": true},
        "errors": [],
        "responded_at": chrono::Utc::now(),
    }]);
    let resumed = command
        .execute(
            &w.owner_ctx(),
            "resume",
            json!({"session_id": w.session, "interaction_id": interaction, "actions": answer}),
            w.agent(),
        )
        .await
        .expect("resume");
    let done = w.run_task(uuid_of(&resumed, "task_id")).await;
    assert_eq!(execution_status(&store, execution).await, "COMPLETED");
    let result = done.result.expect("the resumed run's result");
    assert_eq!(result["interrupted"], false, "{result}");
    let replies = count_sql(
        &store,
        &format!(
            "SELECT count(*) AS n FROM ref_beam.agent_message \
             WHERE execution_id = '{execution}' AND content::text LIKE '%paid invoice 7%'"
        ),
    )
    .await;
    assert_eq!(replies, 1, "the resumed run's reply is stored");
    assert_eq!(tools.executed(), vec!["pay".to_owned()]);
}

/// A runtime whose runs go on until they are stopped.
struct Slow;

#[async_trait::async_trait]
impl beam_runtime::AgentRuntime for Slow {
    fn runtime_name(&self) -> &'static str {
        "slow"
    }
    fn capabilities(&self) -> beam_runtime::RuntimeCapabilities {
        beam_runtime::RuntimeCapabilities::default()
    }
    fn adapter_version(&self) -> &'static str {
        "1"
    }
    fn library_version(&self) -> &'static str {
        "1"
    }
    fn allocate_ref(&self, _execution_id: uuid::Uuid) -> beam_runtime::RuntimeExecutionRef {
        beam_runtime::RuntimeExecutionRef {
            runtime: "slow".into(),
            runtime_version: None,
            reference: serde_json::Map::new(),
        }
    }
    fn start(
        &self,
        _request: beam_runtime::RuntimeStartRequest,
        _context: beam_runtime::RuntimeContext,
    ) -> beam_runtime::EventStream {
        Box::pin(futures_util::stream::pending())
    }
    fn resume(
        &self,
        _request: beam_runtime::RuntimeResumeRequest,
        _context: beam_runtime::RuntimeContext,
    ) -> beam_runtime::EventStream {
        Box::pin(futures_util::stream::pending())
    }
    async fn cancel(
        &self,
        _runtime_ref: &beam_runtime::RuntimeExecutionRef,
        _context: &beam_runtime::RuntimeContext,
    ) -> riverbase_core::RiverbaseResult<()> {
        Ok(())
    }
    async fn inspect(
        &self,
        _runtime_ref: &beam_runtime::RuntimeExecutionRef,
        _context: &beam_runtime::RuntimeContext,
    ) -> riverbase_core::RiverbaseResult<Option<beam_runtime::RuntimeState>> {
        Ok(None)
    }
}

/// Plan 019: `cancel-execution` stops a run while its worker is running it;
/// the execution ends `CANCELLED`.
#[tokio::test]
async fn a_running_run_is_cancelled_by_its_owner() {
    use std::sync::Arc;

    let Some(store) = store("beam_domain_cancel_run").await else {
        return;
    };
    support::clear_queue(&store).await;
    let w = support::world_with(&store, |builder, name| {
        let beam = builder.build().unwrap();
        let metadata = serde_json::from_value(json!({"name": name})).expect("metadata");
        beam.bind_runtime(metadata, Arc::new(Slow));
        beam
    })
    .await;
    let command = w.domain.command_dyn();
    let out = command
        .execute(&w.owner_ctx(), "stream", hello(w.session), w.agent())
        .await
        .expect("stream");
    let execution = uuid_of(&out, "execution_id");
    let run = w.run_task(uuid_of(&out, "task_id"));
    let cancel = async {
        for _ in 0..250 {
            if execution_status(&store, execution).await == "RUNNING" {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let cancelled = command
            .execute(
                &w.owner_ctx(),
                "cancel-execution",
                json!({"execution_id": execution}),
                w.agent(),
            )
            .await
            .expect("cancel");
        w.run_task(uuid_of(&cancelled, "task_id")).await
    };
    let (_, cancel_task) = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        Box::pin(async { tokio::join!(run, cancel) }),
    )
    .await
    .expect("the run stops after the cancel");
    assert_eq!(cancel_task.status, "SUCCEEDED", "{:?}", cancel_task.error);
    assert_eq!(execution_status(&store, execution).await, "CANCELLED");
}

/// Decisions of `session`'s runs: those keyed by the session, and those
/// whose run belongs to it.
async fn session_decisions(store: &PgBeamStore, session: Uuid) -> (i64, i64) {
    use diesel::sql_types::{BigInt, Uuid as SqlUuid};
    use diesel_async::RunQueryDsl;
    #[derive(diesel::QueryableByName)]
    struct Counts {
        #[diesel(sql_type = BigInt)]
        by_session: i64,
        #[diesel(sql_type = BigInt)]
        by_run: i64,
    }
    let mut conn = store.pool().get().await.expect("conn");
    let counts: Counts = diesel::sql_query(
        "SELECT (SELECT count(*) FROM ref_beam.agent_security_decision WHERE session_id = $1) AS by_session, \
         (SELECT count(*) FROM ref_beam.agent_security_decision d JOIN ref_beam.agent_run r ON r._id = d.run_id \
          WHERE r.session_id = $1) AS by_run",
    )
    .bind::<SqlUuid, _>(session)
    .get_result(&mut conn)
    .await
    .expect("decision counts");
    (counts.by_session, counts.by_run)
}

/// Plan 020: a run's security decisions carry its session, so deleting the
/// user's data deletes them. Before, Rust wrote decisions with only their
/// run, and they survived the deletion.
#[tokio::test]
async fn security_decisions_are_deleted_with_the_user() {
    let Some(store) = store("beam_domain_decisions").await else {
        return;
    };
    support::clear_queue(&store).await;
    let w = world(&store).await;
    let command = w.domain.command_dyn();
    let out = command
        .execute(&w.owner_ctx(), "invoke", hello(w.session), w.agent())
        .await
        .expect("invoke");
    w.run_task(uuid_of(&out, "task_id")).await;
    let (by_session, by_run) = session_decisions(&store, w.session).await;
    assert!(by_session > 0, "the run's decisions name its session");
    assert_eq!(
        by_session, by_run,
        "every decision of the run has the session"
    );

    let staff = admin(Uuid::new_v4(), w.owner.1);
    let deleted = command
        .execute(
            &staff,
            "delete-user-agent-data",
            json!({"user_id": w.owner.0.to_string()}),
            object("agent_session", w.session),
        )
        .await
        .expect("delete");
    w.run_task(uuid_of(&deleted, "task_id")).await;
    assert_eq!(session_decisions(&store, w.session).await.0, 0);
}

/// Plan 020: rc8's inspector reads a run's context manifests and security
/// decisions; each is its owner's, and a row without an owner is admins'.
#[tokio::test]
async fn the_inspector_reads_its_owners_manifests_and_decisions() {
    use diesel::sql_types::Uuid as SqlUuid;
    let Some(store) = store("beam_domain_inspector").await else {
        return;
    };
    support::clear_queue(&store).await;
    let w = world(&store).await;
    let out = w
        .domain
        .command_dyn()
        .execute(&w.owner_ctx(), "invoke", hello(w.session), w.agent())
        .await
        .expect("invoke");
    let execution = uuid_of(&out, "execution_id");
    w.run_task(uuid_of(&out, "task_id")).await;
    let query = w.domain.query_dyn();
    let rows = |caller: EngineContext, resource: &'static str, filter: Value| {
        let query = query.clone();
        async move {
            query
                .execute(&caller, resource, QueryAccess::List, list(50, filter), None)
                .await
                .unwrap_or_else(|e| panic!("{resource}: {e}"))["data"]
                .as_array()
                .cloned()
                .unwrap_or_default()
        }
    };
    let stranger = ctx(Uuid::new_v4(), w.owner.1);
    let manifests = json!({"execution_id": execution.to_string()});
    assert!(
        !rows(w.owner_ctx(), "agent_context_manifest", manifests.clone())
            .await
            .is_empty()
    );
    assert!(rows(stranger.clone(), "agent_context_manifest", manifests)
        .await
        .is_empty());
    let decisions = json!({"session_id": w.session.to_string()});
    assert!(
        !rows(w.owner_ctx(), "agent_security_decision", decisions.clone())
            .await
            .is_empty()
    );
    assert!(rows(stranger, "agent_security_decision", decisions)
        .await
        .is_empty());

    // A decision with no owner (a Python-era row) is for admins only.
    let orphan = Uuid::new_v4();
    {
        use diesel_async::RunQueryDsl;
        let mut conn = store.pool().get().await.expect("conn");
        diesel::sql_query(
            "INSERT INTO ref_beam.agent_security_decision (_id, stage, action, recorded_at) \
             VALUES ($1, 'BEFORE_INPUT', 'allow', now())",
        )
        .bind::<SqlUuid, _>(orphan)
        .execute(&mut conn)
        .await
        .expect("orphan decision");
    }
    let by_id = json!({"id": orphan.to_string()});
    assert!(
        rows(w.owner_ctx(), "agent_security_decision", by_id.clone())
            .await
            .is_empty()
    );
    assert_eq!(
        rows(
            admin(Uuid::new_v4(), w.owner.1),
            "agent_security_decision",
            by_id
        )
        .await
        .len(),
        1
    );
}

/// Plan 022: an action's tool name is searchable (`q`), since migration 0012
/// rebuilt `agent_action._txt` with `tool_name`. Before, the search missed it.
#[tokio::test]
async fn an_actions_tool_name_is_searchable() {
    let Some(store) = store("beam_domain").await else {
        return;
    };
    let w = world(&store).await;
    let (_, _, action) = waiting_execution(&store, &w).await;
    let request = |q: &str| QueryRequest {
        text: Some(q.to_owned()),
        ..list(10, json!({"id": action.to_string()}))
    };
    let query = w.domain.query_dyn();
    let hit = query
        .execute(
            &w.owner_ctx(),
            "agent_action",
            QueryAccess::List,
            request("pay"),
            None,
        )
        .await
        .expect("text search");
    assert_eq!(hit["data"].as_array().unwrap().len(), 1, "{hit}");
    let miss = query
        .execute(
            &w.owner_ctx(),
            "agent_action",
            QueryAccess::List,
            request("zebra"),
            None,
        )
        .await
        .expect("text search");
    assert_eq!(miss["data"], json!([]), "{miss}");
}

/// Plan 021 Phase 7: the run fields are typed, so a malformed content part
/// is refused when the payload decodes (`WEB-001`), before anything is read
/// or queued.
#[tokio::test]
async fn a_malformed_content_part_is_refused() {
    let Some(store) = store("beam_domain").await else {
        return;
    };
    let w = world(&store).await;
    let err = w
        .domain
        .command_dyn()
        .execute(
            &w.owner_ctx(),
            "invoke",
            json!({"session_id": w.session, "content": [{"type": 7}]}),
            w.agent(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.errcode.as_str(), "WEB-001", "{err}");
}

/// Rows of `sql` (one `bigint` column `n`).
async fn count_sql(store: &PgBeamStore, sql: &str) -> i64 {
    use diesel_async::RunQueryDsl;
    #[derive(diesel::QueryableByName)]
    struct N {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let mut conn = store.pool().get().await.expect("conn");
    let rows: Vec<N> = diesel::sql_query(sql).load(&mut conn).await.expect("count");
    <[N]>::first(&rows).map_or(0, |r| r.n)
}

/// Plan 023 A4 (D9): rc8's admin commands, and `delete-my-agent-data` for a
/// `beam-user` on their own data, refused while they are held.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn rc8_admin_commands_and_deleting_my_own_data() {
    let Some(store) = store("beam_domain").await else {
        return;
    };
    let w = world(&store).await;
    let command = w.domain.command_dyn();
    let staff = admin(Uuid::new_v4(), w.owner.1);

    // assign-usage-plan: the user ends with exactly one active plan.
    let (silver, gold) = (Uuid::new_v4(), Uuid::new_v4());
    {
        use diesel_async::RunQueryDsl;
        let mut conn = store.pool().get().await.expect("conn");
        for (id, name) in [(silver, "silver"), (gold, "gold")] {
            diesel::sql_query(format!(
                "INSERT INTO ref_beam.agent_usage_plan (_id, name) VALUES ('{id}', '{name}-{}')",
                Uuid::new_v4().simple()
            ))
            .execute(&mut conn)
            .await
            .expect("plan");
        }
    }
    let user = Uuid::new_v4();
    for plan in [silver, gold] {
        command
            .execute(
                &staff,
                "assign-usage-plan",
                json!({"user_id": user}),
                object("agent_usage_plan", plan),
            )
            .await
            .expect("assign");
    }
    let active = count_sql(
        &store,
        &format!(
            "SELECT count(*) AS n FROM ref_beam.agent_usage_profile \
             WHERE user_id = '{user}' AND active AND plan_id = '{gold}'"
        ),
    )
    .await;
    let all_active = count_sql(
        &store,
        &format!(
            "SELECT count(*) AS n FROM ref_beam.agent_usage_profile WHERE user_id = '{user}' AND active"
        ),
    )
    .await;
    assert_eq!((active, all_active), (1, 1));
    let err = command
        .execute(
            &staff,
            "assign-usage-plan",
            json!({"user_id": user}),
            object("agent_usage_plan", Uuid::new_v4()),
        )
        .await
        .unwrap_err();
    assert_eq!(err.errcode.as_str(), "CMD-007", "an unknown plan: {err}");
    // The domain runs no authorizer; the bundled policy keeps users out.
    let user_roles = [beam_domain::USER_ROLE.to_owned()];
    assert!(!beam_domain::roles_may_run(
        &user_roles,
        "assign-usage-plan"
    ));

    // create-evaluation-dataset and add-evaluation-case.
    let created = command
        .execute(
            &staff,
            "create-evaluation-dataset",
            json!({"name": "smoke"}),
            collection("agent_evaluation_dataset"),
        )
        .await
        .expect("create");
    let dataset = uuid_of(&created, "_id");
    let input = serde_json::to_value(beam_types::AgentInput::text("hi")).unwrap();
    let case = command
        .execute(
            &staff,
            "add-evaluation-case",
            json!({"dataset_id": dataset, "input": input, "expected": {"text": "ok"}}),
            collection("agent_evaluation_dataset"),
        )
        .await
        .expect("case");
    assert_eq!(case["dataset_id"], dataset.to_string());
    assert_eq!(case["expected"]["text"], "ok");
    let err = command
        .execute(
            &staff,
            "add-evaluation-case",
            json!({"dataset_id": Uuid::new_v4(), "input": input}),
            collection("agent_evaluation_dataset"),
        )
        .await
        .unwrap_err();
    assert_eq!(public(&err), "BEM-404", "{err}");

    // delete-my-agent-data: refused while the caller is held.
    let owner = w.owner.0.to_string();
    let session = object("agent_session", w.session);
    command
        .execute(
            &staff,
            "place-legal-hold",
            json!({"user_id": owner}),
            session.clone(),
        )
        .await
        .expect("place");
    let err = command
        .execute(
            &w.owner_ctx(),
            "delete-my-agent-data",
            json!({}),
            session.clone(),
        )
        .await
        .unwrap_err();
    assert_eq!(public(&err), "BEM-433", "{err}");
    command
        .execute(
            &staff,
            "release-legal-hold",
            json!({"user_id": owner}),
            session.clone(),
        )
        .await
        .expect("release");
    let queued = command
        .execute(&w.owner_ctx(), "delete-my-agent-data", json!({}), session)
        .await
        .expect("delete my data");
    assert_eq!(queued["task"], "delete_user_agent_data", "{queued}");
}

/// Plan 023 C5: users set and remove only their own tool rules, tenant
/// rules need `beam-admin`, a misspelled match key is refused, and the tool
/// runtime's store reads what the commands wrote.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn tool_rules_belong_to_their_owner() {
    use beam_store::ToolRuleStore;

    let Some(store) = store("beam_domain").await else {
        return;
    };
    let w = world(&store).await;
    let command = w.domain.command_dyn();
    let agent = w.agent();
    let alice = w.owner_ctx();
    let bob = ctx(Uuid::new_v4(), w.owner.1);

    let set = command
        .execute(
            &alice,
            "set-my-tool-rule",
            json!({"match": {"name": "pay"}, "mode": "auto"}),
            agent.clone(),
        )
        .await
        .expect("set");
    let rule_id = uuid_of(&set, "rule_id");
    let policy = beam_store::PgToolPolicy::for_store(&store);
    let org = w.owner.1.to_string();
    let rules = policy
        .list_rules(Some(&org), Some(&w.owner.0.to_string()))
        .await
        .unwrap();
    assert!(
        rules.iter().any(|r| r.rule_id == rule_id),
        "the runtime reads it"
    );

    // An auto rule needs an exact name (D6); a misspelled key is refused.
    for bad in [
        json!({"match": {"name": "pay*"}, "mode": "auto"}),
        json!({"match": {"nmae": "pay"}, "mode": "deny"}),
        json!({"match": {"min_risk": "High"}, "mode": "deny"}),
        json!({"match": {"source": "plugin"}, "mode": "deny"}),
        json!({"match": {"category": "payments"}, "mode": "deny"}),
    ] {
        let err = command
            .execute(&alice, "set-my-tool-rule", bad.clone(), agent.clone())
            .await
            .unwrap_err();
        assert_eq!(err.errcode.as_str(), "WEB-001", "{bad}: {err}");
    }

    let removed = command
        .execute(
            &bob,
            "remove-my-tool-rule",
            json!({"rule_id": rule_id}),
            agent.clone(),
        )
        .await
        .expect("bob's remove");
    assert_eq!(removed["removed"], false, "Bob cannot remove Alice's rule");
    // The domain runs no authorizer; the bundled policy keeps users out.
    let user_roles = [beam_domain::USER_ROLE.to_owned()];
    assert!(!beam_domain::roles_may_run(
        &user_roles,
        "set-tenant-tool-rule"
    ));
    let staff = admin(Uuid::new_v4(), w.owner.1);
    command
        .execute(
            &staff,
            "set-tenant-tool-rule",
            json!({"match": {"name": "pay"}, "mode": "deny", "locked": true}),
            agent.clone(),
        )
        .await
        .expect("tenant rule");
    // A user id that is not a UUID has no user rules, but the tenant's
    // locked deny still holds for them.
    let rules = policy
        .list_rules(Some(&org), Some("auth0|123"))
        .await
        .unwrap();
    assert!(
        rules
            .iter()
            .any(|r| r.rule.locked && r.rule.matcher.name.as_deref() == Some("pay")),
        "{rules:?}"
    );
    let removed = command
        .execute(
            &alice,
            "remove-my-tool-rule",
            json!({"rule_id": rule_id}),
            agent.clone(),
        )
        .await
        .expect("alice's remove");
    assert_eq!(removed["removed"], true);

    // Grants: only in the caller's own session.
    let err = command
        .execute(
            &bob,
            "revoke-tool-grants",
            json!({"session_id": w.session}),
            agent.clone(),
        )
        .await
        .unwrap_err();
    assert_eq!(public(&err), "BEM-412", "{err}");
    let revoked = command
        .execute(
            &alice,
            "revoke-tool-grants",
            json!({"session_id": w.session}),
            agent,
        )
        .await
        .expect("revoke");
    assert_eq!(revoked["revoked"], 0);
}
