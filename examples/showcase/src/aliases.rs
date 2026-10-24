//! The reference API's deprecated aliases, kept for clients of Python's
//! `examples/beam_app` server: `/invoke` (waits for the run, up to
//! `BEAM_INVOKE_WAIT_SECONDS`), `/astream`, `/astream/{session}/{request}`
//! (live SSE) and `/catalog/agents`, which run through the domain, and
//! `/ws/audio` with voice (feature `audio`).

use std::convert::Infallible;
use std::sync::Arc;

use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::sse::{Event, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use beam_core::RiverBeam;
use beam_domain::{BeamDomainBundle, BeamSql, DomainStore};
use beam_stream::{BeamStream, NatsTransport};
use beam_types::StreamChunk;
use futures_util::StreamExt;
use riverbase_core::base::AggregateRoot;
use riverbase_core::{CommandTarget, Domain, EngineContext};
use riverbase_http::web::RiverbaseApp;
use riverbase_http::Principal;
use serde_json::{json, Value};

use crate::host::astream::{live_until_end, read_replay};
use crate::host::runs::{RunEnd, RunWaiter};

/// State of the deprecated aliases.
pub struct AppState {
    pub domain: Arc<BeamDomainBundle>,
    /// The durable replay `/astream` starts with.
    pub beam: Arc<RiverBeam>,
    pub transport: Arc<NatsTransport>,
    pub store: DomainStore,
    /// Runs an agent command and waits for the run (`/invoke`, audio).
    pub runs: Arc<RunWaiter>,
}

/// Mount the deprecated aliases on `app`, and `/ws/audio` with voice.
///
/// riverbase's Casbin layer skips public routes: the aliases and `/ws/audio`
/// require a principal and check its roles against the beam policy
/// themselves (`roles_may_run`, `roles_may_query`), then run the domain's
/// commands, which keep row ownership.
///
/// # Errors
/// Voice's settings are invalid (feature `audio`).
pub fn mount(app: &mut RiverbaseApp, state: Arc<AppState>) -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "audio")]
    {
        // Each turn is an `invoke` command, waited for.
        let audio = Arc::new(crate::host::audio::AudioState::from_env(state.runs.clone())?);
        app.register_public_routes(
            Router::new()
                .route("/ws/audio", get(crate::host::audio::audio_ws_upgrade))
                .with_state(audio),
            &["/ws/audio"],
        );
    }
    let aliases = Router::new()
        .route("/invoke", post(invoke))
        .route("/astream", post(astream))
        .route("/astream/{session_id}/{request_id}", get(astream_sse))
        .route("/catalog/agents", get(list_agents));
    app.register_public_routes(
        aliases.with_state(state),
        &["/invoke", "/astream", "/catalog/agents"],
    );
    app.register_public_prefix("/astream/");
    Ok(())
}

fn problem(err: riverbase_core::RiverbaseError) -> Response {
    let status = beam_core::public_errors::to_public(err.clone()).http_status;
    let code = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    (
        code,
        Json(beam_core::public_errors::problem_details(err, None)),
    )
        .into_response()
}

fn deprecated(response: Response) -> Response {
    let mut response = response;
    response
        .headers_mut()
        .insert("Deprecation", axum::http::HeaderValue::from_static("true"));
    response
}

/// `403` unless the bundled beam policy lets the caller do this.
fn permitted(allowed: bool) -> Result<(), Response> {
    if allowed {
        Ok(())
    } else {
        Err(deprecated(problem(
            beam_core::BEM_067.with_data("the caller's roles do not allow this"),
        )))
    }
}

/// The engine context of an authenticated caller (`401` without one), as
/// riverbase builds it for its own routes.
fn caller(principal: Option<Extension<Principal>>) -> Result<EngineContext, Response> {
    let Some(Extension(principal)) = principal else {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"detail": "Authentication required"})),
        )
            .into_response());
    };
    Ok(crate::host::http::engine_context(&principal))
}

/// The agent named in the body (`agent_name`), as a command target.
fn agent_target(payload: &Value) -> CommandTarget {
    let name = payload
        .get("agent_name")
        .and_then(Value::as_str)
        .unwrap_or_default();
    CommandTarget::Object(AggregateRoot::new("agent", name))
}

/// Deprecated: `invoke` on the agent named in the body. The command queues
/// the run; this waits for its result (`202` with the ids when it outlasts
/// `BEAM_INVOKE_WAIT_SECONDS`).
async fn invoke(
    State(state): State<Arc<AppState>>,
    principal: Option<Extension<Principal>>,
    Json(payload): Json<Value>,
) -> Response {
    let ctx = match caller(principal) {
        Ok(ctx) => ctx,
        Err(response) => return response,
    };
    if let Err(response) = permitted(beam_domain::roles_may_run(&ctx.roles, "invoke")) {
        return response;
    }
    let target = agent_target(&payload);
    match state.runs.run(&ctx, "invoke", payload, target).await {
        Ok(RunEnd::Done(result)) => deprecated(Json(result).into_response()),
        Ok(RunEnd::Failed(frame)) => {
            deprecated((StatusCode::BAD_GATEWAY, Json(frame)).into_response())
        }
        Ok(RunEnd::Pending(ack)) => deprecated((StatusCode::ACCEPTED, Json(ack)).into_response()),
        Err(e) => deprecated(problem(e)),
    }
}

/// Deprecated: `stream` on the agent named in the body.
async fn astream(
    State(state): State<Arc<AppState>>,
    principal: Option<Extension<Principal>>,
    Json(payload): Json<Value>,
) -> Response {
    let ctx = match caller(principal) {
        Ok(ctx) => ctx,
        Err(response) => return response,
    };
    if let Err(response) = permitted(beam_domain::roles_may_run(&ctx.roles, "stream")) {
        return response;
    }
    let target = agent_target(&payload);
    match state
        .domain
        .command_dyn()
        .execute(&ctx, "stream", payload, target)
        .await
    {
        Ok(out) => deprecated(Json(out).into_response()),
        Err(e) => deprecated(problem(e)),
    }
}

/// Deprecated: a run's frames. It subscribes first, sends the
/// durable replay (from `Last-Event-ID`), then the live frames until `done`
/// or `error`, so a client that connects after the run started misses
/// nothing; a replay of a finished run ends the stream. The caller must own
/// the session. New clients follow their own riverbase RTC channel instead,
/// `/api/rtc/default~ssestream/{org}.{user}`, subscribed before they send.
async fn astream_sse(
    State(state): State<Arc<AppState>>,
    principal: Option<Extension<Principal>>,
    Path((session_id, request_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let ctx = match caller(principal) {
        Ok(ctx) => ctx,
        Err(response) => return response,
    };
    if let Err(response) = permitted(beam_domain::roles_may_run(&ctx.roles, "stream")) {
        return response;
    }
    let owner = beam_domain::Caller::from_context(&ctx);
    if let Err(e) = state.store.owned_session(&session_id, &owner).await {
        return problem(e);
    }
    let Ok(session) = uuid::Uuid::parse_str(&session_id) else {
        return (StatusCode::NOT_FOUND, "unknown session").into_response();
    };
    let channel = format!(
        "ws.channel.{}",
        BeamStream::stream_key(&session_id, &request_id)
    );
    // Subscribe before reading the replay: a frame sent in between is live.
    let live = match state.transport.subscribe(&channel).await {
        Ok(subscriber) => subscriber.map(|message| message.payload),
        Err(err) => {
            let mut frame =
                StreamChunk::error_frame(beam_core::public_errors::stream_error_text(err));
            frame.session_id = Some(session_id);
            let event = Event::default()
                .json_data(&frame)
                .unwrap_or_else(|_| Event::default().data("serialize-error"));
            let once = futures_util::stream::once(async move { Ok::<_, Infallible>(event) });
            return deprecated(Sse::new(once).into_response());
        }
    };
    let user = ctx.actor.user_id.map(|u| u.to_string());
    let org = ctx.organization_id.map(|o| o.to_string());
    let last_event_id = headers.get("Last-Event-ID").and_then(|v| v.to_str().ok());
    // This request's own run only: a queued run replays as just `ready`.
    let replay = match state
        .beam
        .replay_request(
            session,
            &request_id,
            last_event_id,
            user.as_deref(),
            org.as_deref(),
        )
        .await
    {
        Ok(text) => text,
        Err(e) => return problem(e),
    };
    let end = read_replay(&replay);
    let head = futures_util::stream::once(async move { Ok::<_, Infallible>(Bytes::from(replay)) });
    let body = if end.complete {
        Body::from_stream(head)
    } else {
        Body::from_stream(head.chain(live_until_end(live, end.last_sequence)))
    };
    let mut response = Response::new(body);
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    headers.insert("x-accel-buffering", HeaderValue::from_static("no"));
    deprecated(response)
}

/// Deprecated: the agent catalog (use `GET /api/{namespace}/agent.list`).
async fn list_agents(
    State(state): State<Arc<AppState>>,
    principal: Option<Extension<Principal>>,
) -> Response {
    let ctx = match caller(principal) {
        Ok(ctx) => ctx,
        Err(response) => return response,
    };
    if let Err(response) = permitted(beam_domain::roles_may_query(&ctx.roles)) {
        return response;
    }
    let request = riverbase_core::query::QueryRequest {
        limit: 500,
        page: 1,
        count: false,
        ..riverbase_core::query::QueryRequest::default()
    };
    match state
        .domain
        .query_dyn()
        .execute(
            &ctx,
            "agent",
            riverbase_core::query::QueryAccess::List,
            request,
            None,
        )
        .await
    {
        Ok(out) => deprecated(Json(json!({ "agents": out["data"] })).into_response()),
        Err(e) => deprecated(problem(e)),
    }
}
