//! The API server's HTTP surface on `riverbase_http`:
//! the beam domain mounted at `/api/{namespace}/…` (28 commands, 24 queries,
//! `domain.meta`) with Casbin on every command and query, plus
//! Python's `.usage`, `/api/health`, `/api/ready`, `/health/live`,
//! `/health/ready`, the durable SSE replay at
//! `/api/beam/sse/{session_id}/{request_id}` and `/metrics`, and riverbase's
//! RTC bridge at `/api/rtc/default~ssestream/{channel}` and
//! `~websocket/{channel}`, where each caller follows their runs
//! on their own channel.

use std::sync::Arc;

use aide::axum::routing::get_with;
use aide::axum::ApiRouter;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Extension, Json, Router};
use beam_core::RiverBeam;
use beam_domain::{beam_policy_csv, BeamDomainBundle, BeamSql, DomainStore, ADMIN_ROLE, USER_ROLE};
use riverbase_core::transport::StreamBus;
use riverbase_core::{Domain, RiverbaseResult};
use riverbase_http::web::RiverbaseApp;
use riverbase_http::Principal;
use riverbase_http::RtcBridge;
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

/// The caller's user id as riverbase's own routes see it (`to_audit_actor`:
/// with a profile provider `sub` is the profile id), else `sub`.
#[must_use]
pub fn principal_user(principal: &Principal) -> String {
    principal
        .to_audit_actor(None)
        .user_id
        .map_or_else(|| principal.sub.clone(), |u| u.to_string())
}

/// The domain context a route runs a command as: the caller's actor, roles
/// and organization.
#[must_use]
pub fn engine_context(principal: &Principal) -> riverbase_core::EngineContext {
    let mut ctx = riverbase_core::EngineContext::new(beam_domain::beam_namespace());
    ctx.actor = principal.to_audit_actor(None);
    if ctx.actor.user_id.is_none() {
        ctx.actor.user_id = uuid::Uuid::parse_str(&principal.sub).ok();
    }
    // Profile and IAM roles, as riverbase's Casbin layer reads them.
    ctx.roles = principal_roles(principal);
    ctx.organization_id = ["organization_id", "org_id"]
        .iter()
        .find_map(|k| principal.claims.get(*k).and_then(serde_json::Value::as_str))
        .and_then(|o| uuid::Uuid::parse_str(o).ok());
    ctx
}

/// The roles riverbase's Casbin layer accepts: profile roles and IAM realm
/// roles (a bearer JWT carries only the latter).
#[must_use]
pub fn principal_roles(principal: &Principal) -> Vec<String> {
    let mut roles = principal.roles.clone();
    for role in &principal.iam_roles {
        if !roles.contains(role) {
            roles.push(role.clone());
        }
    }
    roles
}

/// What the extra routes read.
pub struct ApiState {
    /// Durable replay, readiness and metrics (the API process runs no agents).
    pub beam: Arc<RiverBeam>,
    /// `.usage` (Postgres).
    pub store: Option<DomainStore>,
    /// Riverbase's bus for its RTC bridge; without it the bridge is not
    /// mounted.
    pub rtc_bus: Option<Arc<dyn StreamBus>>,
}

/// The wire namespace of riverbase's RTC bridge (`/api/rtc/…`).
pub const RTC_NAMESPACE: &str = "rtc";

/// The one RTC route callers may use: SSE on a channel, `{prefix}{channel}`.
const RTC_SSE_PREFIX: &str = "/api/rtc/default~ssestream/";

/// The caller's own RTC channel as riverbase names it, `{org}.{sub}`
/// (`organization_id`, else `realm`, else `_`; then the token subject).
#[must_use]
pub fn own_rtc_channel(principal: &Principal) -> String {
    format!(
        "{}.{}",
        riverbase_http::websocket::tenant_segment(principal),
        principal.subject()
    )
}

/// Riverbase's RTC ACL lets any caller subscribe to any `ws.channel.*`
/// channel, NATS wildcards included, and its bridge has no hook to narrow
/// that. Run frames are mirrored to each caller's own channel, so only
/// `GET {RTC_SSE_PREFIX}{own channel}` passes; every other bridge route
/// (other channels, `~websocket`, `.meta`, `.info`) is refused.
async fn rtc_gate(request: axum::extract::Request, next: axum::middleware::Next) -> Response {
    let own = request.extensions().get::<Principal>().map(own_rtc_channel);
    let wanted = request.uri().path().strip_prefix(RTC_SSE_PREFIX);
    if request.method() == axum::http::Method::GET && own.is_some() && wanted == own.as_deref() {
        return next.run(request).await;
    }
    (
        StatusCode::FORBIDDEN,
        Json(json!({"detail": "subscribe to your own RTC channel only: {org}.{sub} over SSE"})),
    )
        .into_response()
}

/// The extra policy rows: `.usage` is a query on the domain itself, and
/// both roles may follow their own RTC channel (riverbase's channel ACL
/// keeps each caller to their own).
fn extra_policy(namespace: &str) -> String {
    format!(
        "p, beam-user, {namespace}, .query, *, *, allow\n\
         p, beam-user, {RTC_NAMESPACE}, rtc.stream, rtc, *, allow\n\
         p, beam-admin, {RTC_NAMESPACE}, rtc.stream, rtc, *, allow\n"
    )
}

/// The roles riverbase's Casbin layer enforces as, most privileged first:
/// a caller holding several is enforced as the first one listed here
/// (`resolve_casbin_subject`), so `beam-admin` must lead.
pub const CASBIN_KNOWN_ROLES: [&str; 2] = [ADMIN_ROLE, USER_ROLE];

/// Mount the beam domain and its extra routes on `app`.
///
/// # Errors
/// The domain's discovery or route validation.
pub async fn compose_api(
    app: &mut RiverbaseApp,
    domain: Arc<BeamDomainBundle>,
    state: Arc<ApiState>,
) -> RiverbaseResult<()> {
    let namespace = domain.namespace().to_owned();
    app.with_casbin_policy(beam_policy_csv(&namespace) + extra_policy(&namespace).as_str());
    app.with_casbin_known_roles(CASBIN_KNOWN_ROLES);
    app.mount_domain(domain).await?;
    app.merge(documented_routes(&namespace, state.clone()));
    // Riverbase's RTC bridge: a caller subscribes to their own channel,
    // `ws.channel.{org}.{user}`, where the worker mirrors their runs' frames.
    if let Some(bus) = state.rtc_bus.clone() {
        app.register_policy_identity(RTC_NAMESPACE, RTC_NAMESPACE);
        let rtc = RtcBridge::new("default", format!("/api/{RTC_NAMESPACE}"), bus)
            .register_builtins()?
            .into_router()
            .layer(axum::middleware::from_fn(rtc_gate));
        app.merge(rtc);
    }
    for path in ["/api/health", "/api/ready", "/health/live", "/health/ready"] {
        app.register_public_prefix(path);
    }
    let undocumented = Router::new()
        .route("/api/beam/sse/{session_id}/{request_id}", get(sse_replay))
        .route("/metrics", get(metrics))
        .with_state(state);
    app.register_public_routes(undocumented, &["/metrics"]);
    // Python's replay route reads the caller's claims when it has any.
    app.register_public_prefix("/api/beam/sse/");
    Ok(())
}

fn documented_routes(namespace: &str, state: Arc<ApiState>) -> ApiRouter {
    ApiRouter::new()
        .api_route(
            "/api/health",
            get_with(health, |op| op.summary("Liveness").tag("beam:health")),
        )
        .api_route(
            "/api/ready",
            get_with(ready, |op| op.summary("Readiness").tag("beam:health")),
        )
        // Python `configure_beam_health` serves both pairs.
        .api_route(
            "/health/live",
            get_with(health, |op| op.summary("Beam liveness").tag("beam:health")),
        )
        .api_route(
            "/health/ready",
            get_with(ready, |op| op.summary("Beam readiness").tag("beam:health")),
        )
        .api_route(
            &format!("/api/{namespace}/.usage"),
            get_with(usage, |op| {
                op.summary("Usage summary for the caller").tag("beam:usage")
            }),
        )
        .with_state(state)
}

/// The process answers.
async fn health() -> Json<Value> {
    Json(RiverBeam::liveness())
}

/// The database, the migrations and not draining.
async fn ready(State(state): State<Arc<ApiState>>) -> Response {
    let report = state.beam.readiness().await;
    let code = if report["status"] == "ok" {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (code, Json(report)).into_response()
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct UsageParams {
    period: Option<String>,
}

/// The caller's usage (401 without a user).
async fn usage(
    State(state): State<Arc<ApiState>>,
    principal: Option<Extension<Principal>>,
    Query(params): Query<UsageParams>,
) -> Response {
    let Some(Extension(principal)) = principal else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"detail": "Authentication required for usage"})),
        )
            .into_response();
    };
    let Some(store) = &state.store else {
        return problem(beam_core::BEM_380.with_data("usage needs the beam database"));
    };
    match store
        .usage_summary(
            &principal_user(&principal),
            params.period.as_deref(),
            &usage_settings(&state.beam),
        )
        .await
    {
        Ok(summary) => Json(summary).into_response(),
        Err(e) => problem(e),
    }
}

/// The usage switch and default plan this app enforces: the
/// beam's `usage_enabled` and the plan `app.rs` gives users without a profile.
#[must_use]
pub fn usage_settings(beam: &RiverBeam) -> beam_domain::UsageSummarySettings {
    beam_domain::UsageSummarySettings {
        enabled: beam.config().usage_enabled,
        default_plan: Some(crate::host::plans::DEFAULT_PLAN.to_owned()),
    }
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

/// Python's durable replay (`stream/sse.py`): `text/event-stream` text, or
/// 403 `forbidden` for another caller's session.
async fn sse_replay(
    State(state): State<Arc<ApiState>>,
    Path((session_id, request_id)): Path<(String, String)>,
    principal: Option<Extension<Principal>>,
    headers: HeaderMap,
) -> Response {
    let Ok(session) = Uuid::parse_str(&session_id) else {
        return (StatusCode::NOT_FOUND, "unknown session").into_response();
    };
    let claim_user = principal.as_ref().map(|Extension(p)| principal_user(p));
    let claim_org = principal.as_ref().and_then(|Extension(p)| {
        ["organization_id", "org_id"]
            .iter()
            .find_map(|k| p.claims.get(*k).and_then(Value::as_str).map(str::to_owned))
    });
    let last_event_id = headers.get("Last-Event-ID").and_then(|v| v.to_str().ok());
    match state
        .beam
        .replay_stream(
            session,
            &request_id,
            last_event_id,
            claim_user.as_deref(),
            claim_org.as_deref(),
        )
        .await
    {
        Ok(text) => (
            [(
                axum::http::header::CONTENT_TYPE,
                "text/event-stream; charset=utf-8",
            )],
            text,
        )
            .into_response(),
        Err(e) if e.errcode.as_str() == beam_core::BEM_069.errcode => {
            (StatusCode::FORBIDDEN, "forbidden").into_response()
        }
        Err(e) => problem(e),
    }
}

/// `/metrics`: the Prometheus text of the process counters.
async fn metrics() -> Response {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        beam_telemetry::render_prometheus(beam_telemetry::metrics()),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn caller(sub: &str) -> Principal {
        riverbase_http::principal_from_mock_claims(&json!({
            "sub": sub,
            "organization_id": "org-1",
            "realm_access": {"roles": ["beam-user"]},
        }))
        .expect("mock principal")
    }

    async fn status(path: &str, principal: Option<Principal>) -> StatusCode {
        let app = Router::new()
            .route(
                "/api/rtc/default~ssestream/{*channel}",
                get(|| async { "stream" }),
            )
            .route(
                "/api/rtc/default~websocket/{*channel}",
                get(|| async { "socket" }),
            )
            .layer(axum::middleware::from_fn(rtc_gate));
        let mut request = Request::get(path).body(Body::empty()).expect("request");
        if let Some(principal) = principal {
            request.extensions_mut().insert(principal);
        }
        app.oneshot(request).await.expect("response").status()
    }

    #[tokio::test]
    async fn only_the_callers_own_sse_channel_passes() {
        assert_eq!(own_rtc_channel(&caller("user-1")), "org-1.user-1");
        let me = || Some(caller("user-1"));
        let ok = status("/api/rtc/default~ssestream/org-1.user-1", me()).await;
        assert_eq!(ok, StatusCode::OK);
        for path in [
            "/api/rtc/default~ssestream/org-1.user-2",
            "/api/rtc/default~ssestream/org-1.*",
            "/api/rtc/default~ssestream/%3E",
            "/api/rtc/default~websocket/org-1.user-1",
        ] {
            assert_eq!(status(path, me()).await, StatusCode::FORBIDDEN, "{path}");
        }
        let anonymous = status("/api/rtc/default~ssestream/org-1.user-1", None).await;
        assert_eq!(anonymous, StatusCode::FORBIDDEN);
    }

    /// A caller holding both roles is enforced as `beam-admin`, whichever
    /// order its claims list them in.
    #[test]
    fn a_caller_with_both_roles_is_enforced_as_admin() {
        let known = CASBIN_KNOWN_ROLES.map(String::from).to_vec();
        let subject = |roles: &[&str]| {
            let principal = riverbase_http::principal_from_mock_claims(&json!({
                "sub": "user-1",
                "organization_id": "org-1",
                "roles": roles,
                "realm_access": {"roles": roles},
            }))
            .expect("mock principal");
            riverbase_http::web::resolve_casbin_subject(Some(&principal), "dev-user", &known)
        };
        assert_eq!(subject(&["beam-user", "beam-admin"]), "beam-admin");
        assert_eq!(subject(&["beam-admin", "beam-user"]), "beam-admin");
        assert_eq!(subject(&["beam-user"]), "beam-user");
    }
}
