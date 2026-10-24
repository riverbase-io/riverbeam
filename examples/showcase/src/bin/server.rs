//! `showcase-server`: every showcase's API and the UI on riverbase.
//!
//! - `/api/auth/*`: riverbase auth (mock, from `riverbase.toml`).
//! - `/api/{BEAM_DOMAIN_NAMESPACE}/…`: the beam domain (runs, actions,
//!   sessions, revisions, evals, ops), with Casbin, plus `.usage`, the
//!   health and readiness routes, `/metrics` and the durable SSE replay
//!   ([`showcase::host::http`]).
//! - `/api/rtc/…`: each caller's run frames (riverbase's RTC bridge).
//! - `/api/showcase/catalog`: the showcases, their personas and agents.
//! - Each showcase's own routes: `/api/loan/…`, `/api/support/…`,
//!   `/api/claims/…`, `/api/incident/…`, `/api/invoice/…`.
//! - The deprecated aliases (`/invoke`, `/astream`, `/catalog/agents`) and,
//!   with feature `audio`, `/ws/audio` and the UI's `/ws/voice`.
//! - `/showcase`: the UI (`ui/dist/index.html`, or `SHOWCASE_UI_DIR`);
//!   `/desk` redirects there. riverbase owns `/`.
//!
//! Commands queue their work: rows in `agent_task` and hints on riverbase's
//! bus, which `showcase-worker` runs. With `BEAM_EMBEDDED_WORKER` on, this
//! process runs the work queue too, so one process serves the API and runs
//! every showcase's agents. The domain's database must be riverbase's
//! (`RIVERBASE_DB_URL` = `BEAM_DATABASE_URL`): a command's rows and
//! riverbase's command log commit together.
//!
//! `--migrate-only` applies riverbase's framework migrations and exits;
//! `showcase-worker migrate` applies Beam's.

use std::sync::Arc;

use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::get;
use axum::Router;
use beam_core::{BeamConfig, RiverBeam};
use beam_domain::{BeamDomainBundle, DomainStore};
use beam_stream::NatsTransport;
use riverbase_http::web::RiverbaseApp;
use showcase::aliases::{self, AppState};
use showcase::host::http::{compose_api, ApiState};
use showcase::host::queue::{embedded_worker_enabled, start_queue};
use showcase::host::runs::RunWaiter;
use showcase::host::{connect_bus, connect_store, nats_url};
use showcase::registry::{catalog_routes, RouteContext, ALL};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut app = RiverbaseApp::init_portal(
        "Riverbeam Showcase",
        "Real applications on riverbeam: loan desk, support desk, claims, incident desk, accounts payable",
        "BEAM_BIND_ADDR",
        "127.0.0.1:8097",
        None,
    )
    .await?;
    if RiverbaseApp::migrate_only() {
        return Ok(());
    }
    app.mount_auth_routes().await?;

    let nats = nats_url();
    let store = connect_store().await?;
    store.ensure_migrated().await?;
    // Without this `/ready` stays 503 `migrations_pending`.
    app.mark_migrations_ready();
    // The domain runs on riverbase's runtime: its command log and outbox,
    // and its bus for the queue hints. They must share the beam database.
    let runtime = app.runtime().ok_or("riverbase has no domain runtime")?;
    if let Some(dsn) = runtime.db_dsn() {
        if !store.same_database_as(dsn).await? {
            return Err(beam_core::BEM_509
                .with_data("RIVERBASE_DB_URL and BEAM_DATABASE_URL must name the same database")
                .into());
        }
    }
    for showcase in &ALL {
        showcase.migrate(&store).await?;
    }

    // With the embedded worker, the API serves replay, readiness and metrics
    // from the beam that runs the agents. Without it, the API runs no agents:
    // a bare beam over the store serves them.
    let (beam, queue) = if embedded_worker_enabled() {
        let (beam, queue) = start_queue(store.clone()).await?;
        (beam, Some(queue))
    } else {
        let beam = RiverBeam::builder(BeamConfig::from_env())
            .pg_store(store.clone())
            .build()?;
        (Arc::new(beam), None)
    };
    // rc8's reports answer from this beam. The tool policy explanation needs
    // its tool catalog, which only a beam that runs agents has.
    beam_domain::set_reports(Arc::new(beam_domain::RiverBeamReports::new(
        beam.clone(),
        Some(DomainStore::for_store(&store)),
        showcase::host::http::usage_settings(&beam),
    )));
    let (logstore, msgbus) = (runtime.logstore.clone(), runtime.msgbus.clone());
    let domain = Arc::new(BeamDomainBundle::spawn(&store, logstore, msgbus).await?);
    compose_api(
        &mut app,
        domain.clone(),
        Arc::new(ApiState {
            beam: beam.clone(),
            store: Some(DomainStore::for_store(&store)),
            // Riverbase's RTC bridge reads the bus the worker mirrors frames to.
            rtc_bus: Some(connect_bus().await?),
        }),
    )
    .await?;

    let transport = Arc::new(NatsTransport::connect(&nats).await?);
    let runs = Arc::new(RunWaiter::new(
        domain.clone(),
        transport.clone(),
        beam.clone(),
    ));
    #[cfg(feature = "audio")]
    voice::mount(&mut app, runs.clone())?;
    aliases::mount(
        &mut app,
        Arc::new(AppState {
            domain,
            beam: beam.clone(),
            transport,
            store: DomainStore::for_store(&store),
            runs,
        }),
    )?;

    // Each showcase's routes check the caller themselves, like the aliases.
    let ctx = RouteContext {
        store: store.clone(),
        agents: queue.is_some().then(|| beam.clone()),
        domain: DomainStore::for_store(&store),
    };
    for showcase in &ALL {
        app.register_public_routes(showcase.routes(&ctx), &[]);
        app.register_public_prefix(showcase.api_prefix());
    }
    app.register_public_routes(catalog_routes(), &["/api/showcase/catalog"]);
    app.register_public_routes(
        Router::new()
            .route("/showcase", get(ui_page))
            .route("/desk", get(|| async { Redirect::temporary("/showcase") })),
        &["/showcase", "/desk"],
    );

    if queue.is_some() {
        println!("showcase ready, running the agents too (frames on {nats})");
    } else {
        println!("showcase ready (work queue hints on riverbase's bus, frames on {nats})");
    }
    // riverbase drains open connections (SSE, websockets) on Ctrl+C with no
    // limit, so the wait is bounded; then the queue lets running tasks finish.
    showcase::host::shutdown::serve_until_shutdown(app, queue).await
}

const UI_NOT_BUILT: &str = "<!doctype html><meta charset=utf-8><title>Riverbeam Showcase</title>\
<body style=\"font:15px system-ui;max-width:40rem;margin:3rem auto\">\
<h1>The showcase UI is not built</h1><pre>just showcase-ui-build</pre>";

/// The UI, read on each request so a rebuild needs no restart.
async fn ui_page() -> Response {
    let dir = std::env::var("SHOWCASE_UI_DIR")
        .ok()
        .filter(|d| !d.trim().is_empty())
        .map_or_else(
            || std::path::PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/ui/dist")),
            std::path::PathBuf::from,
        );
    // A file read: off the async threads the work queue may share.
    let page =
        tokio::task::spawn_blocking(move || std::fs::read_to_string(dir.join("index.html"))).await;
    match page {
        Ok(Ok(page)) => Html(page).into_response(),
        _ => (StatusCode::NOT_FOUND, Html(UI_NOT_BUILT)).into_response(),
    }
}

/// `/ws/voice`: the UI's voice turns (feature `audio`).
///
/// A browser cannot set headers on a WebSocket, so in this dev setup (mock
/// auth) the persona's `MockAuth-…` token rides in `?token=`; the socket is
/// then the host's audio socket for that caller, with the agent the UI names.
#[cfg(feature = "audio")]
mod voice {
    use std::sync::Arc;

    use axum::extract::ws::WebSocketUpgrade;
    use axum::extract::{Query, State};
    use axum::http::StatusCode;
    use axum::response::{IntoResponse, Response};
    use axum::routing::get;
    use axum::Router;
    use riverbase_http::web::RiverbaseApp;
    use serde::Deserialize;
    use showcase::host::audio::{serve_audio, AudioQuery, AudioState};
    use showcase::host::runs::RunWaiter;

    #[derive(Deserialize)]
    struct VoiceQuery {
        token: Option<String>,
        agent: Option<String>,
        session_id: Option<String>,
    }

    pub fn mount(
        app: &mut RiverbaseApp,
        runs: Arc<RunWaiter>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let audio = Arc::new(AudioState::from_env(runs)?);
        app.register_public_routes(
            Router::new()
                .route("/ws/voice", get(voice))
                .with_state(audio),
            &["/ws/voice"],
        );
        Ok(())
    }

    #[allow(clippy::unused_async)] // An axum handler.
    async fn voice(
        State(audio): State<Arc<AudioState>>,
        Query(q): Query<VoiceQuery>,
        ws: WebSocketUpgrade,
    ) -> Response {
        let principal = riverbase_http::resolve_mock_principal(q.token.as_deref())
            .ok()
            .flatten();
        let Some(principal) = principal else {
            return (StatusCode::UNAUTHORIZED, "a persona token is required").into_response();
        };
        let ctx = showcase::host::http::engine_context(&principal);
        serve_audio(
            &audio,
            &ctx,
            AudioQuery {
                agent: q.agent,
                session_id: q.session_id,
            },
            ws,
        )
    }
}
