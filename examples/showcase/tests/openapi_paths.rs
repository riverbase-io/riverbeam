//! The Rust API's `OpenAPI` paths: a committed snapshot,
//! `tests/fixtures/openapi_paths.json`, of every beam route and the
//! riverbase routes the app mounts, minus `/api/auth/*` (riverbase's auth
//! module needs a Postgres runtime this harness does not start).
//!
//! The domain now runs on riverbase's engines, which need the beam
//! database: the test skips without `BEAM_DATABASE_URL`. After an intended
//! route change, regenerate the snapshot with
//! `UPDATE_SNAPSHOTS=1 cargo test -p showcase --test openapi_paths` and
//! review the diff.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use axum::body::Body;
use axum::http::Request;
use showcase::host::http::{compose_api, ApiState};
use beam_core::{BeamConfig, RiverBeam};
use beam_domain::{BeamDomainBundle, BEAM_NAMESPACE};
use beam_worker::LocalBus;
use riverbase_core::config::{AuthProvider, RiverbaseConfig};
use riverbase_core::datastore::PostgresDomainLogStore;
use riverbase_http::web::RiverbaseApp;
use serde_json::Value;
use tower::ServiceExt;

const SNAPSHOT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/openapi_paths.json"
);

/// Paths outside the snapshot: riverbase's own extras and its auth module.
fn excluded(path: &str) -> bool {
    path == "/openapi.json"
        || path.starts_with("/api/auth/mock-auth")
        || path == "/api/auth"
        || path.starts_with("/api/auth/")
}

async fn app_router() -> Option<axum::Router> {
    let store = beam_store::testdb::store("showcase_openapi").await?;
    let beam = RiverBeam::new(BeamConfig::default());
    beam.register_echo_agent("demo");
    let beam = Arc::new(beam);
    let logstore = PostgresDomainLogStore::new(store.pool().clone()).into_bundle();
    let domain = Arc::new(
        BeamDomainBundle::spawn_with_namespace(
            BEAM_NAMESPACE,
            &store,
            logstore,
            Arc::new(LocalBus::new()),
        )
        .await
        .expect("domain"),
    );

    // The route set does not depend on auth or Casbin (both need a Postgres
    // runtime, and riverbase's `from_runtime` harness leaves `api_zone` empty).
    let mut config = RiverbaseConfig::default();
    config.auth.auth_provider = AuthProvider::None;
    config.casbin.enabled = false;
    config.auth.application_secret_key = Some("test-session-secret".into());
    config.hook_token.secret = Some("test-hook-secret".into());
    config.hook_token.salt = Some("test-hook-salt".into());
    config.link_token.secret = Some("test-link-secret".into());
    config.link_token.salt = Some("test-link-salt".into());
    config.api_zone = vec!["default".into()];
    let mut app = RiverbaseApp::with_config(config, "Fluvius Beam", "Beam API");
    let state = Arc::new(ApiState {
        beam,
        store: None,
        rtc_bus: Some(Arc::new(LocalBus::new())),
    });
    compose_api(&mut app, domain, state).await.expect("compose");
    Some(app.into_router().await.expect("router"))
}

fn paths(spec: &Value) -> BTreeMap<String, BTreeSet<String>> {
    spec["paths"]
        .as_object()
        .expect("paths")
        .iter()
        .filter(|(path, _)| !excluded(path))
        .map(|(path, ops)| {
            let methods = ops
                .as_object()
                .map(|o| o.keys().cloned().collect())
                .unwrap_or_default();
            (path.clone(), methods)
        })
        .collect()
}

#[tokio::test]
async fn openapi_paths_match_the_snapshot() {
    let Some(router) = Box::pin(app_router()).await else {
        return;
    };
    let response = router
        .oneshot(
            Request::get("/api/openapi.json")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert!(response.status().is_success(), "{}", response.status());
    let body = axum::body::to_bytes(response.into_body(), 10 << 20)
        .await
        .expect("body");
    let rust = paths(&serde_json::from_slice(&body).expect("openapi"));
    if std::env::var("UPDATE_SNAPSHOTS").is_ok_and(|v| v == "1") {
        let text = serde_json::to_string_pretty(&rust).expect("serialize") + "\n";
        std::fs::write(SNAPSHOT, text).expect("write the snapshot");
        return;
    }
    let snapshot: BTreeMap<String, BTreeSet<String>> =
        serde_json::from_str(&std::fs::read_to_string(SNAPSHOT).expect("snapshot"))
            .expect("snapshot json");
    let missing: Vec<&String> = snapshot.keys().filter(|p| !rust.contains_key(*p)).collect();
    let extra: Vec<&String> = rust.keys().filter(|p| !snapshot.contains_key(*p)).collect();
    let methods: Vec<(&String, &BTreeSet<String>, &BTreeSet<String>)> = snapshot
        .iter()
        .filter_map(|(p, want)| {
            rust.get(p)
                .filter(|got| *got != want)
                .map(|got| (p, want, got))
        })
        .collect();
    assert!(missing.is_empty(), "routes no longer served: {missing:#?}");
    assert!(extra.is_empty(), "routes not in the snapshot: {extra:#?}");
    assert!(methods.is_empty(), "method differences: {methods:#?}");
    for gone in ["list-revisions", "list-dead-letters", "research-references"] {
        assert!(
            !rust.keys().any(|p| p.contains(&format!("/{gone}"))),
            "{gone} was dropped in plan 006"
        );
    }
}
