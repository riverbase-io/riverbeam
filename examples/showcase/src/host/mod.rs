//! The wiring every showcase shares: the beam, the work queue, the HTTP
//! surface, runs, voice and shutdown.

pub mod app;
pub mod astream;
#[cfg(feature = "audio")]
pub mod audio;
pub mod http;
pub mod plans;
pub mod queue;
pub mod runs;
pub mod shutdown;
pub mod tools;

pub use app::{
    build_beam, bus_url, connect_bus, connect_store, database_url, load_models, nats_url,
    run_migrations, tool_service, usage_accounting, DEFAULT_NATS_URL,
};
