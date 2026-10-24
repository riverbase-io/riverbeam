//! The work queue's side of a process: the full beam with every
//! showcase's agents, claiming and running `agent_task` rows.
//!
//! `showcase-worker` runs only this. `showcase-server` also runs it when
//! [`embedded_worker_enabled`], so one process serves the API and runs the
//! agents. Both shapes can run together: every process claims from the same
//! queue (`FOR UPDATE SKIP LOCKED` plus a lease), so a task runs once.

use std::error::Error;
use std::sync::Arc;

use beam_core::RiverBeam;
use beam_store::PgBeamStore;
use beam_stream::{BeamStream, NatsTransport, StreamTransport};
use beam_tools::ToolRegistry;
use beam_worker::{rtc_stream_enabled, BeamWorker, QueueConfig, QueueWorker, RtcMirrorTransport};
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// `BEAM_EMBEDDED_WORKER` (`1`, `true`, `yes` or `on`): `showcase-server`
/// runs the work queue in its own process too.
#[must_use]
pub fn embedded_worker_enabled() -> bool {
    beam_env::flag("BEAM_EMBEDDED_WORKER", false)
}

/// A running work queue. [`RunningQueue::stop`] drains it.
pub struct RunningQueue {
    worker: Arc<BeamWorker>,
    stop: watch::Sender<bool>,
    running: JoinHandle<()>,
    /// The tool registry, whose MCP clients (stdio children) close on stop.
    tools: Option<Arc<ToolRegistry>>,
}

impl RunningQueue {
    /// Stop claiming, wait (up to the lease) for running tasks, close the MCP
    /// clients, then shut the worker down.
    ///
    /// # Errors
    /// The worker's shutdown failed.
    pub async fn stop(self) -> Result<(), Box<dyn Error>> {
        let _ = self.stop.send(true);
        let _ = self.running.await;
        if let Some(tools) = &self.tools {
            tools.close().await;
        }
        self.worker.on_shutdown().await?;
        Ok(())
    }
}

/// Build the full beam over `store` and start the work queue.
///
/// - Frames stream out on NATS ([`crate::host::nats_url`]), for the API's SSE.
///   With `BEAM_RTC_STREAM` on (the default), each frame also goes to the
///   session owner's riverbase RTC channel on riverbase's bus.
/// - Queue hints come from riverbase's bus ([`crate::host::bus_url`]), where the
///   API's command outbox publishes them.
/// - The execution hierarchy is kept in Postgres, and the agents register
///   in its catalog.
///
/// Returns the beam too, so a server running the queue serves its API from
/// the same beam.
///
/// # Errors
/// NATS or the bus is unreachable, the usage or production settings are
/// invalid, or the catalog check-in failed.
pub async fn start_queue(
    store: PgBeamStore,
) -> Result<(Arc<RiverBeam>, RunningQueue), Box<dyn Error>> {
    start_queue_with(store, crate::registry::bind_all).await
}

/// What [`BindAgents`] returns.
pub type BindFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Box<dyn Error>>> + 'a>>;

/// Binds an app's agents on the built beam, after its Postgres store is set.
pub type BindAgents = for<'a> fn(&'a RiverBeam, &'a PgBeamStore) -> BindFuture<'a>;

/// [`start_queue`] with another binder: `bind` registers the agents (and
/// their tools), as tests bind one showcase alone. Agents the binder no longer
/// binds are retired from the catalog, so the app must own its schema.
///
/// # Errors
/// As [`start_queue`], and whatever `bind` fails with.
pub async fn start_queue_with(
    store: PgBeamStore,
    bind: BindAgents,
) -> Result<(Arc<RiverBeam>, RunningQueue), Box<dyn Error>> {
    // Queue hints arrive on riverbase's bus, where the API's command outbox
    // publishes: the same kind (NATS, Postgres or Redis) from the same URL.
    let bus = crate::host::connect_bus().await?;

    // Stream frames out over NATS for the API's SSE bridge, and to each
    // run's caller on their riverbase RTC channel.
    let mut transport: Arc<dyn StreamTransport> =
        Arc::new(NatsTransport::connect(&crate::host::nats_url()).await?);
    // Postgres NOTIFY channel names stop at 63 bytes: the RTC channels
    // (`ws.channel.{org}.{sub}`) need NATS or Redis.
    if rtc_stream_enabled() && !crate::host::bus_url().starts_with("postgres") {
        transport = Arc::new(RtcMirrorTransport::new(
            transport,
            bus.clone(),
            Arc::new(store.clone()),
        ));
    }
    // `BEAM_STREAM_PUBLISH_CHUNK_*` coalescing, with its sweeper.
    let stream = BeamStream::shared(transport, beam_stream::StreamConfig::from_env());

    // The beam, on Postgres; agents bind on the built beam.
    let beam = crate::host::build_beam(stream, &store).await?;
    bind(&beam, &store).await?;
    // No development fallbacks.
    if std::env::var("BEAM_PROFILE").is_ok_and(|p| p.trim().eq_ignore_ascii_case("production")) {
        beam.validate_production()?;
    }

    let beam = Arc::new(beam);
    let worker = Arc::new(BeamWorker::new(beam.clone()));
    worker.on_startup().await?; // catalog check-in -> Postgres

    // The showcases own their schema: agents no showcase registers any more
    // leave the catalog.
    let retired = store
        .deactivate_agents_except(&beam.runtimes().bound_names())
        .await?;
    if retired > 0 {
        println!("catalog: {retired} agent(s) this app no longer registers marked INACTIVE");
    }

    let queue =
        Arc::new(QueueWorker::new(worker.clone(), store, QueueConfig::from_env()).with_bus(bus));
    let (stop, stopped) = watch::channel(false);
    let running = tokio::spawn(queue.run(stopped));
    let tools = beam.tools().map(|t| t.registry().clone());
    Ok((
        beam,
        RunningQueue {
            worker,
            stop,
            running,
            tools,
        },
    ))
}
