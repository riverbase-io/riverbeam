//! The beam domain as a [`riverbase_core::Domain`] (namespace
//! `fluvius-beam`, or `BEAM_DOMAIN_NAMESPACE`): riverbase's
//! `command_engine!` engine ([`crate::commands`]) and `query_engine!`
//! engine ([`crate::query`]).

use std::sync::Arc;

use beam_store::PgBeamStore;
use riverbase_core::command::MessageBus;
use riverbase_core::logstore::DomainLogStore;
use riverbase_core::{
    Domain, DomainCommandEngine, DomainQueryEngine, EngineContext, RiverbaseResult,
};

use crate::commands::BeamCommandEngine;
use crate::query::BeamQueryEngine;

/// Python `BEAM_DOMAIN_NAMESPACE`'s default.
pub const BEAM_NAMESPACE: &str = "fluvius-beam";

/// `BEAM_DOMAIN_NAMESPACE`, else [`BEAM_NAMESPACE`].
#[must_use]
pub fn beam_namespace() -> String {
    beam_env::string("BEAM_DOMAIN_NAMESPACE").unwrap_or_else(|| BEAM_NAMESPACE.to_owned())
}

/// The beam domain: its command and query engines on the beam database.
pub struct BeamDomainBundle {
    namespace: String,
    command: Arc<dyn DomainCommandEngine>,
    query: Arc<dyn DomainQueryEngine>,
}

impl BeamDomainBundle {
    /// The domain under `BEAM_DOMAIN_NAMESPACE` (default `fluvius-beam`).
    ///
    /// `logstore` must write to `store`'s database: a command's riverbase log
    /// and outbox rows commit in the same transaction as its beam rows.
    /// `msgbus` carries the queue hints to the workers.
    ///
    /// # Errors
    /// When riverbase cannot start an engine.
    pub async fn spawn(
        store: &PgBeamStore,
        logstore: DomainLogStore,
        msgbus: Arc<dyn MessageBus>,
    ) -> RiverbaseResult<Self> {
        Self::spawn_with_namespace(beam_namespace(), store, logstore, msgbus).await
    }

    /// The domain under `namespace`.
    ///
    /// # Errors
    /// When riverbase cannot start an engine.
    pub async fn spawn_with_namespace(
        namespace: impl Into<String>,
        store: &PgBeamStore,
        logstore: DomainLogStore,
        msgbus: Arc<dyn MessageBus>,
    ) -> RiverbaseResult<Self> {
        let namespace = namespace.into();
        crate::aggregate::set_schema(store.schema());
        let ctx = EngineContext::new(namespace.clone());
        let command = BeamCommandEngine::spawn(
            ctx.clone(),
            logstore.clone(),
            msgbus,
            Arc::clone(store.datastore()),
        )
        .await?;
        let query = BeamQueryEngine::spawn(ctx, Arc::clone(store.datastore()), logstore).await?;
        Ok(Self {
            command: Arc::new(command),
            query: Arc::new(query),
            namespace,
        })
    }
}

impl Domain for BeamDomainBundle {
    fn namespace(&self) -> &str {
        &self.namespace
    }

    fn title(&self) -> &'static str {
        "Fluvius Beam"
    }

    fn description(&self) -> Option<&str> {
        Some("AI agent domain: invoke, stream and resume agents; sessions, memory, knowledge, revisions, evals and operations.")
    }

    fn command_dyn(&self) -> Arc<dyn DomainCommandEngine> {
        self.command.clone()
    }

    fn query_dyn(&self) -> Arc<dyn DomainQueryEngine> {
        self.query.clone()
    }
}
