//! The showcases this server hosts, and how they are put on one beam.
//!
//! A [`Showcase`] names its personas and agents, creates its tables, seeds
//! its demo data, binds its agents and serves its routes under its own
//! `/api/…` prefix. [`ALL`] lists them; [`bind_all`] is the work queue's
//! binder, and [`catalog_routes`] tells the UI what it can switch between.

use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;

use async_trait::async_trait;
use axum::routing::get;
use axum::{Json, Router};
use beam_core::RiverBeam;
use beam_domain::DomainStore;
use beam_store::PgBeamStore;
use serde::Serialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::host::queue::BindFuture;
use crate::host::tools::{connect_mcp_servers, mcp_config_from_env};

/// What a showcase's setup fails with.
pub type BoxError = Box<dyn Error>;

/// A person the UI signs in as (riverbase mock auth).
#[derive(Debug, Clone, Serialize)]
pub struct Persona {
    pub key: &'static str,
    pub user_id: &'static str,
    pub name: &'static str,
    /// The showcase's own role, which picks the persona's view.
    pub role: &'static str,
    /// The usage plan (`host::plans`).
    pub plan: &'static str,
    /// The riverbase roles the UI signs with.
    pub roles: &'static [&'static str],
}

/// What [`bind_all`] sets up once for every showcase.
#[derive(Debug, Default, Clone)]
pub struct Shared {
    /// The MCP tools bridged from `MCP_SERVERS_CONFIG_PATH`, `<server>__<tool>`.
    pub mcp_tools: Vec<String>,
}

impl Shared {
    /// The bridged tools of one MCP server.
    #[must_use]
    pub fn mcp_tools_of(&self, server: &str) -> Vec<String> {
        let prefix = format!("{server}__");
        self.mcp_tools
            .iter()
            .filter(|n| n.starts_with(&prefix))
            .cloned()
            .collect()
    }
}

/// What a showcase's routes are built from.
pub struct RouteContext {
    pub store: PgBeamStore,
    /// The beam that runs the agents, when this process runs them (the
    /// embedded worker).
    pub agents: Option<Arc<RiverBeam>>,
    /// The beam domain's tables (usage, runs) for admin views.
    pub domain: DomainStore,
}

/// One showcase.
#[async_trait(?Send)]
pub trait Showcase: Sync {
    /// Its key, as the UI and the records name it (`loan_desk`, `support`, …).
    fn key(&self) -> &'static str;
    fn title(&self) -> &'static str;
    /// One line on what it is.
    fn tagline(&self) -> &'static str;
    /// The organization its personas belong to (knowledge, usage and audit
    /// stay apart per showcase).
    fn org(&self) -> &'static str;
    fn personas(&self) -> Vec<Persona>;
    /// The agents it binds, in the order the UI lists them.
    fn agents(&self) -> &'static [&'static str];
    /// Where its routes live, `/api/<name>/`.
    fn api_prefix(&self) -> &'static str;

    /// Create its tables (`showcase-worker migrate`, and every start).
    async fn migrate(&self, store: &PgBeamStore) -> Result<(), BoxError>;
    /// Add its demo data where it is missing (`showcase-worker seed`).
    async fn seed(&self, store: &PgBeamStore) -> Result<(), BoxError>;
    /// Register its tools and bind its agents on `beam`. Returns the tools it
    /// registered, so [`bind_all`] can refuse two showcases with one name.
    async fn bind(
        &self,
        beam: &RiverBeam,
        store: &PgBeamStore,
        shared: &Shared,
    ) -> Result<Vec<String>, BoxError>;
    /// Its routes; each checks the caller itself (they are mounted public).
    fn routes(&self, ctx: &RouteContext) -> Router;
}

/// Every showcase, in the order the UI offers them.
pub static ALL: [&dyn Showcase; 5] = [
    &crate::loan_desk::LoanDeskShowcase,
    &crate::support::SupportShowcase,
    &crate::claims::ClaimsShowcase,
    &crate::incident::IncidentShowcase,
    &crate::invoice::InvoiceShowcase,
];

/// The persona with this user id, in any showcase, with its showcase's key.
#[must_use]
pub fn persona(user_id: &str) -> Option<(&'static str, Persona)> {
    ALL.iter().find_map(|showcase| {
        showcase
            .personas()
            .into_iter()
            .find(|p| p.user_id == user_id)
            .map(|p| (showcase.key(), p))
    })
}

/// The showcase role of `user_id` in `showcase`, if they are one of its
/// personas.
#[must_use]
pub fn role_in(showcase: &str, user_id: Option<&str>) -> Option<&'static str> {
    let (key, persona) = persona(user_id?)?;
    (key == showcase).then_some(persona.role)
}

/// The work queue's binder for this server: the MCP servers once, then each
/// showcase's tables, tools and agents, then the personas' usage plans.
#[must_use]
pub fn bind_all<'a>(beam: &'a RiverBeam, store: &'a PgBeamStore) -> BindFuture<'a> {
    Box::pin(bind_showcases(beam, store, &ALL))
}

/// [`bind_all`] over `showcases` (tests bind a few).
///
/// # Errors
/// The tool runtime is missing, a showcase fails to bind, two showcases
/// register a tool with one name, or the usage plans cannot be assigned.
pub async fn bind_showcases(
    beam: &RiverBeam,
    store: &PgBeamStore,
    showcases: &[&dyn Showcase],
) -> Result<(), BoxError> {
    let registry = beam
        .tools()
        .ok_or("the showcases need the tool runtime (host::tool_service)")?
        .registry()
        .clone();
    let mcp_tools = match mcp_config_from_env() {
        Ok(Some(config)) => connect_mcp_servers(&registry, &config).await,
        Ok(None) => Vec::new(),
        Err(err) => {
            eprintln!("warning: MCP_SERVERS_CONFIG_PATH not loaded ({err}); no MCP tools");
            Vec::new()
        }
    };
    let shared = Shared { mcp_tools };
    let mut owners: HashMap<String, &str> = HashMap::new();
    for showcase in showcases {
        showcase.migrate(store).await?;
        let tools = showcase
            .bind(beam, store, &shared)
            .await
            .map_err(|err| format!("{}: {err}", showcase.key()))?;
        for tool in tools.into_iter().filter(|t| !crate::kit::is_shared_tool(t)) {
            if let Some(other) = owners.insert(tool.clone(), showcase.key()) {
                if other != showcase.key() {
                    return Err(format!(
                        "tool {tool} is registered by both {other} and {}",
                        showcase.key()
                    )
                    .into());
                }
            }
        }
        println!("{}: bound {}", showcase.key(), showcase.agents().join(", "));
    }
    // Each persona's usage plan: the plans are checked in first.
    if std::env::var("USAGE_BACKEND").is_ok_and(|b| b.trim() == "sql") {
        beam.checkin_catalog().await?;
        assign_persona_plans(store, showcases).await?;
    }
    Ok(())
}

/// Give each persona its usage plan: one active profile per user. The plans
/// must be checked in first (a profile names its plan row).
///
/// # Errors
/// A user id is not a UUID, or the update fails.
pub async fn assign_persona_plans(
    store: &PgBeamStore,
    showcases: &[&dyn Showcase],
) -> Result<(), BoxError> {
    for persona in showcases.iter().flat_map(|s| s.personas()) {
        let user = Uuid::parse_str(persona.user_id)?;
        store
            .db()
            .usage()
            .set_active_plan(user, persona.plan, beam_store::agent_id_for(persona.plan))
            .await
            .map_err(|e| e.or(crate::errors::SHC_001))?;
    }
    Ok(())
}

/// What the UI reads to offer the showcases.
#[must_use]
pub fn catalog() -> Value {
    Value::Array(
        ALL.iter()
            .map(|s| {
                json!({
                    "key": s.key(),
                    "title": s.title(),
                    "tagline": s.tagline(),
                    "org": s.org(),
                    "api": s.api_prefix(),
                    "agents": s.agents(),
                    "personas": s.personas(),
                })
            })
            .collect(),
    )
}

/// `GET /api/showcase/catalog`: the showcases with their personas and agents
/// (public, like each showcase's persona list).
pub fn catalog_routes() -> Router {
    Router::new().route("/api/showcase/catalog", get(|| async { Json(catalog()) }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn keys_users_orgs_and_agents_are_unique() {
        let keys: HashSet<_> = ALL.iter().map(|s| s.key()).collect();
        assert_eq!(keys.len(), ALL.len());
        let orgs: HashSet<_> = ALL.iter().map(|s| s.org()).collect();
        assert_eq!(orgs.len(), ALL.len(), "one org per showcase");
        let prefixes: HashSet<_> = ALL.iter().map(|s| s.api_prefix()).collect();
        assert_eq!(prefixes.len(), ALL.len());
        let users: Vec<_> = ALL.iter().flat_map(|s| s.personas()).map(|p| p.user_id).collect();
        assert_eq!(users.iter().collect::<HashSet<_>>().len(), users.len());
        for user in &users {
            assert!(Uuid::parse_str(user).is_ok(), "{user} is a UUID");
        }
        let agents: Vec<_> = ALL.iter().flat_map(|s| s.agents().iter().copied()).collect();
        assert_eq!(agents.iter().collect::<HashSet<_>>().len(), agents.len());
    }

    #[test]
    fn every_persona_signs_as_a_beam_user_on_a_known_plan() {
        let plans: HashSet<_> = crate::host::plans::all_plans()
            .into_iter()
            .map(|p| p.name)
            .collect();
        for persona in ALL.iter().flat_map(|s| s.personas()) {
            assert!(persona.roles.contains(&"beam-user"), "{}", persona.key);
            assert!(plans.contains(persona.plan), "{} plan {}", persona.key, persona.plan);
        }
    }

    #[test]
    fn a_persona_resolves_to_its_showcase_and_role() {
        let bob = crate::loan_desk::personas::BOB;
        assert_eq!(role_in("loan_desk", Some(bob)), Some("borrower"));
        assert_eq!(role_in("support", Some(bob)), None);
        assert_eq!(role_in("loan_desk", None), None);
    }
}
