//! The beam domain: riverbase typed commands and queries (plans 006, 007).
//!
//! Commands are one `command_engine!` ([`commands`]): each runs as a typed
//! handler on a [`BeamAggregate`] inside one database transaction and calls
//! one aggregate method. Work for a worker (agent runs, memory, knowledge,
//! revisions, operations) is queued as an `agent_task` row with a hint on
//! the riverbase bus; nothing waits for a worker. Queries are one
//! `query_resource!` per resource over its table's entity ([`query`]),
//! filtered to the caller's rows. Both engines share one datastore, the beam
//! entities ([`beam_store::entity`]).

mod aggregate;
mod authz;
mod catalog;
pub mod commands;
mod domain;
pub mod local;
pub mod query;
pub mod reports;

pub use aggregate::BeamAggregate;
pub use authz::{
    activity_for_command, beam_policy_csv, build_beam_authorizer, roles_may_query, roles_may_run,
    ActivityAuthorizer, ActivityRequest, BeamPolicyAuthorizer, CasbinActivityAuthorizer,
    DomainActivity, ADMIN_ROLE, BEAM_POLICY_CSV, BEAM_RESOURCE, USER_COMMANDS, USER_ROLE,
};
pub use beam_store::entity::ROOT_RESOURCES;
pub use catalog::{AgentInfo, Catalog, ModelInfo, SessionInfo};
pub use commands::BeamCommandEngine;
pub use domain::{beam_namespace, BeamDomainBundle, BEAM_NAMESPACE};
pub use local::{BeamSql, Caller, DomainStore, SessionRecord, UsageSummarySettings};
pub use query::BeamQueryEngine;
pub use reports::{set_reports, BeamReports, RiverBeamReports};
