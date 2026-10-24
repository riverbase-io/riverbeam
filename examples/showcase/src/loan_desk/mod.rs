//! Loan desk: a real riverbeam application.
//!
//! A mortgage lender's desk. Borrowers apply with `loan_assistant`, which
//! drives the application form in their browser (client tools), cites the
//! program guidelines, remembers their preferences and waits for their
//! approval to submit. `paystub_reader` reads an uploaded pay stub into
//! structured income data. Loan officers run `underwriting_precheck`, a step
//! runtime that pulls credit through MCP, applies the program rules, asks the
//! officer to decide exceptions and commits a packet to a git repository.

pub mod admin;
pub mod agents;
pub mod api;
pub mod app;
pub mod data;
pub mod errors;
pub mod mcp_stub;
pub mod packet;
pub mod personas;
pub mod precheck;
pub mod programs;
pub mod tools;

use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use beam_core::RiverBeam;
use beam_store::PgBeamStore;

use crate::registry::{BoxError, Persona, RouteContext, Shared, Showcase};
use data::{Application, ApplicationStore, PgApplications};
use personas::{Role, ORG, PERSONAS};

/// The loan desk in the showcase server.
pub struct LoanDeskShowcase;

#[async_trait(?Send)]
impl Showcase for LoanDeskShowcase {
    fn key(&self) -> &'static str {
        "loan_desk"
    }

    fn title(&self) -> &'static str {
        "Loan desk"
    }

    fn tagline(&self) -> &'static str {
        "Mortgage applications: a borrower assistant, a pay stub reader and an underwriting pre-check"
    }

    fn org(&self) -> &'static str {
        ORG
    }

    fn personas(&self) -> Vec<Persona> {
        PERSONAS
            .iter()
            .map(|p| Persona {
                key: p.key,
                user_id: p.user_id,
                name: p.name,
                role: match p.role {
                    Role::Borrower => "borrower",
                    Role::Officer => "officer",
                    Role::Admin => "admin",
                },
                plan: p.plan,
                roles: p.roles,
            })
            .collect()
    }

    fn agents(&self) -> &'static [&'static str] {
        &[
            agents::LOAN_ASSISTANT,
            agents::PAYSTUB_READER,
            agents::UNDERWRITING_PRECHECK,
        ]
    }

    fn api_prefix(&self) -> &'static str {
        "/api/loan/"
    }

    async fn migrate(&self, store: &PgBeamStore) -> Result<(), BoxError> {
        PgApplications::new(store.clone()).migrate().await?;
        Ok(())
    }

    /// A draft application for each borrower persona that has none.
    async fn seed(&self, store: &PgBeamStore) -> Result<(), BoxError> {
        let applications = PgApplications::new(store.clone());
        for persona in PERSONAS.iter().filter(|p| p.role == Role::Borrower) {
            if applications.list(Some(persona.user_id)).await?.is_empty() {
                let program = if persona.key == "dana" { "va" } else { "fha" };
                applications
                    .insert(&Application::new(persona.user_id, Some(ORG), program))
                    .await?;
                println!("loan_desk: seeded a draft {program} application for {}", persona.name);
            }
        }
        Ok(())
    }

    async fn bind(
        &self,
        beam: &RiverBeam,
        store: &PgBeamStore,
        shared: &Shared,
    ) -> Result<Vec<String>, BoxError> {
        let mut mcp_tools = shared.mcp_tools_of("rates");
        mcp_tools.extend(shared.mcp_tools_of("credit"));
        app::bind_loan_desk(beam, store, mcp_tools).await
    }

    fn routes(&self, ctx: &RouteContext) -> Router {
        let packets = match packet::Packets::from_env() {
            Ok(packets) => Some(packets),
            Err(err) => {
                eprintln!("warning: packet repository unavailable ({err}); packets are not shown");
                None
            }
        };
        api::router(Arc::new(api::ApiState {
            applications: PgApplications::new(ctx.store.clone()),
            packets,
            beam: ctx.agents.clone(),
            usage: Some(ctx.domain.clone()),
        }))
    }
}
