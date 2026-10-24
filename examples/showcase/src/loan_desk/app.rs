//! Wiring: the loan desk's tools, knowledge and agents on a beam that
//! `crate::host` built.

use std::error::Error;
use std::sync::Arc;

use beam_core::RiverBeam;
use beam_knowledge::{IngestRequest, KnowledgeManager, KnowledgeScope};
use beam_store::PgBeamStore;
use beam_tools::KNOWLEDGE_SEARCH;
use beam_types::AgentMetadata;

use crate::loan_desk::admin::{publish_assistant_v2, save_smoke_dataset, sync_holds};
use crate::loan_desk::agents::{
    assistant_def, assistant_metadata, paystub_def, paystub_metadata, precheck_metadata,
};
use crate::loan_desk::data::{ApplicationStore, PgApplications};
use crate::loan_desk::packet::{write_packet_tool, Packets};
use crate::loan_desk::precheck::Precheck;
use crate::loan_desk::tools::register_loan_tools;

/// The program guidelines and FAQ, for RAG: `(file, title, text)`.
pub const GUIDELINES: [(&str, &str, &str); 5] = [
    (
        "conventional.md",
        "Conventional 30-year fixed: guidelines",
        include_str!("../../knowledge/loan_desk/conventional.md"),
    ),
    (
        "fha.md",
        "FHA 30-year fixed: guidelines",
        include_str!("../../knowledge/loan_desk/fha.md"),
    ),
    (
        "va.md",
        "VA 30-year fixed: guidelines",
        include_str!("../../knowledge/loan_desk/va.md"),
    ),
    (
        "jumbo.md",
        "Jumbo 30-year fixed: guidelines",
        include_str!("../../knowledge/loan_desk/jumbo.md"),
    ),
    (
        "faq.md",
        "Borrower FAQ",
        include_str!("../../knowledge/loan_desk/faq.md"),
    ),
];

/// Ingest the guidelines into the lender's org scope (callers search with
/// their org). Ingest dedupes by content, so a restart adds nothing.
///
/// # Errors
/// The knowledge manager fails to ingest.
pub async fn ingest_guidelines(
    knowledge: &KnowledgeManager,
) -> riverbase_core::RiverbaseResult<()> {
    for (file, title, text) in GUIDELINES {
        knowledge
            .ingest(IngestRequest {
                scope: KnowledgeScope::Org,
                org_id: Some(crate::loan_desk::personas::ORG.into()),
                source_label: Some(file.into()),
                filename: Some(file.into()),
                mime_type: Some("text/markdown".into()),
                ..IngestRequest::text(text, title)
            })
            .await?;
    }
    Ok(())
}

/// What the loan desk binds besides the beam: its stores and settings.
pub struct LoanDesk {
    pub applications: Arc<dyn ApplicationStore>,
    /// The MCP tools already bridged (`rates__*`, `credit__*`); the
    /// assistant gets the rates.
    pub mcp_tools: Vec<String>,
    /// The packet repository the pre-check commits to.
    pub packets: Option<Arc<Packets>>,
}

/// Register the loan desk's tools and bind its agents on `beam`. Returns the
/// assistant's tool names. Tests call it with in-memory stores.
///
/// # Errors
/// The tool runtime is missing, or the guidelines cannot be ingested.
pub async fn bind_agents(beam: &RiverBeam, desk: &LoanDesk) -> Result<Vec<String>, Box<dyn Error>> {
    let registry = beam
        .tools()
        .ok_or("the loan desk needs the tool runtime (crate::host::tool_service)")?
        .registry()
        .clone();
    let mut tools = register_loan_tools(&registry, &desk.applications);
    if registry
        .list_tool_names()
        .iter()
        .any(|n| n == KNOWLEDGE_SEARCH)
    {
        tools.push(KNOWLEDGE_SEARCH.to_owned());
    }
    if let Some(knowledge) = beam.knowledge() {
        ingest_guidelines(knowledge).await?;
        for tool in [
            crate::host::tools::remember_fact_tool(knowledge.clone()),
            crate::host::tools::recall_memories_tool(knowledge.clone()),
        ] {
            tools.push(tool.name.clone());
            registry.register(tool);
        }
    }
    // The assistant may quote rates; only the pre-check pulls credit.
    tools.extend(
        desk.mcp_tools
            .iter()
            .filter(|n| n.starts_with("rates__"))
            .cloned(),
    );
    let assistant = beam.bind_rig_agent(assistant_metadata(), assistant_def(tools.clone()));
    // Revision 1 is the bind's; revision 2 (the checklist reply style) is a
    // draft the admin evaluates, canaries and activates.
    let metadata = AgentMetadata {
        tools: tools.clone(),
        ..assistant_metadata()
    };
    publish_assistant_v2(beam, &metadata, assistant).await?;
    save_smoke_dataset(beam).await?;
    beam.bind_rig_agent(paystub_metadata(), paystub_def());
    if let Some(packets) = &desk.packets {
        registry.register(write_packet_tool(packets.clone()));
    }
    beam.bind_runtime(
        precheck_metadata(),
        Arc::new(Precheck::new(
            beam.runtime_states().clone(),
            desk.applications.clone(),
        )),
    );
    Ok(tools)
}

/// Bind the loan desk on the server's beam: applications in the beam's
/// Postgres schema, the MCP tools [`crate::registry::bind_all`] bridged, and
/// packets from the environment. Returns the tools it registered.
///
/// # Errors
/// As [`bind_agents`], and the hold sync.
pub async fn bind_loan_desk(
    beam: &RiverBeam,
    store: &PgBeamStore,
    mcp_tools: Vec<String>,
) -> Result<Vec<String>, Box<dyn Error>> {
    let applications = PgApplications::new(store.clone());
    let packets = Packets::from_env()
        .inspect_err(|err| {
            eprintln!("warning: packet repository unavailable ({err}); no packets");
        })
        .ok();
    let desk = LoanDesk {
        applications,
        mcp_tools,
        packets,
    };
    let mut tools = bind_agents(beam, &desk).await?;
    println!("loan_assistant tools: {}", tools.join(", "));
    // Held borrowers keep their agent data across restarts.
    sync_holds(desk.applications.as_ref(), beam, None).await?;
    // The pre-check's packet tool is the loan desk's too.
    if desk.packets.is_some() {
        tools.push(crate::loan_desk::packet::WRITE_PACKET.to_owned());
    }
    Ok(tools)
}
