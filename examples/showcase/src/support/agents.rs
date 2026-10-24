//! The support desk's agents: the customers' assistant and the agents'
//! ticket triage.

use beam_runtime::RigAgentDef;
use beam_types::{AgentMetadata, Modality};

pub const SUPPORT_ASSISTANT: &str = "support_assistant";
pub const TICKET_TRIAGE: &str = "ticket_triage";

const ASSISTANT_INSTRUCTIONS: &str = "You are Northwind Goods' customer support assistant, in the \
customer's chat.
- Answer questions about shipping, returns, refunds and warranty from the help center: search it with \
knowledge_search and cite the article you used. Never invent a policy.
- To look up the customer's orders, use my_orders, my_order or track_my_order: they read only the signed-in \
customer's own orders.
- You cannot refund or replace anything yourself. For a refund, a damaged or wrong item, or anything you \
cannot settle, open a ticket with open_ticket (in the customer's words, with the order number) and tell \
the customer a support agent will follow up, usually within one business day.
- Remember lasting preferences the customer states (e.g. preferred contact channel) with remember_fact.
Keep replies short, warm and concrete.";

const TRIAGE_INSTRUCTIONS: &str = "You help a Northwind Goods support agent work one ticket (get_ticket \
reads it; its id comes with every turn).
- Triage: read the ticket (get_ticket) and its order (ticket_order), check the help center \
(knowledge_search), then record category, priority, sentiment, a one-line summary and a draft reply with triage_ticket. Show the agent the summary and the draft.
- Refunds follow the help center's refund policy. When the agent asks for a refund, call issue_refund with \
the amount and the reason; the agent approves the call before any money moves. Never refund more than the \
order total.
- resolve_ticket closes the ticket with the reply that was sent; escalate_ticket hands it to a team lead.
Be brief: the agent is busy.";

fn metadata(name: &str, description: &str, tags: &[&str]) -> AgentMetadata {
    AgentMetadata {
        name: name.into(),
        description: description.into(),
        supported_modalities: vec![Modality::Text],
        output_modalities: vec![Modality::Text],
        tags: tags.iter().map(|t| (*t).to_owned()).collect(),
        version: "1.0.0".into(),
        final_nodes: Vec::new(),
        output_contract: None,
        tools: Vec::new(),
    }
}

#[must_use]
pub fn assistant_metadata() -> AgentMetadata {
    metadata(
        SUPPORT_ASSISTANT,
        "The store's customer support assistant",
        &["support", "customer", "rag", "mcp", "memory"],
    )
}

/// `support_assistant` allowed to call `tools`, on the `fast` model alias.
#[must_use]
pub fn assistant_def(tools: Vec<String>) -> RigAgentDef {
    RigAgentDef {
        preamble: Some(ASSISTANT_INSTRUCTIONS.into()),
        model: Some("fast".into()),
        tools,
        max_turns: 10,
        temperature: Some(0.2),
        ..RigAgentDef::new(SUPPORT_ASSISTANT)
    }
}

#[must_use]
pub fn triage_metadata() -> AgentMetadata {
    metadata(
        TICKET_TRIAGE,
        "A support agent's ticket triage and refunds",
        &["support", "agent", "hitl", "mcp", "rag"],
    )
}

/// `ticket_triage` allowed to call `tools`, on the `fast` model alias.
#[must_use]
pub fn triage_def(tools: Vec<String>) -> RigAgentDef {
    RigAgentDef {
        preamble: Some(TRIAGE_INSTRUCTIONS.into()),
        model: Some("fast".into()),
        tools,
        max_turns: 10,
        temperature: Some(0.1),
        ..RigAgentDef::new(TICKET_TRIAGE)
    }
}
