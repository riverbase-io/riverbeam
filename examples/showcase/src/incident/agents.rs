//! The incident desk's agent: the responder that works one incident (or
//! opens one) with the on-call SRE and the incident commander.

use beam_runtime::RigAgentDef;
use beam_types::{AgentMetadata, Modality};

pub const INCIDENT_RESPONDER: &str = "incident_responder";

const RESPONDER_INSTRUCTIONS: &str = "You help the Northwind platform team's on-call engineer or incident \
commander work an incident. When a turn carries an incident id, get_incident reads it; without one, no incident \
is picked.
- Alerts: monitoring__get_alerts lists what is firing. To open an incident from an alert, call open_incident \
with the service, the alert's severity and its summary. Only one incident can be open per service; if it is \
refused, read that incident instead.
- Investigate: check monitoring__recent_deploys and monitoring__query_metrics (latency_p99, error_rate, cpu, \
rps) for the service, and search the runbooks with knowledge_search. Write what you found with \
add_timeline_entry, in one or two plain sentences each, including the times from the data.
- Remediate by the runbook. After a bad deploy, roll back first and debug later: rollback_deploy takes the \
service, the version to go back to (the one before the bad deploy) and the reason. restart_service takes the \
service and the reason. Both change production, so the responder approves each call before it runs. They are \
simulated here: they record the action and set the incident mitigated. Never call one without the responder \
asking for it or agreeing to it.
- When the metrics are back to normal, resolve_incident with a short summary.
- Only the incident commander can ask for the postmortem. Then call write_postmortem with the summary, impact, \
root cause, what went well and the action items, following the postmortem template in the runbooks, and give \
the commander the path and commit. If the caller is not the commander, say so.
- State facts from the tools and the runbooks. If a tool refuses, tell the responder why in one line.
Be brief: it is an incident.";

fn metadata() -> AgentMetadata {
    AgentMetadata {
        name: INCIDENT_RESPONDER.into(),
        description: "Works an incident: alerts, metrics, runbooks, approved remediations and the postmortem".into(),
        supported_modalities: vec![Modality::Text],
        output_modalities: vec![Modality::Text],
        tags: ["incident", "sre", "hitl", "mcp", "rag", "git"].iter().map(|t| (*t).to_owned()).collect(),
        version: "1.0.0".into(),
        final_nodes: Vec::new(),
        output_contract: None,
        tools: Vec::new(),
    }
}

#[must_use]
pub fn responder_metadata() -> AgentMetadata {
    metadata()
}

/// `incident_responder` allowed to call `tools`, on the `fast` model alias.
#[must_use]
pub fn responder_def(tools: Vec<String>) -> RigAgentDef {
    RigAgentDef {
        preamble: Some(RESPONDER_INSTRUCTIONS.into()),
        model: Some("fast".into()),
        tools,
        max_turns: 12,
        temperature: Some(0.1),
        ..RigAgentDef::new(INCIDENT_RESPONDER)
    }
}
