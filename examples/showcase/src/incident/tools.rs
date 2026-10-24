//! The incident desk's server tools over its incidents (records of kind
//! `incident`). Only the two personas (an SRE and the incident commander)
//! reach them. Remediations are high-risk and held for approval; the
//! postmortem is the commander's alone. A tool that takes `incident_id`
//! falls back to the request fact of that name, which the views send with
//! every turn once an incident is picked.

use std::sync::Arc;

use beam_tools::{ToolCallContext, ToolDefinition, ToolRegistry};
use chrono::{SecondsFormat, Utc};
use riverbase_core::RiverbaseResult;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::errors::{SHC_002, SHC_003, SHC_004, SHC_005};
use crate::incident::personas::{display_name, COMMANDER};
use crate::incident::postmortem::{commit_message, path_for, render, Postmortems, Sections};
use crate::incident::{INCIDENT, SHOWCASE};
use crate::kit::{approval_tool, arg_or_fact, object, read_tool, register_all, tool_user, write_tool};
use crate::records::{require, Record, RecordStore};
use crate::registry::role_in;

pub const OPEN_INCIDENT: &str = "open_incident";
pub const GET_INCIDENT: &str = "get_incident";
pub const ADD_TIMELINE_ENTRY: &str = "add_timeline_entry";
pub const ROLLBACK_DEPLOY: &str = "rollback_deploy";
pub const RESTART_SERVICE: &str = "restart_service";
pub const RESOLVE_INCIDENT: &str = "resolve_incident";
pub const WRITE_POSTMORTEM: &str = "write_postmortem";

/// The responder agent's own tools, besides `write_postmortem`, which exists
/// only when the postmortem repository does.
pub const RESPONDER_TOOLS: [&str; 6] = [
    OPEN_INCIDENT,
    GET_INCIDENT,
    ADD_TIMELINE_ENTRY,
    ROLLBACK_DEPLOY,
    RESTART_SERVICE,
    RESOLVE_INCIDENT,
];

/// The severities an incident is opened with.
pub const SEVERITIES: [&str; 3] = ["critical", "high", "warning"];

/// The signed-in persona and their showcase role, else `SHC-004`.
fn responder(ctx: &ToolCallContext) -> RiverbaseResult<(String, &'static str)> {
    let user = tool_user(ctx).ok_or_else(|| SHC_004.with_data("no caller"))?;
    let role = role_in(SHOWCASE, Some(&user))
        .ok_or_else(|| SHC_004.with_data("only the on-call team works incidents"))?;
    Ok((user, role))
}

/// The incident the call names (`SHC-002` when there is none).
async fn incident(store: &dyn RecordStore, args: &Value, ctx: &ToolCallContext) -> RiverbaseResult<Record> {
    let id = arg_or_fact(args, ctx, "incident_id")
        .and_then(|s| Uuid::parse_str(&s).ok())
        .ok_or_else(|| SHC_005.with_data("incident_id is missing or not a UUID"))?;
    let record = require(store, SHOWCASE, id).await?;
    if record.kind != INCIDENT {
        return Err(SHC_002.with_data(format!("incident {id} was not found")));
    }
    Ok(record)
}

/// The incident for a responder, who has checked in as a persona.
async fn responder_incident(
    store: &dyn RecordStore,
    args: &Value,
    ctx: &ToolCallContext,
) -> RiverbaseResult<(Record, String)> {
    let (user, _) = responder(ctx)?;
    Ok((incident(store, args, ctx).await?, user))
}

fn text_arg<'a>(args: &'a Value, key: &str) -> RiverbaseResult<&'a str> {
    args.get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| SHC_005.with_data(format!("{key} is required")))
}

fn not_resolved(record: &Record) -> RiverbaseResult<()> {
    if record.status == "resolved" {
        Err(SHC_003.with_data("the incident is resolved"))
    } else {
        Ok(())
    }
}

/// Append `{at, who, entry}` to the incident's timeline.
fn push_timeline(record: &mut Record, who: &str, entry: &str) {
    let item = json!({
        "at": Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        "who": who,
        "entry": entry,
    });
    match record.data.get_mut("timeline").and_then(Value::as_array_mut) {
        Some(timeline) => timeline.push(item),
        None => {
            record.data.insert("timeline".into(), json!([item]));
        }
    }
}

/// An incident as the agent reads it.
#[must_use]
pub fn incident_view(record: &Record) -> Value {
    json!({
        "incident_id": record.id,
        "status": record.status,
        "service": record.field("service"),
        "severity": record.field("severity"),
        "summary": record.field("summary"),
        "timeline": record.field("timeline").cloned().unwrap_or_else(|| json!([])),
        "remediation": record.field("remediation"),
        "resolution": record.field("resolution"),
        "postmortem": record.field("postmortem"),
        "opened": record.created_at,
    })
}

/// A simulated remediation: records it, adds the timeline entry and sets the
/// status `mitigated`. Nothing outside the desk changes.
async fn remediate(
    store: &dyn RecordStore,
    args: &Value,
    ctx: &ToolCallContext,
    action: &str,
    details: Value,
    entry: String,
) -> RiverbaseResult<Value> {
    let (mut record, user) = responder_incident(store, args, ctx).await?;
    not_resolved(&record)?;
    let service = text_arg(args, "service")?;
    if record.text("service").is_none_or(|s| !s.eq_ignore_ascii_case(service)) {
        return Err(SHC_005.with_data(format!("the incident is not about {service}")));
    }
    let mut remediation = details;
    remediation["action"] = json!(action);
    remediation["service"] = json!(service);
    remediation["simulated"] = json!(true);
    remediation["at"] = json!(Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true));
    record.data.insert("remediation".into(), remediation.clone());
    push_timeline(&mut record, &display_name(&user), &entry);
    record.status = "mitigated".into();
    store.save(&mut record).await?;
    Ok(json!({"done": true, "simulated": true, "status": record.status, "remediation": remediation}))
}

/// Every incident tool over `incidents`. `write_postmortem` is included
/// when there is a postmortem repository.
#[must_use]
#[allow(clippy::too_many_lines)] // One definition per tool, read top to bottom.
pub fn incident_tools(incidents: &Arc<dyn RecordStore>, postmortems: Option<Arc<Postmortems>>) -> Vec<ToolDefinition> {
    let id = json!({"type": "string", "description": "The incident's id; defaults to the current one."});
    let store = incidents.clone();
    let get = read_tool(
        GET_INCIDENT,
        "Read one incident: service, severity, status, summary, timeline, remediation and postmortem.",
        object(&json!({"incident_id": id}), &[]),
        move |args, ctx| {
            let store = store.clone();
            async move { Ok(incident_view(&responder_incident(store.as_ref(), &args, &ctx).await?.0)) }
        },
    );
    let store = incidents.clone();
    let open = write_tool(
        OPEN_INCIDENT,
        "Open an incident for a service, from an alert. Only one incident can be open per service: \
         it is refused while an earlier one for the service is not resolved.",
        object(
            &json!({
                "service": {"type": "string"},
                "severity": {"type": "string", "enum": SEVERITIES},
                "summary": {"type": "string", "description": "The alert's summary."},
            }),
            &["service", "severity", "summary"],
        ),
        move |args, ctx| {
            let store = store.clone();
            async move {
                let (user, _) = responder(&ctx)?;
                let service = text_arg(&args, "service")?.to_lowercase();
                let severity = text_arg(&args, "severity")?;
                if !SEVERITIES.contains(&severity) {
                    return Err(SHC_005.with_data(format!("severity must be one of {}", SEVERITIES.join(", "))));
                }
                let summary = text_arg(&args, "summary")?;
                let existing = store.list(SHOWCASE, INCIDENT, None).await?;
                if let Some(other) = existing
                    .iter()
                    .find(|i| i.status != "resolved" && i.text("service") == Some(service.as_str()))
                {
                    return Err(SHC_003.with_data(format!(
                        "incident {} for {service} is still {}",
                        other.id, other.status
                    )));
                }
                let org = ctx.auth.as_ref().and_then(|a| a.org_id.clone());
                let mut record = Record::new(SHOWCASE, INCIDENT, &user, org.as_deref(), summary);
                record.data.insert("service".into(), json!(service));
                record.data.insert("severity".into(), json!(severity));
                record.data.insert("summary".into(), json!(summary));
                record.data.insert("remediation".into(), Value::Null);
                push_timeline(&mut record, &display_name(&user), &format!("Opened a {severity} incident: {summary}"));
                store.insert(&record).await?;
                Ok(json!({"incident_id": record.id, "status": record.status}))
            }
        },
    );
    let store = incidents.clone();
    let timeline = write_tool(
        ADD_TIMELINE_ENTRY,
        "Add an entry to the incident's timeline (a finding, a decision, a message sent). The first entry \
         moves an open incident to investigating.",
        object(&json!({"incident_id": id, "entry": {"type": "string"}}), &["entry"]),
        move |args, ctx| {
            let store = store.clone();
            async move {
                let (mut record, user) = responder_incident(store.as_ref(), &args, &ctx).await?;
                not_resolved(&record)?;
                push_timeline(&mut record, &display_name(&user), text_arg(&args, "entry")?);
                if record.status == "open" {
                    record.status = "investigating".into();
                }
                store.save(&mut record).await?;
                Ok(json!({"saved": true, "status": record.status}))
            }
        },
    );
    let store = incidents.clone();
    let rollback = approval_tool(
        ROLLBACK_DEPLOY,
        "Roll the service back to an earlier version. Simulated: nothing is deployed, the desk only records \
         the rollback in the incident, adds a timeline entry and sets the status to mitigated. It changes \
         production, so the responder approves each call.",
        object(
            &json!({"incident_id": id, "service": {"type": "string"}, "to_version": {"type": "string"}, "reason": {"type": "string"}}),
            &["service", "to_version", "reason"],
        ),
        move |args, ctx| {
            let store = store.clone();
            async move {
                let (to_version, reason) = (text_arg(&args, "to_version")?, text_arg(&args, "reason")?);
                let service = text_arg(&args, "service")?;
                let entry = format!("Rolled {service} back to {to_version}: {reason}");
                let details = json!({"to_version": to_version, "reason": reason});
                remediate(store.as_ref(), &args, &ctx, "rollback_deploy", details, entry).await
            }
        },
    );
    let store = incidents.clone();
    let restart = approval_tool(
        RESTART_SERVICE,
        "Restart the service. Simulated: nothing is restarted, the desk only records the restart in the \
         incident, adds a timeline entry and sets the status to mitigated. It changes production, so the \
         responder approves each call.",
        object(
            &json!({"incident_id": id, "service": {"type": "string"}, "reason": {"type": "string"}}),
            &["service", "reason"],
        ),
        move |args, ctx| {
            let store = store.clone();
            async move {
                let reason = text_arg(&args, "reason")?;
                let service = text_arg(&args, "service")?;
                let entry = format!("Restarted {service}: {reason}");
                remediate(store.as_ref(), &args, &ctx, "restart_service", json!({"reason": reason}), entry).await
            }
        },
    );
    let store = incidents.clone();
    let resolve = write_tool(
        RESOLVE_INCIDENT,
        "Resolve the incident once it is mitigated and the service is healthy, with a one-paragraph summary \
         of what happened and what fixed it.",
        object(&json!({"incident_id": id, "summary": {"type": "string"}}), &["summary"]),
        move |args, ctx| {
            let store = store.clone();
            async move {
                let (mut record, user) = responder_incident(store.as_ref(), &args, &ctx).await?;
                if record.status != "mitigated" {
                    return Err(SHC_003.with_data(format!("the incident is {}, not mitigated", record.status)));
                }
                let summary = text_arg(&args, "summary")?;
                record.data.insert("resolution".into(), json!(summary));
                push_timeline(&mut record, &display_name(&user), &format!("Resolved: {summary}"));
                record.status = "resolved".into();
                store.save(&mut record).await?;
                Ok(json!({"resolved": true}))
            }
        },
    );
    let mut tools = vec![get, open, timeline, rollback, restart, resolve];
    if let Some(postmortems) = postmortems {
        let store = incidents.clone();
        tools.push(write_tool(
            WRITE_POSTMORTEM,
            "Write the postmortem of a resolved incident to postmortems/<date>-<service>.md in the postmortem \
             repository and commit it. Only the incident commander writes it. The timeline is copied from the \
             incident; you give the other sections.",
            object(
                &json!({
                    "incident_id": id,
                    "summary": {"type": "string"},
                    "impact": {"type": "string"},
                    "root_cause": {"type": "string"},
                    "what_went_well": {"type": "string"},
                    "action_items": {"type": "array", "items": {"type": "string"}},
                }),
                &["summary", "impact", "root_cause", "what_went_well", "action_items"],
            ),
            move |args, ctx| {
                let (store, postmortems) = (store.clone(), postmortems.clone());
                async move {
                    if responder(&ctx)?.1 != COMMANDER {
                        return Err(SHC_004.with_data("only the incident commander writes the postmortem"));
                    }
                    let mut record = incident(store.as_ref(), &args, &ctx).await?;
                    if record.status != "resolved" {
                        return Err(SHC_003.with_data(format!("the incident is {}, not resolved", record.status)));
                    }
                    let action_items: Vec<String> = args
                        .get("action_items")
                        .and_then(Value::as_array)
                        .ok_or_else(|| SHC_005.with_data("action_items must be a list"))?
                        .iter()
                        .filter_map(|i| i.as_str().map(str::trim).filter(|i| !i.is_empty()).map(str::to_owned))
                        .collect();
                    let sections = Sections {
                        summary: text_arg(&args, "summary")?,
                        impact: text_arg(&args, "impact")?,
                        root_cause: text_arg(&args, "root_cause")?,
                        what_went_well: text_arg(&args, "what_went_well")?,
                        action_items: &action_items,
                    };
                    let date = Utc::now().format("%Y-%m-%d").to_string();
                    let path = path_for(&date, record.text("service").unwrap_or("service"));
                    let body = render(&record, &date, &sections);
                    let commit = postmortems.write(&path, &body, &commit_message(&record)).await?;
                    record.data.insert("postmortem".into(), json!({"path": path, "commit": commit}));
                    store.save(&mut record).await?;
                    Ok(json!({"committed": commit, "path": path}))
                }
            },
        ));
    }
    tools
}

/// Register the incident tools; returns their names.
pub fn register_incident_tools(
    registry: &ToolRegistry,
    incidents: &Arc<dyn RecordStore>,
    postmortems: Option<Arc<Postmortems>>,
) -> Vec<String> {
    register_all(registry, incident_tools(incidents, postmortems))
}
