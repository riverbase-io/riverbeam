//! The support desk's server tools over its tickets (records of kind
//! `ticket`). A customer reaches only their own tickets; a support agent
//! reaches all of them and is the only one who may triage, refund, resolve
//! or escalate. A tool that takes `ticket_id` falls back to the request fact
//! of that name, which the agent's view sends with every turn.

use std::sync::Arc;

use beam_tools::{ToolCallContext, ToolDefinition, ToolHandler, ToolRegistry};
use chrono::Utc;
use riverbase_core::RiverbaseResult;
use serde_json::{json, Map, Value};
use uuid::Uuid;

use crate::errors::{SHC_002, SHC_003, SHC_004, SHC_005};
use crate::kit::{approval_tool, arg_or_fact, object, read_tool, register_all, tool_user, write_tool};
use crate::records::{require, Record, RecordStore};
use crate::registry::role_in;
use crate::support::personas::{customer_profile, AGENT, CUSTOMER};
use crate::support::stub::order_for;
use crate::support::{SHOWCASE, TICKET};

pub const GET_CUSTOMER_PROFILE: &str = "get_customer_profile";
pub const OPEN_TICKET: &str = "open_ticket";
pub const LIST_MY_TICKETS: &str = "list_my_tickets";
pub const GET_TICKET: &str = "get_ticket";
pub const TRIAGE_TICKET: &str = "triage_ticket";
pub const ISSUE_REFUND: &str = "issue_refund";
pub const RESOLVE_TICKET: &str = "resolve_ticket";
pub const ESCALATE_TICKET: &str = "escalate_ticket";
pub const MY_ORDERS: &str = "my_orders";
pub const MY_ORDER: &str = "my_order";
pub const TRACK_MY_ORDER: &str = "track_my_order";
pub const TICKET_ORDER: &str = "ticket_order";

/// The order system's tools (MCP, `orders`), which take the customer's
/// email. The agents never get them: they get the wrappers below, which
/// fill in the email from who is asking, so a customer cannot read another
/// customer's orders by naming their email.
pub const LIST_ORDERS: &str = "orders__list_orders";
pub const GET_ORDER: &str = "orders__get_order";
pub const TRACK_SHIPMENT: &str = "orders__track_shipment";

/// The customer assistant's own tools.
pub const CUSTOMER_TOOLS: [&str; 4] = [GET_CUSTOMER_PROFILE, OPEN_TICKET, LIST_MY_TICKETS, GET_TICKET];
/// The triage agent's own tools.
pub const AGENT_TOOLS: [&str; 5] = [GET_TICKET, TRIAGE_TICKET, ISSUE_REFUND, RESOLVE_TICKET, ESCALATE_TICKET];

const CATEGORIES: [&str; 6] = ["question", "delivery", "damaged", "wrong_item", "refund", "other"];

fn user(ctx: &ToolCallContext) -> RiverbaseResult<String> {
    tool_user(ctx).ok_or_else(|| SHC_004.with_data("no caller"))
}

fn is_agent(user: &str) -> bool {
    role_in(SHOWCASE, Some(user)) == Some(AGENT)
}

/// The ticket the call names, if the caller may see it (else `SHC-002`: it
/// does not exist for them).
async fn ticket(store: &dyn RecordStore, args: &Value, ctx: &ToolCallContext) -> RiverbaseResult<Record> {
    let user = user(ctx)?;
    let id = arg_or_fact(args, ctx, "ticket_id")
        .and_then(|s| Uuid::parse_str(&s).ok())
        .ok_or_else(|| SHC_005.with_data("ticket_id is missing or not a UUID"))?;
    let ticket = require(store, SHOWCASE, id).await?;
    if ticket.kind != TICKET || (ticket.owner != user && !is_agent(&user)) {
        return Err(SHC_002.with_data(format!("ticket {id} was not found")));
    }
    Ok(ticket)
}

/// The ticket, for a support agent only.
async fn agent_ticket(store: &dyn RecordStore, args: &Value, ctx: &ToolCallContext) -> RiverbaseResult<Record> {
    if !is_agent(&user(ctx)?) {
        return Err(SHC_004.with_data("only a support agent may do this"));
    }
    ticket(store, args, ctx).await
}

fn text_arg<'a>(args: &'a Value, key: &str) -> RiverbaseResult<&'a str> {
    args.get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| SHC_005.with_data(format!("{key} is required")))
}

fn one_of<'a>(args: &'a Value, key: &str, allowed: &[&str]) -> RiverbaseResult<&'a str> {
    let value = text_arg(args, key)?;
    if allowed.contains(&value) {
        Ok(value)
    } else {
        Err(SHC_005.with_data(format!("{key} must be one of {}", allowed.join(", "))))
    }
}

/// A ticket as the agents read it.
#[must_use]
pub fn ticket_view(ticket: &Record) -> Value {
    json!({
        "ticket_id": ticket.id,
        "subject": ticket.title,
        "status": ticket.status,
        "customer": customer_profile(&ticket.owner),
        "fields": ticket.data,
        "opened": ticket.created_at,
    })
}

/// Every support tool over `tickets`.
#[must_use]
#[allow(clippy::too_many_lines)] // One definition per tool, read top to bottom.
pub fn support_tools(tickets: &Arc<dyn RecordStore>) -> Vec<ToolDefinition> {
    let id = json!({"type": "string", "description": "The ticket's id; defaults to the current one."});
    let store = tickets.clone();
    let get_ticket = read_tool(
        GET_TICKET,
        "Read one ticket: subject, status, the customer, and its fields (order, category, triage, refund).",
        object(&json!({"ticket_id": id}), &[]),
        move |args, ctx| {
            let store = store.clone();
            async move { Ok(ticket_view(&ticket(store.as_ref(), &args, &ctx).await?)) }
        },
    );
    let store = tickets.clone();
    let open_ticket = write_tool(
        OPEN_TICKET,
        "Open a support ticket for the customer, in their words. Use it for refunds, damaged or wrong \
         items, and anything you cannot settle in the chat. category is one of question, delivery, \
         damaged, wrong_item, refund, other.",
        object(
            &json!({
                "subject": {"type": "string"},
                "message": {"type": "string", "description": "What the customer said, in short."},
                "order_id": {"type": "string"},
                "category": {"type": "string", "enum": CATEGORIES},
            }),
            &["subject", "message", "category"],
        ),
        move |args, ctx| {
            let store = store.clone();
            async move {
                let user = user(&ctx)?;
                if role_in(SHOWCASE, Some(&user)) != Some(CUSTOMER) {
                    return Err(SHC_004.with_data("only a customer opens tickets"));
                }
                let org = ctx.auth.as_ref().and_then(|a| a.org_id.clone());
                let mut ticket = Record::new(SHOWCASE, TICKET, &user, org.as_deref(), text_arg(&args, "subject")?);
                let mut data = Map::new();
                data.insert("category".into(), json!(one_of(&args, "category", &CATEGORIES)?));
                data.insert("message".into(), json!(text_arg(&args, "message")?));
                if let Some(order) = args.get("order_id").and_then(Value::as_str).filter(|o| !o.trim().is_empty()) {
                    data.insert("order_id".into(), json!(order.trim().to_uppercase()));
                }
                data.insert("channel".into(), json!("chat"));
                ticket.merge(&data);
                store.insert(&ticket).await?;
                Ok(json!({"ticket_id": ticket.id, "status": ticket.status}))
            }
        },
    );
    let store = tickets.clone();
    let list_mine = read_tool(
        LIST_MY_TICKETS,
        "List the customer's own tickets, newest first.",
        object(&json!({}), &[]),
        move |_args, ctx| {
            let store = store.clone();
            async move {
                let user = user(&ctx)?;
                let rows = store.list(SHOWCASE, TICKET, Some(&user)).await?;
                Ok(json!(rows
                    .iter()
                    .map(|t| json!({"ticket_id": t.id, "subject": t.title, "status": t.status}))
                    .collect::<Vec<_>>()))
            }
        },
    );
    let store = tickets.clone();
    let triage = write_tool(
        TRIAGE_TICKET,
        "Record your triage of the ticket: category, priority, the customer's sentiment, a one-line \
         summary and a draft reply to the customer.",
        object(
            &json!({
                "ticket_id": id,
                "category": {"type": "string", "enum": CATEGORIES},
                "priority": {"type": "string", "enum": ["low", "normal", "high", "urgent"]},
                "sentiment": {"type": "string", "enum": ["positive", "neutral", "negative", "angry"]},
                "summary": {"type": "string"},
                "draft_reply": {"type": "string"},
            }),
            &["category", "priority", "sentiment", "summary", "draft_reply"],
        ),
        move |args, ctx| {
            let store = store.clone();
            async move {
                let mut ticket = agent_ticket(store.as_ref(), &args, &ctx).await?;
                if !["open", "triaged"].contains(&ticket.status.as_str()) {
                    return Err(SHC_003.with_data(format!("the ticket is {}", ticket.status)));
                }
                let triage = json!({
                    "category": one_of(&args, "category", &CATEGORIES)?,
                    "priority": one_of(&args, "priority", &["low", "normal", "high", "urgent"])?,
                    "sentiment": one_of(&args, "sentiment", &["positive", "neutral", "negative", "angry"])?,
                    "summary": text_arg(&args, "summary")?,
                    "draft_reply": text_arg(&args, "draft_reply")?,
                });
                ticket.data.insert("triage".into(), triage);
                ticket.status = "triaged".into();
                store.save(&mut ticket).await?;
                Ok(json!({"saved": true, "status": ticket.status}))
            }
        },
    );
    let store = tickets.clone();
    let refund = approval_tool(
        ISSUE_REFUND,
        "Refund the customer for the ticket's order, up to the order total. Money leaves the store: \
         the support agent approves each call.",
        object(
            &json!({"ticket_id": id, "amount": {"type": "number"}, "reason": {"type": "string"}}),
            &["amount", "reason"],
        ),
        move |args, ctx| {
            let store = store.clone();
            async move {
                let mut ticket = agent_ticket(store.as_ref(), &args, &ctx).await?;
                if ["refunded", "resolved"].contains(&ticket.status.as_str()) {
                    return Err(SHC_003.with_data(format!("the ticket is {}", ticket.status)));
                }
                if ticket.field("refund").is_some() {
                    return Err(SHC_003.with_data("the ticket was already refunded"));
                }
                let from = ticket.status.clone();
                let amount = args
                    .get("amount")
                    .and_then(Value::as_f64)
                    .filter(|a| *a > 0.0)
                    .ok_or_else(|| SHC_005.with_data("amount must be a positive number"))?;
                let email = customer_profile(&ticket.owner)
                    .and_then(|p| p["email"].as_str().map(str::to_owned))
                    .unwrap_or_default();
                let order = ticket
                    .text("order_id")
                    .and_then(|id| order_for(id, &email))
                    .ok_or_else(|| SHC_003.with_data("the ticket names no order of this customer"))?;
                let total = order["total"].as_f64().unwrap_or_default();
                if amount > total {
                    return Err(SHC_005.with_data(format!("amount {amount} is over the order total {total}")));
                }
                let refund = json!({
                    "amount": amount,
                    "reason": text_arg(&args, "reason")?,
                    "order_id": order["order_id"],
                    "refunded_at": Utc::now(),
                    "reference": format!("RF-{}", &ticket.id.simple().to_string()[..8]),
                });
                ticket.data.insert("refund".into(), refund.clone());
                ticket.status = "refunded".into();
                store.save_from(&mut ticket, &from).await?;
                Ok(json!({"refunded": true, "refund": refund}))
            }
        },
    );
    let store = tickets.clone();
    let resolve = write_tool(
        RESOLVE_TICKET,
        "Close the ticket with the reply that was sent to the customer.",
        object(&json!({"ticket_id": id, "resolution": {"type": "string"}}), &["resolution"]),
        move |args, ctx| {
            let store = store.clone();
            async move {
                let mut ticket = agent_ticket(store.as_ref(), &args, &ctx).await?;
                ticket.data.insert("resolution".into(), json!(text_arg(&args, "resolution")?));
                ticket.status = "resolved".into();
                store.save(&mut ticket).await?;
                Ok(json!({"resolved": true}))
            }
        },
    );
    let store = tickets.clone();
    let escalate = write_tool(
        ESCALATE_TICKET,
        "Hand the ticket to a team lead, with the reason.",
        object(&json!({"ticket_id": id, "reason": {"type": "string"}}), &["reason"]),
        move |args, ctx| {
            let store = store.clone();
            async move {
                let mut ticket = agent_ticket(store.as_ref(), &args, &ctx).await?;
                if !["open", "triaged", "escalated"].contains(&ticket.status.as_str()) {
                    return Err(SHC_003.with_data(format!("the ticket is {}", ticket.status)));
                }
                ticket.data.insert("escalation".into(), json!(text_arg(&args, "reason")?));
                ticket.status = "escalated".into();
                store.save(&mut ticket).await?;
                Ok(json!({"escalated": true}))
            }
        },
    );
    let profile = read_tool(
        GET_CUSTOMER_PROFILE,
        "Read the signed-in customer's name and email (the order system looks orders up by email).",
        object(&json!({}), &[]),
        |_args, ctx| async move {
            let user = user(&ctx)?;
            Ok(customer_profile(&user).unwrap_or_else(|| json!({"note": "no customer profile on file"})))
        },
    );
    vec![profile, open_ticket, list_mine, get_ticket, triage, refund, resolve, escalate]
}

/// The bridged order-system tool `name`'s handler, when the MCP server is
/// connected.
fn order_tool(registry: &ToolRegistry, name: &str) -> Option<Arc<dyn ToolHandler>> {
    registry.get(name).and_then(|t| t.callable)
}

/// The signed-in customer's email.
fn caller_email(ctx: &ToolCallContext) -> RiverbaseResult<String> {
    let user = user(ctx)?;
    if role_in(SHOWCASE, Some(&user)) != Some(CUSTOMER) {
        return Err(SHC_004.with_data("only a customer looks up their own orders"));
    }
    customer_profile(&user)
        .and_then(|p| p["email"].as_str().map(str::to_owned))
        .ok_or_else(|| SHC_004.with_data("no customer profile on file"))
}

/// The customer's order lookups and the agent's lookup of a ticket's order,
/// over the order system's MCP tools; none when the server is not
/// connected.
#[must_use]
pub fn order_tools(registry: &ToolRegistry, tickets: &Arc<dyn RecordStore>) -> Vec<ToolDefinition> {
    let mut tools = Vec::new();
    if let Some(list) = order_tool(registry, LIST_ORDERS) {
        tools.push(read_tool(
            MY_ORDERS,
            "List the signed-in customer's recent orders (order number, date, total, status).",
            object(&json!({}), &[]),
            move |_args, ctx| {
                let list = list.clone();
                async move {
                    let email = caller_email(&ctx)?;
                    list.call(json!({"email": email}), &ctx).await
                }
            },
        ));
    }
    let order = json!({"type": "string", "description": "The order number, e.g. ORD-1001."});
    if let Some(get) = order_tool(registry, GET_ORDER) {
        let get_for_ticket = get.clone();
        tools.push(read_tool(
            MY_ORDER,
            "Read one of the signed-in customer's orders: items, total, status, carrier.",
            object(&json!({"order_id": order}), &["order_id"]),
            move |args, ctx| {
                let get = get.clone();
                async move {
                    let email = caller_email(&ctx)?;
                    get.call(json!({"order_id": text_arg(&args, "order_id")?, "email": email}), &ctx)
                        .await
                }
            },
        ));
        let store = tickets.clone();
        tools.push(read_tool(
            TICKET_ORDER,
            "Read the order a ticket names, for its customer: items, total, status, carrier.",
            object(&json!({"ticket_id": {"type": "string", "description": "The ticket's id; defaults to the current one."}}), &[]),
            move |args, ctx| {
                let (get, store) = (get_for_ticket.clone(), store.clone());
                async move {
                    let ticket = agent_ticket(store.as_ref(), &args, &ctx).await?;
                    let email = customer_profile(&ticket.owner)
                        .and_then(|p| p["email"].as_str().map(str::to_owned))
                        .ok_or_else(|| SHC_003.with_data("the ticket's customer has no profile"))?;
                    let order_id = ticket
                        .text("order_id")
                        .ok_or_else(|| SHC_003.with_data("the ticket names no order"))?
                        .to_owned();
                    get.call(json!({"order_id": order_id, "email": email}), &ctx).await
                }
            },
        ));
    }
    if let Some(track) = order_tool(registry, TRACK_SHIPMENT) {
        tools.push(read_tool(
            TRACK_MY_ORDER,
            "The carrier's tracking events for one of the signed-in customer's orders.",
            object(&json!({"order_id": order}), &["order_id"]),
            move |args, ctx| {
                let track = track.clone();
                async move {
                    let email = caller_email(&ctx)?;
                    track
                        .call(json!({"order_id": text_arg(&args, "order_id")?, "email": email}), &ctx)
                        .await
                }
            },
        ));
    }
    tools
}

/// Register the support tools; returns their names.
pub fn register_support_tools(registry: &ToolRegistry, tickets: &Arc<dyn RecordStore>) -> Vec<String> {
    let mut tools = support_tools(tickets);
    tools.extend(order_tools(registry, tickets));
    register_all(registry, tools)
}
