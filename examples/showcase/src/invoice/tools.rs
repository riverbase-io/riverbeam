//! Accounts payable's server tools over its invoices (records of kind
//! `invoice`). Only AP staff reach invoices. The clerk may hold an invoice
//! and schedule a clean one; only the controller may reject one, or schedule
//! one that had exceptions (separation of duties). A tool that takes
//! `invoice_id` falls back to the request fact of that name, which the views
//! send with every turn. `ap_match` ([`crate::invoice::runtime`]) calls
//! `schedule_payment`, `hold_invoice` and `reject_invoice` itself, after the
//! person has decided.

use std::sync::Arc;

use beam_tools::{ToolCallContext, ToolDefinition, ToolRegistry};
use chrono::{NaiveDate, Utc};
use riverbase_core::RiverbaseResult;
use serde_json::{json, Map, Value};
use uuid::Uuid;

use crate::errors::{SHC_002, SHC_003, SHC_004, SHC_005};
use crate::invoice::matching::{normalize, num};
use crate::invoice::personas::{CLERK, CONTROLLER};
use crate::invoice::{INVOICE, SHOWCASE};
use crate::kit::{approval_tool, arg_or_fact, object, read_tool, register_all, tool_user, write_tool};
use crate::records::{require, Record, RecordStore};
use crate::registry::role_in;

pub const GET_INVOICE: &str = "get_invoice";
pub const LIST_INVOICES: &str = "list_invoices";
pub const SCHEDULE_PAYMENT: &str = "schedule_payment";
pub const HOLD_INVOICE: &str = "hold_invoice";
pub const REJECT_INVOICE: &str = "reject_invoice";

/// The fields of the vision reader's output an invoice record keeps.
const EXTRACTED: [&str; 10] = [
    "vendor_name",
    "invoice_number",
    "invoice_date",
    "po_number",
    "currency",
    "lines",
    "subtotal",
    "tax",
    "total",
    "confidence",
];

/// Statuses after which an invoice no longer changes.
const FINAL: [&str; 2] = ["scheduled", "rejected"];

fn user(ctx: &ToolCallContext) -> RiverbaseResult<String> {
    tool_user(ctx).ok_or_else(|| SHC_004.with_data("no caller"))
}

/// Whether `user` is AP staff (the clerk or the controller).
#[must_use]
pub fn is_staff(user: &str) -> bool {
    matches!(role_in(SHOWCASE, Some(user)), Some(CLERK | CONTROLLER))
}

/// Whether `user` is the controller.
#[must_use]
pub fn is_controller(user: &str) -> bool {
    role_in(SHOWCASE, Some(user)) == Some(CONTROLLER)
}

/// The caller of the tool, if AP staff.
fn staff(ctx: &ToolCallContext) -> RiverbaseResult<String> {
    let user = user(ctx)?;
    if is_staff(&user) {
        Ok(user)
    } else {
        Err(SHC_004.with_data("only accounts payable staff may do this"))
    }
}

fn text_arg<'a>(args: &'a Value, key: &str) -> RiverbaseResult<&'a str> {
    args.get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| SHC_005.with_data(format!("{key} is required")))
}

/// The invoice the call names, for AP staff.
async fn invoice(store: &dyn RecordStore, args: &Value, ctx: &ToolCallContext) -> RiverbaseResult<Record> {
    staff(ctx)?;
    let id = arg_or_fact(args, ctx, "invoice_id")
        .and_then(|s| Uuid::parse_str(&s).ok())
        .ok_or_else(|| SHC_005.with_data("invoice_id is missing or not a UUID"))?;
    let record = require(store, SHOWCASE, id).await?;
    if record.kind != INVOICE {
        return Err(SHC_002.with_data(format!("invoice {id} was not found")));
    }
    Ok(record)
}

/// An invoice as the agents and the UI read it.
#[must_use]
pub fn invoice_view(invoice: &Record) -> Value {
    json!({
        "invoice_id": invoice.id,
        "title": invoice.title,
        "status": invoice.status,
        "fields": invoice.data,
        "received": invoice.created_at,
    })
}

/// The other open invoices from the same vendor with the same invoice
/// number (a rejected one does not count).
///
/// # Errors
/// The record store fails.
pub async fn duplicates_of(
    store: &dyn RecordStore,
    vendor_name: &str,
    invoice_number: &str,
    except: Option<Uuid>,
) -> RiverbaseResult<Vec<Record>> {
    let (vendor, number) = (normalize(vendor_name), normalize(invoice_number));
    Ok(store
        .list(SHOWCASE, INVOICE, None)
        .await?
        .into_iter()
        .filter(|r| Some(r.id) != except && r.status != "rejected")
        .filter(|r| {
            normalize(r.text("vendor_name").unwrap_or_default()) == vendor
                && normalize(r.text("invoice_number").unwrap_or_default()) == number
        })
        .collect())
}

/// Add an invoice the vision reader read from `file_name`, as `received`.
/// The same vendor, invoice number and total twice is refused (`SHC-003`); a
/// different total under one number is kept, and the match flags it.
///
/// # Errors
/// `SHC-005` when the vendor or the invoice number is missing, `SHC-003` for
/// an exact duplicate, or the store fails.
pub async fn receive_invoice(
    store: &dyn RecordStore,
    owner: &str,
    org: Option<&str>,
    file_name: &str,
    extracted: &Value,
) -> RiverbaseResult<Record> {
    let source = extracted
        .as_object()
        .ok_or_else(|| SHC_005.with_data("extracted must be the reader's JSON object"))?;
    let field = |key: &str| source.get(key).and_then(Value::as_str).map(str::trim).filter(|v| !v.is_empty());
    let (Some(vendor), Some(number)) = (field("vendor_name"), field("invoice_number")) else {
        return Err(SHC_005.with_data("the invoice needs a vendor name and an invoice number"));
    };
    let total = source.get("total").and_then(num);
    let twin = duplicates_of(store, vendor, number, None)
        .await?
        .into_iter()
        .find(|r| r.number("total").zip(total).is_some_and(|(a, b)| (a - b).abs() < 0.005));
    if let Some(twin) = twin {
        return Err(SHC_003.with_data(format!("{} was already received as {}", twin.title, twin.id)));
    }
    let mut record = Record::new(SHOWCASE, INVOICE, owner, org, &format!("{vendor} {number}"));
    record.status = "received".into();
    let mut data = Map::new();
    for key in EXTRACTED {
        if let Some(value) = source.get(key) {
            data.insert(key.into(), value.clone());
        }
    }
    data.insert("file_name".into(), json!(file_name));
    record.merge(&data);
    store.insert(&record).await?;
    Ok(record)
}

/// Every accounts payable tool over `invoices`.
#[must_use]
#[allow(clippy::too_many_lines)] // One definition per tool, read top to bottom.
pub fn invoice_tools(invoices: &Arc<dyn RecordStore>) -> Vec<ToolDefinition> {
    let id = json!({"type": "string", "description": "The invoice's id; defaults to the current one."});
    let store = invoices.clone();
    let get_invoice = read_tool(
        GET_INVOICE,
        "Read one invoice: vendor, number, date, PO, lines, totals, status, and the match findings and \
         decision once there are any.",
        object(&json!({"invoice_id": id}), &[]),
        move |args, ctx| {
            let store = store.clone();
            async move { Ok(invoice_view(&invoice(store.as_ref(), &args, &ctx).await?)) }
        },
    );
    let store = invoices.clone();
    let list_invoices = read_tool(
        LIST_INVOICES,
        "List the invoices, newest first, optionally only those in one status (received, matched, \
         pending_approval, approved, scheduled, rejected, on_hold).",
        object(&json!({"status": {"type": "string"}}), &[]),
        move |args, ctx| {
            let store = store.clone();
            async move {
                staff(&ctx)?;
                let status = args.get("status").and_then(Value::as_str).filter(|s| !s.is_empty());
                let rows = store.list(SHOWCASE, INVOICE, None).await?;
                Ok(json!(rows
                    .iter()
                    .filter(|r| status.is_none_or(|s| r.status == s))
                    .map(|r| json!({
                        "invoice_id": r.id,
                        "title": r.title,
                        "status": r.status,
                        "total": r.number("total"),
                        "po_number": r.text("po_number"),
                    }))
                    .collect::<Vec<_>>()))
            }
        },
    );
    let store = invoices.clone();
    let schedule = approval_tool(
        SCHEDULE_PAYMENT,
        "Schedule the payment of an invoice for its pay date (invoice date plus the vendor's terms). Money \
         leaves the company: the person approves each call. A matched invoice may be scheduled by the \
         clerk or the controller; an invoice with exceptions only by the controller, after deciding them.",
        object(
            &json!({"invoice_id": id, "amount": {"type": "number"}, "pay_date": {"type": "string", "description": "YYYY-MM-DD"}}),
            &["amount", "pay_date"],
        ),
        move |args, ctx| {
            let store = store.clone();
            async move {
                let mut record = invoice(store.as_ref(), &args, &ctx).await?;
                let who = user(&ctx)?;
                match record.status.as_str() {
                    "matched" => {}
                    "approved" if is_controller(&who) => {}
                    "approved" => return Err(SHC_004.with_data("only the controller schedules an invoice with exceptions")),
                    other => return Err(SHC_003.with_data(format!("the invoice is {other}; only a matched or approved invoice is scheduled"))),
                }
                if record.field("payment").is_some() {
                    return Err(SHC_003.with_data("the invoice already has a scheduled payment"));
                }
                let from = record.status.clone();
                let amount = args
                    .get("amount")
                    .and_then(num)
                    .filter(|a| *a > 0.0)
                    .ok_or_else(|| SHC_005.with_data("amount must be a positive number"))?;
                let approved = record.number("approved_amount").or_else(|| record.number("total")).unwrap_or_default();
                if (amount - approved).abs() >= 0.005 {
                    return Err(SHC_005.with_data(format!("amount {amount:.2} is not the approved amount {approved:.2}")));
                }
                let date = text_arg(&args, "pay_date")?;
                NaiveDate::parse_from_str(date, "%Y-%m-%d")
                    .map_err(|_| SHC_005.with_data("pay_date must be YYYY-MM-DD"))?;
                let payment = json!({
                    "amount": amount,
                    "pay_date": date,
                    "reference": format!("PAY-{}", &record.id.simple().to_string()[..8]),
                    "scheduled_by": who,
                    "scheduled_at": Utc::now(),
                });
                record.data.insert("payment".into(), payment.clone());
                record.status = "scheduled".into();
                store.save_from(&mut record, &from).await?;
                Ok(json!({"scheduled": true, "payment": payment}))
            }
        },
    );
    let store = invoices.clone();
    let hold = write_tool(
        HOLD_INVOICE,
        "Put an invoice on hold with the reason (a question to the vendor, a missing document). Nothing \
         is paid while it is on hold.",
        object(&json!({"invoice_id": id, "reason": {"type": "string"}}), &["reason"]),
        move |args, ctx| {
            let store = store.clone();
            async move {
                let mut record = invoice(store.as_ref(), &args, &ctx).await?;
                if FINAL.contains(&record.status.as_str()) {
                    return Err(SHC_003.with_data(format!("the invoice is {}", record.status)));
                }
                record.data.insert("hold_reason".into(), json!(text_arg(&args, "reason")?));
                record.status = "on_hold".into();
                store.save(&mut record).await?;
                Ok(json!({"on_hold": true}))
            }
        },
    );
    let store = invoices.clone();
    let reject = write_tool(
        REJECT_INVOICE,
        "Reject an invoice with the reason. Only the controller rejects an invoice.",
        object(&json!({"invoice_id": id, "reason": {"type": "string"}}), &["reason"]),
        move |args, ctx| {
            let store = store.clone();
            async move {
                let mut record = invoice(store.as_ref(), &args, &ctx).await?;
                if !is_controller(&user(&ctx)?) {
                    return Err(SHC_004.with_data("only the controller rejects an invoice"));
                }
                if FINAL.contains(&record.status.as_str()) {
                    return Err(SHC_003.with_data(format!("the invoice is {}", record.status)));
                }
                record.data.insert("rejection_reason".into(), json!(text_arg(&args, "reason")?));
                record.status = "rejected".into();
                store.save(&mut record).await?;
                Ok(json!({"rejected": true}))
            }
        },
    );
    vec![get_invoice, list_invoices, schedule, hold, reject]
}

/// Register the accounts payable tools; returns their names.
pub fn register_invoice_tools(registry: &ToolRegistry, invoices: &Arc<dyn RecordStore>) -> Vec<String> {
    register_all(registry, invoice_tools(invoices))
}
