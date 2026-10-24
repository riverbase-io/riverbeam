//! The claims showcase's server tools over its claims (records of kind
//! `claim`). A policyholder reaches only their own claims and may change a
//! claim only while it is a draft; an adjuster reads every claim and is the
//! only one who may pay out. A tool that takes `claim_id` falls back to the
//! request fact of that name, and then to the policyholder's newest draft.

use std::sync::Arc;

use beam_tools::{ToolCallContext, ToolDefinition, ToolRegistry};
use chrono::Utc;
use riverbase_core::RiverbaseResult;
use serde_json::{json, Map, Value};
use uuid::Uuid;

use crate::claims::personas::{ADJUSTER, POLICYHOLDER};
use crate::claims::policies::{parse_date, policy, policy_of, Policy};
use crate::claims::rules::{assess, cents, number, LOSS_TYPES, SEVERITIES};
use crate::claims::{CLAIM, SHOWCASE};
use crate::errors::{SHC_002, SHC_003, SHC_004, SHC_005};
use crate::kit::{
    approval_tool, arg_or_fact, object, read_tool, register_all, shared_client_tools, tool_user, write_tool,
};
use crate::records::{require, Record, RecordStore};
use crate::registry::role_in;

pub const GET_MY_POLICY: &str = "get_my_policy";
pub const START_CLAIM: &str = "start_claim";
pub const UPDATE_CLAIM: &str = "update_claim";
pub const GET_CLAIM: &str = "get_claim";
pub const FILE_CLAIM: &str = "file_claim";
pub const APPROVE_PAYOUT: &str = "approve_payout";

/// The intake assistant's own tools.
pub const INTAKE_TOOLS: [&str; 5] = [GET_MY_POLICY, START_CLAIM, UPDATE_CLAIM, GET_CLAIM, FILE_CLAIM];

/// What a claim needs before it can be filed.
pub const REQUIRED_FIELDS: [&str; 5] = ["loss_date", "location", "description", "other_party", "drivable"];

/// The fields `update_claim` may set.
const FIELDS: [&str; 9] = [
    "loss_date",
    "loss_type",
    "location",
    "description",
    "other_party",
    "police_report",
    "drivable",
    "estimated_repair_cost",
    "severity",
];

/// The statuses a payout may be issued from.
const PAYABLE: [&str; 3] = ["in_review", "approved", "partially_approved"];

fn user(ctx: &ToolCallContext) -> RiverbaseResult<String> {
    tool_user(ctx).ok_or_else(|| SHC_004.with_data("no caller"))
}

fn is_adjuster(user: &str) -> bool {
    role_in(SHOWCASE, Some(user)) == Some(ADJUSTER)
}

fn is_policyholder(user: &str) -> bool {
    role_in(SHOWCASE, Some(user)) == Some(POLICYHOLDER)
}

/// The claim the call names, if the caller may see it (else `SHC-002`: it
/// does not exist for them). With no id it is the policyholder's newest
/// draft.
async fn claim(store: &dyn RecordStore, args: &Value, ctx: &ToolCallContext) -> RiverbaseResult<Record> {
    let user = user(ctx)?;
    let Some(id) = arg_or_fact(args, ctx, "claim_id") else {
        return store
            .list(SHOWCASE, CLAIM, Some(&user))
            .await?
            .into_iter()
            .find(|c| c.status == "draft")
            .ok_or_else(|| SHC_002.with_data("there is no draft claim; start one first"));
    };
    let id = Uuid::parse_str(&id).map_err(|_| SHC_005.with_data("claim_id is not a UUID"))?;
    let claim = require(store, SHOWCASE, id).await?;
    if claim.kind != CLAIM || (claim.owner != user && !is_adjuster(&user)) {
        return Err(SHC_002.with_data(format!("claim {id} was not found")));
    }
    Ok(claim)
}

/// The caller's own draft claim, for the tools that change it.
async fn own_draft(store: &dyn RecordStore, args: &Value, ctx: &ToolCallContext) -> RiverbaseResult<Record> {
    let user = user(ctx)?;
    let claim = claim(store, args, ctx).await?;
    if claim.owner != user {
        return Err(SHC_004.with_data("only the policyholder who owns the claim may change it"));
    }
    if claim.status != "draft" {
        return Err(SHC_003.with_data(format!("the claim is {}; only a draft can change", claim.status)));
    }
    Ok(claim)
}

fn text_arg<'a>(args: &'a Value, key: &str) -> RiverbaseResult<&'a str> {
    args.get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| SHC_005.with_data(format!("{key} is required")))
}

/// The required fields a claim still lacks.
#[must_use]
pub fn missing_fields(claim: &Record) -> Vec<&'static str> {
    REQUIRED_FIELDS
        .iter()
        .copied()
        .filter(|key| match claim.field(key) {
            Some(Value::String(s)) => s.trim().is_empty(),
            Some(Value::Null) | None => true,
            Some(_) => false,
        })
        .collect()
}

/// A claim as the agents read it.
#[must_use]
pub fn claim_view(claim: &Record) -> Value {
    json!({
        "claim_id": claim.id,
        "title": claim.title,
        "status": claim.status,
        "policy": claim.text("policy_number").and_then(policy).map(Policy::view),
        "fields": claim.data,
        "missing_fields": missing_fields(claim),
        "opened": claim.created_at,
    })
}

/// Check `fields` against what `update_claim` accepts, and normalize them.
fn clean_fields(fields: &Map<String, Value>) -> RiverbaseResult<Map<String, Value>> {
    let mut out = Map::new();
    for (key, value) in fields {
        if !FIELDS.contains(&key.as_str()) {
            return Err(SHC_005.with_data(format!("{key} is not a claim field; use {}", FIELDS.join(", "))));
        }
        let bad = |what: &str| SHC_005.with_data(format!("{key} must be {what}"));
        let clean = match key.as_str() {
            "loss_date" => {
                let date = value.as_str().and_then(parse_date).ok_or_else(|| bad("a date, YYYY-MM-DD"))?;
                json!(date.to_string())
            }
            "loss_type" => json!(value
                .as_str()
                .filter(|t| LOSS_TYPES.contains(t))
                .ok_or_else(|| bad("collision or comprehensive"))?),
            "severity" => json!(value
                .as_str()
                .filter(|t| SEVERITIES.contains(t))
                .ok_or_else(|| bad("minor, moderate, severe or total_loss"))?),
            "drivable" => match value {
                Value::Bool(b) => json!(b),
                Value::String(s) if s.eq_ignore_ascii_case("yes") => json!(true),
                Value::String(s) if s.eq_ignore_ascii_case("no") => json!(false),
                _ => return Err(bad("true or false")),
            },
            "estimated_repair_cost" => json!(number(value).filter(|n| *n >= 0.0).ok_or_else(|| bad("a number"))?),
            _ => json!(value
                .as_str()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| bad("text"))?),
        };
        out.insert(key.clone(), clean);
    }
    Ok(out)
}

/// Every claims tool over `claims`.
#[must_use]
#[allow(clippy::too_many_lines)] // One definition per tool, read top to bottom.
pub fn claims_tools(claims: &Arc<dyn RecordStore>) -> Vec<ToolDefinition> {
    let id = json!({"type": "string", "description": "The claim's id; defaults to the current one."});
    let get_my_policy = read_tool(
        GET_MY_POLICY,
        "Read the signed-in policyholder's policy: vehicle, term, coverages, deductibles, limits, rental \
         and exclusions.",
        object(&json!({"policy_number": {"type": "string"}}), &[]),
        |args, ctx| async move {
            let user = user(&ctx)?;
            let found = match args.get("policy_number").and_then(Value::as_str) {
                Some(number) => policy(number).filter(|p| p.holder == user || is_adjuster(&user)),
                None => policy_of(&user),
            };
            found
                .map(Policy::view)
                .ok_or_else(|| SHC_002.with_data("no policy was found for the caller"))
        },
    );
    let store = claims.clone();
    let get_claim = read_tool(
        GET_CLAIM,
        "Read one claim: status, its policy, the fields saved so far, and which required fields are missing.",
        object(&json!({"claim_id": id}), &[]),
        move |args, ctx| {
            let store = store.clone();
            async move { Ok(claim_view(&claim(store.as_ref(), &args, &ctx).await?)) }
        },
    );
    let store = claims.clone();
    let start_claim = write_tool(
        START_CLAIM,
        "Start a draft claim on the policyholder's policy. If a draft already exists it is returned instead \
         of a second one.",
        object(&json!({}), &[]),
        move |_args, ctx| {
            let store = store.clone();
            async move {
                let user = user(&ctx)?;
                if !is_policyholder(&user) {
                    return Err(SHC_004.with_data("only a policyholder starts a claim"));
                }
                let policy = policy_of(&user).ok_or_else(|| SHC_002.with_data("no policy was found for the caller"))?;
                let drafts = store.list(SHOWCASE, CLAIM, Some(&user)).await?;
                if let Some(draft) = drafts.iter().find(|c| c.status == "draft") {
                    return Ok(json!({"claim_id": draft.id, "status": draft.status, "existing": true}));
                }
                let org = ctx.auth.as_ref().and_then(|a| a.org_id.clone());
                let mut claim = Record::new(SHOWCASE, CLAIM, &user, org.as_deref(), &format!("Claim on the {}", policy.vehicle));
                claim.status = "draft".into();
                let mut data = Map::new();
                data.insert("policy_number".into(), json!(policy.number));
                data.insert("vehicle".into(), json!(policy.vehicle));
                data.insert("documents".into(), json!([]));
                claim.merge(&data);
                store.insert(&claim).await?;
                Ok(json!({"claim_id": claim.id, "status": claim.status, "existing": false}))
            }
        },
    );
    let store = claims.clone();
    let update_claim = write_tool(
        UPDATE_CLAIM,
        "Save facts on the draft claim. fields may hold loss_date (YYYY-MM-DD), loss_type (collision or \
         comprehensive), location, description, other_party (or none), police_report (or none), drivable \
         (true or false), estimated_repair_cost and severity (minor, moderate, severe or total_loss; both \
         from the damage reader).",
        object(
            &json!({"claim_id": id, "fields": {"type": "object", "description": "The facts to save."}}),
            &["fields"],
        ),
        move |args, ctx| {
            let store = store.clone();
            async move {
                let mut claim = own_draft(store.as_ref(), &args, &ctx).await?;
                let fields = args
                    .get("fields")
                    .and_then(Value::as_object)
                    .ok_or_else(|| SHC_005.with_data("fields must be an object"))?;
                let clean = clean_fields(fields)?;
                claim.merge(&clean);
                store.save(&mut claim).await?;
                Ok(json!({"saved": clean.keys().collect::<Vec<_>>(), "missing_fields": missing_fields(&claim)}))
            }
        },
    );
    let store = claims.clone();
    let file_claim = write_tool(
        FILE_CLAIM,
        "File the draft claim with Harbor Mutual once the required facts are in (loss_date, location, \
         description, other_party, drivable). The policyholder asked for it; no one else approves it.",
        object(&json!({"claim_id": id}), &[]),
        move |args, ctx| {
            let store = store.clone();
            async move {
                let mut claim = own_draft(store.as_ref(), &args, &ctx).await?;
                let missing = missing_fields(&claim);
                if !missing.is_empty() {
                    return Err(SHC_005.with_data(format!("the claim still needs {}", missing.join(", "))));
                }
                claim.status = "filed".into();
                claim.data.insert("filed_at".into(), json!(Utc::now()));
                store.save(&mut claim).await?;
                Ok(json!({"filed": true, "claim_id": claim.id, "status": claim.status}))
            }
        },
    );
    let store = claims.clone();
    let approve_payout = approval_tool(
        APPROVE_PAYOUT,
        "Pay the claim: money leaves Harbor Mutual, so the adjuster approves each call. amount is at most \
         the estimate minus the deductible, and at most the policy limit.",
        object(
            &json!({"claim_id": id, "amount": {"type": "number"}, "reason": {"type": "string"}}),
            &["amount", "reason"],
        ),
        move |args, ctx| {
            let store = store.clone();
            async move {
                if !is_adjuster(&user(&ctx)?) {
                    return Err(SHC_004.with_data("only an adjuster may approve a payout"));
                }
                let mut claim = claim(store.as_ref(), &args, &ctx).await?;
                if !PAYABLE.contains(&claim.status.as_str()) {
                    return Err(SHC_003.with_data(format!("the claim is {}; it cannot be paid", claim.status)));
                }
                if claim.field("payout").is_some() {
                    return Err(SHC_003.with_data("the claim is already paid"));
                }
                let from = claim.status.clone();
                let amount = args
                    .get("amount")
                    .and_then(number)
                    .filter(|a| *a > 0.0)
                    .map(cents)
                    .ok_or_else(|| SHC_005.with_data("amount must be a positive number"))?;
                let assessment = assess(&claim, claim.text("policy_number").and_then(policy));
                let most = assessment
                    .payable
                    .ok_or_else(|| SHC_003.with_data("the claim has no estimate or no policy to pay under"))?;
                if amount > cents(most) {
                    return Err(SHC_005.with_data(format!(
                        "amount {amount} is over the {most:.2} the policy pays (estimate minus deductible, within the limit)"
                    )));
                }
                let payout = json!({
                    "amount": amount,
                    "reason": text_arg(&args, "reason")?,
                    "deductible": assessment.deductible,
                    "reference": format!("PAY-{}", claim.id.simple().to_string()[..8].to_uppercase()),
                    "paid_at": Utc::now(),
                });
                claim.data.insert("payout".into(), payout.clone());
                claim.status = "paid".into();
                store.save_from(&mut claim, &from).await?;
                Ok(json!({"paid": true, "payout": payout}))
            }
        },
    );
    vec![get_my_policy, start_claim, update_claim, get_claim, file_claim, approve_payout]
}

/// Register the claims tools and the shared client tools (`request_input`,
/// `request_document`); returns their names.
pub fn register_claims_tools(registry: &ToolRegistry, claims: &Arc<dyn RecordStore>) -> Vec<String> {
    let mut tools = claims_tools(claims);
    tools.extend(shared_client_tools());
    register_all(registry, tools)
}
