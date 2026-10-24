//! The loan desk's tools.
//!
//! - **Server tools** read and write the application store. The caller is the
//!   run's user: a borrower reaches only their own applications, an officer
//!   or admin any of them.
//! - **Client tools** run in the borrower's browser: the form is
//!   the UI's, so the assistant fills it, highlights fields, moves between
//!   steps and asks for input or documents through them.
//!
//! A tool that takes `application_id` falls back to the request fact of that
//! name, which the UI sends with every turn.

use std::sync::Arc;

use async_trait::async_trait;
use beam_tools::{ToolCallContext, ToolDefinition, ToolHandler, ToolRegistry, ToolRisk};
use riverbase_core::RiverbaseResult;
use serde_json::{json, Map, Value};
use uuid::Uuid;

use crate::loan_desk::data::{require, Application, ApplicationStore};
use crate::loan_desk::errors::{LND_002, LND_005, LND_006, LND_007};
use crate::loan_desk::personas::{persona, Role};
use crate::loan_desk::programs::{borrower_profile, program, PROGRAMS};

pub const GET_BORROWER_PROFILE: &str = "get_borrower_profile";
pub const LIST_PROGRAMS: &str = "list_programs";
pub const GET_PROGRAM: &str = "get_program";
pub const GET_APPLICATION: &str = "get_application";
pub const UPDATE_APPLICATION: &str = "update_application";
pub const SUBMIT_APPLICATION: &str = "submit_application";

pub const FILL_FORM_FIELDS: &str = "fill_form_fields";
pub const HIGHLIGHT_FIELD: &str = "highlight_field";
pub const NAVIGATE_STEP: &str = "navigate_step";
pub const SHOW_PROGRAM_COMPARISON: &str = "show_program_comparison";
/// The form card and the document upload every showcase's chat runs.
pub use crate::kit::{REQUEST_DOCUMENT, REQUEST_INPUT};

/// The client tools the borrower UI runs and declares.
pub const CLIENT_TOOLS: [&str; 6] = [
    FILL_FORM_FIELDS,
    HIGHLIGHT_FIELD,
    NAVIGATE_STEP,
    SHOW_PROGRAM_COMPARISON,
    REQUEST_INPUT,
    REQUEST_DOCUMENT,
];

/// The run's user id.
#[must_use]
pub fn caller(ctx: &ToolCallContext) -> Option<String> {
    ctx.auth
        .as_ref()
        .and_then(|a| a.user_id.clone())
        .or_else(|| ctx.input.as_ref().and_then(|i| i.user_id()))
}

/// The request fact `key`, as the UI sent it with the turn.
#[must_use]
pub fn fact(ctx: &ToolCallContext, key: &str) -> Option<Value> {
    ctx.input
        .as_ref()?
        .context
        .as_ref()?
        .facts
        .iter()
        .find(|f| f.key == key)
        .map(|f| f.value.clone())
}

fn application_id(args: &Value, ctx: &ToolCallContext) -> RiverbaseResult<Uuid> {
    args.get("application_id")
        .cloned()
        .or_else(|| fact(ctx, "application_id"))
        .and_then(|v| v.as_str().and_then(|s| Uuid::parse_str(s).ok()))
        .ok_or_else(|| LND_007.with_data("application_id is missing or not a UUID"))
}

/// Whether `user` may act on `application`: its borrower, or staff.
#[must_use]
pub fn may_access(user: Option<&str>, application: &Application) -> bool {
    match user {
        Some(user) if user == application.owner => true,
        Some(user) => persona(user).is_some_and(|p| p.role != Role::Borrower),
        None => false,
    }
}

#[derive(Clone, Copy)]
enum Kind {
    Profile,
    Programs,
    Program,
    GetApplication,
    UpdateApplication,
    SubmitApplication,
}

struct LoanTool {
    kind: Kind,
    applications: Arc<dyn ApplicationStore>,
}

impl LoanTool {
    async fn application(
        &self,
        args: &Value,
        ctx: &ToolCallContext,
    ) -> RiverbaseResult<Application> {
        let id = application_id(args, ctx)?;
        let application = require(self.applications.as_ref(), id).await?;
        if !may_access(caller(ctx).as_deref(), &application) {
            // Not the caller's: it does not exist for them.
            return Err(LND_002.with_data(format!("application {id} was not found")));
        }
        Ok(application)
    }
}

#[async_trait]
impl ToolHandler for LoanTool {
    async fn call(&self, args: Value, ctx: &ToolCallContext) -> RiverbaseResult<Value> {
        match self.kind {
            Kind::Profile => {
                let user = caller(ctx).ok_or_else(|| LND_005.with_data("no caller"))?;
                Ok(
                    borrower_profile(&user)
                        .unwrap_or_else(|| json!({"note": "no profile on file"})),
                )
            }
            Kind::Programs => Ok(json!(PROGRAMS)),
            Kind::Program => {
                let id = args
                    .get("program_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                program(id)
                    .map(|p| json!(p))
                    .ok_or_else(|| LND_006.with_data(id.to_owned()))
            }
            Kind::GetApplication => {
                let application = self.application(&args, ctx).await?;
                Ok(json!({
                    "application": application,
                    "missing_fields": application.missing_fields(),
                    "has_pay_stub": application.has_pay_stub(),
                }))
            }
            Kind::UpdateApplication => {
                let mut application = self.application(&args, ctx).await?;
                if let Some(fields) = args.get("fields").and_then(Value::as_object) {
                    application.merge_fields(fields)?;
                }
                if let Some(document) = args.get("document").filter(|d| d.is_object()) {
                    application.documents.push(document.clone());
                }
                self.applications.save(&mut application).await?;
                Ok(json!({
                    "saved": true,
                    "missing_fields": application.missing_fields(),
                    "has_pay_stub": application.has_pay_stub(),
                }))
            }
            Kind::SubmitApplication => {
                let mut application = self.application(&args, ctx).await?;
                application.submit()?;
                self.applications.save(&mut application).await?;
                Ok(json!({"submitted": true, "status": application.status.as_str()}))
            }
        }
    }
}

fn object(properties: &Value, required: &[&str]) -> Value {
    json!({"type": "object", "properties": properties, "required": required})
}

fn server(
    name: &str,
    description: &str,
    input_schema: Value,
    kind: Kind,
    applications: &Arc<dyn ApplicationStore>,
) -> ToolDefinition {
    ToolDefinition {
        description: description.into(),
        input_schema,
        risk: ToolRisk::Low,
        side_effect: false,
        ..ToolDefinition::new(name)
    }
    .with_handler(Arc::new(LoanTool {
        kind,
        applications: applications.clone(),
    }))
}

/// The server tools over `applications`.
#[must_use]
pub fn server_tools(applications: &Arc<dyn ApplicationStore>) -> Vec<ToolDefinition> {
    let id = json!({"type": "string", "description": "The application's id; defaults to the current one."});
    vec![
        server(
            GET_BORROWER_PROFILE,
            "Read what the lender already knows about the borrower (name, contact, employer). \
             Never infer values that are absent.",
            object(&json!({}), &[]),
            Kind::Profile,
            applications,
        ),
        server(
            LIST_PROGRAMS,
            "List the loan programs with their factual criteria (credit score, LTV, DTI, down payment, limits).",
            object(&json!({}), &[]),
            Kind::Programs,
            applications,
        ),
        server(
            GET_PROGRAM,
            "Read one loan program's criteria.",
            object(&json!({"program_id": {"type": "string"}}), &["program_id"]),
            Kind::Program,
            applications,
        ),
        server(
            GET_APPLICATION,
            "Read the application: its saved fields, documents, status, and what is still missing.",
            object(&json!({"application_id": id}), &[]),
            Kind::GetApplication,
            applications,
        ),
        ToolDefinition {
            side_effect: true,
            idempotent: true,
            ..server(
                UPDATE_APPLICATION,
                "Save borrower-confirmed fields (and optionally one document's metadata) to the draft application. \
                 Fields: first_name, last_name, email, phone, employer, gross_monthly_income, monthly_debts, \
                 purchase_price, down_payment, program_id.",
                object(
                    &json!({"application_id": id, "fields": {"type": "object"}, "document": {"type": "object"}}),
                    &[],
                ),
                Kind::UpdateApplication,
                applications,
            )
        },
        ToolDefinition {
            risk: ToolRisk::High,
            side_effect: true,
            idempotent: true,
            require_approval: true,
            ..server(
                SUBMIT_APPLICATION,
                "Submit the completed application for underwriting. The borrower must ask for it explicitly, \
                 and approves the call itself.",
                object(&json!({"application_id": id}), &[]),
                Kind::SubmitApplication,
                applications,
            )
        },
    ]
}

/// The client tools the borrower UI runs (Python rc8's loan client tools),
/// with the form card and document upload every showcase shares.
#[must_use]
pub fn client_tools() -> Vec<ToolDefinition> {
    let client = |name: &str, description: &str, input_schema: Value| ToolDefinition {
        description: description.into(),
        input_schema,
        ..ToolDefinition::client(name)
    };
    vec![
        client(
            FILL_FORM_FIELDS,
            "Fill known fields in the borrower's visible application form. provenance_source is where each \
             value came from: \"Borrower profile\", \"You provided\" or \"Pay stub\".",
            object(&json!({"fields": {"type": "object"}, "provenance_source": {"type": "string"}}), &["fields"]),
        ),
        client(
            HIGHLIGHT_FIELD,
            "Focus and highlight one field of the form so the borrower sees what needs attention.",
            object(&json!({"field": {"type": "string"}}), &["field"]),
        ),
        client(
            NAVIGATE_STEP,
            "Move the borrower's form to one step: borrower, income or review.",
            object(&json!({"step": {"type": "string"}}), &["step"]),
        ),
        client(
            SHOW_PROGRAM_COMPARISON,
            "Open the program comparison table in the borrower's UI.",
            object(&json!({"program_ids": {"type": "array"}}), &[]),
        ),
    ]
    .into_iter()
    .chain(crate::kit::shared_client_tools())
    .collect()
}

/// Register the server and client tools; returns their names in order.
pub fn register_loan_tools(
    registry: &ToolRegistry,
    applications: &Arc<dyn ApplicationStore>,
) -> Vec<String> {
    let mut names = Vec::new();
    for tool in server_tools(applications).into_iter().chain(client_tools()) {
        names.push(tool.name.clone());
        registry.register(tool);
    }
    names
}

/// `fields` as a JSON object, for tools whose callers pass a map.
#[must_use]
pub fn as_object(value: &Value) -> Map<String, Value> {
    value.as_object().cloned().unwrap_or_default()
}
