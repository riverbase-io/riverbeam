//! The loan desk's outside services, as MCP servers (`showcase-mcp`): deterministic
//! stand-ins for a pricing engine and a credit bureau.
//!
//! - `/rates/mcp`: `get_rate_sheet`.
//! - `/credit/mcp`: `pull_report`, refused without the bureau key header
//!   (`x-bureau-key` = `LOAN_DESK_BUREAU_KEY`), which the worker sends through
//!   its MCP config (`headers_env`).
//!
//! They speak JSON-RPC over POST, the way `beam_tools::mcp` calls a server.

use std::sync::Arc;

use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{json, Value};

use crate::loan_desk::personas::{BOB, DANA};

/// The header the credit server checks.
pub const BUREAU_KEY_HEADER: &str = "x-bureau-key";

#[derive(Clone)]
struct Stub {
    bureau_key: Arc<String>,
}

/// The stub's routes. `bureau_key` is what `/credit/mcp` expects.
pub fn router(bureau_key: impl Into<String>) -> Router {
    let stub = Stub {
        bureau_key: Arc::new(bureau_key.into()),
    };
    Router::new()
        .route("/rates/mcp", post(rates))
        .route("/credit/mcp", post(credit))
        .with_state(stub)
}

fn reply(id: &Value, result: &Value) -> Response {
    Json(json!({"jsonrpc": "2.0", "id": id, "result": result})).into_response()
}

fn text(value: &Value) -> Value {
    json!({"content": [{"type": "text", "text": value.to_string()}], "isError": false})
}

fn failure(message: &str) -> Value {
    json!({"content": [{"type": "text", "text": message}], "isError": true})
}

/// `initialize`, `notifications/*` and `tools/list`, shared by both servers.
fn protocol(body: &Value, name: &str, tools: &Value) -> Option<Response> {
    let id = body["id"].clone();
    match body["method"].as_str().unwrap_or_default() {
        "initialize" => {
            let result = json!({
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": name, "version": "1"}
            });
            Some(([("mcp-session-id", "loan-desk")], reply(&id, &result)).into_response())
        }
        method if method.starts_with("notifications/") => {
            Some(axum::http::StatusCode::ACCEPTED.into_response())
        }
        "tools/list" => Some(reply(&id, &json!({ "tools": tools }))),
        _ => None,
    }
}

async fn rates(Json(body): Json<Value>) -> Response {
    let tools = json!([{
        "name": "get_rate_sheet",
        "description": "Today's indicative 30-year fixed rates by program (APR and points). Not a rate lock.",
        "inputSchema": {"type": "object", "properties": {"program_id": {"type": "string"}}}
    }]);
    if let Some(response) = protocol(&body, "rates", &tools) {
        return response;
    }
    let program = body["params"]["arguments"]["program_id"].as_str();
    let sheet = rate_sheet();
    let result = match program {
        Some(id) => sheet
            .iter()
            .find(|r| r["program_id"] == id)
            .map_or_else(|| failure(&format!("unknown program {id}")), text),
        None => text(&Value::Array(sheet)),
    };
    reply(&body["id"], &result)
}

/// The rate sheet: fixed numbers, so runs are repeatable.
#[must_use]
pub fn rate_sheet() -> Vec<Value> {
    vec![
        json!({"program_id": "conventional", "rate": 6.375, "apr": 6.52, "points": 0.5}),
        json!({"program_id": "fha", "rate": 6.125, "apr": 6.98, "points": 0.0}),
        json!({"program_id": "va", "rate": 5.875, "apr": 6.04, "points": 0.0}),
        json!({"program_id": "jumbo", "rate": 6.625, "apr": 6.71, "points": 0.25}),
        json!({"as_of": "today", "note": "Indicative only; not a rate lock."}),
    ]
}

async fn credit(State(stub): State<Stub>, headers: HeaderMap, Json(body): Json<Value>) -> Response {
    let tools = json!([{
        "name": "pull_report",
        "description": "Soft-pull a borrower's credit report: score, open monthly debts, derogatory marks.",
        "inputSchema": {
            "type": "object",
            "properties": {"borrower_id": {"type": "string"}},
            "required": ["borrower_id"]
        }
    }]);
    if let Some(response) = protocol(&body, "credit", &tools) {
        return response;
    }
    let key = headers
        .get(BUREAU_KEY_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    if stub.bureau_key.is_empty() || key != stub.bureau_key.as_str() {
        return reply(&body["id"], &failure("bureau key missing or wrong"));
    }
    let borrower = body["params"]["arguments"]["borrower_id"]
        .as_str()
        .unwrap_or_default();
    reply(&body["id"], &text(&credit_report(borrower)))
}

/// A borrower's credit report. Bob is near the FHA floor with a late
/// payment; Dana is clean; anyone else is thin-file.
#[must_use]
pub fn credit_report(borrower_id: &str) -> Value {
    match borrower_id {
        BOB => json!({"borrower_id": borrower_id, "score": 641, "monthly_debts": 820,
            "derogatory": ["30-day late payment, 14 months ago"], "inquiries_12m": 3}),
        DANA => json!({"borrower_id": borrower_id, "score": 748, "monthly_debts": 410,
            "derogatory": [], "inquiries_12m": 1}),
        _ => json!({"borrower_id": borrower_id, "score": null, "monthly_debts": 0,
            "derogatory": [], "inquiries_12m": 0, "note": "thin file: no score"}),
    }
}
