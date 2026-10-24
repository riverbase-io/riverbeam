//! The store's order system as an MCP server (`/orders/mcp`): orders are
//! looked up by order number and the customer's email, as a support desk
//! verifies a caller.

use serde_json::{json, Value};

use crate::mcp_stub::{required, StubResult, StubServer, StubTool};

/// Every order the stub knows: fixed data, so runs are repeatable.
#[must_use]
pub fn orders_table() -> Vec<Value> {
    vec![
        json!({"order_id": "ORD-1001", "email": "mia@example.com", "placed": "2026-10-14",
            "items": [{"sku": "HP-220", "name": "Arden wireless headphones", "qty": 1, "price": 129.00}],
            "total": 129.00, "status": "shipped", "carrier": "UPS", "tracking": "1Z999AA10123456784"}),
        json!({"order_id": "ORD-1002", "email": "mia@example.com", "placed": "2026-10-02",
            "items": [{"sku": "CG-15", "name": "Burr coffee grinder", "qty": 1, "price": 89.50}],
            "total": 89.50, "status": "delivered", "delivered": "2026-10-08", "carrier": "USPS",
            "tracking": "9400111899223344556677"}),
        json!({"order_id": "ORD-2001", "email": "omar@example.com", "placed": "2026-10-11",
            "items": [{"sku": "JK-77-NVY", "name": "Harbor rain jacket, navy, L", "qty": 1, "price": 149.00}],
            "total": 149.00, "status": "delivered", "delivered": "2026-10-16", "carrier": "FedEx",
            "tracking": "612999AA10NE"}),
    ]
}

/// The order `order_id`, if `email` placed it.
#[must_use]
pub fn order_for(order_id: &str, email: &str) -> Option<Value> {
    orders_table().into_iter().find(|o| {
        o["order_id"].as_str().is_some_and(|id| id.eq_ignore_ascii_case(order_id))
            && o["email"].as_str().is_some_and(|e| e.eq_ignore_ascii_case(email))
    })
}

fn list_orders(args: &Value) -> StubResult {
    let email = required(args, "email")?;
    let orders: Vec<Value> = orders_table()
        .into_iter()
        .filter(|o| o["email"].as_str().is_some_and(|e| e.eq_ignore_ascii_case(email)))
        .map(|o| json!({"order_id": o["order_id"], "placed": o["placed"], "total": o["total"], "status": o["status"]}))
        .collect();
    Ok(json!({"orders": orders}))
}

fn get_order(args: &Value) -> StubResult {
    let id = required(args, "order_id")?;
    let email = required(args, "email")?;
    order_for(id, email).ok_or_else(|| format!("no order {id} for {email}"))
}

fn track_shipment(args: &Value) -> StubResult {
    let order = get_order(args)?;
    let events = match order["order_id"].as_str() {
        Some("ORD-1001") => json!([
            {"at": "2026-10-15 09:12", "where": "Louisville, KY", "event": "Departed UPS hub"},
            {"at": "2026-10-18 17:40", "where": "Portland, OR", "event": "Delayed: weather, new estimate 2026-10-27"},
        ]),
        _ => json!([{"at": order["delivered"], "event": "Delivered, left at front door"}]),
    };
    Ok(json!({"order_id": order["order_id"], "carrier": order["carrier"], "tracking": order["tracking"],
        "status": order["status"], "events": events}))
}

/// The order system's tools.
#[must_use]
pub fn orders() -> StubServer {
    let lookup = json!({
        "type": "object",
        "properties": {"order_id": {"type": "string"}, "email": {"type": "string"}},
        "required": ["order_id", "email"]
    });
    StubServer {
        name: "orders",
        tools: vec![
            StubTool {
                name: "list_orders",
                description: "List a customer's recent orders by their email.",
                input_schema: json!({"type": "object", "properties": {"email": {"type": "string"}}, "required": ["email"]}),
                call: list_orders,
            },
            StubTool {
                name: "get_order",
                description: "Read one order (items, total, status, carrier) by order number and the customer's email.",
                input_schema: lookup.clone(),
                call: get_order,
            },
            StubTool {
                name: "track_shipment",
                description: "The carrier's tracking events for an order, by order number and the customer's email.",
                input_schema: lookup,
                call: track_shipment,
            },
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_order_needs_the_email_that_placed_it() {
        assert!(order_for("ORD-1002", "mia@example.com").is_some());
        assert!(order_for("ord-1002", "MIA@example.com").is_some());
        assert!(order_for("ORD-1002", "omar@example.com").is_none());
        let err = get_order(&json!({"order_id": "ORD-2001", "email": "mia@example.com"})).unwrap_err();
        assert!(err.contains("no order"), "{err}");
        let tracked = track_shipment(&json!({"order_id": "ORD-1001", "email": "mia@example.com"})).unwrap();
        assert!(tracked["events"].to_string().contains("Delayed"));
    }
}
