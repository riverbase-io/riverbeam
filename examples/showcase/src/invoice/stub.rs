//! The store's ERP as an MCP server (`/erp/mcp`): vendors, purchase orders
//! and goods receipts, the other two sides of a three-way match.
//!
//! The data is fixed, so runs are repeatable:
//!
//! - V-210 Acme Packaging Co. (Net 30, bank account ending 4417) with
//!   PO-4410: 500 boxes and 200 rolls of tape ordered, 500 boxes and 180
//!   rolls received (GR-4410).
//! - V-305 Delta Office Supply (Net 15) with PO-4520, fully received
//!   (GR-4520), so a clean match exists too.

use serde_json::{json, Value};

use crate::mcp_stub::{required, StubResult, StubServer, StubTool};

/// The vendor `vendor_id`: name, payment terms, the bank account on file
/// (last four digits only) and whether it is approved.
#[must_use]
pub fn vendor(vendor_id: &str) -> Option<Value> {
    match vendor_id.to_uppercase().as_str() {
        "V-210" => Some(json!({
            "vendor_id": "V-210", "name": "Acme Packaging Co.", "terms": "Net 30", "terms_days": 30,
            "bank_account_last4": "4417", "approved": true
        })),
        "V-305" => Some(json!({
            "vendor_id": "V-305", "name": "Delta Office Supply", "terms": "Net 15", "terms_days": 15,
            "bank_account_last4": "9026", "approved": true
        })),
        _ => None,
    }
}

/// The purchase order `po_number`: its vendor and ordered lines.
#[must_use]
pub fn purchase_order(po_number: &str) -> Option<Value> {
    match po_number.to_uppercase().as_str() {
        "PO-4410" => Some(json!({
            "po_number": "PO-4410", "vendor_id": "V-210", "vendor_name": "Acme Packaging Co.",
            "currency": "USD", "ordered_on": "2026-10-05", "status": "open",
            "lines": [
                {"sku": "BX-18", "description": "Shipping boxes, 18 in", "quantity": 500, "unit_price": 1.20},
                {"sku": "TP-48", "description": "Packing tape, 48 mm roll", "quantity": 200, "unit_price": 3.50}
            ]
        })),
        "PO-4520" => Some(json!({
            "po_number": "PO-4520", "vendor_id": "V-305", "vendor_name": "Delta Office Supply",
            "currency": "USD", "ordered_on": "2026-10-01", "status": "open",
            "lines": [
                {"sku": "PP-500", "description": "Copy paper, 500 sheets", "quantity": 40, "unit_price": 4.25},
                {"sku": "PN-12", "description": "Ballpoint pens, box of 12", "quantity": 12, "unit_price": 8.00}
            ]
        })),
        _ => None,
    }
}

/// The goods receipt for the purchase order `po_number`: what the warehouse
/// counted in.
#[must_use]
pub fn goods_receipt(po_number: &str) -> Option<Value> {
    match po_number.to_uppercase().as_str() {
        "PO-4410" => Some(json!({
            "receipt_id": "GR-4410", "po_number": "PO-4410", "received_on": "2026-10-14",
            "lines": [
                {"sku": "BX-18", "quantity_received": 500},
                {"sku": "TP-48", "quantity_received": 180}
            ]
        })),
        "PO-4520" => Some(json!({
            "receipt_id": "GR-4520", "po_number": "PO-4520", "received_on": "2026-10-10",
            "lines": [
                {"sku": "PP-500", "quantity_received": 40},
                {"sku": "PN-12", "quantity_received": 12}
            ]
        })),
        _ => None,
    }
}

fn get_purchase_order(args: &Value) -> StubResult {
    let number = required(args, "po_number")?;
    purchase_order(number).ok_or_else(|| format!("no purchase order {number}"))
}

fn get_goods_receipt(args: &Value) -> StubResult {
    let number = required(args, "po_number")?;
    goods_receipt(number).ok_or_else(|| format!("no goods receipt for purchase order {number}"))
}

fn get_vendor(args: &Value) -> StubResult {
    let id = required(args, "vendor_id")?;
    vendor(id).ok_or_else(|| format!("no vendor {id}"))
}

/// The ERP's tools.
#[must_use]
pub fn erp() -> StubServer {
    let by_po = json!({
        "type": "object",
        "properties": {"po_number": {"type": "string"}},
        "required": ["po_number"]
    });
    StubServer {
        name: "erp",
        tools: vec![
            StubTool {
                name: "get_purchase_order",
                description: "Read a purchase order (vendor, ordered lines with SKU, quantity and unit price) by its number.",
                input_schema: by_po.clone(),
                call: get_purchase_order,
            },
            StubTool {
                name: "get_goods_receipt",
                description: "Read the goods receipt for a purchase order (SKU and quantity received) by the PO number.",
                input_schema: by_po,
                call: get_goods_receipt,
            },
            StubTool {
                name: "get_vendor",
                description: "Read a vendor (name, payment terms, bank account last four digits, approved or not) by vendor id.",
                input_schema: json!({
                    "type": "object",
                    "properties": {"vendor_id": {"type": "string"}},
                    "required": ["vendor_id"]
                }),
                call: get_vendor,
            },
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acme_was_short_shipped_and_delta_is_complete() {
        let po = get_purchase_order(&json!({"po_number": "po-4410"})).unwrap();
        assert_eq!(po["vendor_id"], "V-210");
        assert_eq!(po["lines"][1]["unit_price"], 3.5);
        let receipt = get_goods_receipt(&json!({"po_number": "PO-4410"})).unwrap();
        assert_eq!(receipt["receipt_id"], "GR-4410");
        assert_eq!(receipt["lines"][1]["quantity_received"], 180);
        let delta = goods_receipt("PO-4520").unwrap();
        let po = purchase_order("PO-4520").unwrap();
        for (ordered, received) in po["lines"].as_array().unwrap().iter().zip(delta["lines"].as_array().unwrap()) {
            assert_eq!(ordered["quantity"], received["quantity_received"]);
        }
    }

    #[test]
    fn vendors_carry_terms_and_unknown_ids_are_errors() {
        let acme = get_vendor(&json!({"vendor_id": "V-210"})).unwrap();
        assert_eq!(acme["terms_days"], 30);
        assert_eq!(acme["bank_account_last4"], "4417");
        assert_eq!(acme["approved"], true);
        assert_eq!(get_vendor(&json!({"vendor_id": "V-305"})).unwrap()["name"], "Delta Office Supply");
        assert!(get_vendor(&json!({"vendor_id": "V-999"})).unwrap_err().contains("no vendor"));
        assert!(get_purchase_order(&json!({"po_number": "PO-1"})).unwrap_err().contains("no purchase order"));
        assert!(get_goods_receipt(&json!({})).unwrap_err().contains("required"));
    }
}
