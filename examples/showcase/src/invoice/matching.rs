//! The three-way match as plain functions: the invoice against the purchase
//! order, the goods receipt and the vendor, with the AP policy's tolerances.
//! `ap_match` ([`crate::invoice::runtime`]) calls these after it has read the
//! ERP; nothing here does I/O.

use chrono::{Duration, NaiveDate};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// An invoiced unit price may differ from the PO's by this many percent.
pub const PRICE_TOLERANCE_PCT: f64 = 2.0;
/// The invoice total may differ from its lines plus tax by this many dollars.
pub const TOTAL_TOLERANCE: f64 = 5.0;

/// One rule's outcome.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Finding {
    pub rule: String,
    pub value: String,
    pub limit: String,
    pub pass: bool,
}

impl Finding {
    fn new(rule: impl Into<String>, value: impl Into<String>, limit: impl Into<String>, pass: bool) -> Self {
        Self {
            rule: rule.into(),
            value: value.into(),
            limit: limit.into(),
            pass,
        }
    }
}

/// One invoiced line.
#[derive(Debug, Clone, PartialEq)]
pub struct Line {
    pub sku: String,
    pub quantity: f64,
    pub unit_price: f64,
}

/// A number, or a string such as `"$1,436.40"`.
#[must_use]
pub fn num(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|s| s.replace([',', '$'], "").trim().parse().ok()))
}

/// `x` rounded to cents.
#[must_use]
pub fn cents(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}

/// `$1,436.40`-style text for `x`.
#[must_use]
pub fn dollars(x: f64) -> String {
    let whole = format!("{:.2}", x.abs());
    let (int, frac) = whole.split_once('.').unwrap_or((whole.as_str(), "00"));
    let mut grouped = String::new();
    for (i, digit) in int.chars().enumerate() {
        if i > 0 && (int.len() - i) % 3 == 0 {
            grouped.push(',');
        }
        grouped.push(digit);
    }
    format!("{}${grouped}.{frac}", if x < 0.0 { "-" } else { "" })
}

/// A name for comparing vendors and invoice numbers: lowercase letters and
/// digits only.
#[must_use]
pub fn normalize(text: &str) -> String {
    text.chars()
        .filter(char::is_ascii_alphanumeric)
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

/// The invoice's lines.
#[must_use]
pub fn lines(invoice: &Map<String, Value>) -> Vec<Line> {
    invoice
        .get("lines")
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter()
                .map(|row| Line {
                    sku: row.get("sku").and_then(Value::as_str).unwrap_or_default().trim().to_uppercase(),
                    quantity: row.get("quantity").and_then(num).unwrap_or(0.0),
                    unit_price: row.get("unit_price").and_then(num).unwrap_or(0.0),
                })
                .collect()
        })
        .unwrap_or_default()
}

/// The lines' quantity times unit price, plus the invoice's tax.
#[must_use]
pub fn computed_total(invoice: &Map<String, Value>) -> f64 {
    let subtotal: f64 = lines(invoice).iter().map(|l| l.quantity * l.unit_price).sum();
    cents(subtotal + invoice.get("tax").and_then(num).unwrap_or(0.0))
}

fn po_line<'a>(po: &'a Value, sku: &str) -> Option<&'a Value> {
    po["lines"]
        .as_array()?
        .iter()
        .find(|l| l["sku"].as_str().is_some_and(|s| s.eq_ignore_ascii_case(sku)))
}

fn received(receipt: Option<&Value>, sku: &str) -> f64 {
    receipt
        .and_then(|r| r["lines"].as_array())
        .and_then(|rows| {
            rows.iter()
                .find(|l| l["sku"].as_str().is_some_and(|s| s.eq_ignore_ascii_case(sku)))
        })
        .and_then(|l| num(&l["quantity_received"]))
        .unwrap_or(0.0)
}

/// The policy's rules on an invoice: the PO and its vendor, each line's
/// quantity against the receipt and its price against the PO, the total
/// against the lines, and a duplicate check. `duplicates` names the other
/// invoices with the same vendor and invoice number.
#[must_use]
pub fn findings(
    invoice: &Map<String, Value>,
    po: Option<&Value>,
    receipt: Option<&Value>,
    vendor: Option<&Value>,
    duplicates: &[String],
) -> Vec<Finding> {
    let mut out = Vec::new();
    let po_number = invoice.get("po_number").and_then(Value::as_str).unwrap_or("none");
    let billed = invoice.get("vendor_name").and_then(Value::as_str).unwrap_or_default();
    match po {
        None => out.push(Finding::new("purchase order", format!("{po_number} not found"), "an open purchase order", false)),
        Some(po) => {
            out.push(Finding::new("purchase order", po_number, "an open purchase order", true));
            let po_vendor = po["vendor_name"].as_str().unwrap_or_default();
            out.push(Finding::new(
                "vendor matches PO",
                billed,
                format!("the PO's vendor, {po_vendor}"),
                !billed.is_empty() && normalize(billed) == normalize(po_vendor),
            ));
            let approved = vendor.and_then(|v| v["approved"].as_bool());
            out.push(Finding::new(
                "vendor approved",
                match approved {
                    Some(true) => "approved",
                    Some(false) => "not approved",
                    None => "not in the vendor master",
                },
                "an approved vendor",
                approved == Some(true),
            ));
            for line in lines(invoice) {
                let Some(ordered) = po_line(po, &line.sku) else {
                    out.push(Finding::new(format!("line {}", line.sku), "not on the PO", "only ordered items", false));
                    continue;
                };
                let got = received(receipt, &line.sku);
                out.push(Finding::new(
                    format!("quantity {}", line.sku),
                    format!("{} invoiced, {got} received", line.quantity),
                    "no more than received",
                    line.quantity <= got + 1e-9,
                ));
                let price = num(&ordered["unit_price"]).unwrap_or(0.0);
                let off = if price > 0.0 { (line.unit_price - price) / price * 100.0 } else { 100.0 };
                out.push(Finding::new(
                    format!("unit price {}", line.sku),
                    format!("{} ({off:+.1}%)", dollars(line.unit_price)),
                    format!("{} within {PRICE_TOLERANCE_PCT}%", dollars(price)),
                    off.abs() <= PRICE_TOLERANCE_PCT + 1e-9,
                ));
            }
        }
    }
    let total = invoice.get("total").and_then(num).unwrap_or(0.0);
    let computed = computed_total(invoice);
    out.push(Finding::new(
        "invoice total",
        dollars(total),
        format!("within {} of {}", dollars(TOTAL_TOLERANCE), dollars(computed)),
        (total - computed).abs() <= TOTAL_TOLERANCE + 1e-9,
    ));
    out.push(Finding::new(
        "duplicate invoice",
        if duplicates.is_empty() { "none".into() } else { duplicates.join("; ") },
        "no other invoice with this number from the vendor",
        duplicates.is_empty(),
    ));
    out
}

/// What the PO and the receipt support: each line at the lower of the
/// invoiced and received quantity and the lower of the invoiced and PO
/// price, with tax in the invoice's proportion. This is the amount to pay
/// when the controller pays only what matched.
#[must_use]
pub fn matched_amount(invoice: &Map<String, Value>, po: Option<&Value>, receipt: Option<&Value>) -> f64 {
    let Some(po) = po else { return 0.0 };
    let subtotal: f64 = lines(invoice)
        .iter()
        .filter_map(|line| {
            let ordered = po_line(po, &line.sku)?;
            let price = num(&ordered["unit_price"])?;
            Some(line.quantity.min(received(receipt, &line.sku)) * line.unit_price.min(price))
        })
        .sum();
    let billed: f64 = lines(invoice).iter().map(|l| l.quantity * l.unit_price).sum();
    let tax = invoice.get("tax").and_then(num).unwrap_or(0.0);
    let rate = if billed > 0.0 { tax / billed } else { 0.0 };
    cents(subtotal * (1.0 + rate))
}

/// The pay date: `invoice_date` plus `terms_days`, as `YYYY-MM-DD`.
#[must_use]
pub fn pay_date(invoice_date: &str, terms_days: i64) -> Option<String> {
    let date = NaiveDate::parse_from_str(invoice_date.trim(), "%Y-%m-%d").ok()?;
    Some((date + Duration::days(terms_days)).format("%Y-%m-%d").to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::invoice::stub::{goods_receipt, purchase_order, vendor};
    use serde_json::json;

    fn acme() -> Map<String, Value> {
        json!({
            "vendor_name": "Acme Packaging Co.", "invoice_number": "INV-88213", "invoice_date": "2026-10-20",
            "po_number": "PO-4410", "currency": "USD",
            "lines": [
                {"sku": "BX-18", "description": "Shipping boxes", "quantity": 500, "unit_price": 1.20, "amount": 600.0},
                {"sku": "TP-48", "description": "Packing tape", "quantity": 200, "unit_price": 3.65, "amount": 730.0}
            ],
            "subtotal": 1330.0, "tax": 106.40, "total": 1436.40
        })
        .as_object()
        .unwrap()
        .clone()
    }

    fn failed(findings: &[Finding]) -> Vec<&str> {
        findings.iter().filter(|f| !f.pass).map(|f| f.rule.as_str()).collect()
    }

    #[test]
    fn the_acme_invoice_fails_on_price_and_quantity() {
        let found = findings(
            &acme(),
            purchase_order("PO-4410").as_ref(),
            goods_receipt("PO-4410").as_ref(),
            vendor("V-210").as_ref(),
            &[],
        );
        assert_eq!(failed(&found), ["quantity TP-48", "unit price TP-48"]);
        let price = found.iter().find(|f| f.rule == "unit price TP-48").unwrap();
        assert!(price.value.contains("+4.3%"), "{}", price.value);
        let quantity = found.iter().find(|f| f.rule == "quantity TP-48").unwrap();
        assert_eq!(quantity.value, "200 invoiced, 180 received");
    }

    #[test]
    fn the_matched_amount_pays_what_was_received_at_the_po_price() {
        let amount = matched_amount(&acme(), purchase_order("PO-4410").as_ref(), goods_receipt("PO-4410").as_ref());
        // 500 x 1.20 + 180 x 3.50 = 1230.00, plus 8% tax.
        assert!((amount - 1328.40).abs() < 1e-9, "{amount}");
    }

    #[test]
    fn a_clean_invoice_passes_and_a_duplicate_or_a_bad_total_does_not() {
        let delta = json!({
            "vendor_name": "Delta Office Supply", "invoice_number": "INV-5531", "po_number": "PO-4520",
            "lines": [
                {"sku": "PP-500", "quantity": 40, "unit_price": 4.25},
                {"sku": "PN-12", "quantity": 12, "unit_price": 8.00}
            ],
            "tax": 21.28, "total": 287.28
        })
        .as_object()
        .unwrap()
        .clone();
        let po = purchase_order("PO-4520");
        let receipt = goods_receipt("PO-4520");
        let vendor = vendor("V-305");
        let clean = findings(&delta, po.as_ref(), receipt.as_ref(), vendor.as_ref(), &[]);
        assert!(failed(&clean).is_empty(), "{clean:?}");

        let twice = findings(&delta, po.as_ref(), receipt.as_ref(), vendor.as_ref(), &["Delta Office Supply INV-5531".into()]);
        assert_eq!(failed(&twice), ["duplicate invoice"]);

        let mut padded = delta.clone();
        padded.insert("total".into(), json!(295.00));
        let off = findings(&padded, po.as_ref(), receipt.as_ref(), vendor.as_ref(), &[]);
        assert_eq!(failed(&off), ["invoice total"]);
        padded.insert("total".into(), json!("$291.00"));
        assert!(failed(&findings(&padded, po.as_ref(), receipt.as_ref(), vendor.as_ref(), &[])).is_empty(), "$3.72 is within $5");
    }

    #[test]
    fn a_missing_po_a_wrong_vendor_and_an_unlisted_line_are_exceptions() {
        let none = findings(&acme(), None, None, None, &[]);
        assert_eq!(failed(&none), ["purchase order"]);

        let mut other = acme();
        other.insert("vendor_name".into(), json!("Acme Packaging Corp"));
        other.insert("lines".into(), json!([{"sku": "ZZ-1", "quantity": 1, "unit_price": 1.0}]));
        other.insert("total".into(), json!(1.0));
        other.insert("tax".into(), json!(0.0));
        let found = findings(&other, purchase_order("PO-4410").as_ref(), goods_receipt("PO-4410").as_ref(), vendor("V-210").as_ref(), &[]);
        assert_eq!(failed(&found), ["vendor matches PO", "line ZZ-1"]);

        let unapproved = json!({"approved": false});
        let found = findings(&acme(), purchase_order("PO-4410").as_ref(), goods_receipt("PO-4410").as_ref(), Some(&unapproved), &[]);
        assert!(failed(&found).contains(&"vendor approved"));
    }

    #[test]
    fn money_dates_and_names_format_as_the_ui_expects() {
        assert_eq!(dollars(1436.4), "$1,436.40");
        assert_eq!(dollars(5.0), "$5.00");
        assert_eq!(pay_date("2026-10-20", 30).as_deref(), Some("2026-11-19"));
        assert_eq!(pay_date("20 Oct", 30), None);
        assert_eq!(normalize("Acme Packaging Co."), normalize("ACME packaging co"));
        assert_eq!(num(&json!("$1,430.50")), Some(1430.5));
    }
}
