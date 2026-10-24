// What the clerk's and the controller's views share: the invoice list, the
// invoice detail with its match findings, and the drawn sample invoice.

import { useCallback, useEffect, useState } from "react";
import { Api, Row } from "../../lib/api";
import { ShowcaseRecord, money } from "../../lib/showcase";
import { drawDocument } from "../../components/Cards";
import { Fields } from "../../components/Workbench";

/** The invoices of accounts payable (`/api/invoice/invoices`), reloaded on demand. */
export function useInvoices(api: Api, onStatus: (s: string) => void) {
  const [invoices, setInvoices] = useState<ShowcaseRecord[]>([]);
  const reload = useCallback(async () => {
    try {
      setInvoices(await api.http<ShowcaseRecord[]>("GET", "/api/invoice/invoices"));
    } catch (e) {
      onStatus(String((e as Error).message));
    }
  }, [api, onStatus]);
  useEffect(() => {
    void reload();
  }, [reload]);
  return { invoices, reload };
}

/** The sample: Acme Packaging's invoice, with a tape price and quantity the ERP will not match. */
export function sampleInvoice(): Promise<File> {
  return drawDocument(
    "sample-invoice.png",
    ["Acme Packaging Co.", "INVOICE INV-88213", "Invoice date 2026-10-20   PO PO-4410   Terms Net 30"],
    [
      ["BX-18 Shipping boxes, 500 @ $1.20", "$600.00"],
      ["TP-48 Packing tape, 200 @ $3.65", "$730.00"],
      ["Subtotal", "$1,330.00"],
      ["Tax", "$106.40"],
      ["Total due", "$1,436.40"],
    ],
  );
}

/** One invoice: its fields, lines, the match findings, the decision and the payment. */
export function InvoiceDetail(props: { invoice: ShowcaseRecord }) {
  const { invoice } = props;
  const lines = (invoice.data.lines as Row[] | undefined) ?? [];
  const findings = (invoice.data.findings as Row[] | undefined) ?? [];
  const decision = invoice.data.decision as Row | undefined;
  const payment = invoice.data.payment as Row | undefined;
  return (
    <>
      <h3>{invoice.title}</h3>
      <Fields
        data={invoice.data}
        labels={[
          ["invoice_date", "Invoice date"],
          ["po_number", "PO"],
          ["terms", "Terms"],
          ["pay_date", "Pay date"],
          ["subtotal", "Subtotal"],
          ["tax", "Tax"],
          ["total", "Total"],
          ["matched_amount", "Matched amount"],
          ["file_name", "File"],
        ]}
        format={(key, value) => (["subtotal", "tax", "total", "matched_amount"].includes(key) && typeof value === "number" ? money(value) : String(value ?? "—"))}
      />
      {lines.length > 0 && (
        <table className="grid">
          <thead>
            <tr>
              <th>SKU</th>
              <th>Description</th>
              <th>Qty</th>
              <th>Unit price</th>
              <th>Amount</th>
            </tr>
          </thead>
          <tbody>
            {lines.map((l, i) => (
              <tr key={`${l.sku}-${i}`}>
                <td>{String(l.sku)}</td>
                <td>{String(l.description ?? "")}</td>
                <td>{String(l.quantity)}</td>
                <td>{money(l.unit_price)}</td>
                <td>{money(l.amount)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {findings.length > 0 && (
        <div className="panel">
          <h3>Three-way match</h3>
          <table className="grid">
            <tbody>
              {findings.map((f) => (
                <tr key={String(f.rule)}>
                  <td>{String(f.rule)}</td>
                  <td>{String(f.value)}</td>
                  <td className="muted">{String(f.limit)}</td>
                  <td className={f.pass ? "" : "status exception"}>{f.pass ? "✓" : "exception"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      {decision && (
        <div className="panel">
          <h3>Controller: {String(decision.label ?? decision.outcome)}</h3>
          {decision.amount ? <div className="muted small">Approved amount {money(decision.amount)}</div> : null}
          {decision.note ? <p>{String(decision.note)}</p> : null}
          {decision.rationale ? <p className="muted">{String(decision.rationale)}</p> : null}
        </div>
      )}
      {invoice.data.hold_reason ? <div className="panel">On hold: {String(invoice.data.hold_reason)}</div> : null}
      {invoice.data.rejection_reason ? <div className="panel">Rejected: {String(invoice.data.rejection_reason)}</div> : null}
      {payment && (
        <div className="panel">
          <h3>Payment scheduled: {money(payment.amount)}</h3>
          <div className="muted small">
            {String(payment.reference)} · pays on {String(payment.pay_date)}
          </div>
        </div>
      )}
    </>
  );
}
