// The AP clerk: the invoice queue with an upload (invoice_reader reads the
// image), and ap_match, which runs the three-way match and, for a clean
// invoice, asks the clerk to approve the payment.

import { useEffect, useMemo, useState } from "react";
import { runOnce, useAgentChat } from "../../lib/chat";
import { ShowcaseRecord, ViewProps, money } from "../../lib/showcase";
import { ChatPanel } from "../../components/ChatPanel";
import { parseReply, readFile } from "../../components/Cards";
import { ChatPlaceholder, Workbench } from "../../components/Workbench";
import { InvoiceDetail, sampleInvoice, useInvoices } from "./shared";

export function Clerk(props: ViewProps) {
  const { invoices, reload } = useInvoices(props.api, props.onStatus);
  const [selected, setSelected] = useState<string | null>(null);
  const [reading, setReading] = useState<string | null>(null);
  const onStatus = props.onStatus;

  const invoice = invoices.find((i) => i.id === selected) ?? null;
  const facts = useMemo(() => (invoice ? [{ key: "invoice_id", value: invoice.id }] : []), [invoice]);
  const chat = useAgentChat({
    api: props.api,
    rtc: props.rtc,
    agent: props.agents.ap_match ?? null,
    sessionKey: `invoice.clerk.${props.persona.user_id}.${selected ?? "none"}`,
    facts,
    onError: onStatus,
  });

  // After each run the findings and the status may have changed.
  useEffect(() => {
    if (!chat.busy) void reload();
  }, [chat.busy, reload]);

  const upload = async (file: File) => {
    const reader = props.agents.invoice_reader;
    if (!reader) return onStatus("The invoice reader agent is not running.");
    try {
      setReading(`Reading ${file.name}…`);
      const { base64 } = await readFile(file);
      const result = await runOnce(props.api, props.rtc, reader, [
        { type: "text", text: "Read this invoice." },
        { type: "image_base64", data: base64, mime_type: file.type || "image/png" },
      ]);
      const created = await props.api.http<ShowcaseRecord>("POST", "/api/invoice/invoices", { file_name: file.name, extracted: parseReply(result) });
      await reload();
      setSelected(created.id);
    } catch (e) {
      onStatus(String((e as Error).message));
    } finally {
      setReading(null);
    }
  };

  return (
    <Workbench
      eyebrow="Invoices"
      records={invoices}
      columns={[
        { label: "Vendor", render: (i) => String(i.data.vendor_name ?? i.title) },
        { label: "Invoice", render: (i) => String(i.data.invoice_number ?? "—") },
        { label: "PO", render: (i) => String(i.data.po_number ?? "—") },
        { label: "Total", render: (i) => money(i.data.total) },
      ]}
      selected={selected}
      onSelect={setSelected}
      empty="No invoices yet. Upload one."
      toolbar={
        <div className="row">
          <label className="muted small">Upload invoice</label>
          <input type="file" accept="image/*" disabled={!!reading} onChange={(e) => e.target.files?.[0] && void upload(e.target.files[0])} />
          <button disabled={!!reading} onClick={() => void sampleInvoice().then(upload)} title="Draw a sample Acme Packaging invoice">
            Use a sample
          </button>
          {reading && <span className="muted small">{reading}</span>}
        </div>
      }
      detail={invoice && <InvoiceDetail invoice={invoice} />}
      chat={
        invoice ? (
          <ChatPanel
            title="Three-way match"
            subtitle="Checks the invoice against the purchase order, goods receipt and vendor. A clean invoice waits for your approval to pay; exceptions go to the controller."
            items={chat.items}
            busy={chat.busy}
            suggestions={["Run the three-way match."]}
            placeholder="Run the match"
            onSend={(t) => void chat.send(t)}
            onRespond={(item, d, label) => void chat.respond(item, d, label)}
            onCancel={() => void chat.cancel()}
            onReset={chat.reset}
          />
        ) : (
          <ChatPlaceholder text="Pick an invoice, or upload one." />
        )
      }
    />
  );
}
