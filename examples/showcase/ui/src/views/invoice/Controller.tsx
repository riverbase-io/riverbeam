// The controller: invoices with exceptions first, and ap_match, which asks
// for the decision (pay, pay the matched amount, reject, hold), a note, and
// the approval to schedule the payment.

import { useEffect, useMemo, useState } from "react";
import { useAgentChat } from "../../lib/chat";
import { ViewProps, money } from "../../lib/showcase";
import { ChatPanel } from "../../components/ChatPanel";
import { ChatPlaceholder, Workbench } from "../../components/Workbench";
import { InvoiceDetail, useInvoices } from "./shared";

export function Controller(props: ViewProps) {
  const { invoices, reload } = useInvoices(props.api, props.onStatus);
  const [selected, setSelected] = useState<string | null>(null);
  const onStatus = props.onStatus;

  // Invoices waiting for a decision come first; the rest keep their order.
  const sorted = useMemo(() => [...invoices].sort((a, b) => Number(b.status === "pending_approval") - Number(a.status === "pending_approval")), [invoices]);
  const invoice = sorted.find((i) => i.id === selected) ?? null;
  const facts = useMemo(() => (invoice ? [{ key: "invoice_id", value: invoice.id }] : []), [invoice]);
  const chat = useAgentChat({
    api: props.api,
    rtc: props.rtc,
    agent: props.agents.ap_match ?? null,
    sessionKey: `invoice.controller.${props.persona.user_id}.${selected ?? "none"}`,
    facts,
    onError: onStatus,
  });

  // After each run the decision and the status may have changed.
  useEffect(() => {
    if (!chat.busy) void reload();
  }, [chat.busy, reload]);

  const waiting = sorted.filter((i) => i.status === "pending_approval").length;

  return (
    <Workbench
      eyebrow={waiting > 0 ? `Invoices (${waiting} waiting for you)` : "Invoices"}
      records={sorted}
      columns={[
        { label: "Vendor", render: (i) => String(i.data.vendor_name ?? i.title) },
        { label: "Invoice", render: (i) => String(i.data.invoice_number ?? "—") },
        { label: "Exceptions", render: (i) => String(i.data.exceptions ?? "—") },
        { label: "Total", render: (i) => money(i.data.total) },
      ]}
      selected={selected}
      onSelect={setSelected}
      empty="No invoices yet. The clerk uploads them."
      detail={invoice && <InvoiceDetail invoice={invoice} />}
      chat={
        invoice ? (
          <ChatPanel
            title="Exceptions and payment"
            subtitle="Shows the findings, asks for your decision and a note, and schedules the payment after you approve it."
            items={chat.items}
            busy={chat.busy}
            suggestions={["Review the exceptions and decide."]}
            placeholder="Review the exceptions"
            onSend={(t) => void chat.send(t)}
            onRespond={(item, d, label) => void chat.respond(item, d, label)}
            onCancel={() => void chat.cancel()}
            onReset={chat.reset}
          />
        ) : (
          <ChatPlaceholder text="Pick an invoice from the list." />
        )
      }
    />
  );
}
