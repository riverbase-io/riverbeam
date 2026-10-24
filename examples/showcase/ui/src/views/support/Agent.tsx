// The support desk's agent: the ticket queue, the picked ticket, and
// ticket_triage, which triages it and issues refunds the agent approves.

import { useCallback, useEffect, useMemo, useState } from "react";
import { Row } from "../../lib/api";
import { useAgentChat } from "../../lib/chat";
import { ShowcaseRecord, ViewProps, money } from "../../lib/showcase";
import { ChatPanel } from "../../components/ChatPanel";
import { ChatPlaceholder, Fields, Workbench } from "../../components/Workbench";

const NAMES: Record<string, string> = {
  "51a00000-0000-4000-8000-000000000011": "Mia Chen",
  "0a400000-0000-4000-8000-000000000013": "Omar Haddad",
};

export function Agent(props: ViewProps) {
  const [tickets, setTickets] = useState<ShowcaseRecord[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const onStatus = props.onStatus;

  const reload = useCallback(async () => {
    try {
      setTickets(await props.api.http<ShowcaseRecord[]>("GET", "/api/support/tickets"));
    } catch (e) {
      onStatus(String((e as Error).message));
    }
  }, [props.api, onStatus]);

  useEffect(() => {
    void reload();
  }, [reload]);

  const ticket = tickets.find((t) => t.id === selected) ?? null;
  const facts = useMemo(() => (ticket ? [{ key: "ticket_id", value: ticket.id }] : []), [ticket]);
  const chat = useAgentChat({
    api: props.api,
    rtc: props.rtc,
    agent: props.agents.ticket_triage ?? null,
    sessionKey: `support.agent.${props.persona.user_id}.${selected ?? "none"}`,
    facts,
    onError: onStatus,
  });

  // After each run the triage, refund or status may have changed.
  useEffect(() => {
    if (!chat.busy) void reload();
  }, [chat.busy, reload]);

  const triage = ticket?.data.triage as Row | undefined;
  const refund = ticket?.data.refund as Row | undefined;

  return (
    <Workbench
      eyebrow="Ticket queue"
      records={tickets}
      columns={[
        { label: "Customer", render: (t) => NAMES[t.owner] ?? t.owner.slice(0, 8) },
        { label: "Subject", render: (t) => t.title },
        { label: "Priority", render: (t) => String((t.data.triage as Row | undefined)?.priority ?? "—") },
      ]}
      selected={selected}
      onSelect={setSelected}
      empty="No tickets yet. A customer opens one through the assistant."
      detail={
        ticket && (
          <>
            <h3>{ticket.title}</h3>
            <Fields data={ticket.data} labels={[["order_id", "Order"], ["category", "Category"], ["channel", "Channel"], ["message", "Message"]]} />
            {triage && (
              <div className="panel">
                <h3>
                  Triage: {String(triage.priority)} · {String(triage.sentiment)}
                </h3>
                <p>{String(triage.summary)}</p>
                <div className="muted small">Draft reply</div>
                <p>{String(triage.draft_reply)}</p>
              </div>
            )}
            {refund && (
              <div className="panel">
                <h3>Refunded {money(refund.amount)}</h3>
                <div className="muted small">
                  {String(refund.reference)} · {String(refund.reason)}
                </div>
              </div>
            )}
          </>
        )
      }
      chat={
        ticket ? (
          <ChatPanel
            title="Ticket triage"
            subtitle="Triages the ticket, drafts the reply, and issues refunds after you approve them."
            items={chat.items}
            busy={chat.busy}
            suggestions={["Triage this ticket.", "Refund the order in full.", "Resolve it with the draft reply."]}
            placeholder="Triage, refund, resolve or escalate"
            onSend={(t) => void chat.send(t)}
            onRespond={(item, d, label) => void chat.respond(item, d, label)}
            onCancel={() => void chat.cancel()}
            onReset={chat.reset}
          />
        ) : (
          <ChatPlaceholder text="Pick a ticket from the queue." />
        )
      }
    />
  );
}
