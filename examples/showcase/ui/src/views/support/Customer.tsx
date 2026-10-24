// The support desk's customer: their tickets on the left, the support
// assistant on the right (help center, order lookups, tickets).

import { useCallback, useEffect, useState } from "react";
import { useAgentChat } from "../../lib/chat";
import { ShowcaseRecord, ViewProps, money } from "../../lib/showcase";
import { ChatPanel } from "../../components/ChatPanel";
import { VoiceButton } from "../../components/Voice";
import { Fields, Workbench } from "../../components/Workbench";

const SUGGESTIONS = ["Where is my order ORD-1001?", "My coffee grinder arrived broken. Can I get a refund?", "What is your return policy?", "Show my tickets."];

export function Customer(props: ViewProps) {
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

  const chat = useAgentChat({
    api: props.api,
    rtc: props.rtc,
    agent: props.agents.support_assistant ?? null,
    sessionKey: `support.customer.${props.persona.user_id}`,
    onError: onStatus,
  });

  // A run may have opened a ticket.
  useEffect(() => {
    if (!chat.busy) void reload();
  }, [chat.busy, reload]);

  const ticket = tickets.find((t) => t.id === selected) ?? null;
  const refund = ticket?.data.refund as Record<string, unknown> | undefined;

  return (
    <Workbench
      eyebrow="Your tickets"
      records={tickets}
      columns={[
        { label: "Subject", render: (t) => t.title },
        { label: "Order", render: (t) => String(t.data.order_id ?? "—") },
      ]}
      selected={selected}
      onSelect={setSelected}
      empty="No tickets. Ask the assistant anything; it opens a ticket when an agent is needed."
      detail={
        ticket && (
          <>
            <h3>{ticket.title}</h3>
            <Fields data={ticket.data} labels={[["order_id", "Order"], ["category", "Category"], ["message", "Your message"], ["resolution", "Resolution"]]} />
            {refund && (
              <div className="panel">
                <h3>Refunded {money(refund.amount)}</h3>
                <div className="muted small">
                  Reference {String(refund.reference)} · back on your card in 5–7 business days
                </div>
              </div>
            )}
          </>
        )
      }
      chat={
        <ChatPanel
          title="Northwind support"
          subtitle="Answers from the help center, looks up your orders, and opens a ticket for an agent when needed."
          items={chat.items}
          busy={chat.busy}
          suggestions={SUGGESTIONS}
          placeholder="Ask about an order, a return or a refund"
          onSend={(t) => void chat.send(t)}
          onRespond={(item, d, label) => void chat.respond(item, d, label)}
          onCancel={() => void chat.cancel()}
          onFeedback={(item, thumb) => void chat.feedback(item, thumb)}
          onReset={chat.reset}
          extra={<VoiceButton api={props.api} sessionId={chat.sessionId} agent="support_assistant" onStatus={onStatus} onTurn={chat.reload} />}
        />
      }
    />
  );
}
