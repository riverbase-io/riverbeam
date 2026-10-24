// The adjuster: the queue of filed claims, the picked claim with its estimate,
// findings and decision, and claim_review, which applies the coverage rules
// and fraud signals, asks for the decision and the note, and pays after the
// adjuster approves the payout.

import { useCallback, useEffect, useMemo, useState } from "react";
import { useAgentChat } from "../../lib/chat";
import { ShowcaseRecord, ViewProps, money } from "../../lib/showcase";
import { ChatPanel } from "../../components/ChatPanel";
import { ChatPlaceholder, Workbench } from "../../components/Workbench";
import { ClaimDetail } from "./ClaimDetail";

const NAMES: Record<string, string> = {
  "1e000000-0000-4000-8000-0000000000c1": "Leo Park",
  "0a1ba000-0000-4000-8000-0000000000c3": "Nina Alvarez",
};
/** The claims an adjuster works, and the ones already decided. */
const QUEUE = ["filed", "in_review", "approved", "partially_approved", "investigating", "denied", "paid"];

export function Adjuster(props: ViewProps) {
  const [claims, setClaims] = useState<ShowcaseRecord[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const onStatus = props.onStatus;

  const reload = useCallback(async () => {
    try {
      const list = await props.api.http<ShowcaseRecord[]>("GET", "/api/claims/claims");
      setClaims(list.filter((c) => QUEUE.includes(c.status)));
    } catch (e) {
      onStatus(String((e as Error).message));
    }
  }, [props.api, onStatus]);

  useEffect(() => {
    void reload();
  }, [reload]);

  const claim = claims.find((c) => c.id === selected) ?? null;
  const facts = useMemo(() => (claim ? [{ key: "claim_id", value: claim.id }] : []), [claim]);
  const chat = useAgentChat({
    api: props.api,
    rtc: props.rtc,
    agent: props.agents.claim_review ?? null,
    sessionKey: `claims.adjuster.${props.persona.user_id}.${selected ?? "none"}`,
    facts,
    onError: onStatus,
  });

  // After each run the status, the decision and the payout may have changed.
  useEffect(() => {
    if (!chat.busy) void reload();
  }, [chat.busy, reload]);

  const reviewable = claim?.status === "filed" || claim?.status === "in_review";

  return (
    <Workbench
      eyebrow="Claims queue"
      records={claims}
      columns={[
        { label: "Policyholder", render: (c) => NAMES[c.owner] ?? c.owner.slice(0, 8) },
        { label: "Claim", render: (c) => c.title },
        { label: "Estimate", render: (c) => (c.data.estimated_repair_cost === undefined ? "—" : money(c.data.estimated_repair_cost)) },
      ]}
      selected={selected}
      onSelect={setSelected}
      empty="No filed claims yet. A policyholder files one through the intake assistant."
      detail={claim && <ClaimDetail claim={claim} review />}
      chat={
        claim ? (
          <ChatPanel
            title="Claim review"
            subtitle="Applies the coverage rules and fraud signals, asks you to decide exceptions, and pays after you approve."
            items={chat.items}
            busy={chat.busy}
            suggestions={reviewable ? ["Review this claim."] : []}
            placeholder="Review the claim, or add a note"
            onSend={(t) => void chat.send(t)}
            onRespond={(item, d, label) => void chat.respond(item, d, label)}
            onCancel={() => void chat.cancel()}
            onReset={chat.reset}
          />
        ) : (
          <ChatPlaceholder text="Pick a claim from the queue." />
        )
      }
    />
  );
}
