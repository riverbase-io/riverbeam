// The policyholder: their claims on the left, the intake assistant on the
// right. The assistant asks for missing facts with a form card (request_input)
// and for the damage estimate with an upload card (request_document) that
// damage_reader reads.

import { useCallback, useEffect, useMemo, useState } from "react";
import { Row } from "../../lib/api";
import { Decision, Item, useAgentChat } from "../../lib/chat";
import { ShowcaseRecord, ViewProps, tabId } from "../../lib/showcase";
import { ChatPanel } from "../../components/ChatPanel";
import { DocumentCard, InputCard, drawDocument } from "../../components/Cards";
import { VoiceButton } from "../../components/Voice";
import { Workbench } from "../../components/Workbench";
import { ClaimDetail } from "./ClaimDetail";

type ActionItem = Extract<Item, { kind: "action" }>;

const SUGGESTIONS = [
  "I was in an accident. Help me file a claim.",
  "What is my collision deductible?",
  "Does my policy cover a rental car?",
  "What happens after I file?",
];
const CLIENT_TOOLS = ["request_input", "request_document"];

/** A sample body-shop estimate, drawn on a canvas (no fixture file needed). */
function sampleEstimate(): Promise<File> {
  return drawDocument(
    "bayside-estimate.png",
    ["Bayside Collision Center", "Repair estimate · 2021 Subaru Outback · Policyholder: Leo Park", "412 Harbor Road, Portland OR 97201"],
    [
      ["Rear bumper cover - replace", "$640.00"],
      ["Rear body panel - repair", "$1,150.00"],
      ["Tail lamp - replace", "$310.00"],
      ["Paint and materials", "$720.00"],
      ["Labor: 9.5 h at $95.00/h", "$902.50"],
      ["TOTAL ESTIMATE", "$3,722.50"],
    ],
  );
}

export function Policyholder(props: ViewProps) {
  const [claims, setClaims] = useState<ShowcaseRecord[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const onStatus = props.onStatus;

  const load = useCallback(() => props.api.http<ShowcaseRecord[]>("GET", "/api/claims/claims"), [props.api]);
  const reload = useCallback(async () => {
    try {
      setClaims(await load());
    } catch (e) {
      onStatus(String((e as Error).message));
    }
  }, [load, onStatus]);

  // The newest draft is the claim the assistant works on.
  const draft = claims.find((c) => c.status === "draft") ?? null;
  const facts = useMemo(() => (draft ? [{ key: "claim_id", value: draft.id }] : []), [draft]);
  const client = useMemo(() => ({ client_id: tabId(), kind: "web", tools: CLIENT_TOOLS }), []);
  const chat = useAgentChat({
    api: props.api,
    rtc: props.rtc,
    agent: props.agents.claims_intake ?? null,
    sessionKey: `claims.policyholder.${props.persona.user_id}`,
    facts,
    client,
    // request_input and request_document wait for the policyholder.
    runClientTool: () => null,
    onError: onStatus,
  });

  // A run may have started, saved or filed a claim (and a held card follows a
  // new item): refresh the list.
  const itemCount = chat.items.length;
  useEffect(() => {
    void reload();
  }, [chat.busy, itemCount, reload]);

  // The draft may have been started in this very run, so ask the server.
  const saveDocument = useCallback(
    async (document: Row) => {
      const current = (await load()).find((c) => c.status === "draft");
      if (!current) throw new Error("There is no draft claim to attach the document to.");
      await props.api.http("POST", `/api/claims/claims/${current.id}/documents`, document);
      void reload();
    },
    [load, reload, props.api],
  );

  const clientCard = (item: ActionItem, respond: (d: Decision, label?: string) => void, disabled: boolean) => {
    const req = item.entry.action_request;
    if (req.kind !== "beam.client_tool") return null;
    const tool = String((req.payload as Row)?.tool_name ?? "");
    const args = ((req.payload as Row)?.args ?? {}) as Row;
    if (tool === "request_input") return <InputCard args={args} disabled={disabled} respond={respond} />;
    if (tool === "request_document")
      return (
        <DocumentCard
          args={args}
          disabled={disabled}
          respond={respond}
          api={props.api}
          rtc={props.rtc}
          reader={props.agents.damage_reader}
          prompt="Read this damage estimate."
          hint="Upload a photo of the damage or the body shop's estimate. It is read automatically."
          sample={sampleEstimate}
          save={saveDocument}
        />
      );
    return null;
  };

  const claim = claims.find((c) => c.id === selected) ?? draft ?? claims[0] ?? null;

  return (
    <Workbench
      eyebrow="Your claims"
      records={claims}
      columns={[
        { label: "Claim", render: (c) => c.title },
        { label: "Date of loss", render: (c) => String(c.data.loss_date ?? "—") },
      ]}
      selected={claim?.id ?? null}
      onSelect={setSelected}
      empty="No claims yet. Tell the assistant what happened and it starts one."
      detail={claim && <ClaimDetail claim={claim} />}
      chat={
        <ChatPanel
          title="Claims assistant"
          subtitle="Takes your first notice of loss, reads your damage estimate, and files the claim."
          items={chat.items}
          busy={chat.busy}
          suggestions={SUGGESTIONS}
          placeholder="Describe what happened, or ask about your coverage"
          onSend={(t) => void chat.send(t)}
          onRespond={(item, d, label) => void chat.respond(item, d, label)}
          onCancel={() => void chat.cancel()}
          onFeedback={(item, thumb) => void chat.feedback(item, thumb)}
          onReset={chat.reset}
          clientCard={clientCard}
          extra={<VoiceButton api={props.api} sessionId={chat.sessionId} agent="claims_intake" onStatus={onStatus} onTurn={chat.reload} facts={facts} client={client} />}
        />
      }
    />
  );
}
