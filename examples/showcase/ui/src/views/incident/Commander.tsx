// The incident commander: the same incident list and detail, the committed
// postmortem, and incident_responder to summarize an incident and write its
// postmortem.

import { useEffect, useMemo, useState } from "react";
import { useAgentChat } from "../../lib/chat";
import { ViewProps } from "../../lib/showcase";
import { ChatPanel } from "../../components/ChatPanel";
import { ChatPlaceholder, Workbench } from "../../components/Workbench";
import { IncidentDetail, PostmortemPanel, service, severity, useIncidents, usePostmortem } from "./Detail";

export function Commander(props: ViewProps) {
  const { incidents, reload } = useIncidents(props);
  const [selected, setSelected] = useState<string | null>(null);
  const incident = incidents.find((i) => i.id === selected) ?? null;
  const facts = useMemo(() => (incident ? [{ key: "incident_id", value: incident.id }] : []), [incident]);
  const chat = useAgentChat({
    api: props.api,
    rtc: props.rtc,
    agent: props.agents.incident_responder ?? null,
    sessionKey: `incident.commander.${props.persona.user_id}.${selected ?? "none"}`,
    facts,
    onError: props.onStatus,
  });
  const postmortem = usePostmortem(props, selected, chat.busy);

  // After each run the status or the postmortem may have changed.
  useEffect(() => {
    if (!chat.busy) void reload();
  }, [chat.busy, reload]);

  return (
    <Workbench
      eyebrow="Incidents"
      records={incidents}
      columns={[
        { label: "Service", render: service },
        { label: "Severity", render: severity },
      ]}
      selected={selected}
      onSelect={setSelected}
      empty="No incidents yet. The on-call SRE opens one from an alert."
      detail={
        incident && (
          <>
            <IncidentDetail incident={incident} />
            <PostmortemPanel postmortem={postmortem} />
          </>
        )
      }
      chat={
        incident ? (
          <ChatPanel
            title="Incident responder"
            subtitle="Summarizes the incident from its timeline and writes the postmortem once it is resolved."
            items={chat.items}
            busy={chat.busy}
            suggestions={["Summarize the incident.", "Write the postmortem."]}
            placeholder="Summarize, resolve or write the postmortem"
            onSend={(t) => void chat.send(t)}
            onRespond={(item, d, label) => void chat.respond(item, d, label)}
            onCancel={() => void chat.cancel()}
            onReset={chat.reset}
          />
        ) : (
          <ChatPlaceholder text="Pick an incident from the list." />
        )
      }
    />
  );
}
