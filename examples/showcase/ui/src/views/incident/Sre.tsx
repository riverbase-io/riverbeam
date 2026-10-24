// The on-call SRE: the incident list, the picked incident, and
// incident_responder, which investigates with monitoring and the runbooks and
// runs the rollbacks and restarts the SRE approves.

import { useEffect, useMemo, useState } from "react";
import { useAgentChat } from "../../lib/chat";
import { ViewProps } from "../../lib/showcase";
import { ChatPanel } from "../../components/ChatPanel";
import { Workbench } from "../../components/Workbench";
import { IncidentDetail, service, severity, useIncidents } from "./Detail";

const SUGGESTIONS = ["Check the active alerts.", "Open an incident for checkout-api and investigate.", "Roll back the bad deploy."];

export function Sre(props: ViewProps) {
  const { incidents, reload } = useIncidents(props);
  const [selected, setSelected] = useState<string | null>(null);
  const incident = incidents.find((i) => i.id === selected) ?? null;
  const facts = useMemo(() => (incident ? [{ key: "incident_id", value: incident.id }] : []), [incident]);
  const chat = useAgentChat({
    api: props.api,
    rtc: props.rtc,
    agent: props.agents.incident_responder ?? null,
    sessionKey: `incident.sre.${props.persona.user_id}.${selected ?? "none"}`,
    facts,
    onError: props.onStatus,
  });

  // After each run the timeline, remediation or status may have changed.
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
      empty="No incidents. Ask the responder to check the active alerts."
      toolbar={
        <button className="ghost" onClick={() => setSelected(null)}>
          New investigation
        </button>
      }
      detail={incident && <IncidentDetail incident={incident} />}
      chat={
        <ChatPanel
          title="Incident responder"
          subtitle={
            incident
              ? "Investigates this incident with monitoring and the runbooks. Rollbacks and restarts run after you approve them."
              : "Reads the alerts and opens an incident. Pick an incident to work on it."
          }
          items={chat.items}
          busy={chat.busy}
          suggestions={SUGGESTIONS}
          placeholder="Check alerts, investigate, roll back or resolve"
          onSend={(t) => void chat.send(t)}
          onRespond={(item, d, label) => void chat.respond(item, d, label)}
          onCancel={() => void chat.cancel()}
          onReset={chat.reset}
        />
      }
    />
  );
}
