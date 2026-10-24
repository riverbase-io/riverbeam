// What the incident desk's two views share: the incident list and the picked
// incident's detail (fields, timeline, remediation, committed postmortem).

import { useCallback, useEffect, useState } from "react";
import { Row, when } from "../../lib/api";
import { ShowcaseRecord, ViewProps } from "../../lib/showcase";
import { Fields } from "../../components/Workbench";

export type Postmortem = { files: { path: string; body: string }[]; log: string[] };
export type TimelineEntry = { at: string; who: string; entry: string };

/** The incidents, newest first, and a reload for after each agent run. */
export function useIncidents(props: ViewProps) {
  const [incidents, setIncidents] = useState<ShowcaseRecord[]>([]);
  const { api, onStatus } = props;

  const reload = useCallback(async () => {
    try {
      setIncidents(await api.http<ShowcaseRecord[]>("GET", "/api/incident/incidents"));
    } catch (e) {
      onStatus(String((e as Error).message));
    }
  }, [api, onStatus]);

  useEffect(() => {
    void reload();
  }, [reload]);

  return { incidents, reload };
}

/** The committed postmortem of the picked incident; reload it with `busy` going false. */
export function usePostmortem(props: ViewProps, id: string | null, busy: boolean) {
  const [postmortem, setPostmortem] = useState<Postmortem | null>(null);
  const { api } = props;

  useEffect(() => {
    if (!id || busy) return;
    let live = true;
    api
      .http<Postmortem>("GET", `/api/incident/incidents/${id}/postmortem`)
      .then((p) => live && setPostmortem(p))
      .catch(() => live && setPostmortem(null));
    return () => {
      live = false;
    };
  }, [api, id, busy]);

  return id ? postmortem : null;
}

export const service = (i: ShowcaseRecord) => String(i.data.service ?? "—");
export const severity = (i: ShowcaseRecord) => String(i.data.severity ?? "—");

/** The incident's fields, remediation and timeline. */
export function IncidentDetail(props: { incident: ShowcaseRecord }) {
  const { incident } = props;
  const timeline = (incident.data.timeline ?? []) as TimelineEntry[];
  const remediation = incident.data.remediation as Row | null | undefined;
  return (
    <>
      <h3>{incident.title}</h3>
      <Fields
        data={{ ...incident.data, status: incident.status.replace(/_/g, " ") }}
        labels={[
          ["service", "Service"],
          ["severity", "Severity"],
          ["status", "Status"],
          ["summary", "Summary"],
          ["resolution", "Resolution"],
        ]}
      />
      {remediation && (
        <div className="panel">
          <h3>Remediation: {String(remediation.action).replace(/_/g, " ")}</h3>
          <div className="muted small">
            {remediation.to_version ? `Back to ${String(remediation.to_version)} · ` : ""}
            {String(remediation.reason)}
            {remediation.simulated ? " · simulated: nothing was deployed or restarted" : ""}
          </div>
        </div>
      )}
      <div className="panel">
        <h3>Timeline</h3>
        <table className="kv-table">
          <tbody>
            {timeline.map((t, i) => (
              <tr key={i}>
                <td className="muted small">{when(t.at)}</td>
                <td>{t.who}</td>
                <td>{t.entry}</td>
              </tr>
            ))}
            {timeline.length === 0 && (
              <tr>
                <td className="muted">No entries yet.</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </>
  );
}

/** The committed postmortem files and the git log that names the incident. */
export function PostmortemPanel(props: { postmortem: Postmortem | null }) {
  const { postmortem } = props;
  if (!postmortem || postmortem.files.length === 0) return null;
  return (
    <div className="panel">
      <h3>Postmortem</h3>
      <div className="muted small">{postmortem.log.join(" · ")}</div>
      {postmortem.files.map((f) => (
        <details key={f.path} open>
          <summary>{f.path}</summary>
          <pre>{f.body}</pre>
        </details>
      ))}
    </div>
  );
}
