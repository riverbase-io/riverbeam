import { useCallback, useEffect, useMemo, useState } from "react";
import { Api, Row, when } from "../../lib/api";
import { RtcChannel } from "../../lib/stream";
import { useAgentChat } from "../../lib/chat";
import { Application, FORM, Loan, money } from "../../lib/loan";
import { ChatPanel } from "../../components/ChatPanel";

const QUEUE = ["submitted", "precheck", "approved", "referred", "declined"];

function borrowerName(app: Application): string {
  const f = app.fields;
  return [f.first_name, f.last_name].filter(Boolean).join(" ") || app.owner.slice(0, 8);
}

export function Officer(props: { api: Api; rtc: RtcChannel; agents: Record<string, Row>; userId: string; onStatus: (s: string) => void }) {
  const loan = useMemo(() => new Loan(props.api), [props.api]);
  const [apps, setApps] = useState<Application[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [packet, setPacket] = useState<{ files: { path: string; body: string }[]; log: string[] } | null>(null);
  const onStatus = props.onStatus;

  const reload = useCallback(async () => {
    try {
      const list = await loan.applications();
      setApps(list.filter((a) => QUEUE.includes(a.status)));
    } catch (e) {
      onStatus(String((e as Error).message));
    }
  }, [loan, onStatus]);

  useEffect(() => {
    void reload();
  }, [reload]);

  const app = apps.find((a) => a.id === selected) ?? null;
  const facts = useMemo(() => (app ? [{ key: "application_id", value: app.id }] : []), [app]);
  const chat = useAgentChat({
    api: props.api,
    rtc: props.rtc,
    agent: props.agents.underwriting_precheck ?? null,
    sessionKey: `officer.${props.userId}.${selected ?? "none"}`,
    facts,
    onError: onStatus,
  });

  const loadPacket = useCallback(async () => {
    if (!selected) return setPacket(null);
    try {
      setPacket(await loan.packet(selected));
    } catch {
      setPacket(null);
    }
  }, [loan, selected]);

  // After each run: the status, the decision and the packet may have changed.
  useEffect(() => {
    if (!chat.busy) {
      void reload();
      void loadPacket();
    }
  }, [chat.busy, reload, loadPacket]);

  return (
    <div className="split">
      <section className="application">
        <div className="eyebrow">Underwriting queue</div>
        <table className="grid">
          <thead>
            <tr>
              <th>Borrower</th>
              <th>Program</th>
              <th>Status</th>
              <th>Updated</th>
            </tr>
          </thead>
          <tbody>
            {apps.map((a) => (
              <tr key={a.id} className={`click ${a.id === selected ? "on" : ""}`} onClick={() => setSelected(a.id)}>
                <td>{borrowerName(a)}</td>
                <td>{a.program_id}</td>
                <td className={`status ${a.status}`}>{a.status}</td>
                <td className="muted small">{when(a.updated_at)}</td>
              </tr>
            ))}
            {apps.length === 0 && (
              <tr>
                <td colSpan={4} className="muted">
                  No submitted applications yet. A borrower submits one first.
                </td>
              </tr>
            )}
          </tbody>
        </table>
        {app && (
          <>
            <h3>{borrowerName(app)}</h3>
            <table className="kv-table">
              <tbody>
                {FORM.flatMap((s) => s.fields).map((f) => (
                  <tr key={f.name}>
                    <td>{f.label}</td>
                    <td>{f.type === "number" ? money(app.fields[f.name]) : String(app.fields[f.name] ?? "—")}</td>
                  </tr>
                ))}
                <tr>
                  <td>Documents</td>
                  <td>{app.documents.map((d) => String(d.file_name)).join(", ") || "—"}</td>
                </tr>
              </tbody>
            </table>
            {app.decision && (
              <div className="panel">
                <h3>Decision: {String(app.decision.outcome)}</h3>
                {app.decision.note ? <p className="muted">{String(app.decision.note)}</p> : null}
                <table className="grid">
                  <tbody>
                    {((app.decision.findings as Row[]) ?? []).map((f) => (
                      <tr key={String(f.rule)}>
                        <td>{String(f.rule)}</td>
                        <td>{String(f.value)}</td>
                        <td className="muted">{String(f.limit)}</td>
                        <td>{f.pass ? "✓" : "exception"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
            {packet && packet.files.length > 0 && (
              <div className="panel">
                <h3>Packet</h3>
                <div className="muted small">{packet.log.join(" · ")}</div>
                {packet.files.map((f) => (
                  <details key={f.path}>
                    <summary>{f.path}</summary>
                    <pre>{f.body}</pre>
                  </details>
                ))}
              </div>
            )}
          </>
        )}
      </section>
      {app ? (
        <ChatPanel
          title="Underwriting pre-check"
          subtitle="Pulls credit, applies the program rules, asks you on exceptions, and commits the packet after you approve."
          items={chat.items}
          busy={chat.busy}
          suggestions={app.status === "submitted" || app.status === "precheck" ? ["Run the pre-check."] : []}
          placeholder="Run the pre-check, or add a note"
          onSend={(t) => void chat.send(t)}
          onRespond={(item, d, label) => void chat.respond(item, d, label)}
          onCancel={() => void chat.cancel()}
          onReset={chat.reset}
        />
      ) : (
        <section className="chat">
          <div className="empty muted">Pick an application from the queue.</div>
        </section>
      )}
    </div>
  );
}
