// The admin's desk: agent revisions (canary, validate, activate, retire), the
// smoke eval, feedback, each persona's usage, run traces, the audit trail, and
// the borrower data operations (legal hold, erase). Revision, eval and erase
// commands queue worker tasks; each button waits for its task to finish.

import { ReactNode, useCallback, useEffect, useMemo, useState } from "react";
import { Api, Row, errorText, short, when } from "../../lib/api";
import { RtcChannel } from "../../lib/stream";
import { Application, Loan, Persona, personas as loadPersonas } from "../../lib/loan";

type Tab = "revisions" | "feedback" | "usage" | "traces" | "activity" | "ops";

const TABS: [Tab, string][] = [
  ["revisions", "Revisions & evals"],
  ["feedback", "Feedback"],
  ["usage", "Usage"],
  ["traces", "Traces"],
  ["activity", "Activity"],
  ["ops", "Borrower data"],
];

const AGENTS = ["loan_assistant", "paystub_reader", "underwriting_precheck"];

type Props = { api: Api; onStatus: (s: string) => void; who: (id?: string) => string };

const lower = (v: unknown) => String(v ?? "").toLowerCase();

function pretty(value: unknown): string {
  if (typeof value === "string") {
    try {
      return JSON.stringify(JSON.parse(value), null, 2);
    } catch {
      return value;
    }
  }
  return JSON.stringify(value ?? null, null, 2);
}

/** Run a queued domain command and wait for its worker task. */
async function runTask(api: Api, key: string, resource: string, id: string, payload: Row): Promise<Row> {
  const queued = await api.command<Row>(key, resource, id, payload);
  const task = await api.task(String(queued.task_id));
  if (task.status !== "SUCCEEDED") throw new Error(`${key}: ${errorText(task.error) || task.status}`);
  return task;
}

function Panel(props: { title: string; actions?: ReactNode; children: ReactNode }) {
  return (
    <section className="panel">
      <div className="row between">
        <h3>{props.title}</h3>
        <div className="row">{props.actions}</div>
      </div>
      {props.children}
    </section>
  );
}

export function Admin(props: { api: Api; rtc: RtcChannel; agents: Record<string, Row>; userId: string; onStatus: (s: string) => void }) {
  const [tab, setTab] = useState<Tab>("revisions");
  const [people, setPeople] = useState<Persona[]>([]);
  useEffect(() => {
    loadPersonas()
      .then((p) => setPeople(p.personas))
      .catch(() => setPeople([]));
  }, []);
  const who = useCallback((id?: string) => people.find((p) => p.user_id === id)?.name ?? short(id), [people]);
  const shared: Props = { api: props.api, onStatus: props.onStatus, who };
  return (
    <div className="page">
      <div className="tabs">
        {TABS.map(([key, label]) => (
          <button key={key} className={tab === key ? "on" : ""} onClick={() => setTab(key)}>
            {label}
          </button>
        ))}
      </div>
      {tab === "revisions" && <Revisions {...shared} />}
      {tab === "feedback" && <Feedback {...shared} />}
      {tab === "usage" && <Usage {...shared} />}
      {tab === "traces" && <Traces {...shared} />}
      {tab === "activity" && <Activity {...shared} />}
      {tab === "ops" && <BorrowerData {...shared} people={people} />}
    </div>
  );
}

// --- revisions and evals -----------------------------------------------------

function Revisions(props: Props) {
  const { api, onStatus } = props;
  const loan = useMemo(() => new Loan(api), [api]);
  const [agent, setAgent] = useState(AGENTS[0]);
  const [agentRow, setAgentRow] = useState<Row | null>(null);
  const [revisions, setRevisions] = useState<Row[]>([]);
  const [runs, setRuns] = useState<Row[]>([]);
  const [datasets, setDatasets] = useState<Row[]>([]);
  const [busy, setBusy] = useState<string | null>(null);
  const [note, setNote] = useState<string | null>(null);

  const reload = useCallback(async () => {
    try {
      const [agents, revs, tasks] = await Promise.all([
        api.list("agent", { "name.eq": agent }, { limit: 1 }),
        api.list("agent_revision", { "agent_name.eq": agent }, { limit: 50 }),
        api.list("agent_task", { "task.eq": "run_evaluation" }, { limit: 30 }),
      ]);
      setAgentRow(agents[0] ?? null);
      setRevisions(revs);
      setRuns(tasks);
    } catch (e) {
      onStatus(String((e as Error).message));
    }
  }, [api, agent, onStatus]);

  useEffect(() => {
    void reload();
  }, [reload]);
  useEffect(() => {
    loan
      .evals()
      .then(setDatasets)
      .catch(() => setDatasets([]));
  }, [loan]);

  const act = async (label: string, key: string, payload: Row) => {
    if (!agentRow) return;
    setBusy(label);
    setNote(null);
    try {
      await runTask(api, key, "agent", String(agentRow.id), payload);
      setNote(`${label}: done.`);
    } catch (e) {
      onStatus(String((e as Error).message));
    } finally {
      setBusy(null);
      void reload();
    }
  };

  const version = (id?: string) => revisions.find((r) => r.id === id)?.version ?? short(id);
  const cases: Row[] = datasets.flatMap((d) => (d.cases as Row[]) ?? []);
  const question = (caseId: string) => {
    const c = cases.find((x) => x.id === caseId);
    const part = (c?.input?.content as Row[] | undefined)?.[0];
    return String(part?.text ?? short(caseId));
  };
  const primary = agentRow?.active_revision_id;
  // Activating the canary leaves it named as the canary too: all traffic is the primary's.
  const canary = agentRow?.canary_revision_id !== primary ? agentRow?.canary_revision_id : undefined;
  const share = canary ? Number(agentRow?.canary_percent ?? 0) : 0;
  const ownRuns = runs.filter((t) => revisions.some((r) => r.id === t.result?.agent_revision_id) || !t.result);

  return (
    <>
      <Panel
        title="Revisions"
        actions={
          <select value={agent} onChange={(e) => setAgent(e.target.value)}>
            {AGENTS.map((a) => (
              <option key={a}>{a}</option>
            ))}
          </select>
        }
      >
        <p className="muted small">
          New runs go to the primary revision, or to the canary for its share (by execution). A run stays on the
          revision it started with. An eval runs on the revision it names, whatever the traffic. Validate a draft
          to make its activation wait for a passing eval; a draft activated without it does not wait.
        </p>
        <table className="grid">
          <thead>
            <tr>
              <th>Version</th>
              <th>Prompt</th>
              <th>Status</th>
              <th>Traffic</th>
              <th>Created</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {revisions.map((r) => {
              const isPrimary = r.id === primary;
              const isCanary = r.id === canary && share > 0;
              const retired = lower(r.status) === "retired";
              return (
                <tr key={r.id} className={isPrimary ? "on" : ""}>
                  <td>
                    {r.version} <span className="muted small">{short(r.id)}</span>
                  </td>
                  <td>{r.prompt_revision ?? "—"}</td>
                  <td className="status">{lower(r.status)}</td>
                  <td>{isPrimary ? `primary ${100 - share}%` : isCanary ? `canary ${share}%` : "—"}</td>
                  <td className="muted small">{when(r.created_at)}</td>
                  <td>
                    <div className="row">
                      {!isPrimary && !retired && (
                        <>
                          {lower(r.status) === "draft" && (
                            <button disabled={!!busy} onClick={() => void act(`Validate ${r.version}`, "mark-revision-validating", { revision_id: r.id })}>
                              Validate
                            </button>
                          )}
                          <button disabled={!!busy} onClick={() => void act(`Activate ${r.version}`, "activate-revision", { revision_id: r.id })}>
                            Activate
                          </button>
                        </>
                      )}
                      {datasets.length > 0 && !retired && (
                        <button disabled={!!busy} onClick={() => void act(`Smoke eval of ${r.version}`, "run-evaluation", { dataset_id: datasets[0].id, agent_revision_id: r.id })}>
                          Run smoke eval
                        </button>
                      )}
                      {!isPrimary && ["active", "deprecated"].includes(lower(r.status)) && (
                        <button className="ghost" disabled={!!busy} onClick={() => void act(`Retire ${r.version}`, "retire-revision", { revision_id: r.id })}>
                          Retire
                        </button>
                      )}
                    </div>
                  </td>
                </tr>
              );
            })}
            {revisions.length === 0 && (
              <tr>
                <td colSpan={6} className="muted">
                  No revisions recorded yet.
                </td>
              </tr>
            )}
          </tbody>
        </table>
        <div className="row small">
          {busy && <span className="muted">{busy}…</span>}
          {note && <span className="muted">{note}</span>}
        </div>
      </Panel>

      <Panel title="Eval runs" actions={<button className="ghost" onClick={() => void reload()}>Refresh</button>}>
        {datasets.map((d) => (
          <p key={d.id} className="muted small">
            <b>{d.name}</b> ({((d.cases as Row[]) ?? []).length} cases, the reply must contain the expected text; tools in
            dry run):{" "}
            {((d.cases as Row[]) ?? []).map((c) => `“${question(c.id)}” → ${c.expected?.text}`).join(" · ")}
          </p>
        ))}
        <table className="grid">
          <thead>
            <tr>
              <th>When</th>
              <th>Revision</th>
              <th>Result</th>
              <th>Cases</th>
            </tr>
          </thead>
          <tbody>
            {ownRuns.map((t) => {
              const results = (t.result?.results as Row[]) ?? [];
              const passed = results.filter((r) => r.score?.passed).length;
              return (
                <tr key={t.id}>
                  <td className="muted small">{when(t.finished_at ?? t._created)}</td>
                  <td>{t.result ? version(t.result.agent_revision_id) : "—"}</td>
                  <td className={t.status === "FAILED" ? "risk high" : ""}>
                    {t.status === "SUCCEEDED" ? (results.length && passed === results.length ? "passed" : "failed") : lower(t.status)}
                    {t.status === "SUCCEEDED" && ` (${passed}/${results.length})`}
                    {t.error ? <div className="muted small">{errorText(t.error)}</div> : null}
                  </td>
                  <td className="small">
                    {results.map((r) => (
                      <div key={r.case_id}>
                        {r.score?.passed ? "✓" : "✗"} {question(r.case_id)}
                      </div>
                    ))}
                  </td>
                </tr>
              );
            })}
            {ownRuns.length === 0 && (
              <tr>
                <td colSpan={4} className="muted">
                  No eval runs yet.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </Panel>
    </>
  );
}

// --- feedback -------------------------------------------------------------------

function Feedback(props: Props) {
  const { api, onStatus } = props;
  const [rows, setRows] = useState<Row[]>([]);
  const [revisions, setRevisions] = useState<Row[]>([]);
  const reload = useCallback(async () => {
    try {
      const [feedback, revs] = await Promise.all([api.list("agent_feedback", undefined, { limit: 200 }), api.list("agent_revision", undefined, { limit: 100 })]);
      setRows(feedback);
      setRevisions(revs);
    } catch (e) {
      onStatus(String((e as Error).message));
    }
  }, [api, onStatus]);
  useEffect(() => {
    void reload();
  }, [reload]);
  const label = (id?: string) => {
    const r = revisions.find((x) => x.id === id);
    return r ? `${r.agent_name} ${r.version}` : short(id);
  };
  const tally = new Map<string, { up: number; down: number }>();
  for (const f of rows) {
    const key = label(f.agent_revision_id);
    const t = tally.get(key) ?? { up: 0, down: 0 };
    if (f.thumb === "up") t.up++;
    if (f.thumb === "down") t.down++;
    tally.set(key, t);
  }
  return (
    <Panel title="Feedback" actions={<button className="ghost" onClick={() => void reload()}>Refresh</button>}>
      <div className="row small">
        {[...tally].map(([k, t]) => (
          <span key={k} className="chip" style={{ border: "1px solid var(--line)", padding: "2px 10px" }}>
            {k}: 👍 {t.up} · 👎 {t.down}
          </span>
        ))}
      </div>
      <table className="grid">
        <thead>
          <tr>
            <th>When</th>
            <th>Thumb</th>
            <th>Revision</th>
            <th>Comment</th>
            <th>Execution</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((f) => (
            <tr key={f.id}>
              <td className="muted small">{when(f.created_at)}</td>
              <td>{f.thumb === "up" ? "👍" : f.thumb === "down" ? "👎" : f.rating ?? "—"}</td>
              <td>{label(f.agent_revision_id)}</td>
              <td>{f.comment ?? ""}</td>
              <td className="muted small">{short(f.execution_id)}</td>
            </tr>
          ))}
          {rows.length === 0 && (
            <tr>
              <td colSpan={5} className="muted">
                No feedback yet. Borrowers rate the assistant's replies with 👍 / 👎.
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </Panel>
  );
}

// --- usage ------------------------------------------------------------------------

function Usage(props: Props) {
  const { api, onStatus } = props;
  const [rows, setRows] = useState<Row[]>([]);
  const reload = useCallback(async () => {
    try {
      setRows(await api.http<Row[]>("GET", "/api/loan/usage"));
    } catch (e) {
      onStatus(String((e as Error).message));
    }
  }, [api, onStatus]);
  useEffect(() => {
    void reload();
  }, [reload]);
  return (
    <Panel title="Usage today, per persona" actions={<button className="ghost" onClick={() => void reload()}>Refresh</button>}>
      <table className="grid">
        <thead>
          <tr>
            <th>Persona</th>
            <th>Plan</th>
            <th>Requests</th>
            <th>Tokens</th>
            <th>Limits</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => {
            const u = r.usage ?? {};
            const limits = u.limits ?? {};
            return (
              <tr key={r.persona.user_id}>
                <td>
                  {r.persona.name} <span className="muted small">{r.persona.role}</span>
                </td>
                <td>{String(u.plan ?? r.persona.plan)}</td>
                <td>
                  {String(u.total_requests ?? 0)}
                  {limits.max_requests_per_day ? ` / ${limits.max_requests_per_day}` : ""}
                </td>
                <td>
                  {Number(u.total_tokens ?? 0).toLocaleString()}
                  {limits.max_tokens_per_day ? ` / ${Number(limits.max_tokens_per_day).toLocaleString()}` : ""}
                </td>
                <td className="muted small">{Object.entries(limits).map(([k, v]) => `${k} ${v}`).join(" · ")}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </Panel>
  );
}

// --- traces -----------------------------------------------------------------------

function Traces(props: Props) {
  const { api, onStatus } = props;
  const [runs, setRuns] = useState<Row[]>([]);
  const [selected, setSelected] = useState<Row | null>(null);
  const [steps, setSteps] = useState<Row[]>([]);
  const reload = useCallback(async () => {
    try {
      setRuns(await api.list("agent_run", undefined, { limit: 60 }));
    } catch (e) {
      onStatus(String((e as Error).message));
    }
  }, [api, onStatus]);
  useEffect(() => {
    void reload();
  }, [reload]);
  useEffect(() => {
    if (!selected) return setSteps([]);
    api
      .list("agent_step", { "run_id.eq": selected.id }, { limit: 200 })
      .then((s) => setSteps(s.sort((a, b) => String(a._created).localeCompare(String(b._created)))))
      .catch((e) => onStatus(String(e.message)));
  }, [api, selected, onStatus]);
  const seconds = (r: Row) => (r.started_at && r.ended_at ? `${((Date.parse(r.ended_at) - Date.parse(r.started_at)) / 1000).toFixed(1)}s` : "—");
  return (
    <div className="split" style={{ height: "auto" }}>
      <Panel title="Runs" actions={<button className="ghost" onClick={() => void reload()}>Refresh</button>}>
        <table className="grid">
          <thead>
            <tr>
              <th>Started</th>
              <th>Agent</th>
              <th>Status</th>
              <th>Time</th>
              <th>Trace</th>
            </tr>
          </thead>
          <tbody>
            {runs.map((r) => (
              <tr key={r.id} className={`click ${selected?.id === r.id ? "on" : ""}`} onClick={() => setSelected(r)}>
                <td className="muted small">{when(r.started_at ?? r._created)}</td>
                <td>{r.agent_name}</td>
                <td className="status">{lower(r.status)}</td>
                <td>{seconds(r)}</td>
                <td className="small">
                  <code title={r.trace_id ?? ""}>{r.trace_id ? String(r.trace_id).slice(0, 12) : "—"}</code>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </Panel>
      <Panel title={selected ? `Run ${short(selected.id)} · ${selected.agent_name}` : "Steps"}>
        {!selected && <p className="muted">Pick a run to see its steps.</p>}
        {selected && (
          <>
            <div className="kv">
              <span>trace</span>
              <code>{selected.trace_id ?? "—"}</code>
              <span>exec</span>
              <code>{selected.execution_id}</code>
              <span>session</span>
              <code>{selected.session_id}</code>
              <span>attempt</span>
              <span>
                {selected.attempt_no} {selected.retry_reason ? `(${selected.retry_reason})` : ""}
              </span>
            </div>
            {steps.map((s) => (
              <details key={s.id} className="tool-row">
                <summary>
                  {s.name} · {lower(s.status)}
                </summary>
                <div className="small">input</div>
                <pre>{pretty(s.input)}</pre>
                <div className="small">result</div>
                <pre>{pretty(s.result)}</pre>
              </details>
            ))}
            {steps.length === 0 && <p className="muted small">No steps recorded for this run.</p>}
          </>
        )}
      </Panel>
    </div>
  );
}

// --- activity ---------------------------------------------------------------------

function Activity(props: Props) {
  const { api, onStatus, who } = props;
  const [rows, setRows] = useState<Row[]>([]);
  const reload = useCallback(async () => {
    try {
      setRows(await api.list("agent_event", undefined, { limit: 100 }));
    } catch (e) {
      onStatus(String((e as Error).message));
    }
  }, [api, onStatus]);
  useEffect(() => {
    void reload();
  }, [reload]);
  return (
    <Panel title="Audit trail" actions={<button className="ghost" onClick={() => void reload()}>Refresh</button>}>
      <table className="grid">
        <thead>
          <tr>
            <th>When</th>
            <th>Action</th>
            <th>Agent</th>
            <th>Who</th>
            <th>Status</th>
            <th>Model</th>
            <th>Latency</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((e) => (
            <tr key={e.id} title={String(e.error ?? e.output_preview ?? e.input_preview ?? "")}>
              <td className="muted small">{when(e.recorded_at)}</td>
              <td>{lower(e.action)}</td>
              <td>{e.agent_name ?? "—"}</td>
              <td>{e.user_id ? who(e.user_id) : "—"}</td>
              <td className={e.error ? "risk high" : ""}>{lower(e.status)}</td>
              <td className="small">{e.model_used ?? ""}</td>
              <td>{e.latency_ms != null ? `${e.latency_ms} ms` : ""}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </Panel>
  );
}

// --- borrower data ------------------------------------------------------------------

function BorrowerData(props: Props & { people: Persona[] }) {
  const { api, onStatus } = props;
  const loan = useMemo(() => new Loan(api), [api]);
  const [apps, setApps] = useState<Application[]>([]);
  const [busy, setBusy] = useState<string | null>(null);
  const [notes, setNotes] = useState<Record<string, string>>({});
  const reload = useCallback(async () => {
    try {
      setApps(await loan.applications());
    } catch (e) {
      onStatus(String((e as Error).message));
    }
  }, [loan, onStatus]);
  useEffect(() => {
    void reload();
  }, [reload]);

  const hold = async (app: Application) => {
    try {
      await loan.hold(app.id, !app.held);
      await reload();
    } catch (e) {
      onStatus(String((e as Error).message));
    }
  };

  const erase = async (borrower: Persona) => {
    if (!window.confirm(`Erase ${borrower.name}'s applications and agent data (conversations, memories, runs)? Held applications are kept.`)) return;
    setBusy(borrower.user_id);
    const note = (text: string) => setNotes((n) => ({ ...n, [borrower.user_id]: text }));
    try {
      const out = await loan.erase(borrower.user_id);
      const erased = `${out.erased_applications} application(s) erased`;
      if (Number(out.held_applications) > 0) {
        note(`${erased}; ${out.held_applications} held, so the agent data is kept (legal hold).`);
      } else {
        const task = await runTask(api, "delete-user-agent-data", "agent_session", borrower.user_id, { user_id: borrower.user_id });
        const r = task.result ?? {};
        note(`${erased}; agent data deleted: ${r.sessions ?? 0} session(s), ${r.audit ?? 0} audit event(s), ${Object.values((r.tables as Row) ?? {}).reduce((a: number, b) => a + Number(b), 0)} row(s) in all.`);
      }
    } catch (e) {
      onStatus(String((e as Error).message));
    } finally {
      setBusy(null);
      void reload();
    }
  };

  const borrowers = props.people.filter((p) => p.role === "borrower");
  return (
    <>
      {borrowers.map((b) => {
        const own = apps.filter((a) => a.owner === b.user_id);
        return (
          <Panel
            key={b.user_id}
            title={`${b.name} · ${b.plan} plan`}
            actions={
              <button disabled={busy === b.user_id} onClick={() => void erase(b)}>
                {busy === b.user_id ? "Erasing…" : "Erase borrower's data"}
              </button>
            }
          >
            <table className="grid">
              <thead>
                <tr>
                  <th>Application</th>
                  <th>Program</th>
                  <th>Status</th>
                  <th>Updated</th>
                  <th>Legal hold</th>
                </tr>
              </thead>
              <tbody>
                {own.map((a) => (
                  <tr key={a.id}>
                    <td>{short(a.id)}</td>
                    <td>{a.program_id}</td>
                    <td className={`status ${a.status}`}>{a.status}</td>
                    <td className="muted small">{when(a.updated_at)}</td>
                    <td>
                      <label className="row">
                        <input type="checkbox" checked={a.held} onChange={() => void hold(a)} /> {a.held ? "held" : "no hold"}
                      </label>
                    </td>
                  </tr>
                ))}
                {own.length === 0 && (
                  <tr>
                    <td colSpan={5} className="muted">
                      No applications.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
            {notes[b.user_id] && <p className="muted small">{notes[b.user_id]}</p>}
          </Panel>
        );
      })}
      <p className="muted small">
        A borrower exports their own conversation from the Export button in their chat (the domain lets only a
        session's owner export it).
      </p>
    </>
  );
}
