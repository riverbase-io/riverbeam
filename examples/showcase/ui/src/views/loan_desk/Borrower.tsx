import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Api, ActionEntry, Row } from "../../lib/api";
import { RtcChannel } from "../../lib/stream";
import { ClientAnswer, Decision, Item, clientDecision, runOnce, useAgentChat } from "../../lib/chat";
import { Application, BORROWER_CLIENT_TOOLS, FORM, Loan, Program, money } from "../../lib/loan";
import { ChatPanel } from "../../components/ChatPanel";
import { VoiceButton } from "../../components/Voice";
import { InputCard, readFile, parseReply } from "../../components/Cards";

type Step = "borrower" | "income" | "review";
type ActionItem = Extract<Item, { kind: "action" }>;

const SUGGESTIONS = [
  "Which program fits me best?",
  "Help me fill in my application.",
  "What are today's rates?",
  "What documents do I need?",
];

function tabId(): string {
  try {
    const known = sessionStorage.getItem("loan-desk.client");
    if (known) return known;
    const id = `tab-${crypto.randomUUID()}`;
    sessionStorage.setItem("loan-desk.client", id);
    return id;
  } catch {
    return `tab-${crypto.randomUUID()}`;
  }
}

export function Borrower(props: { api: Api; rtc: RtcChannel; agents: Record<string, Row>; userId: string; onStatus: (s: string) => void }) {
  const loan = useMemo(() => new Loan(props.api), [props.api]);
  const [apps, setApps] = useState<Application[]>([]);
  const [app, setApp] = useState<Application | null>(null);
  const [programs, setPrograms] = useState<Program[]>([]);
  const [form, setForm] = useState<Row>({});
  const [sources, setSources] = useState<Record<string, string>>({});
  const [step, setStep] = useState<Step>("borrower");
  const [highlight, setHighlight] = useState<string | null>(null);
  const [compare, setCompare] = useState<string[] | null>(null);
  const appRef = useRef(app);
  appRef.current = app;

  const reload = useCallback(async () => {
    const list = await loan.applications();
    setApps(list);
    const current = list.find((a) => a.id === appRef.current?.id) ?? list[0] ?? null;
    setApp(current);
    if (current) setForm(current.fields);
  }, [loan]);

  const onStatus = props.onStatus;
  useEffect(() => {
    void reload().catch((e) => onStatus(String(e.message)));
    void loan.programs().then(setPrograms).catch(() => {});
  }, [reload, loan, onStatus]);

  const save = useCallback(
    async (fields: Row) => {
      const current = appRef.current;
      if (!current || current.status !== "draft") return;
      try {
        const saved = await loan.update(current.id, fields);
        setApp(saved);
      } catch (e) {
        onStatus(String((e as Error).message));
      }
    },
    [loan, onStatus],
  );

  // Client tools the assistant runs in this tab.
  const runClientTool = useCallback(
    (tool: string, args: Row): ClientAnswer | null => {
      switch (tool) {
        case "fill_form_fields": {
          const fields = (args.fields ?? {}) as Row;
          const source = String(args.provenance_source ?? "Assistant");
          setForm((f) => ({ ...f, ...fields }));
          setSources((s) => ({ ...s, ...Object.fromEntries(Object.keys(fields).map((k) => [k, source])) }));
          void save(fields);
          return { output: { filled: Object.keys(fields) } };
        }
        case "highlight_field":
          setHighlight(String(args.field ?? ""));
          setTimeout(() => setHighlight(null), 4000);
          return { output: { highlighted: args.field } };
        case "navigate_step": {
          const target = String(args.step ?? "");
          if (!["borrower", "income", "review"].includes(target)) return { error: `unknown step ${target}` };
          setStep(target as Step);
          return { output: { step: target } };
        }
        case "show_program_comparison":
          setCompare(Array.isArray(args.program_ids) && args.program_ids.length ? (args.program_ids as string[]) : programs.map((p) => p.id));
          return { output: { shown: true } };
        default:
          return null; // request_input and request_document wait for the borrower.
      }
    },
    [save, programs],
  );

  const facts = useMemo(() => (app ? [{ key: "application_id", value: app.id }, { key: "step", value: step }] : []), [app, step]);
  const client = useMemo(() => ({ client_id: tabId(), kind: "web", tools: BORROWER_CLIENT_TOOLS }), []);
  const chat = useAgentChat({
    api: props.api,
    rtc: props.rtc,
    agent: props.agents.loan_assistant ?? null,
    sessionKey: `borrower.${props.userId}.${app?.id ?? "none"}`,
    facts,
    client,
    runClientTool,
    onError: props.onStatus,
  });

  // A run that ended may have saved or submitted: refresh the application.
  useEffect(() => {
    if (!chat.busy) void reload().catch(() => {});
  }, [chat.busy, reload]);

  const clientCard = (item: ActionItem, respond: (d: Decision, label?: string) => void, disabled: boolean) => {
    const req = item.entry.action_request;
    if (req.kind !== "beam.client_tool") return null;
    const tool = String((req.payload as Row)?.tool_name ?? "");
    const args = ((req.payload as Row)?.args ?? {}) as Row;
    if (tool === "request_input") return <InputCard args={args} disabled={disabled} respond={respond} />;
    if (tool === "request_document")
      return <DocumentCard args={args} entry={item.entry} disabled={disabled} respond={respond} api={props.api} rtc={props.rtc} agents={props.agents} loan={loan} app={app} />;
    return null;
  };

  if (!app) {
    return (
      <div className="empty">
        <p>You have no application yet.</p>
        <button className="primary" onClick={() => void loan.create("conventional").then(() => reload())}>
          Start an application
        </button>
      </div>
    );
  }
  const program = programs.find((p) => p.id === app.program_id);
  const frozen = app.status !== "draft";
  return (
    <div className="split">
      <section className="application">
        <header className="app-head">
          <div>
            <div className="eyebrow">Mortgage application</div>
            <h2>{program?.name ?? app.program_id}</h2>
            <div className="muted small">
              {apps.length > 1 ? `${apps.length} applications · ` : ""}
              status <b className={`status ${app.status}`}>{app.status}</b>
              {app.missing_fields.length > 0 && !frozen ? ` · ${app.missing_fields.length} fields missing` : ""}
              {!app.has_pay_stub && !frozen ? " · pay stub missing" : ""}
            </div>
          </div>
          <select value={app.program_id} disabled={frozen} onChange={(e) => void save({ program_id: e.target.value })}>
            {programs.map((p) => (
              <option key={p.id} value={p.id}>
                {p.name}
              </option>
            ))}
          </select>
        </header>
        <nav className="steps">
          {FORM.map((s, i) => (
            <button key={s.step} className={step === s.step ? "on" : ""} onClick={() => setStep(s.step)}>
              {i + 1}. {s.title}
            </button>
          ))}
        </nav>
        {FORM.filter((s) => s.step === step).map((s) =>
          s.step === "review" ? (
            <Review key="review" app={app} form={form} sources={sources} />
          ) : (
            <div key={s.step} className="fields">
              {s.fields.map((f) => (
                <label key={f.name} className={highlight === f.name ? "highlight" : ""}>
                  <span>
                    {f.label}
                    {sources[f.name] && <em className="source">{sources[f.name]}</em>}
                  </span>
                  <input
                    type={f.type ?? "text"}
                    disabled={frozen}
                    value={form[f.name] ?? ""}
                    onChange={(e) => setForm((v) => ({ ...v, [f.name]: f.type === "number" && e.target.value !== "" ? Number(e.target.value) : e.target.value }))}
                    onBlur={(e) => {
                      if (String(app.fields[f.name] ?? "") !== e.target.value) {
                        setSources((x) => ({ ...x, [f.name]: "You typed" }));
                        void save({ [f.name]: f.type === "number" && e.target.value !== "" ? Number(e.target.value) : e.target.value });
                      }
                    }}
                  />
                </label>
              ))}
            </div>
          ),
        )}
        {app.documents.length > 0 && (
          <div className="docs">
            <div className="eyebrow">Documents</div>
            {app.documents.map((d, i) => (
              <div key={i} className="doc">
                📄 {String(d.file_name)} <span className="muted small">{String(d.document_type ?? "")}</span>
                {d.extracted ? <span className="muted small"> · read: {money((d.extracted as Row).gross_monthly_income)}/month</span> : null}
              </div>
            ))}
          </div>
        )}
        {compare && <Comparison programs={programs.filter((p) => compare.includes(p.id))} onClose={() => setCompare(null)} />}
      </section>
      <ChatPanel
        title="Loan assistant"
        subtitle="Fills your form, explains the programs, and asks before submitting."
        items={chat.items}
        busy={chat.busy}
        suggestions={SUGGESTIONS}
        placeholder="Ask about programs, rates, or your application"
        onSend={(t) => void chat.send(t)}
        onRespond={(item, d, label) => void chat.respond(item, d, label)}
        onCancel={() => void chat.cancel()}
        onFeedback={(item, thumb) => void chat.feedback(item, thumb)}
        onReset={chat.reset}
        clientCard={clientCard}
        extra={<VoiceButton api={props.api} sessionId={chat.sessionId} agent="loan_assistant" onStatus={props.onStatus} onTurn={chat.reload} facts={facts} client={client} />}
      />
    </div>
  );
}

function Review(props: { app: Application; form: Row; sources: Record<string, string> }) {
  const income = Number(props.form.gross_monthly_income) || 0;
  const debts = Number(props.form.monthly_debts) || 0;
  const price = Number(props.form.purchase_price) || 0;
  const down = Number(props.form.down_payment) || 0;
  const ltv = price ? Math.round(((price - down) / price) * 1000) / 10 : null;
  return (
    <div className="review">
      <table className="kv-table">
        <tbody>
          {FORM.flatMap((s) => s.fields).map((f) => (
            <tr key={f.name} className={props.app.missing_fields.includes(f.name) ? "missing" : ""}>
              <td>{f.label}</td>
              <td>{f.type === "number" ? money(props.form[f.name]) : String(props.form[f.name] ?? "—")}</td>
              <td className="muted small">{props.sources[f.name] ?? ""}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="muted small">
        Loan-to-value {ltv ?? "—"}% · other debts {income ? Math.round((debts / income) * 100) : "—"}% of income. Ask the assistant to submit when
        everything is in; you approve the submission yourself.
      </p>
    </div>
  );
}

function Comparison(props: { programs: Program[]; onClose: () => void }) {
  return (
    <div className="comparison">
      <div className="row between">
        <div className="eyebrow">Program comparison</div>
        <button className="ghost" onClick={props.onClose}>
          Close
        </button>
      </div>
      <table className="grid">
        <thead>
          <tr>
            <th>Program</th>
            <th>Min score</th>
            <th>Min down</th>
            <th>Max DTI</th>
            <th>Max loan</th>
          </tr>
        </thead>
        <tbody>
          {props.programs.map((p) => (
            <tr key={p.id}>
              <td title={p.summary}>{p.name}</td>
              <td>{p.min_credit_score}</td>
              <td>{p.min_down_payment_pct}%</td>
              <td>{p.max_dti_pct}%</td>
              <td>{money(p.max_loan)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/** A sample biweekly pay stub, drawn on a canvas (no fixture file needed). */
async function samplePayStub(): Promise<File> {
  const canvas = document.createElement("canvas");
  canvas.width = 640;
  canvas.height = 400;
  const g = canvas.getContext("2d")!;
  g.fillStyle = "#fff";
  g.fillRect(0, 0, 640, 400);
  g.fillStyle = "#111";
  g.font = "bold 22px Arial";
  g.fillText("RIVERSIDE LOGISTICS INC.", 24, 40);
  g.font = "14px Arial";
  g.fillText("1200 Harbor Way, Portland OR 97201", 24, 62);
  g.font = "bold 18px Arial";
  g.fillText("EARNINGS STATEMENT", 400, 40);
  g.font = "15px Arial";
  const rows = [
    ["Employee", "Bob Tran"],
    ["Pay frequency", "Biweekly"],
    ["Pay period", "2026-09-28 to 2026-10-11"],
    ["Pay date", "2026-10-16"],
    ["Gross pay (this period)", "$2,861.54"],
    ["Federal tax", "-$343.38"],
    ["Social Security", "-$177.42"],
    ["Medicare", "-$41.49"],
    ["Net pay", "$2,299.25"],
    ["YTD gross", "$57,230.80"],
  ];
  rows.forEach(([k, v], i) => {
    g.fillText(k, 24, 110 + i * 27);
    g.fillText(v, 330, 110 + i * 27);
  });
  g.strokeRect(10, 80, 620, 300);
  const blob = await new Promise<Blob>((resolve) => canvas.toBlob((b) => resolve(b!), "image/png"));
  return new File([blob], "sample-paystub.png", { type: "image/png" });
}

/**
 * request_document: the borrower picks an image; paystub_reader reads it
 * (vision, structured output); the document is saved on the application and
 * what was read goes back to the assistant.
 */
function DocumentCard(props: {
  args: Row;
  entry: ActionEntry;
  disabled: boolean;
  respond: (d: Decision, label?: string) => void;
  api: Api;
  rtc: RtcChannel;
  agents: Record<string, Row>;
  loan: Loan;
  app: Application | null;
}) {
  const [status, setStatus] = useState<string | null>(null);
  const upload = async (file: File) => {
    const reader = props.agents.paystub_reader;
    if (!reader || !props.app) return;
    try {
      setStatus("Reading the document…");
      const { base64 } = await readFile(file);
      const result = await runOnce(props.api, props.rtc, reader, [
        { type: "text", text: "Read this pay stub." },
        { type: "image_base64", data: base64, mime_type: file.type || "image/png" },
      ]);
      const extracted = parseReply(result);
      const document = { file_name: file.name, document_type: String(props.args.document_type ?? "pay_stub"), extracted };
      await props.loan.addDocument(props.app.id, document);
      setStatus(null);
      props.respond(clientDecision({ output: document }), `uploaded ${file.name}`);
    } catch (e) {
      setStatus(String((e as Error).message));
    }
  };
  return (
    <>
      <div className="card-head">
        <span className="pause">📄</span>
        <div>
          <div className="title">{String(props.args.title ?? "Upload a document")}</div>
          <div className="muted small">An image of your most recent pay stub. It is read automatically.</div>
        </div>
      </div>
      <div className="row">
        <input type="file" accept="image/*" disabled={props.disabled || !!status} onChange={(e) => e.target.files?.[0] && void upload(e.target.files[0])} />
        <button disabled={props.disabled || !!status} onClick={() => void samplePayStub().then(upload)} title="Draw a sample pay stub for this borrower">
          Use a sample
        </button>
        <button className="ghost" disabled={props.disabled} onClick={() => props.respond({ status: "cancelled" }, "declined")}>
          Not now
        </button>
      </div>
      {status && <div className="muted small">{status}</div>}
    </>
  );
}
