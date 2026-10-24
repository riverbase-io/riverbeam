import { ReactNode, useEffect, useRef, useState } from "react";
import { ActionEntry, Row } from "../lib/api";
import { Decision, Item } from "../lib/chat";

type ActionItem = Extract<Item, { kind: "action" }>;
type AiItem = Extract<Item, { kind: "ai" }>;

function pretty(value: unknown): string {
  if (typeof value === "string") {
    try {
      return JSON.stringify(JSON.parse(value), null, 2);
    } catch {
      return value;
    }
  }
  return JSON.stringify(value ?? {}, null, 2);
}

/** `**bold**` inside a line. */
function inline(text: string): ReactNode[] {
  return text.split(/(\*\*[^*]+\*\*)/g).map((part, i) =>
    part.startsWith("**") && part.endsWith("**") && part.length > 4 ? <b key={i}>{part.slice(2, -2)}</b> : part,
  );
}

/** The little markdown the agents write: tables, headings, bold. */
export function Rich(props: { text: string }) {
  const blocks: ReactNode[] = [];
  const lines = props.text.split("\n");
  for (let i = 0; i < lines.length; ) {
    if (lines[i].trim().startsWith("|")) {
      const rows: string[][] = [];
      while (i < lines.length && lines[i].trim().startsWith("|")) {
        const cells = lines[i].trim().replace(/^\||\|$/g, "").split("|").map((c) => c.trim());
        if (!cells.every((c) => /^-*$/.test(c))) rows.push(cells);
        i++;
      }
      const [head, ...body] = rows;
      blocks.push(
        <table key={i} className="grid md">
          <thead>
            <tr>{head?.map((c, j) => <th key={j}>{inline(c)}</th>)}</tr>
          </thead>
          <tbody>
            {body.map((r, k) => (
              <tr key={k}>{r.map((c, j) => <td key={j}>{inline(c)}</td>)}</tr>
            ))}
          </tbody>
        </table>,
      );
      continue;
    }
    const heading = /^#{1,3}\s+(.*)/.exec(lines[i]);
    blocks.push(heading ? <div key={i} className="md-h">{inline(heading[1])}</div> : <div key={i}>{inline(lines[i]) }{lines[i] === "" ? "\u00a0" : null}</div>);
    i++;
  }
  return <>{blocks}</>;
}

/** Cards for client tools the person acts on; return null for the default card. */
export type ClientCard = (item: ActionItem, respond: (d: Decision, label?: string) => void, disabled: boolean) => ReactNode | null;

export function ChatPanel(props: {
  title: string;
  subtitle?: string;
  items: Item[];
  busy: boolean;
  placeholder?: string;
  suggestions?: string[];
  onSend: (text: string) => void;
  onRespond: (item: ActionItem, decision: Decision, label?: string) => void;
  onCancel?: () => void;
  onFeedback?: (item: AiItem, thumb: "up" | "down") => void;
  onReset?: () => void;
  clientCard?: ClientCard;
  extra?: ReactNode;
  /** Header buttons besides Stop and New chat. */
  actions?: ReactNode;
}) {
  const [text, setText] = useState("");
  const end = useRef<HTMLDivElement>(null);
  useEffect(() => {
    // A block body: newer Chromium returns a Promise from scrollIntoView,
    // which React would take for a cleanup.
    end.current?.scrollIntoView({ block: "end" });
  }, [props.items.length, props.busy]);
  const submit = () => {
    const t = text.trim();
    if (!t || props.busy) return;
    setText("");
    props.onSend(t);
  };
  return (
    <section className="chat">
      <header className="chat-head">
        <div>
          <div className="chat-title">{props.title}</div>
          {props.subtitle && <div className="muted small">{props.subtitle}</div>}
        </div>
        <div className="row">
          {props.actions}
          {props.busy && props.onCancel && (
            <button className="ghost" onClick={props.onCancel}>
              Stop
            </button>
          )}
          {props.onReset && (
            <button className="ghost" onClick={props.onReset} title="Start a new conversation">
              New chat
            </button>
          )}
        </div>
      </header>
      <div className="chat-body">
        {props.items.length === 0 && props.suggestions && (
          <div className="suggestions">
            {props.suggestions.map((s) => (
              <button key={s} className="chip" onClick={() => props.onSend(s)} disabled={props.busy}>
                {s}
              </button>
            ))}
          </div>
        )}
        {props.items.map((item) => {
          switch (item.kind) {
            case "human":
              return (
                <div key={item.id} className="bubble human">
                  {item.text}
                  {item.images?.map((src) => <img key={src.slice(-24)} src={src} alt="attachment" />)}
                </div>
              );
            case "ai":
              if (!item.text && !item.streaming) return null;
              return (
                <div key={item.id} className={`bubble ai ${item.streaming ? "streaming" : ""}`}>
                  {item.text ? <Rich text={item.text} /> : <span className="muted">…</span>}
                  {!item.streaming && item.executionId && props.onFeedback && (
                    <span className="thumbs">
                      <button className={item.feedback === "up" ? "on" : ""} onClick={() => props.onFeedback!(item, "up")} title="Helpful">
                        👍
                      </button>
                      <button className={item.feedback === "down" ? "on" : ""} onClick={() => props.onFeedback!(item, "down")} title="Not helpful">
                        👎
                      </button>
                    </span>
                  )}
                </div>
              );
            case "tool":
              return (
                <details key={item.id} className="tool-row">
                  <summary>⚙ {item.name}</summary>
                  <pre>{pretty(item.args)}</pre>
                  <pre>{pretty(item.result)}</pre>
                </details>
              );
            case "note":
              return (
                <div key={item.id} className={`note ${item.tone ?? ""}`}>
                  {item.text}
                </div>
              );
            case "action": {
              if (item.auto) {
                const payload = (item.entry.action_request.payload ?? {}) as Row;
                return (
                  <div key={item.id} className="tool-row client">
                    ↳ {String(payload.tool_name ?? "client tool")} ran in this tab {item.resolved === "failed" ? "(failed)" : ""}
                  </div>
                );
              }
              const respond = (d: Decision, label?: string) => props.onRespond(item, d, label);
              const custom = !item.resolved && props.clientCard ? props.clientCard(item, respond, props.busy) : null;
              return (
                <div key={item.id} className={`card action ${item.resolved ? "resolved" : ""}`}>
                  {custom ?? <ActionCard entry={item.entry} resolved={item.resolved} disabled={props.busy} respond={respond} />}
                </div>
              );
            }
          }
        })}
        <div ref={end} />
      </div>
      {props.extra}
      <footer className="chat-input">
        <textarea
          value={text}
          placeholder={props.placeholder ?? "Message"}
          rows={2}
          onChange={(e) => setText(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && !e.shiftKey) {
              e.preventDefault();
              submit();
            }
          }}
        />
        <button className="primary" onClick={submit} disabled={props.busy || !text.trim()}>
          Send
        </button>
      </footer>
    </section>
  );
}

/** A card for any beam action: approvals, forms, choices, confirmations. */
export function ActionCard(props: { entry: ActionEntry; resolved?: string; disabled: boolean; respond: (d: Decision, label?: string) => void }) {
  const req = props.entry.action_request;
  const payload = (req.payload ?? {}) as Row;
  const [comment, setComment] = useState("");
  const [values, setValues] = useState<Row>(() => ({ ...((payload.values as Row) ?? {}) }));
  const [choice, setChoice] = useState<string>(() => ((payload.choices as string[]) ?? [])[0] ?? "");
  const header = (
    <div className="card-head">
      <span className="pause">⏸</span>
      <div>
        <div className="title">{req.title || "Action required"}</div>
        {req.description && <div className="muted small">{req.description}</div>}
      </div>
      <span className="badge">{req.kind.replace("beam.", "")}</span>
    </div>
  );
  if (props.resolved) {
    return (
      <>
        {header}
        <div className="muted small">Answered: {props.resolved}</div>
      </>
    );
  }
  const approval = req.kind === "beam.tool_approval" || req.kind === "beam.approval" || req.kind === "beam.confirmation";
  if (approval) {
    return (
      <>
        {header}
        {payload.tool_name ? (
          <div className="kv">
            <span>tool</span>
            <code>{String(payload.tool_name)}</code>
            {payload.risk ? (
              <>
                <span>risk</span>
                <code className={`risk ${payload.risk}`}>{String(payload.risk)}</code>
              </>
            ) : null}
            <span>args</span>
            <pre>{pretty(payload.args)}</pre>
          </div>
        ) : (
          <p>{String(payload.question ?? "")}</p>
        )}
        <div className="row">
          <button className="primary" disabled={props.disabled} onClick={() => props.respond({ status: "submitted", payload: { approved: true } }, "approved")}>
            Approve
          </button>
          <input placeholder="Reason (to deny)" value={comment} onChange={(e) => setComment(e.target.value)} />
          <button
            disabled={props.disabled}
            onClick={() => props.respond({ status: "submitted", payload: { approved: false, comment: comment || "denied" } }, "denied")}
          >
            Deny
          </button>
        </div>
      </>
    );
  }
  if (req.kind === "beam.choice_question") {
    const choices = (payload.choices as string[]) ?? [];
    return (
      <>
        {header}
        <p>{String(payload.question ?? "")}</p>
        <div className="choices">
          {choices.map((c) => (
            <label key={c} className="choice">
              <input type="radio" checked={choice === c} onChange={() => setChoice(c)} /> {c}
            </label>
          ))}
        </div>
        <div className="row">
          <button className="primary" disabled={props.disabled || !choice} onClick={() => props.respond({ status: "submitted", payload: { selected_choices: [choice] } }, choice)}>
            Choose
          </button>
        </div>
      </>
    );
  }
  if (req.kind === "beam.form_input") {
    const keys = Object.keys(values).length ? Object.keys(values) : ["note"];
    return (
      <>
        {header}
        <div className="form-grid">
          {keys.map((k) => (
            <label key={k}>
              <span>{k.replace(/_/g, " ")}</span>
              <textarea rows={k === "note" ? 3 : 1} value={String(values[k] ?? "")} onChange={(e) => setValues((v) => ({ ...v, [k]: e.target.value }))} />
            </label>
          ))}
        </div>
        <div className="row">
          <button className="primary" disabled={props.disabled} onClick={() => props.respond({ status: "submitted", payload: { values } }, "submitted")}>
            Submit
          </button>
        </div>
      </>
    );
  }
  // A client tool with no special card: this tab cannot run it.
  return (
    <>
      {header}
      <pre>{pretty(payload.args ?? payload)}</pre>
      <div className="row">
        <button disabled={props.disabled} onClick={() => props.respond({ status: "cancelled" }, "declined")}>
          Decline
        </button>
      </div>
    </>
  );
}
