// The layout most showcase views share: a list of records on the left (a
// table you pick from, then the picked record's detail) and the agent's chat
// on the right.

import { ReactNode } from "react";
import { when } from "../lib/api";
import { ShowcaseRecord } from "../lib/showcase";

export type Column = { label: string; render: (r: ShowcaseRecord) => ReactNode };

export function Workbench(props: {
  /** The list's heading, e.g. "Your tickets". */
  eyebrow: string;
  records: ShowcaseRecord[];
  columns: Column[];
  selected: string | null;
  onSelect: (id: string) => void;
  /** Shown when the list is empty. */
  empty: string;
  /** Buttons above the list. */
  toolbar?: ReactNode;
  /** The picked record's detail. */
  detail?: ReactNode;
  /** The chat panel (or a placeholder section). */
  chat: ReactNode;
}) {
  return (
    <div className="split">
      <section className="application">
        <div className="row between">
          <div className="eyebrow">{props.eyebrow}</div>
          {props.toolbar}
        </div>
        <table className="grid">
          <thead>
            <tr>
              {props.columns.map((c) => (
                <th key={c.label}>{c.label}</th>
              ))}
              <th>Status</th>
              <th>Updated</th>
            </tr>
          </thead>
          <tbody>
            {props.records.map((r) => (
              <tr key={r.id} className={`click ${r.id === props.selected ? "on" : ""}`} onClick={() => props.onSelect(r.id)}>
                {props.columns.map((c) => (
                  <td key={c.label}>{c.render(r)}</td>
                ))}
                <td className={`status ${r.status}`}>{r.status.replace(/_/g, " ")}</td>
                <td className="muted small">{when(r.updated_at)}</td>
              </tr>
            ))}
            {props.records.length === 0 && (
              <tr>
                <td colSpan={props.columns.length + 2} className="muted">
                  {props.empty}
                </td>
              </tr>
            )}
          </tbody>
        </table>
        {props.detail}
      </section>
      {props.chat}
    </div>
  );
}

/** A record's fields as a two-column table; `labels` orders and names them. */
export function Fields(props: { data: Record<string, unknown>; labels: [string, string][]; format?: (key: string, value: unknown) => ReactNode }) {
  return (
    <table className="kv-table">
      <tbody>
        {props.labels.map(([key, label]) => (
          <tr key={key}>
            <td>{label}</td>
            <td>{props.format ? props.format(key, props.data[key]) : String(props.data[key] ?? "—")}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

/** The chat column when nothing is picked yet. */
export function ChatPlaceholder(props: { text: string }) {
  return (
    <section className="chat">
      <div className="empty muted">{props.text}</div>
    </section>
  );
}
