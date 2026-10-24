// Cards every showcase's chat shows for the shared client tools:
// request_input (a form card) and request_document (an upload that a vision
// agent reads), and the helpers behind them.

import { useState } from "react";
import { AgentResult, Api, Row } from "../lib/api";
import { RtcChannel } from "../lib/stream";
import { Decision, clientDecision, runOnce } from "../lib/chat";

/** A file as base64 (for an image content part) and a data URL. */
export function readFile(file: File): Promise<{ base64: string; dataUrl: string }> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => {
      const dataUrl = String(reader.result);
      resolve({ dataUrl, base64: dataUrl.split(",")[1] ?? "" });
    };
    reader.onerror = () => reject(reader.error);
    reader.readAsDataURL(file);
  });
}

/** A structured-output agent's reply as JSON (fences stripped), else `{raw}`. */
export function parseReply(result: AgentResult): Row {
  const text = (result.content ?? []).map((p) => p.text ?? "").join("");
  try {
    return JSON.parse(text.replace(/^```(json)?|```$/g, "").trim());
  } catch {
    return { raw: text };
  }
}

/** Draw `rows` as a white document on a canvas, as a PNG file. */
export async function drawDocument(fileName: string, heading: string[], rows: [string, string][]): Promise<File> {
  const canvas = document.createElement("canvas");
  canvas.width = 640;
  canvas.height = 120 + rows.length * 27 + 40;
  const g = canvas.getContext("2d")!;
  g.fillStyle = "#fff";
  g.fillRect(0, 0, canvas.width, canvas.height);
  g.fillStyle = "#111";
  g.font = "bold 22px Arial";
  g.fillText(heading[0] ?? "", 24, 40);
  g.font = "14px Arial";
  heading.slice(1).forEach((line, i) => g.fillText(line, 24, 62 + i * 18));
  g.font = "15px Arial";
  rows.forEach(([k, v], i) => {
    g.fillText(k, 24, 120 + i * 27);
    g.fillText(v, 360, 120 + i * 27);
  });
  g.strokeRect(10, 96, 620, rows.length * 27 + 20);
  const blob = await new Promise<Blob>((resolve) => canvas.toBlob((b) => resolve(b!), "image/png"));
  return new File([blob], fileName, { type: "image/png" });
}

/** request_input: a form card for missing details. */
export function InputCard(props: { args: Row; disabled: boolean; respond: (d: Decision, label?: string) => void }) {
  const fields = (Array.isArray(props.args.fields) ? props.args.fields : []) as Row[];
  const [values, setValues] = useState<Row>({});
  const missing = fields.filter((f) => f.required && !String(values[f.name] ?? "").trim());
  return (
    <>
      <div className="card-head">
        <span className="pause">✎</span>
        <div>
          <div className="title">{String(props.args.title ?? "A few details")}</div>
          {props.args.description ? <div className="muted small">{String(props.args.description)}</div> : null}
        </div>
      </div>
      <div className="form-grid">
        {fields.map((f) => (
          <label key={String(f.name)}>
            <span>
              {String(f.label ?? f.name)}
              {f.required ? " *" : ""}
            </span>
            <input type={f.type === "number" ? "number" : "text"} value={String(values[f.name] ?? "")} onChange={(e) => setValues((v) => ({ ...v, [f.name]: e.target.value }))} />
          </label>
        ))}
      </div>
      <div className="row">
        <button
          className="primary"
          disabled={props.disabled || missing.length > 0}
          onClick={() => {
            const output: Row = {};
            for (const f of fields) {
              const raw = String(values[f.name] ?? "").trim();
              if (raw) output[f.name] = f.type === "number" && !Number.isNaN(Number(raw)) ? Number(raw) : raw;
            }
            props.respond(clientDecision({ output }), "answered");
          }}
        >
          Send
        </button>
        <button className="ghost" disabled={props.disabled} onClick={() => props.respond({ status: "cancelled" }, "declined")}>
          Not now
        </button>
      </div>
    </>
  );
}

/**
 * request_document for any showcase: the person picks an image (or a drawn
 * sample), `reader` (a vision agent with structured output) reads it, `save`
 * stores the document, and what was read goes back to the assistant.
 */
export function DocumentCard(props: {
  args: Row;
  disabled: boolean;
  respond: (d: Decision, label?: string) => void;
  api: Api;
  rtc: RtcChannel;
  /** The vision agent's catalog row. */
  reader: Row | undefined;
  /** What the reader is asked, e.g. "Read this invoice." */
  prompt: string;
  /** One line under the card's title. */
  hint: string;
  sample: () => Promise<File>;
  /** Store the document (file name, type and what was read). */
  save: (document: Row) => Promise<void>;
}) {
  const [status, setStatus] = useState<string | null>(null);
  const upload = async (file: File) => {
    if (!props.reader) return setStatus("The reader agent is not running.");
    try {
      setStatus("Reading the document…");
      const { base64 } = await readFile(file);
      const result = await runOnce(props.api, props.rtc, props.reader, [
        { type: "text", text: props.prompt },
        { type: "image_base64", data: base64, mime_type: file.type || "image/png" },
      ]);
      const document = { file_name: file.name, document_type: String(props.args.document_type ?? "document"), extracted: parseReply(result) };
      await props.save(document);
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
          <div className="muted small">{props.hint}</div>
        </div>
      </div>
      <div className="row">
        <input type="file" accept="image/*" disabled={props.disabled || !!status} onChange={(e) => e.target.files?.[0] && void upload(e.target.files[0])} />
        <button disabled={props.disabled || !!status} onClick={() => void props.sample().then(upload)} title="Draw a sample document">
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
