// The beam domain API: mock auth as a
// persona, domain queries (`{resource}.list`), domain commands
// (`{key}:exec/{resource}/{id}`), the usage summary and readiness.

export type Settings = { ns: string; sub: string; org: string; name: string; roles: string[] };

export type Row = Record<string, any>;

export type ContentPart = {
  type: string;
  text?: string;
  url?: string;
  data?: string;
  mime_type?: string;
};

export type ToolCallRecord = {
  tool_name: string;
  args?: unknown;
  result?: string;
  latency_ms?: number;
  tool_call_id?: string;
};

export type AgentResult = {
  content: ContentPart[];
  agent_name?: string;
  model_used?: string;
  session_id?: string;
  request_id?: string;
  interaction_id?: string;
  execution_id?: string;
  token_usage?: { prompt_tokens: number; completion_tokens: number; total_tokens: number };
  tool_calls?: ToolCallRecord[];
  latency_ms?: number;
  interrupted?: boolean;
  action_required?: ActionEntry[];
  citations?: Row[];
  metadata?: Row;
};

export type ActionEntry = {
  action_id: string;
  lc_action_id?: string;
  action_request: {
    kind: string;
    request_id: string;
    title?: string;
    description?: string;
    payload?: Row;
    metadata?: Row;
  };
};

/** A run frame (`StreamChunk`). */
export type Frame = {
  type: "chunk" | "done" | "error" | string;
  data?: {
    role?: string;
    type?: string;
    content?: ContentPart[];
    is_delta?: boolean;
    phase?: string;
    name?: string;
    tool_name?: string;
    tool_args?: unknown;
    tool_call_id?: string;
    metadata?: Row;
    execution_id?: string;
  };
  result?: AgentResult;
  error?: string;
  execution_id?: string;
  interaction_id?: string;
  session_id?: string;
  [key: string]: unknown;
};

/** riverbase mock auth: `MockAuth-` + base64url(JSON claims), as the persona. */
export function authHeader(s: Settings): string {
  const claims = {
    sub: s.sub,
    preferred_username: s.name,
    name: s.name,
    roles: s.roles,
    realm_access: { roles: s.roles },
    organization_id: s.org,
  };
  const b64 = btoa(unescape(encodeURIComponent(JSON.stringify(claims))));
  return "MockAuth-" + b64.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

export class ApiError extends Error {
  constructor(message: string, public status: number, public body: unknown) {
    super(message);
  }
}

export class Api {
  constructor(public settings: Settings) {}

  get auth(): string {
    return authHeader(this.settings);
  }

  path(rest: string): string {
    return `/api/${this.settings.ns.trim()}/${rest}`;
  }

  async http<T = any>(method: string, url: string, body?: unknown): Promise<T> {
    const headers: Record<string, string> = { Authorization: this.auth };
    if (body !== undefined) headers["content-type"] = "application/json";
    const res = await fetch(url, {
      method,
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    const text = await res.text();
    let data: any = text;
    try {
      data = text ? JSON.parse(text) : null;
    } catch {
      /* a text body */
    }
    if (!res.ok) {
      const detail =
        data && typeof data === "object"
          ? data.detail || data.title || data.message || JSON.stringify(data)
          : text;
      throw new ApiError(`${method} ${url.replace(/\?.*/, "")} → ${res.status}: ${detail}`, res.status, data);
    }
    return data as T;
  }

  /** `GET {resource}.list` with a riverbase filter (`{"field.eq": value}`). */
  async list(resource: string, filter?: Row, opts: { limit?: number; sort?: string } = {}): Promise<Row[]> {
    const params = new URLSearchParams({ limit: String(opts.limit ?? 200) });
    if (filter && Object.keys(filter).length) params.set("query", JSON.stringify(filter));
    if (opts.sort) params.set("sort", opts.sort);
    const out = await this.http<{ data?: Row[] }>("GET", this.path(`${resource}.list?${params}`));
    return out?.data ?? [];
  }

  /**
   * A domain command on one object. riverbase answers
   * `{type, data: {"default-response": out}, meta}`; this returns `out`.
   */
  async command<T = Row>(key: string, resource: string, id: string, payload: Row): Promise<T> {
    const raw = await this.http<Row>("POST", this.path(`${key}:exec/${resource}/${id}`), payload);
    const data = raw && typeof raw === "object" ? raw.data : undefined;
    if (data && typeof data === "object" && !Array.isArray(data)) {
      const responses = Object.values(data);
      return (data["default-response"] ?? (responses.length === 1 ? responses[0] : data)) as T;
    }
    return raw as T;
  }

  /** A queued task's `agent_task` row once it has finished (polls). */
  async task(id: string, timeoutMs = 180_000): Promise<Row> {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const [row] = await this.list("agent_task", { "id.eq": id }, { limit: 1 });
      if (row && ["SUCCEEDED", "FAILED", "CANCELLED"].includes(row.status)) return row;
      if (Date.now() > deadline) throw new Error(`task ${short(id)} is still ${row?.status ?? "queued"}`);
      await new Promise((r) => setTimeout(r, 800));
    }
  }

  usage(period?: string): Promise<Row> {
    return this.http("GET", this.path(`.usage${period ? `?period=${encodeURIComponent(period)}` : ""}`));
  }

  async ready(): Promise<boolean> {
    return fetch("/api/ready").then((r) => r.ok).catch(() => false);
  }
}

/** The text of message parts (images and files as placeholders). */
export function textOf(content: unknown): string {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return content == null ? "" : JSON.stringify(content);
  return content
    .filter((p: ContentPart) => p.type !== "reasoning")
    .map((p: ContentPart) => p.text ?? (p.url ? `[${p.type}: ${p.url}]` : `[${p.type}]`))
    .join("");
}

/** The reasoning parts of a message, if any. */
export function reasoningOf(content: unknown): string {
  if (!Array.isArray(content)) return "";
  return content
    .filter((p: ContentPart) => p.type === "reasoning")
    .map((p: ContentPart) => p.text ?? "")
    .join("");
}

/** A task's or problem's error as one line: `BEM-430 The model provider call failed.` */
export function errorText(error: unknown): string {
  if (error == null) return "";
  if (typeof error !== "object") return String(error);
  const e = error as Row;
  const code = e.errcode ?? e.code;
  const message = e.errmesg ?? e.message ?? e.title ?? e.detail;
  return [code, message].filter(Boolean).join(" ") || JSON.stringify(error);
}

export const short = (id?: string | null) => (id ? String(id).slice(0, 8) : "–");

export const when = (iso?: string) => (iso ? new Date(iso).toLocaleString() : "");

export const uuid = () => crypto.randomUUID();
