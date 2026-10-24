// The showcases the server hosts (`/api/showcase/catalog`), and the client
// for the records the new showcases keep (`/api/<showcase>/…`).

import { Api, Row } from "./api";

export type Persona = {
  key: string;
  user_id: string;
  name: string;
  /** The showcase's own role: it picks the persona's view. */
  role: string;
  plan: string;
  roles: string[];
};

export type ShowcaseInfo = {
  key: string;
  title: string;
  tagline: string;
  org: string;
  /** Its routes' prefix, e.g. `/api/support/`. */
  api: string;
  agents: string[];
  personas: Persona[];
};

export async function catalog(): Promise<ShowcaseInfo[]> {
  const res = await fetch("/api/showcase/catalog");
  if (!res.ok) throw new Error(`catalog ${res.status}`);
  return res.json();
}

/** A record of a new showcase (src/records.rs). */
export type ShowcaseRecord = {
  id: string;
  showcase: string;
  kind: string;
  owner: string;
  org?: string | null;
  status: string;
  title: string;
  data: Row;
  created_at: string;
  updated_at: string;
};

/** What a persona's view gets. */
export type ViewProps = {
  api: Api;
  rtc: import("./stream").RtcChannel;
  /** Active agents by name. */
  agents: Record<string, Row>;
  persona: Persona;
  showcase: ShowcaseInfo;
  onStatus: (s: string) => void;
};

/** This browser tab's client id (client capabilities name it). */
export function tabId(): string {
  try {
    const known = sessionStorage.getItem("showcase.client");
    if (known) return known;
    const id = `tab-${crypto.randomUUID()}`;
    sessionStorage.setItem("showcase.client", id);
    return id;
  } catch {
    return `tab-${crypto.randomUUID()}`;
  }
}

export const money = (n: unknown) =>
  typeof n === "number" ? n.toLocaleString(undefined, { style: "currency", currency: "USD", maximumFractionDigits: 2 }) : String(n ?? "—");
