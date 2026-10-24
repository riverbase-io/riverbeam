// One agent conversation: send turns, follow their runs (RTC, /astream and the
// durable replay, lib/stream.ts), show tool rows and action cards, and answer
// held actions. A held run waits for every one of its actions, so answers are
// sent together, in one `resume`, once each card has one.
//
// Client tools (beam.client_tool) go to `runClientTool`: it answers at once
// (fill a field, move a step) or returns null when the person must act, and
// the card is shown until they do.

import { useCallback, useEffect, useRef, useState } from "react";
import { ActionEntry, AgentResult, Api, ContentPart, Frame, Row, reasoningOf, textOf, uuid } from "./api";
import { RtcChannel, RunFollower } from "./stream";

export type Decision = { status: "submitted" | "cancelled"; payload?: Row };

export type Item =
  | { kind: "human"; id: string; text: string; images?: string[] }
  | { kind: "ai"; id: string; text: string; thinking?: string; streaming?: boolean; executionId?: string | null; feedback?: "up" | "down"; citations?: Row[] }
  | { kind: "tool"; id: string; name: string; args?: unknown; result?: unknown }
  | {
      kind: "action";
      id: string;
      entry: ActionEntry;
      interactionId?: string;
      /** The hold these cards belong to: they are answered together, in one resume. */
      group: string;
      decision?: Decision;
      resolved?: string;
      auto?: boolean;
    }
  | { kind: "note"; id: string; text: string; tone?: "error" | "info" };

export type ClientAnswer = { output?: unknown; error?: string; declined?: boolean };
/** Answer a client tool now, or null when the person must act on its card. */
export type ClientToolRunner = (tool: string, args: Row, entry: ActionEntry) => ClientAnswer | null;

let counter = 0;
export const itemId = () => `i${Date.now().toString(36)}${(counter++).toString(36)}`;

function parseJson(value: unknown): unknown {
  if (typeof value !== "string") return value;
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

/** Stored `agent_message` rows as chat items (the session's history). */
export function itemsFromMessages(rows: Row[]): Item[] {
  const items: Item[] = [];
  for (const m of [...rows].sort((a, b) => (a.sequence ?? 0) - (b.sequence ?? 0))) {
    // Stored labels are upper case (HUMAN, AI, TOOL_RESULT).
    const type = String(m.message_type || "message").toLowerCase();
    const role = String(m.role ?? "").toLowerCase();
    const phase = String(m.phase ?? "").toLowerCase();
    if (m.tool_name || type === "tool_result" || role === "tool") {
      items.push({ kind: "tool", id: m.id, name: m.tool_name || "tool", args: parseJson(m.tool_args), result: m.tool_result ?? textOf(m.content) });
    } else if (type === "action_result" || type === "action") {
      continue;
    } else if (role === "human") {
      const text = textOf(m.content);
      if (text) items.push({ kind: "human", id: m.id, text });
    } else if (role === "ai" && phase !== "thinking") {
      const text = textOf(m.content);
      if (text) items.push({ kind: "ai", id: m.id, text, executionId: m.execution_id });
    }
  }
  return items;
}

/** Pending `agent_action` rows as cards that can still be answered. */
export function pendingCards(rows: Row[]): Item[] {
  return rows.flatMap((row): Item[] => {
    const id = row.id ?? row._id;
    const request = row.payload?.request;
    if (!id || !request?.kind) return [];
    const entry: ActionEntry = { action_id: String(id), lc_action_id: row.lc_action_id ?? undefined, action_request: request };
    return [{ kind: "action", id: `pending-${id}`, entry, interactionId: row.interaction_id, group: `pending-${row.run_id ?? row.interaction_id}` }];
  });
}

function storedSession(key: string): string {
  try {
    const known = localStorage.getItem(`showcase.session.${key}`);
    if (known) return known;
    const id = uuid();
    localStorage.setItem(`showcase.session.${key}`, id);
    return id;
  } catch {
    return uuid();
  }
}

function forgetSession(key: string): string {
  const id = uuid();
  try {
    localStorage.setItem(`showcase.session.${key}`, id);
  } catch {
    /* in memory only */
  }
  return id;
}

export type ChatOptions = {
  api: Api;
  rtc: RtcChannel;
  /** The agent's catalog row (`id`, `name`). */
  agent: Row | null;
  /** Names the conversation: one session per key, kept across reloads. */
  sessionKey: string;
  /** Request facts sent with every turn (`application_id`, `step`, …). */
  facts?: Row[];
  /** This tab's client capabilities (client tools it runs). */
  client?: Row;
  runClientTool?: ClientToolRunner;
  onError?: (message: string) => void;
};

export function useAgentChat(opts: ChatOptions) {
  const { api, rtc, agent, sessionKey } = opts;
  const [sessionId, setSessionId] = useState(() => storedSession(sessionKey));
  const [items, setItems] = useState<Item[]>([]);
  const [busy, setBusy] = useState(false);
  const [last, setLast] = useState<AgentResult | null>(null);
  const itemsRef = useRef(items);
  itemsRef.current = items;
  const optsRef = useRef(opts);
  optsRef.current = opts;
  const known = useRef(false);
  const run = useRef<RunFollower | null>(null);
  const [version, setVersion] = useState(0);

  const patch = useCallback((id: string, fn: (item: Item) => Item | null) => {
    setItems((prev) => prev.flatMap((item) => (item.id !== id ? [item] : (fn(item) ?? []) as Item[])));
  }, []);

  // Load the session's history and its pending actions.
  useEffect(() => {
    const id = storedSession(sessionKey);
    setSessionId(id);
    setItems([]);
    known.current = false;
    let gone = false;
    void (async () => {
      try {
        const [rows, pending] = await Promise.all([
          api.list("agent_message", { "session_id.eq": id }, { limit: 500 }),
          api.list("agent_action", { "session_id.eq": id, "status.eq": "PENDING" }, { limit: 50 }).catch(() => []),
        ]);
        if (gone) return;
        known.current = rows.length > 0;
        setItems([...itemsFromMessages(rows), ...pendingCards(pending)]);
      } catch {
        /* a new session */
      }
    })();
    return () => {
      gone = true;
    };
  }, [api, sessionKey, version]);

  const fail = useCallback((message: string) => {
    setItems((prev) => [...prev, { kind: "note", id: itemId(), text: message, tone: "error" }]);
    optsRef.current.onError?.(message);
  }, []);

  /** Answer the client tools that need no one; the rest stay as cards. */
  const autoAnswer = useCallback((cards: Item[]): Item[] => {
    const runner = optsRef.current.runClientTool;
    return cards.map((card) => {
      if (card.kind !== "action" || card.entry.action_request.kind !== "beam.client_tool" || !runner) return card;
      const payload = (card.entry.action_request.payload ?? {}) as Row;
      if (payload.requires_confirmation) return card;
      const answer = runner(String(payload.tool_name ?? ""), (payload.args ?? {}) as Row, card.entry);
      if (!answer) return card;
      return { ...card, decision: clientDecision(answer), resolved: answer.error ? "failed" : "done", auto: true };
    });
  }, []);

  const runTurn = useCallback(
    async (key: "stream" | "resume", payload: Row): Promise<boolean> => {
      if (!agent) return false;
      const o = optsRef.current;
      const requestId = uuid();
      payload.request_id = requestId;
      payload.session_id = sessionId;
      if (o.client) payload.client = o.client;
      if (o.facts?.length) payload.context = { ...(payload.context ?? {}), facts: o.facts };
      const aiId = itemId();
      setItems((prev) => [...prev, { kind: "ai", id: aiId, text: "", streaming: true }]);
      setBusy(true);
      let segment = aiId;
      let streamed = "";
      const follower = new RunFollower(api, rtc, sessionId, requestId, {
        onLog: () => {},
        onFrame: (frame: Frame) => {
          const m = frame.data;
          if (m?.role === "tool" && m.type === "tool_result") {
            const closed = segment;
            segment = itemId();
            streamed = "";
            setItems((prev) => [
              ...prev.flatMap((it): Item[] => (it.id !== closed ? [it] : it.kind === "ai" && it.text ? [{ ...it, streaming: false }] : [])),
              { kind: "tool", id: itemId(), name: m.tool_name || "tool", args: m.tool_args, result: textOf(m.content) },
              { kind: "ai", id: segment, text: "", streaming: true },
            ]);
            return;
          }
          if (!m || m.role !== "ai" || m.type !== "message" || m.phase === "thinking") return;
          const text = textOf(m.content);
          if (reasoningOf(m.content)) return;
          streamed = m.is_delta ? streamed + text : text;
          const shown = streamed;
          patch(segment, (it) => ({ ...it, text: shown }) as Item);
        },
        onEnd: (end) => {
          run.current = null;
          setBusy(false);
          known.current = true;
          if (end.kind === "error") {
            patch(segment, (it) => (it.kind === "ai" && it.text ? { ...it, streaming: false } : null));
            fail(errorNote(end.error));
            return;
          }
          const result = end.result;
          setLast(result);
          const text = textOf(result.content) || streamed;
          const executionId = result.execution_id ?? follower.executionId;
          patch(segment, (it) =>
            text ? ({ ...it, text, streaming: false, executionId, citations: result.citations } as Item) : null,
          );
          const cards: Item[] = (result.action_required ?? []).map((entry) => ({
            kind: "action",
            id: itemId(),
            entry,
            interactionId: result.interaction_id,
            group: requestId,
          }));
          if (cards.length) {
            const answered = autoAnswer(cards);
            setItems((prev) => [...prev, ...answered]);
            if (answered.every((c) => c.kind === "action" && c.decision)) {
              void sendAnswers(answered as Extract<Item, { kind: "action" }>[]);
            }
          }
        },
      });
      run.current = follower;
      try {
        await follower.beforeCommand(known.current);
        const ack = await api.command<Row>(key, "agent", agent.id, payload);
        await follower.afterAck(ack);
        return true;
      } catch (e) {
        follower.stop();
        run.current = null;
        setBusy(false);
        patch(segment, () => null);
        fail(errorNote(String((e as Error).message)));
        return false;
      }
    },
    // sendAnswers is defined below and stable through its own refs.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [agent, api, rtc, sessionId, patch, fail, autoAnswer],
  );

  const sendAnswers = useCallback(
    async (cards: Extract<Item, { kind: "action" }>[]) => {
      const sent = await runTurn("resume", {
        interaction_id: cards[0]?.interactionId,
        content: [],
        actions: cards.map(({ entry, decision }) => ({
          action_id: entry.action_id,
          lc_action_id: entry.lc_action_id,
          kind: entry.action_request.kind,
          request_id: entry.action_request.request_id,
          status: decision!.status,
          payload: decision!.payload ?? null,
        })),
      });
      if (!sent) {
        // Refused: the cards can be answered again.
        for (const card of cards) patch(card.id, (it) => ({ ...it, decision: undefined, resolved: undefined }) as Item);
      }
    },
    [runTurn, patch],
  );

  const send = useCallback(
    async (text: string, extra: ContentPart[] = [], images: string[] = []) => {
      const content: ContentPart[] = text ? [{ type: "text", text }, ...extra] : extra;
      setItems((prev) => [...prev, { kind: "human", id: itemId(), text, images }]);
      await runTurn("stream", { content });
    },
    [runTurn],
  );

  /** Answer one card; the run resumes once every card of it has an answer. */
  const respond = useCallback(
    async (item: Extract<Item, { kind: "action" }>, decision: Decision, label?: string) => {
      const resolved = label ?? (decision.status === "cancelled" ? "declined" : decision.payload?.approved === false ? "denied" : "answered");
      const decided = { ...item, decision, resolved };
      // Only this hold's cards: an earlier hold of the same run was answered already.
      const cards = itemsRef.current
        .filter((i): i is Extract<Item, { kind: "action" }> => i.kind === "action" && i.group === item.group)
        .map((i) => (i.id === item.id ? decided : i));
      const waiting = cards.filter((c) => !c.decision).length;
      patch(item.id, () => (waiting ? { ...decided, resolved: `${resolved} · ${waiting} more to answer` } : decided));
      if (!waiting) await sendAnswers(cards);
    },
    [patch, sendAnswers],
  );

  const cancel = useCallback(async () => {
    const executionId = run.current?.executionId;
    if (!agent || !executionId) return;
    try {
      await api.command("cancel-execution", "agent", agent.id, { execution_id: executionId });
    } catch (e) {
      fail(String((e as Error).message));
    }
  }, [agent, api, fail]);

  const feedback = useCallback(
    async (item: Extract<Item, { kind: "ai" }>, thumb: "up" | "down") => {
      if (!agent || !item.executionId) return;
      try {
        await api.command("submit-agent-feedback", "agent", agent.id, { execution_id: item.executionId, thumb });
        patch(item.id, (it) => ({ ...it, feedback: thumb }) as Item);
      } catch (e) {
        fail(String((e as Error).message));
      }
    },
    [agent, api, patch, fail],
  );

  const reset = useCallback(() => {
    run.current?.stop();
    setSessionId(forgetSession(sessionKey));
    known.current = false;
    setItems([]);
    setLast(null);
  }, [sessionKey]);

  /** Read the session again (a turn ran elsewhere, e.g. by voice). */
  const reload = useCallback(() => setVersion((v) => v + 1), []);

  return { sessionId, items, busy, last, send, respond, cancel, feedback, reset, reload };
}

/** A client tool's answer as an action response (Python rc8's shape). */
export function clientDecision(answer: ClientAnswer): Decision {
  if (answer.declined) return { status: "cancelled" };
  return { status: "submitted", payload: answer.error ? { error: answer.error } : { output: answer.output ?? "done" } };
}

/** A run error, with what to do when the session is held by a waiting run. */
export function errorNote(text: string): string {
  if (/BEM-429|BEM-372/.test(text)) {
    return `${text} This conversation already has a run waiting for an answer: answer its card, or start over.`;
  }
  if (/BEM-061|BEM-421/.test(text)) {
    return `${text} The security policy refused the message: contact details, card numbers and similar data are not taken in chat. Type them into the form instead.`;
  }
  return text;
}

/**
 * Run `agent` once in its own session and wait for its result (no chat):
 * the UI's way to hand work to another agent, e.g. a pay stub to
 * paystub_reader.
 */
export async function runOnce(api: Api, rtc: RtcChannel, agent: Row, content: ContentPart[], facts: Row[] = []): Promise<AgentResult> {
  const session = uuid();
  const request = uuid();
  return new Promise<AgentResult>((resolve, reject) => {
    const follower = new RunFollower(api, rtc, session, request, {
      onLog: () => {},
      onFrame: () => {},
      onEnd: (end) => (end.kind === "done" ? resolve(end.result) : reject(new Error(end.error))),
    });
    void (async () => {
      try {
        await follower.beforeCommand(false);
        const ack = await api.command<Row>("stream", "agent", agent.id, {
          session_id: session,
          request_id: request,
          content,
          ...(facts.length ? { context: { facts } } : {}),
        });
        await follower.afterAck(ack);
      } catch (e) {
        follower.stop();
        reject(e);
      }
    })();
  });
}
