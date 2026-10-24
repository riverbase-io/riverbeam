// How the console follows a run's frames (plan 009):
//
// 1. The caller's RTC channel, `/api/rtc/default~ssestream/{org}.{sub}`,
//    subscribed once at connect. Every run frame of the caller's sessions
//    arrives there as `{type: "beam.stream", topic: "{session}:{request}",
//    payload: frame}`, so a run is followed before its command is sent.
// 2. Without RTC: `/astream/{session}/{request}`, which sends the durable
//    replay (`event:` lines, only logged here) and then the live frames.
// 3. Always: the durable replay `/api/beam/sse/{session}/{request}`, polled
//    until the run ends, in case a live frame was missed.
//
// Frames from more than one source are de-duplicated by their `id`.

import { Api, Frame, AgentResult } from "./api";

export type RtcStatus = "off" | "connecting" | "live" | "unavailable";

type SseHandler = (data: any, event?: string) => void;

/** Read an SSE body until it ends or `signal` aborts. */
async function readSse(res: Response, onEvent: SseHandler, signal: AbortSignal): Promise<void> {
  const reader = res.body!.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  while (!signal.aborted) {
    const { value, done } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    let i: number;
    while ((i = buffer.indexOf("\n\n")) >= 0) {
      const lines = buffer.slice(0, i).split("\n");
      buffer = buffer.slice(i + 2);
      const data = lines
        .filter((l) => l.startsWith("data:"))
        .map((l) => l.slice(5).trim())
        .join("\n");
      const event = lines.find((l) => l.startsWith("event:"))?.slice(6).trim();
      if (!data) continue; // keep-alive comment
      try {
        onEvent(JSON.parse(data), event);
      } catch {
        /* not JSON */
      }
    }
  }
}

/** The caller's own RTC channel, reconnecting until stopped. */
export class RtcChannel {
  status: RtcStatus = "off";
  private listeners = new Map<string, Set<(frame: Frame) => void>>();
  private abort: AbortController | null = null;

  constructor(private api: Api, private onStatus: (s: RtcStatus) => void) {}

  get path(): string {
    const { org, sub } = this.api.settings;
    return `/api/rtc/default~ssestream/${org.trim() || "_"}.${sub.trim()}`;
  }

  start(): void {
    this.stop();
    const abort = new AbortController();
    this.abort = abort;
    void this.loop(abort.signal);
  }

  stop(): void {
    this.abort?.abort();
    this.abort = null;
    this.set("off");
  }

  /** Frames of one run (`{session}:{request}`) until the returned unfollow. */
  follow(topic: string, fn: (frame: Frame) => void): () => void {
    const set = this.listeners.get(topic) ?? new Set();
    set.add(fn);
    this.listeners.set(topic, set);
    return () => {
      set.delete(fn);
      if (!set.size) this.listeners.delete(topic);
    };
  }

  private set(status: RtcStatus) {
    this.status = status;
    this.onStatus(status);
  }

  private async loop(signal: AbortSignal) {
    let delay = 1000;
    while (!signal.aborted) {
      this.set("connecting");
      try {
        const res = await fetch(this.path, { headers: { Authorization: this.api.auth }, signal });
        if (res.status === 403 || res.status === 404) {
          // Not served (BEAM_RTC_STREAM off, an older server) or refused.
          this.set("unavailable");
          return;
        }
        if (!res.ok) throw new Error(`rtc ${res.status}`);
        this.set("live");
        delay = 1000;
        await readSse(res, (msg) => this.dispatch(msg), signal);
      } catch {
        /* reconnect below */
      }
      if (signal.aborted) return;
      this.set("connecting");
      await new Promise((r) => setTimeout(r, delay));
      delay = Math.min(delay * 2, 15000);
    }
  }

  private dispatch(msg: any) {
    if (msg?.type !== "beam.stream" || typeof msg.topic !== "string") return;
    this.listeners.get(msg.topic.toLowerCase())?.forEach((fn) => fn(msg.payload as Frame));
  }
}

export type RunEnd =
  | { kind: "done"; result: AgentResult; frame?: Frame }
  | { kind: "error"; error: string; result?: AgentResult };

export type RunCallbacks = {
  /** A live frame of this run (de-duplicated, before `onEnd`). */
  onFrame(frame: Frame): void;
  /** Every frame seen, for the inspector (`source`: rtc, astream, replay, ack). */
  onLog(source: string, frame: unknown): void;
  /** Once, when the run ends (or is interrupted for an action). */
  onEnd(end: RunEnd): void;
};

/** The durable replay as frames, and whether the run has ended. */
export async function fetchReplay(api: Api, session: string, request: string) {
  const res = await fetch(`/api/beam/sse/${session}/${request}`, {
    headers: { Authorization: api.auth },
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`replay ${res.status}: ${text}`);
  const frames = text
    .split("\n")
    .filter((l) => l.startsWith("data:"))
    .map((l) => {
      try {
        return JSON.parse(l.slice(5));
      } catch {
        return null;
      }
    })
    .filter(Boolean) as Frame[];
  return { frames, complete: /\nevent: done\n/.test(text) };
}

/** Execution states after which a run sends nothing more on its request. */
const ENDED = ["completed", "failed", "cancelled", "timed_out", "waiting_for_input"];

/**
 * Follows one run. Call `beforeCommand` before sending the command, then
 * `afterAck` with its acknowledgement (or `stop` when it was refused).
 */
export class RunFollower {
  executionId: string | null = null;
  ended = false;
  private acked = false;
  private held: [Frame, string][] = [];
  private seen = new Set<string>();
  /** Frames the session's replay held before this run: never this run's. */
  private stale = new Set<string>();
  private abort = new AbortController();
  private unfollow: (() => void) | null = null;
  private astreamOpen = false;
  private astreamOpens = 0;
  private readonly topic: string;

  constructor(
    private api: Api,
    private rtc: RtcChannel,
    readonly session: string,
    readonly request: string,
    private cb: RunCallbacks,
  ) {
    this.topic = `${session}:${request}`.toLowerCase();
  }

  async beforeCommand(sessionKnown: boolean): Promise<void> {
    if (sessionKnown) {
      // Without a run for this request, the replay route shows the session's
      // last one, which a `resume` shares: its frames are not this run's.
      try {
        const { frames } = await fetchReplay(this.api, this.session, this.request);
        frames.forEach((f) => typeof f.id === "string" && f.id && this.stale.add(f.id));
      } catch {
        /* no replay yet */
      }
    }
    if (this.rtc.status === "live") {
      this.unfollow = this.rtc.follow(this.topic, (f) => this.live(f, "rtc"));
    } else if (sessionKnown) {
      await this.openAstream();
    }
  }

  async afterAck(ack: { execution_id?: string } | null): Promise<void> {
    this.cb.onLog("ack", ack);
    this.executionId = ack?.execution_id ?? null;
    this.acked = true;
    this.held.splice(0).forEach(([f, source]) => this.live(f, source));
    if (!this.unfollow && !this.astreamOpen) await this.openAstream();
    void this.pollReplay();
  }

  stop(): void {
    this.ended = true;
    this.abort.abort();
    this.unfollow?.();
    this.unfollow = null;
  }

  private end(end: RunEnd) {
    if (this.ended) return;
    this.stop();
    this.cb.onEnd(end);
  }

  /** A frame of this run, live or replayed; ends the run on `done`/`error`. */
  private live(frame: Frame, source: string) {
    if (this.ended) return;
    if (!this.acked) {
      this.held.push([frame, source]);
      return;
    }
    const id = typeof frame.id === "string" && frame.id ? frame.id : null;
    if (id && this.stale.has(id)) return;
    if (id) {
      if (this.seen.has(id)) return;
      this.seen.add(id);
    }
    this.cb.onLog(source, frame);
    if (!["chunk", "done", "error"].includes(frame.type)) return; // replay bookkeeping
    const exec = frame.execution_id || frame.data?.execution_id || frame.result?.execution_id;
    if (exec && this.executionId && exec !== this.executionId) return;
    if (frame.type === "done") {
      // The replay's closing `{"type": "done"}` carries no result: not a frame.
      if (!frame.result) return;
      this.end({ kind: "done", result: frame.result, frame });
    } else if (frame.type === "error") {
      this.end({ kind: "error", error: frame.error || "run failed", result: frame.result });
    } else {
      this.cb.onFrame(frame);
    }
  }

  /**
   * `/astream`: this request's durable replay (`event:` frames), then its live
   * frames; it closes once the run has ended or waits for input. Resolves
   * once subscribed (headers back) or after 2 s.
   */
  private async openAstream(): Promise<void> {
    this.astreamOpen = true;
    this.astreamOpens += 1;
    const subscribed = (async () => {
      const res = await fetch(`/astream/${this.session}/${this.request}`, {
        headers: { Authorization: this.api.auth },
        signal: this.abort.signal,
      });
      if (!res.ok) throw new Error(`astream ${res.status}`);
      readSse(res, (frame, event) => this.live(frame as Frame, event ? "replay" : "astream"), this.abort.signal)
        .catch(() => {})
        .finally(() => {
          this.astreamOpen = false;
          // Closed early (a network drop): follow again, a few times at most.
          // The replay poll still ends the run.
          if (!this.ended && this.acked && this.astreamOpens < 5) {
            setTimeout(() => !this.ended && !this.astreamOpen && void this.openAstream(), 1000 * this.astreamOpens);
          }
        });
    })();
    subscribed.catch(() => {
      this.astreamOpen = false;
    });
    await Promise.race([subscribed.catch(() => {}), new Promise((r) => setTimeout(r, 2000))]);
  }

  /** The durable replay, polled until it holds this run's end. */
  private async pollReplay() {
    for (let i = 0; i < 400 && !this.ended; i++) {
      await new Promise((r) => setTimeout(r, i < 5 ? 700 : 1500));
      if (this.ended) return;
      try {
        const { frames } = await fetchReplay(this.api, this.session, this.request);
        if (this.ended) return;
        const fresh = frames.filter((f) => !(typeof f.id === "string" && this.stale.has(f.id)));
        const mine = (f: Frame) => !this.executionId || !f.execution_id || f.execution_id === this.executionId;
        // A replayed `done` or `error` frame of this run ends it: its result
        // carries the answer, tool calls, citations and any held actions.
        const terminal = fresh.filter((f) => (f.type === "done" && f.result) || f.type === "error").filter(mine).pop();
        if (terminal) {
          this.live(terminal, "replay");
          if (this.ended) return;
        }
        const state = fresh.find((f) => f.type === "state") as any;
        if (!state || !mine(state) || !ENDED.includes(state.status) || state.status === "waiting_for_input") continue;
        // Ended without a `done` frame in the replay: its final message.
        const final = fresh.filter((f: any) => f.type === "message" && f.role === "ai").pop() as any;
        if (["failed", "timed_out"].includes(state.status)) {
          this.end({ kind: "error", error: `run ${state.status} (see the replay frames)` });
        } else {
          this.end({
            kind: "done",
            result: { content: final?.content ?? [], execution_id: state.execution_id, metadata: { status: state.status } },
          });
        }
        return;
      } catch {
        /* try again */
      }
    }
  }
}
