// Voice turns with an agent over `/ws/voice` (the host's audio socket): hold
// to talk, the speech goes up as WebM chunks, and the server sends back the
// transcript, the agent's reply as text, and the reply as audio. The turn is
// an ordinary run in the same session, so the chat shows it after a reload.
//
// "Sample" sends a turn without the microphone: with the mock speech backend
// (`AUDIO_STT_BACKEND=mock`) the transcript is `AUDIO_MOCK_TRANSCRIPT`.

import { useRef, useState } from "react";
import { Api } from "../lib/api";

type Line = { who: "you" | "agent" | "info"; text: string };

type Row = Record<string, unknown>;

export function VoiceButton(props: {
  api: Api;
  sessionId: string;
  agent: string;
  onStatus: (s: string) => void;
  onTurn?: () => void;
  /** Request facts and client capabilities, as the chat sends them (plan 020). */
  facts?: Row[];
  client?: Row;
}) {
  const [lines, setLines] = useState<Line[]>([]);
  const [state, setState] = useState("idle");
  const [talking, setTalking] = useState(false);
  const socket = useRef<WebSocket | null>(null);
  const recorder = useRef<MediaRecorder | null>(null);
  const audio = useRef<Blob[]>([]);
  const audioType = useRef("audio/wav");

  const connect = (): Promise<WebSocket> =>
    new Promise((resolve, reject) => {
      const open = socket.current;
      if (open && open.readyState === WebSocket.OPEN) return resolve(open);
      const proto = location.protocol === "https:" ? "wss" : "ws";
      const params = new URLSearchParams({ token: props.api.auth, agent: props.agent, session_id: props.sessionId });
      const ws = new WebSocket(`${proto}://${location.host}/ws/voice?${params}`);
      ws.binaryType = "blob";
      ws.onopen = () => {
        // Every voice turn carries the chat's facts and client capabilities.
        if (props.facts?.length || props.client) {
          ws.send(JSON.stringify({ type: "context", context: props.facts?.length ? { facts: props.facts } : null, client: props.client ?? null }));
        }
        resolve(ws);
      };
      ws.onerror = () => reject(new Error("voice is not available (the server runs without the audio feature?)"));
      ws.onclose = () => {
        socket.current = null;
        setState("idle");
      };
      ws.onmessage = (event) => {
        if (event.data instanceof Blob) {
          audio.current.push(event.data);
          return;
        }
        let msg: any;
        try {
          msg = JSON.parse(String(event.data));
        } catch {
          return;
        }
        switch (msg.type) {
          case "ping":
            ws.send(JSON.stringify({ type: "pong" }));
            break;
          case "state":
            setState(msg.state);
            break;
          case "text":
            // Phases: stt and llm (progress), user_transcript, assistant (final), error.
            if (msg.phase === "user_transcript") {
              setLines((l) => [...l, { who: "you", text: msg.text }]);
            } else if (msg.phase === "assistant" && msg.final) {
              setLines((l) => [...l, { who: "agent", text: msg.text }]);
              props.onTurn?.();
            } else if (msg.phase === "error") {
              setLines((l) => [...l, { who: "info", text: msg.text }]);
            }
            break;
          case "audio_start":
            audio.current = [];
            audioType.current = msg.format === "mp3" ? "audio/mpeg" : `audio/${msg.format || "wav"}`;
            break;
          case "audio_end": {
            const blob = new Blob(audio.current, { type: audioType.current });
            if (blob.size > 64) void new Audio(URL.createObjectURL(blob)).play().catch(() => {});
            break;
          }
          case "error":
            setLines((l) => [...l, { who: "info", text: `error: ${msg.error}` }]);
            break;
        }
      };
      socket.current = ws;
    });

  const start = async () => {
    try {
      const ws = await connect();
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
      const rec = new MediaRecorder(stream, { mimeType: "audio/webm" });
      rec.ondataavailable = (e) => e.data.size && ws.send(e.data);
      rec.onstop = () => {
        stream.getTracks().forEach((t) => t.stop());
        ws.send(JSON.stringify({ type: "end_of_speech" }));
      };
      ws.send(JSON.stringify({ type: "start" }));
      rec.start(250);
      recorder.current = rec;
      setTalking(true);
    } catch (e) {
      props.onStatus(String((e as Error).message));
    }
  };

  const stop = () => {
    recorder.current?.stop();
    recorder.current = null;
    setTalking(false);
  };

  const sample = async () => {
    try {
      const ws = await connect();
      ws.send(JSON.stringify({ type: "start" }));
      // A second of silent "speech": the mock backend transcribes any audio
      // longer than the socket's minimum utterance.
      ws.send(new Blob([new Uint8Array(32000)], { type: "audio/webm" }));
      ws.send(JSON.stringify({ type: "end_of_speech" }));
    } catch (e) {
      props.onStatus(String((e as Error).message));
    }
  };

  return (
    <div className="voice">
      <button
        className={talking ? "on" : ""}
        onMouseDown={() => void start()}
        onMouseUp={stop}
        onMouseLeave={() => talking && stop()}
        title="Hold to talk"
      >
        🎙 {talking ? "Listening…" : "Hold to talk"}
      </button>
      <button className="ghost" onClick={() => void sample()} title="Send a voice turn without the microphone (mock speech)">
        Sample
      </button>
      <span className="muted small">{state !== "idle" ? state : ""}</span>
      {lines.length > 0 && (
        <div className="voice-lines">
          {lines.slice(-4).map((l, i) => (
            <div key={i} className={`muted small ${l.who}`}>
              {l.who === "you" ? "You said: " : l.who === "agent" ? "Assistant: " : ""}
              {l.text}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
