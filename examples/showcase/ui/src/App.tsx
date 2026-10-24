import { useEffect, useMemo, useState } from "react";
import { Api, Row, Settings } from "./lib/api";
import { RtcChannel, RtcStatus } from "./lib/stream";
import { ShowcaseInfo, catalog as loadCatalog } from "./lib/showcase";
import { VIEWS } from "./views";

/** The beam domain's namespace (BEAM_DOMAIN_NAMESPACE). */
const NAMESPACE = "fluvius-beam";

function stored(key: string): string | null {
  try {
    return localStorage.getItem(key);
  } catch {
    return null;
  }
}

function store(key: string, value: string) {
  try {
    localStorage.setItem(key, value);
  } catch {
    /* this tab only */
  }
}

export function App() {
  const [showcases, setShowcases] = useState<ShowcaseInfo[]>([]);
  const [showcaseKey, setShowcaseKey] = useState<string | null>(() => stored("showcase.key"));
  const [personaKey, setPersonaKey] = useState<string | null>(null);
  const [agents, setAgents] = useState<Record<string, Row>>({});
  const [rtcStatus, setRtcStatus] = useState<RtcStatus>("off");
  const [usage, setUsage] = useState<Row | null>(null);
  const [status, setStatus] = useState<string | null>(null);

  useEffect(() => {
    loadCatalog()
      .then(setShowcases)
      .catch((e) => setStatus(`Cannot reach the showcase server: ${e.message}`));
  }, []);

  const showcase = showcases.find((s) => s.key === showcaseKey) ?? showcases[0] ?? null;
  // Each showcase remembers its own persona.
  useEffect(() => {
    if (showcase) setPersonaKey(stored(`showcase.persona.${showcase.key}`));
  }, [showcase]);
  const persona = showcase?.personas.find((p) => p.key === personaKey) ?? null;

  const api = useMemo(() => {
    if (!persona || !showcase) return null;
    const settings: Settings = { ns: NAMESPACE, sub: persona.user_id, org: showcase.org, name: persona.name, roles: persona.roles };
    return new Api(settings);
  }, [persona, showcase]);
  const rtc = useMemo(() => (api ? new RtcChannel(api, setRtcStatus) : null), [api]);

  useEffect(() => {
    if (!rtc) return;
    rtc.start();
    return () => rtc.stop();
  }, [rtc]);

  useEffect(() => {
    if (!api) return;
    setAgents({});
    api
      .list("agent", { "status.eq": "ACTIVE" })
      .then((rows) => setAgents(Object.fromEntries(rows.map((r) => [r.name, r]))))
      .catch((e) => setStatus(String(e.message)));
    const refresh = () => api.usage().then(setUsage).catch(() => setUsage(null));
    void refresh();
    const timer = setInterval(refresh, 15000);
    return () => clearInterval(timer);
  }, [api]);

  const pickShowcase = (key: string) => {
    store("showcase.key", key);
    setStatus(null);
    setUsage(null);
    setShowcaseKey(key);
  };

  const pickPersona = (key: string) => {
    if (showcase) store(`showcase.persona.${showcase.key}`, key);
    setStatus(null);
    setPersonaKey(key);
  };

  const View = showcase && persona ? VIEWS[showcase.key]?.[persona.role] : undefined;
  const roles = showcase ? [...new Set(showcase.personas.map((p) => p.role))] : [];

  return (
    <div className="app">
      <header className="top">
        <div className="brand">
          <span className="logo">◈</span> Riverbeam Showcase
          <span className={`dot ${rtcStatus}`} title={`live updates: ${rtcStatus}`} />
        </div>
        <label className="showcase-pick" title={showcase?.tagline}>
          <select value={showcase?.key ?? ""} onChange={(e) => pickShowcase(e.target.value)}>
            {showcases.map((s) => (
              <option key={s.key} value={s.key}>
                {s.title}
              </option>
            ))}
          </select>
          {showcase && <span className="tagline">{showcase.tagline}</span>}
        </label>
        <div className="personas">
          {showcase?.personas.map((p) => (
            <button key={p.key} className={p.key === persona?.key ? "on" : ""} onClick={() => pickPersona(p.key)} title={`${p.role} · ${p.plan} plan`}>
              {p.name}
              <span className="role">{p.role.replace(/_/g, " ")}</span>
            </button>
          ))}
        </div>
        {usage && persona && (
          <div className="usage muted small" title="Today's usage on your plan">
            {String(usage.plan ?? persona.plan)} plan · {String(usage.total_requests ?? 0)}
            {usage.limits?.max_requests_per_day ? `/${usage.limits.max_requests_per_day}` : ""} requests ·{" "}
            {Number(usage.total_tokens ?? 0).toLocaleString()} tokens today
          </div>
        )}
      </header>
      {status && (
        <div className="banner" onClick={() => setStatus(null)}>
          {status}
        </div>
      )}
      <main>
        {!showcase || !persona || !api || !rtc ? (
          <div className="empty">
            <h2>{showcase ? showcase.title : "Riverbeam Showcase"}</h2>
            <p className="muted">{showcase?.tagline}</p>
            <p className="muted">Pick a persona above{roles.length ? `: ${roles.join(", ").replace(/_/g, " ")}` : ""}.</p>
          </div>
        ) : View ? (
          <View key={`${showcase.key}.${persona.key}`} api={api} rtc={rtc} agents={agents} persona={persona} showcase={showcase} onStatus={setStatus} />
        ) : (
          <div className="empty muted">This showcase has no view for {persona.role}.</div>
        )}
      </main>
    </div>
  );
}
