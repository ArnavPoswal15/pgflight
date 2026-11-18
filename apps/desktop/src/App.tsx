import { useCallback, useEffect, useMemo, useState, type ReactNode } from "react";
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import { EmptyRadarIllustration, LogoMark, NavIcon, type NavIconName } from "./brand/Logo";

type Tab = "live" | "patterns" | "replay" | "compare";

type UiPattern = {
  fingerprint_id: number;
  canonical_sql: string;
  exec_count: number;
  p95_us: number;
};

type UiFlightLine = { text: string };

type UiShadowStatus = {
  enabled: boolean;
  url: string;
  connected: boolean;
  queued_jobs: number;
  dropped_jobs: number;
};

type RecorderSnapshot = {
  listen: string;
  upstream: string;
  recording: boolean;
  eventsRecorded: number;
};

const DOCKER_DEMO_DSN = "postgresql://postgres:postgres@127.0.0.1:5433/shop";

const TAB_META: Record<Tab, { title: string; description: string }> = {
  live: {
    title: "Live log",
    description: "Real-time flight events via control socket subscribe",
  },
  patterns: {
    title: "Patterns",
    description: "Expensive query fingerprints from the leaderboard snapshot",
  },
  replay: {
    title: "Replay",
    description: "Re-run a recorded `.pgrec` flight against the shadow database",
  },
  compare: {
    title: "Compare plans",
    description: "Diff EXPLAIN plans between replay A and replay B",
  },
};

function parseRecorderStatus(raw: string): RecorderSnapshot | null {
  if (!raw) return null;
  try {
    const parsed: unknown = JSON.parse(raw);
    const items = Array.isArray(parsed) ? parsed : [parsed];
    for (const item of items) {
      if (!item || typeof item !== "object") continue;
      const status =
        "Status" in item && item.Status && typeof item.Status === "object"
          ? (item.Status as Record<string, unknown>)
          : (item as Record<string, unknown>);
      const listen = status.listen;
      const upstream = status.upstream;
      if (typeof listen !== "string") continue;
      const rec =
        status.recorder && typeof status.recorder === "object"
          ? (status.recorder as Record<string, unknown>)
          : {};
      return {
        listen,
        upstream: typeof upstream === "string" ? upstream : "",
        recording: Boolean(rec.recording),
        eventsRecorded:
          typeof rec.events_recorded === "number" ? rec.events_recorded : 0,
      };
    }
  } catch {
    return null;
  }
  return null;
}

function parseFlightLine(text: string): {
  sql: string;
  fp: string;
  ms: string;
  rows: string;
} {
  const m = text.match(/^(.+) fp=(\d+) ([\d.]+)ms rows=(\d+)$/);
  if (m) {
    return { sql: m[1], fp: m[2], ms: m[3], rows: m[4] };
  }
  return { sql: text, fp: "—", ms: "—", rows: "—" };
}

function latencyClass(ms: string): string {
  if (ms === "—") return "";
  const n = Number.parseFloat(ms);
  if (!Number.isFinite(n)) return "";
  if (n < 5) return "latency--fast";
  if (n < 25) return "latency--ok";
  if (n < 100) return "latency--warn";
  return "latency--slow";
}

export default function App() {
  const [tab, setTab] = useState<Tab>("live");
  const [liveLog, setLiveLog] = useState<UiFlightLine[]>([]);
  const [patterns, setPatterns] = useState<UiPattern[]>([]);
  const [shadow, setShadow] = useState<UiShadowStatus | null>(null);
  const [statusJson, setStatusJson] = useState<string>("");
  const [replayPgrec, setReplayPgrec] = useState("");
  const [replayWarmup, setReplayWarmup] = useState(0);
  const [replayExplain, setReplayExplain] = useState(true);
  const [replayOut, setReplayOut] = useState("");
  const [baselineId, setBaselineId] = useState("");
  const [variantId, setVariantId] = useState("");
  const [compareOut, setCompareOut] = useState("");
  const [explainFp, setExplainFp] = useState("");
  const [explainOut, setExplainOut] = useState("");
  const [error, setError] = useState<string | null>(null);

  const recorder = useMemo(() => parseRecorderStatus(statusJson), [statusJson]);

  const proxyHint = recorder?.listen ?? "127.0.0.1:5433";

  const refreshLive = useCallback(async () => {
    try {
      const lines = await invoke<UiFlightLine[]>("fetch_live_log");
      setLiveLog(lines);
    } catch (e) {
      setError(String(e));
    }
  }, []);

  const refreshPatterns = useCallback(async () => {
    try {
      const rows = await invoke<UiPattern[]>("fetch_patterns");
      setPatterns(rows);
    } catch (e) {
      setError(String(e));
    }
  }, []);

  const refreshShadow = useCallback(async () => {
    try {
      const s = await invoke<UiShadowStatus>("shadow_status");
      setShadow(s);
      const raw = await invoke<string>("recorder_status");
      setStatusJson(raw);
    } catch (e) {
      setError(String(e));
    }
  }, []);

  useEffect(() => {
    refreshLive();
    refreshPatterns();
    refreshShadow();
    const unsubs: Array<() => void> = [];
    (async () => {
      unsubs.push(
        await listen("pgflight://live-log", () => {
          void refreshLive();
        }),
      );
      unsubs.push(
        await listen("pgflight://patterns", () => {
          void refreshPatterns();
        }),
      );
    })();
    const poll = window.setInterval(() => {
      void refreshLive();
    }, 2000);
    return () => {
      window.clearInterval(poll);
      for (const u of unsubs) u();
    };
  }, [refreshLive, refreshPatterns, refreshShadow]);

  async function runReplay(label: "A" | "B") {
    setError(null);
    try {
      const out = await invoke<string>("replay_run", {
        pgrecPath: replayPgrec,
        label,
        warmupIters: replayWarmup,
        explain: replayExplain,
      });
      setReplayOut(out);
      const parsed = JSON.parse(out) as { replay?: { replay_id?: string } };
      const id = parsed.replay?.replay_id ?? "";
      if (label === "A") setBaselineId(id);
      else setVariantId(id);
    } catch (e) {
      setError(String(e));
    }
  }

  async function runCompare() {
    setError(null);
    try {
      const out = await invoke<string>("compare_replays", {
        baselineReplayId: baselineId,
        baselineLabel: "A",
        variantReplayId: variantId,
        variantLabel: "B",
      });
      setCompareOut(out);
    } catch (e) {
      setError(String(e));
    }
  }

  async function runExplain() {
    setError(null);
    const fp = Number(explainFp);
    if (!Number.isFinite(fp)) {
      setError("fingerprint_id must be a number");
      return;
    }
    try {
      const out = await invoke<string>("explain_query", {
        fingerprintId: fp,
        flightId: null,
        eventId: null,
      });
      setExplainOut(out);
    } catch (e) {
      setError(String(e));
    }
  }

  const liveRows = useMemo(
    () =>
      liveLog
        .slice()
        .reverse()
        .map((line, i) => ({ key: `${i}-${line.text.slice(0, 24)}`, ...parseFlightLine(line.text) })),
    [liveLog],
  );

  const meta = TAB_META[tab];

  return (
    <div className="cockpit">
      <aside className="cockpit__sidebar" aria-label="Primary navigation">
        <div className="cockpit__brand">
          <LogoMark className="cockpit__brand-mark" />
          <div className="cockpit__brand-text">
            <span className="cockpit__brand-name">pgflight</span>
            <span className="cockpit__brand-sub">Query flight recorder</span>
          </div>
        </div>
        <nav className="cockpit__nav">
          <NavItem active={tab === "live"} onClick={() => setTab("live")} icon="live" label="Live" />
          <NavItem
            active={tab === "patterns"}
            onClick={() => setTab("patterns")}
            icon="patterns"
            label="Patterns"
          />
          <NavItem
            active={tab === "replay"}
            onClick={() => setTab("replay")}
            icon="replay"
            label="Replay"
          />
          <NavItem
            active={tab === "compare"}
            onClick={() => setTab("compare")}
            icon="compare"
            label="Compare"
          />
        </nav>
        <div className="cockpit__sidebar-foot">Proxy {proxyHint}</div>
      </aside>

      <div className="cockpit__main">
        <header className="cockpit__topbar">
          <div className="cockpit__topbar-lead">
            <div className="cockpit__wordmark">
              <span className="cockpit__wordmark-name">pgflight</span>
              <span className="cockpit__wordmark-tag">Instrument panel · PostgreSQL wire proxy</span>
            </div>
            <h2 className="cockpit__topbar-title">{meta.title}</h2>
            <p className="cockpit__topbar-desc">{meta.description}</p>
          </div>
          <StatusChips recorder={recorder} shadow={shadow} onRefreshShadow={() => void refreshShadow()} />
        </header>

        {error && (
          <div className="alert-bar">
            <p className="alert" role="alert">
              {error}
            </p>
          </div>
        )}

        <main className="cockpit__panel">
          {tab === "live" && (
            <section aria-labelledby="live-panel">
              <div className="card">
                <div className="card__header">
                  <h3 className="card__title" id="live-panel">
                    Incoming flights
                  </h3>
                  <div className="toolbar">
                    <button type="button" className="btn btn--ghost" onClick={() => void refreshLive()}>
                      Refresh
                    </button>
                  </div>
                </div>
                {liveRows.length === 0 ? (
                  <EmptyLive proxyHint={proxyHint} />
                ) : (
                  <div className="table-scroll">
                    <table className="data-table">
                      <thead>
                        <tr>
                          <th>Query</th>
                          <th>fp</th>
                          <th className="col-num">latency</th>
                          <th className="col-num">rows</th>
                        </tr>
                      </thead>
                      <tbody>
                        {liveRows.map((row) => (
                          <tr key={row.key}>
                            <td className="col-sql" title={row.sql}>
                              {row.sql}
                            </td>
                            <td className="col-mono">{row.fp}</td>
                            <td className={`col-num col-mono ${latencyClass(row.ms)}`}>
                              {row.ms === "—" ? row.ms : `${row.ms} ms`}
                            </td>
                            <td className="col-num col-mono">{row.rows}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            </section>
          )}

          {tab === "patterns" && (
            <section aria-labelledby="patterns-panel">
              <div className="card">
                <div className="card__header">
                  <h3 className="card__title" id="patterns-panel">
                    Leaderboard
                  </h3>
                  <button type="button" className="btn btn--ghost" onClick={() => void refreshPatterns()}>
                    Refresh
                  </button>
                </div>
                {patterns.length === 0 ? (
                  <motionEmpty message="No fingerprints yet. Traffic through the proxy will populate this view." />
                ) : (
                  <div className="table-scroll">
                    <table className="data-table">
                      <thead>
                        <tr>
                          <th>fp</th>
                          <th className="col-num">execs</th>
                          <th className="col-num">p95 µs</th>
                          <th>canonical sql</th>
                        </tr>
                      </thead>
                      <tbody>
                        {patterns.map((p) => (
                          <tr key={p.fingerprint_id}>
                            <td className="col-mono">{p.fingerprint_id}</td>
                            <td className="col-num col-mono">{p.exec_count}</td>
                            <td className="col-num col-mono">{p.p95_us}</td>
                            <td className="col-sql" title={p.canonical_sql}>
                              {p.canonical_sql}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>

              <div className="card section-gap">
                <div className="card__header">
                  <h3 className="card__title">Shadow EXPLAIN</h3>
                </div>
                <div className="card__body card__body--padded">
                  <div className="row-fields">
                    <div className="field">
                      <span className="field__label">fingerprint_id</span>
                      <input
                        className="input input--narrow"
                        value={explainFp}
                        onChange={(e) => setExplainFp(e.target.value)}
                        inputMode="numeric"
                        placeholder="42"
                      />
                    </div>
                    <button type="button" className="btn btn--primary" onClick={() => void runExplain()}>
                      Run EXPLAIN
                    </button>
                  </div>
                  {explainOut && <pre className="code-block">{explainOut}</pre>}
                </div>
              </div>
            </section>
          )}

          {tab === "replay" && (
            <section aria-labelledby="replay-panel">
              <p className="panel-intro" id="replay-panel">
                Replays execute on the shadow DB with safety guards (read-only, timeouts).
              </p>
              <div className="card">
                <div className="card__header">
                  <h3 className="card__title">Flight tape</h3>
                </div>
                <div className="card__body card__body--padded">
                  <div className="form-stack">
                    <div className="field">
                      <span className="field__label">pgrec path</span>
                      <input
                        className="input"
                        value={replayPgrec}
                        onChange={(e) => setReplayPgrec(e.target.value)}
                        placeholder="~/.pgflight/pgflight-1.pgrec"
                      />
                    </div>
                    <div className="field">
                      <span className="field__label">warmup iterations</span>
                      <input
                        className="input input--number"
                        type="number"
                        value={replayWarmup}
                        onChange={(e) => setReplayWarmup(Number(e.target.value))}
                        min={0}
                      />
                    </div>
                    <label className="checkbox-row">
                      <input
                        type="checkbox"
                        checked={replayExplain}
                        onChange={(e) => setReplayExplain(e.target.checked)}
                      />
                      Collect EXPLAIN plans during replay
                    </label>
                    <div className="btn-group">
                      <button type="button" className="btn btn--primary" onClick={() => void runReplay("A")}>
                        Replay A
                      </button>
                      <button type="button" className="btn" onClick={() => void runReplay("B")}>
                        Replay B
                      </button>
                    </div>
                  </div>
                  {replayOut && <pre className="code-block">{replayOut}</pre>}
                </div>
              </div>
            </section>
          )}

          {tab === "compare" && (
            <section aria-labelledby="compare-panel">
              <p className="panel-intro" id="compare-panel">
                Compare two replay IDs to spot plan regressions and index wins.
              </p>
              <div className="card">
                <div className="card__header">
                  <h3 className="card__title">Plan diff</h3>
                </div>
                <div className="card__body card__body--padded">
                  <div className="row-fields">
                    <div className="field">
                      <span className="field__label">baseline replay_id (A)</span>
                      <input
                        className="input"
                        value={baselineId}
                        onChange={(e) => setBaselineId(e.target.value)}
                        placeholder="replay uuid"
                      />
                    </div>
                    <div className="field">
                      <span className="field__label">variant replay_id (B)</span>
                      <input
                        className="input"
                        value={variantId}
                        onChange={(e) => setVariantId(e.target.value)}
                        placeholder="replay uuid"
                      />
                    </div>
                  </div>
                  <div className="btn-group section-gap">
                    <button type="button" className="btn btn--primary" onClick={() => void runCompare()}>
                      Compare
                    </button>
                    <button type="button" className="btn btn--ghost" onClick={() => void refreshShadow()}>
                      Refresh status
                    </button>
                  </div>
                  {compareOut && <pre className="code-block">{compareOut}</pre>}
                  {statusJson && (
                    <details className="details-block">
                      <summary>Recorder status (JSON)</summary>
                      <pre className="code-block">{statusJson}</pre>
                    </details>
                  )}
                </div>
              </div>
            </section>
          )}
        </main>
      </div>
    </div>
  );
}

function NavItem({
  active,
  onClick,
  icon,
  label,
}: {
  active: boolean;
  onClick: () => void;
  icon: NavIconName;
  label: string;
}) {
  return (
    <button
      type="button"
      className={`nav-item${active ? " nav-item--active" : ""}`}
      onClick={onClick}
      aria-current={active ? "page" : undefined}
    >
      <span className="nav-item__icon" aria-hidden>
        <NavIcon name={icon} />
      </span>
      {label}
    </button>
  );
}

function StatusChips({
  recorder,
  shadow,
  onRefreshShadow,
}: {
  recorder: RecorderSnapshot | null;
  shadow: UiShadowStatus | null;
  onRefreshShadow: () => void;
}) {
  return (
    <div className="status-chips">
      {recorder ? (
        <span
          className={`chip ${recorder.recording ? "chip--ok" : "chip--warn"}`}
          title={`Upstream ${recorder.upstream || "—"} · ${recorder.eventsRecorded} events`}
        >
          <span className="chip__dot" aria-hidden />
          Recorder {recorder.recording ? "on" : "idle"} · {recorder.listen}
        </span>
      ) : (
        <button type="button" className="chip chip--off" onClick={onRefreshShadow}>
          <span className="chip__dot" aria-hidden />
          Recorder unknown
        </button>
      )}
      {shadow ? (
        <span
          className={`chip ${
            !shadow.enabled ? "chip--off" : shadow.connected ? "chip--ok" : "chip--warn"
          }`}
          title={
            shadow.enabled
              ? `${shadow.url}${shadow.queued_jobs ? ` · queued ${shadow.queued_jobs}` : ""}${
                  shadow.dropped_jobs ? ` · dropped ${shadow.dropped_jobs}` : ""
                }`
              : "Shadow DB disabled in daemon"
          }
        >
          <span className="chip__dot" aria-hidden />
          Shadow {shadow.enabled ? (shadow.connected ? "connected" : "waiting") : "off"}
        </span>
      ) : (
        <span className="chip chip--off">
          <span className="chip__dot" aria-hidden />
          Shadow —
        </span>
      )}
    </div>
  );
}

function EmptyLive({ proxyHint }: { proxyHint: string }) {
  void proxyHint;
  return (
    <motionEmpty>
      <div className="empty-state">
        <EmptyRadarIllustration />
        <h3>No flights on radar</h3>
        <p>Point your application at the pgflight wire proxy. Queries will stream here in real time.</p>
        <code className="connection-cta">{DOCKER_DEMO_DSN}</code>
        <p className="empty-state__footnote">
          Proxy listening on {proxyHint}. Start `pgflightd` if the recorder chip stays unknown.
        </p>
      </div>
    </motionEmpty>
  );
}

function motionEmpty({
  children,
  message,
}: {
  children?: ReactNode;
  message?: string;
}) {
  if (children) return <div className="card__body">{children}</div>;
  return (
    <div className="card__body card__body--padded">
      <p className="field__hint" style={{ textAlign: "center", margin: "24px 0" }}>
        {message}
      </p>
    </div>
  );
}