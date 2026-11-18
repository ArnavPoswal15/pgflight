import { useCallback, useEffect, useState } from "react";
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";

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

  return (
    <motionWrap>
      <header>
        <h1>pgflight — Query Flight Recorder</h1>
        <nav>
          <TabButton active={tab === "live"} onClick={() => setTab("live")}>
            Live Log
          </TabButton>
          <TabButton active={tab === "patterns"} onClick={() => setTab("patterns")}>
            Patterns
          </TabButton>
          <TabButton active={tab === "replay"} onClick={() => setTab("replay")}>
            Replay
          </TabButton>
          <TabButton active={tab === "compare"} onClick={() => setTab("compare")}>
            Compare plans
          </TabButton>
        </nav>
        {shadow && (
          <p style={{ fontSize: 14, color: "#444" }}>
            Shadow: {shadow.enabled ? shadow.url : "disabled"}{" "}
            {shadow.connected ? "· connected" : ""}
            {shadow.queued_jobs ? ` · queued=${shadow.queued_jobs}` : ""}
          </p>
        )}
        {error && (
          <p style={{ color: "crimson", fontSize: 14 }} role="alert">
            {error}
          </p>
        )}
      </header>
      <main>
        {tab === "live" && (
          <section>
            <p style={{ fontSize: 14 }}>Live flight log (background subscribe on events topic).</p>
            <button type="button" onClick={() => void refreshLive()}>
              Refresh
            </button>
            <ul style={{ fontFamily: "ui-monospace, monospace", fontSize: 12 }}>
              {liveLog.length === 0 && <li>No events yet — connect through :5433.</li>}
              {liveLog
                .slice()
                .reverse()
                .map((line, i) => (
                  <li key={`${i}-${line.text.slice(0, 24)}`}>{line.text}</li>
                ))}
            </ul>
          </section>
        )}
        {tab === "patterns" && (
          <section>
            <p style={{ fontSize: 14 }}>Expensive fingerprints (leaderboard subscribe snapshot).</p>
            <button type="button" onClick={() => void refreshPatterns()}>
              Refresh
            </button>
            <table style={{ width: "100%", fontSize: 12, borderCollapse: "collapse" }}>
              <thead>
                <tr>
                  <th align="left">fp</th>
                  <th align="left">count</th>
                  <th align="left">p95 µs</th>
                  <th align="left">sql</th>
                </tr>
              </thead>
              <tbody>
                {patterns.map((p) => (
                  <tr key={p.fingerprint_id}>
                    <td>{p.fingerprint_id}</td>
                    <td>{p.exec_count}</td>
                    <td>{p.p95_us}</td>
                    <td>{p.canonical_sql}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            <div style={{ marginTop: 16 }}>
              <label>
                EXPLAIN fingerprint_id{" "}
                <input value={explainFp} onChange={(e) => setExplainFp(e.target.value)} size={8} />
              </label>
              <button type="button" onClick={() => void runExplain()}>
                Run EXPLAIN
              </button>
              {explainOut && (
                <pre style={{ fontSize: 11, overflow: "auto", maxHeight: 240 }}>{explainOut}</pre>
              )}
            </div>
          </section>
        )}
        {tab === "replay" && (
          <section>
            <p style={{ fontSize: 14 }}>Replay a `.pgrec` flight on the shadow DB.</p>
            <div style={{ display: "flex", flexDirection: "column", gap: 8, maxWidth: 560 }}>
              <label>
                pgrec path{" "}
                <input
                  style={{ width: "100%" }}
                  value={replayPgrec}
                  onChange={(e) => setReplayPgrec(e.target.value)}
                  placeholder="~/.pgflight/pgflight-1.pgrec"
                />
              </label>
              <label>
                warmup iters{" "}
                <input
                  type="number"
                  value={replayWarmup}
                  onChange={(e) => setReplayWarmup(Number(e.target.value))}
                  min={0}
                />
              </label>
              <label>
                <input
                  type="checkbox"
                  checked={replayExplain}
                  onChange={(e) => setReplayExplain(e.target.checked)}
                />{" "}
                collect EXPLAIN plans
              </label>
              <div>
                <button type="button" onClick={() => void runReplay("A")}>
                  Replay A
                </button>
                <button type="button" onClick={() => void runReplay("B")} style={{ marginLeft: 8 }}>
                  Replay B
                </button>
              </div>
            </div>
            {replayOut && (
              <pre style={{ fontSize: 11, overflow: "auto", maxHeight: 320 }}>{replayOut}</pre>
            )}
          </section>
        )}
        {tab === "compare" && (
          <section>
            <p style={{ fontSize: 14 }}>Compare replay A vs B (Seq Scan → index improvements).</p>
            <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
              <label>
                baseline replay_id{" "}
                <input value={baselineId} onChange={(e) => setBaselineId(e.target.value)} size={24} />
              </label>
              <label>
                variant replay_id{" "}
                <input value={variantId} onChange={(e) => setVariantId(e.target.value)} size={24} />
              </label>
              <button type="button" onClick={() => void runCompare()}>
                Compare
              </button>
              <button type="button" onClick={() => void refreshShadow()}>
                Shadow status
              </button>
            </div>
            {compareOut && (
              <pre style={{ fontSize: 11, overflow: "auto", maxHeight: 360 }}>{compareOut}</pre>
            )}
            {statusJson && (
              <details style={{ marginTop: 12 }}>
                <summary>Recorder status (JSON)</summary>
                <pre style={{ fontSize: 10 }}>{statusJson}</pre>
              </details>
            )}
          </section>
        )}
      </main>
    </motionWrap>
  );
}

function motionWrap({ children }: { children: React.ReactNode }) {
  return (
    <div style={{ fontFamily: "system-ui", padding: 16, maxWidth: 960, margin: "0 auto" }}>
      {children}
    </div>
  );
}

function TabButton(
  props: React.ButtonHTMLAttributes<HTMLButtonElement> & { active?: boolean },
) {
  const { active, children, ...rest } = props;
  return (
    <button
      type="button"
      style={{
        marginRight: 8,
        fontWeight: active ? 700 : 400,
        opacity: rest.disabled ? 0.5 : 1,
      }}
      {...rest}
    >
      {children}
    </button>
  );
}