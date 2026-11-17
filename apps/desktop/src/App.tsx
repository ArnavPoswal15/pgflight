import { useState } from "react";

type Tab = "live" | "patterns" | "replay" | "compare";

export default function App() {
  const [tab, setTab] = useState<Tab>("live");

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
          <TabButton disabled title="Coming in Phase 5">
            Replay
          </TabButton>
          <TabButton disabled title="Coming in Phase 5">
            Compare plans
          </TabButton>
        </nav>
      </header>
      <main>
        {tab === "live" && (
          <section>
            <p>Live flight log (SQL fingerprint, exec_ms, rows) — connect pgflightd control socket.</p>
          </section>
        )}
        {tab === "patterns" && (
          <section>
            <p>Expensive fingerprints (p95, count) and N+1 anomaly cards.</p>
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