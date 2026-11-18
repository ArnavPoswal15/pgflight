<p align="center">
  <img src="docs/assets/pgflight-logo.png" alt="pgflight — Observe. Detect. Replay. Neon PostgreSQL flight control for macOS." width="800">
</p>

<h1 align="center">pgflight</h1>

<p align="center">
  <strong>Observe. Detect. Replay.</strong><br>
  <em>Transparent proxy, analyzer, and shadow replay — one daemon between your app and Postgres.</em>
</p>

<p align="center">
  <a href="LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-blue.svg"></a>
  <img alt="Status: early development" src="https://img.shields.io/badge/status-early%20development-orange.svg">
  <img alt="Platform: macOS" src="https://img.shields.io/badge/platform-macOS-lightgrey.svg">
  <img alt="Rust 1.78+" src="https://img.shields.io/badge/rust-1.78%2B-dea584.svg">
</p>

<p align="center">
  <a href="#quick-start">Quick start</a> ·
  <a href="#architecture">Architecture</a> ·
  <a href="docs/ROADMAP.md">Roadmap</a> ·
  <a href="docs/ARCHITECTURE.md">Docs</a> ·
  <a href="#contributing">Contributing</a>
</p>

**License:** MIT · **Status:** production-ready for **single-user local macOS** (Phases 0–5; see [Roadmap](docs/ROADMAP.md)) · **Platform:** macOS (daemon + UI) · **Rust:** 1.78+ (`rust-toolchain.toml`)

Latency SLO claims require a local run of [`scripts/bench-overhead.sh`](scripts/bench-overhead.sh); see [benchmarks](docs/benchmarks/overhead.md).

**pgflight** is a transparent PostgreSQL wire proxy, N+1 detector, and shadow query replayer for macOS, written in Rust.

Put it between your app and Postgres. Traffic keeps flowing with near-zero-copy forwarding, while a side channel fingerprints queries, spots N+1 patterns, records flights, and safely replays them against a shadow database for `EXPLAIN` plans and A/B benchmarks. The daemon owns the hot path; the CLI and desktop UI are thin clients over a local control socket.

> **Status:** early development. The Docker harness and N+1 demo script work today; the pass-through proxy and analysis pipeline are active roadmap work ([Phases 1–6](docs/ROADMAP.md)).

---

## Architecture

```mermaid
%%{init: {"flowchart": {"nodeSpacing": 50, "rankSpacing": 70, "padding": 24, "curve": "basis"}}}%%
flowchart TB
  %% Dark text on light fills for readable contrast in light and dark themes
  classDef cluster fill:#f8fafc,stroke:#0f172a,stroke-width:2px,rx:8,ry:8,color:#0f172a,font-size:14px;
  classDef clients fill:#fff1f2,stroke:#b91c1c,stroke-width:1.5px,rx:6,ry:6,color:#071133,font-size:14px;
  classDef gate fill:#eef2ff,stroke:#3730a3,stroke-width:1.5px,rx:6,ry:6,color:#071133,font-size:14px;
  classDef svc fill:#fff7ed,stroke:#b45309,stroke-width:1.5px,rx:6,ry:6,color:#071133,font-size:14px;
  classDef store fill:#f0fdf4,stroke:#166534,stroke-width:1.5px,rx:6,ry:6,color:#071133,font-size:14px;
  classDef db fill:#ecfeff,stroke:#075985,stroke-width:1.5px,rx:6,ry:6,color:#071133,font-size:14px;
  classDef inv fill:#f8fafc,stroke:#064e3b,stroke-width:1.5px,rx:6,ry:6,color:#071133,font-size:13px;
  classDef pad fill:none,stroke:none,color:transparent;
  linkStyle default stroke:#94a3b8,stroke-width:1.2px;

  subgraph clients_g [Clients]
    direction LR
    APP["App / ORM<br/>connects to :5433"]:::clients
    PSQL["psql / DB tools"]:::clients
    DEMO["docker/n_plus_one.py<br/>N+1 demo"]:::clients
  end

  subgraph daemon [pgflightd — one daemon]
    direction TB

    subgraph dataplane [Data plane · hot path]
      PROXY["pg-proxy :5433<br/>zero-copy forwarding<br/>expected-op queue · cancel registry"]:::gate
    end

    subgraph analysis [Analysis plane · bounded channels]
      direction LR
      AN["pg-analyzer<br/>fingerprints · N+1 detection"]:::svc
      ST["pg-storage<br/>ring buffer · SQLite WAL · .pgrec"]:::store
      SW["pg-shadow<br/>AST guard · EXPLAIN · replay"]:::svc
    end

    CTL["Control socket<br/>~/.pgflight/control.sock · mode 0600"]:::gate
  end

  subgraph databases [Databases]
    direction LR
    PG[("Primary Postgres<br/>:5432")]:::db
    SH[("Shadow Postgres :5434<br/>read-only role")]:::db
  end

  subgraph presentation [Presentation · thin clients]
    direction LR
    CLI["pgflight-cli"]:::clients
    UI["Tauri v2 + React UI"]:::clients
  end

  subgraph invariants [Core invariants]
    direction LR
    I1["Never block the client<br/>push-before-write"]:::inv
    I2["Zero-copy DataRow<br/>no SQL parsing on hot path"]:::inv
    I3["Shadow never mutates<br/>the primary"]:::inv
  end

  %% Request path
  APP -->|wire protocol| PROXY
  PSQL -->|wire protocol| PROXY
  DEMO -->|wire protocol| PROXY
  PROXY <-->|forwarded bytes| PG

  %% Lossy telemetry tap and analysis pipeline
  PROXY -.->|try_send · drops newest if full| AN
  AN --> ST
  ST --> SW
  SW -->|BEGIN READ ONLY| SH

  %% Control plane
  CTL <--> AN
  CTL <--> ST
  CTL <--> SW
  CLI <-->|Unix socket| CTL
  UI <-->|Unix socket| CTL

  %% Invariants (dashed, so they do not cover nodes)
  PROXY -.-> I1
  PROXY -.-> I2
  SW -.-> I3

  %% Invisible spacer at the bottom-right so the viewer's zoom / pan buttons
  %% sit on empty space instead of covering the diagram
  PADR[" "]:::pad
  I3 ~~~ PADR

  class clients_g,daemon,dataplane,analysis,databases,presentation,invariants cluster;
```

Solid arrows are the request path or control flow. The dotted arrow from `pg-proxy` to `pg-analyzer` is a lossy telemetry tap: the proxy never blocks on analysis.

Three planes, kept strictly separate:

1. **Data plane:** forwards bytes, streams large frames, and locks `ConnState` only for brief, non-`await` sections. It never parses SQL or touches disk.
2. **Analysis plane:** fingerprinting, detection, persistence, and shadow work run over bounded channels (`try_send`).
3. **Presentation:** the CLI and Tauri UI talk to the daemon over a Unix socket. There is no embedded proxy in the UI.

**Failure isolation:** SQLite backpressure drops telemetry, not client traffic. If the shadow database is unreachable, only status is affected. If the UI exits, the proxy keeps forwarding.

Full detail: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) · behavior catalog: [docs/FEATURES_AND_LOGIC.md](docs/FEATURES_AND_LOGIC.md).

---

## Why pgflight?

pgflight is for teams that feel query pain in production but don't want another "always-on" observer slowing every request.

| Feature | What it does |
|---|---|
| **Drop-in proxy** | Point your connection string at `127.0.0.1:5433`. SSL negotiation, pipelined extended queries, `COPY`, and cancel keys behave as if you were talking to Postgres directly. |
| **See what actually runs** | Fingerprints (via `pg_query`), latency histograms, row counts, and transaction boundaries land in SQLite with bounded telemetry — when the analysis channel is full, the **newest** events are dropped, never client traffic. |
| **Catch N+1 early** | Frequency-based detection inside transactions flags repeated read patterns before they become a 50-query loop in prod. See `docker/n_plus_one.py` for a reproducer. |
| **Shadow safely** | Read-only shadow role, AST guard, `BEGIN READ ONLY`, `statement_timeout`, and a function denylist. `EXPLAIN` and replay never mutate your primary. |
| **Record and compare** | ZSTD `.pgrec` flight logs plus snapshot reset on shadow enable A/B replays (e.g. index before/after) without treating shadow wall-clock as production truth. |
| **One daemon, many clients** | `pgflightd` holds the proxy and control socket (`~/.pgflight/control.sock`, mode `0600`). CLI and Tauri chat with the daemon; a UI crash does not stop forwarding. |
| **Engineered for agents** | `AGENTS.md`, ADRs under `docs/adr/`, and `.agents/skills/` encode the invariants (zero-copy `DataRow`, push-before-write, no heavy work on the forward path) so humans and coding agents implement the same contract. |

> **Note:** [Phases 0–5](docs/ROADMAP.md) are implemented (harness through shadow replay and plan compare). Phase 6 adds fuzzing, criterion benches, and measured overhead — see [benchmarks](docs/benchmarks/overhead.md).

---

## Quick start

### 1. Prerequisites

| Tool | Purpose |
|------|---------|
| [Rust](https://rustup.rs/) (stable, pinned by `rust-toolchain.toml`) | Build the workspace |
| [Docker](https://docs.docker.com/get-docker/) | Primary + shadow Postgres for local dev |
| `psql` or any Postgres client | Smoke tests through the proxy |

Optional: a C toolchain for `pg_query` ([ADR-001](docs/adr/ADR-001-pg_query-vs-sqlparser.md)), `pgbench` for latency SLO checks, and `cargo-fuzz` (nightly) for `crates/pg-wire/fuzz`.

### 2. Start the databases

This starts the primary on **:5432**, the shadow on **:5434**, and a seeded `shop` schema:

```bash
docker compose -f docker/docker-compose.yml up -d
```

Shadow EXPLAIN/replay uses a SELECT-only role on **:5434** (see [`docker/README.md`](docker/README.md)):

```text
postgresql://pgflight_shadow:pgflight_shadow@127.0.0.1:5434/shop
```

Verify directly (before the proxy exists):

```bash
psql "postgres://postgres:postgres@127.0.0.1:5432/shop" -c 'SELECT count(*) FROM orders;'
```

### 3. Build and test

```bash
cargo build --workspace
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

### 4. Run the daemon *(roadmap: Phase 1)*

> Today `pgflightd` prints a Phase 1 placeholder. The invocation below is the intended one once Phase 1 lands. Acceptance criteria (`psql` + ORM + cancel) are in [docs/ROADMAP.md](docs/ROADMAP.md).

```bash
cargo run -p pgflightd -- --listen 127.0.0.1:5433 --upstream 127.0.0.1:5432
```

Then route your app through the proxy:

```bash
psql "postgres://postgres:postgres@127.0.0.1:5433/shop"
```

### 5. Reproduce an N+1 pattern *(roadmap: Phase 3)*

### Verify Phases 0–2

```bash
make harness                    # Phase 0: compose + n_plus_one on :5432
./scripts/accept-phase1.sh      # Phase 1: psql through pgflightd
./scripts/accept-phase2.sh      # Phase 2: ignored Rust tests (daemon must be running)
cargo test -p pg-proxy -- --ignored
```

GitLab CI runs fmt, clippy, unit tests, and Docker integration — see [.gitlab-ci.yml](.gitlab-ci.yml).

### 5. Reproduce an N+1 pattern

Run the demo against **:5433** (daemon with `--record` recommended):

```bash
pip install 'psycopg[binary]'
python docker/n_plus_one.py
```

The script issues one parent `SELECT` and fifty per-row child queries, which is exactly the shape pgflight is designed to flag.

### 6. CLI and desktop UI *(roadmap: Phase 4)*

```bash
cargo run -p pgflight-cli -- tail    # when implemented
```

The desktop app is Tauri v2 + React and a thin client of the control socket. See [apps/desktop/README.md](apps/desktop/README.md).

**60s demo:** `./scripts/demo-60s.sh` (compose + `pgflightd` + `n_plus_one.py` + `pgflight status`).

> **Tip:** Don't scaffold the UI until `psql`, an ORM, and `Ctrl+C` cancel all work cleanly through the proxy.

### Default ports

| Port | Service |
|------|---------|
| `5432` | Primary Postgres |
| `5433` | pgflight proxy (point your app here) |
| `5434` | Shadow Postgres |

**Tip:** Do not scaffold the UI until `psql`, an ORM, and `Ctrl+C` cancel work cleanly through the proxy.

---

## What you get

| Capability | Where it lives | Doc |
|------------|----------------|-----|
| Frame codec & startup parsing | `crates/pg-wire` | [Architecture](docs/ARCHITECTURE.md) |
| Tokio proxy, expected-op queue, cancel registry | `crates/pg-proxy` | [Features](docs/FEATURES_AND_LOGIC.md) |
| Fingerprinting & N+1 detection | `crates/pg-analyzer` | [ADR-002](docs/adr/ADR-002-zero-copy-vs-row-inspection.md) |
| Ring buffer, SQLite WAL, `.pgrec` | `crates/pg-storage` | [Architecture](docs/ARCHITECTURE.md) |
| Shadow guard & EXPLAIN runner | `crates/pg-shadow` | [ADR-003](docs/adr/ADR-003-shadow-safety.md) |
| Long-running proxy + control socket | `apps/daemon` (`pgflightd`) | [ADR-004](docs/adr/ADR-004-daemon-vs-embedded.md) |
| Operator CLI | `apps/cli` | — |
| Native UI | `apps/desktop` (Phase 4) | [apps/desktop/README.md](apps/desktop/README.md) |

---

## Repository layout

```
pgflight/
├── crates/
│   ├── pg-wire/       # PostgreSQL wire protocol codec
│   ├── pg-proxy/      # Connection proxy & expected-op queue
│   ├── pg-analyzer/   # Fingerprints & N+1
│   ├── pg-storage/    # Ring buffer, SQLite, .pgrec
│   └── pg-shadow/     # Safe EXPLAIN & replay
├── apps/
│   ├── daemon/        # pgflightd
│   ├── cli/           # Control socket client
│   └── desktop/       # Tauri UI (Phase 4)
├── docker/            # Compose, seed SQL, N+1 demo
├── docs/              # Architecture, roadmap, ADRs
└── AGENTS.md          # Engineering rules for contributors & agents
```

---

## Design decisions (ADRs)

| ADR | Decision |
|-----|----------|
| [001](docs/adr/ADR-001-pg_query-vs-sqlparser.md) | `pg_query` for fingerprinting and the shadow guard |
| [002](docs/adr/ADR-002-zero-copy-vs-row-inspection.md) | Zero-copy forwarding vs row inspection for N+1 |
| [003](docs/adr/ADR-003-shadow-safety.md) | Shadow DB safety model |
| [004](docs/adr/ADR-004-daemon-vs-embedded.md) | Standalone daemon vs embedded proxy |

---

## Performance target

**&lt; 0.3 ms p99 added latency** on the proxy path vs direct Postgres is a **target SLO**, not a guarantee — measure with `./scripts/bench-overhead.sh` (`pgbench` on :5432 vs :5433). Results: [docs/benchmarks/overhead.md](docs/benchmarks/overhead.md). Micro-benches: `cargo bench -p pg-wire -p pg-analyzer -p pg-storage`. Phase 4 acceptance details are in [docs/ROADMAP.md](docs/ROADMAP.md).

---

## Security (local)

Control socket **0600**, optional future peer-uid gate — [docs/SECURITY.md](docs/SECURITY.md).

---

## Contributing

1. Read [AGENTS.md](AGENTS.md). Invariants matter more than feature count on the hot path.
2. Pick a phase in [docs/ROADMAP.md](docs/ROADMAP.md) and its acceptance checks.
3. Before opening a PR, run:

   ```bash
   cargo fmt --all -- --check
   cargo clippy --workspace --all-targets -- -D warnings
   cargo test --workspace
   ```

Skills for agents live in `.agents/skills/` (`pg-wire-codec/`, `sql-ast-analyzer/`, `tauri-ipc-bridge/`).

---

## Related reading

- [Roadmap (6 weeks)](docs/ROADMAP.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Features & logic](docs/FEATURES_AND_LOGIC.md)

---

<p align="center"><sub>MIT licensed · Built with Rust 🦀</sub></p>
