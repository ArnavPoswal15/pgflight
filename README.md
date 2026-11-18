<p align="center">
  <img src="docs/assets/pgflight-logo.png" alt="pgflight — Observe. Detect. Replay. Neon PostgreSQL flight control for macOS." width="800">
</p>

<p align="center"><em>Transparent proxy, analyzer, and shadow replay — one daemon between your app and Postgres.</em></p>

# pgflight — Query Flight Recorder

**Record what happened. Spot expensive patterns. Investigate regressions.** A **Query Flight Recorder** for PostgreSQL on macOS — in Rust.

The transparent proxy on `:5433` is the **sensor**: traffic keeps flowing with near–zero-copy forwarding, while a bounded side channel fingerprints queries, writes a durable flight log (ring buffer + SQLite WAL), and flags N+1 bursts. Phase 5 adds `.pgrec` replay and shadow plan compare. The CLI and Tauri desktop app are **cockpit displays** over `~/.pgflight/control.sock` (mode `0600`), not alternate proxies.

**License:** MIT · **Status:** production-ready for **single-user local macOS** (Phases 0–5; see [Roadmap](docs/ROADMAP.md)) · **Platform:** macOS (daemon + UI) · **Rust:** 1.78+ (`rust-toolchain.toml`)

Latency SLO claims require a local run of [`scripts/bench-overhead.sh`](scripts/bench-overhead.sh); see [benchmarks](docs/benchmarks/overhead.md).

```
App / ORM ──► :5433  pgflightd (proxy)  ──►  Postgres :5432
                    │  bounded mpsc (try_send)
                    ▼
             analyzer + storage ──► ~/.pgflight/control.sock ──► CLI / Tauri UI
                    │
                    ▼
             shadow worker ──► Shadow Postgres :5434  (EXPLAIN / replay)
```

---

## Why pgflight?

pgflight is built for teams that feel query pain in production but do not want another “always-on” observer slowing every request.

- **🔀 Drop-in proxy.** Point your connection string at `127.0.0.1:5433`. SSL negotiation, pipelined extended queries, `COPY`, and cancel keys behave like talking to Postgres directly — the data plane never parses SQL or touches disk.

- **📊 See what actually runs.** Fingerprints (via `pg_query`), latency histograms, row counts, and transaction boundaries land in SQLite with bounded telemetry — when the analysis channel is full, **newest** events are dropped, not client traffic.

- **🐛 Catch N+1 early.** Frequency-based detection inside transactions flags repeated read patterns before they become a 50-query loop in prod (see `docker/n_plus_one.py` for a reproducer).

- **🪞 Shadow safely.** Read-only shadow role, AST guard, `BEGIN READ ONLY`, `statement_timeout`, and a function denylist — EXPLAIN and replay never mutate your primary.

- **✈️ Record and compare.** ZSTD `.pgrec` flight logs plus snapshot reset on shadow enable A/B replays (e.g. index before/after) without trusting shadow wall-clock as production truth.

- **🖥️ One daemon, many clients.** `pgflightd` holds the proxy and control socket (`~/.pgflight/control.sock`, mode `0600`). CLI and Tauri chat with the daemon; a UI crash does not stop forwarding.

- **📐 Engineered for agents.** `AGENTS.md`, ADRs under `docs/adr/`, and `.agents/skills/` encode invariants (zero-copy `DataRow`, push-before-write, no heavy work on the forward path) so humans and coding agents implement the same contract.

> **Note:** [Phases 0–5](docs/ROADMAP.md) are implemented (harness through shadow replay and plan compare). Phase 6 adds fuzzing, criterion benches, and measured overhead — see [benchmarks](docs/benchmarks/overhead.md).

---

## Quick start

### 1. Prerequisites

| Tool | Purpose |
|------|---------|
| [Rust](https://rustup.rs/) (stable via `rust-toolchain.toml`) | Build workspace |
| [Docker](https://docs.docker.com/get-docker/) | Primary + shadow Postgres for local dev |
| `psql` or any Postgres client | Smoke tests through the proxy |

Optional: C toolchain for `pg_query` (ADR-001), `pgbench` + `./scripts/bench-overhead.sh` for latency SLO checks, `cargo-fuzz` (nightly) for `crates/pg-wire/fuzz`.

### 2. Start databases

Primary on **:5432**, shadow on **:5434**, seeded `shop` schema:

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

### 3. Build the workspace

```bash
cargo build --workspace
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

### 4. Run the daemon

```bash
cargo run -p pgflightd -- --listen 127.0.0.1:5433 --upstream 127.0.0.1:5432
```

TLS upstream (optional): add `--upstream-tls` and, if needed, `--upstream-ca path/to/ca.pem`.

Then route the app through the proxy:

```bash
psql "postgres://postgres:postgres@127.0.0.1:5433/shop"
```

Control socket (JSON ping): `~/.pgflight/control.sock` (mode `0600`).

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

The script issues one parent `SELECT` and fifty per-row child queries — the shape pgflight is designed to flag.

### 6. CLI and desktop UI (Phase 4)

```bash
cargo run -p pgflight-cli -- tail    # when implemented
```

Desktop: Tauri v2 + React, thin client of the control socket — see [apps/desktop/README.md](apps/desktop/README.md).

**60s demo:** `./scripts/demo-60s.sh` (compose + `pgflightd` + `n_plus_one.py` + `pgflight status`).

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
| Engineering audit (security, perf, invariants) | — | [AUDIT.md](docs/AUDIT.md) |
| Native UI | `apps/desktop` (Phase 4) | [apps/desktop/README.md](apps/desktop/README.md) |

---

## Architecture in one minute

Three planes — keep them separate:

1. **Data plane** — forward bytes; stream large frames; lock `ConnState` only for brief, non-`await` sections.
2. **Control / analysis plane** — fingerprint, detect, persist, shadow work over bounded channels (`try_send`).
3. **Presentation** — CLI and Tauri over Unix socket; no embedded proxy in the UI.

Failure isolation: SQLite backpressure drops telemetry; shadow unreachable → status only; UI exit → proxy continues.

Full detail: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) · behavior catalog: [docs/FEATURES_AND_LOGIC.md](docs/FEATURES_AND_LOGIC.md).

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
| [001](docs/adr/ADR-001-pg_query-vs-sqlparser.md) | `pg_query` for fingerprinting and shadow guard |
| [002](docs/adr/ADR-002-zero-copy-vs-row-inspection.md) | Zero-copy forwarding vs row inspection for N+1 |
| [003](docs/adr/ADR-003-shadow-safety.md) | Shadow DB safety model |
| [004](docs/adr/ADR-004-daemon-vs-embedded.md) | Standalone daemon vs embedded proxy |

---

## Performance target

**&lt; 0.3 ms p99 added latency** on the proxy path vs direct Postgres is a **target SLO**, not a guarantee — measure with `./scripts/bench-overhead.sh` (`pgbench` on :5432 vs :5433). Results: [docs/benchmarks/overhead.md](docs/benchmarks/overhead.md). Micro-benches: `cargo bench -p pg-wire -p pg-analyzer -p pg-storage`.

---

## Security (local)

Control socket **0600**, optional future peer-uid gate — [docs/SECURITY.md](docs/SECURITY.md).

---

## Contributing

1. Read [AGENTS.md](AGENTS.md) — invariants matter more than feature count on the hot path.
2. Pick a phase in [docs/ROADMAP.md](docs/ROADMAP.md) and its acceptance checks.
3. Before opening a PR:

   ```bash
   cargo fmt --all -- --check
   cargo clippy --workspace --all-targets -- -D warnings
   cargo test --workspace
   ```

Skills for agents: `.agents/skills/pg-wire-codec/`, `sql-ast-analyzer/`, `tauri-ipc-bridge/`.

---

## Related reading

- [Roadmap (6 weeks)](docs/ROADMAP.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Features & logic](docs/FEATURES_AND_LOGIC.md)