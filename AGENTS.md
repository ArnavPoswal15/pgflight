# AGENTS.md: Engineering Rules (`pgflight`)

## Overview
`pgflight` is a **Query Flight Recorder** for PostgreSQL on macOS (Rust): a transparent wire proxy (`:5433`) is the **sensor**; `pgflightd` fingerprints, persists, and detects patterns; the CLI and Tauri UI are **cockpit** clients over `~/.pgflight/control.sock`.

## Workspace
- `crates/pg-wire`: frame codec, separate `FrontendTag` / `BackendTag` enums, startup parsing.
- `crates/pg-proxy`: Tokio proxy, expected-op queue, cancel registry.
- `crates/pg-analyzer`: fingerprinting (`pg_query`, ADR-001), N+1 detection (ADR-002).
- `crates/pg-storage`: ring buffer, SQLite WAL, `.pgrec`.
- `crates/pg-shadow`: safety guard, EXPLAIN runner, replayer (ADR-003).
- `apps/daemon` (`pgflightd`), `apps/cli`, `apps/desktop` (Tauri v2, Phase 4).

## Invariants
1. **Zero-copy hot path.** Never deserialize `DataRow`. Read the 5-byte header, count rows/bytes, forward. Stream `DataRow`/`CopyData` bodies (or any body > 64KB) in chunks; buffer only small control frames.
2. **No heavy work on the forwarding task**: no SQLite, no SQL parsing, no shadow-DB calls.
3. **Non-blocking telemetry.** Use bounded `mpsc::try_send`. When full, the NEWEST event is dropped and an `AtomicU64` counter increments. Never apply backpressure to client traffic.
4. **Mutex discipline.** `ConnState` uses `parking_lot::Mutex`. Both the client-read and upstream-read tasks lock it briefly. Never hold a guard across `.await`.
5. **Push before write.** Push the expected-op entry BEFORE writing the frame upstream.
6. **Async backend messages** (`NoticeResponse`, `ParameterStatus`, `NotificationResponse`) never pop the expected-op queue.
7. **Shadow safety**: AST guard + SELECT-only role + `BEGIN READ ONLY` / `statement_timeout` / `ROLLBACK` + function denylist (ADR-003).
8. **No `unwrap()`/`expect()`** in `crates/*`. Use `thiserror`.
9. **Control socket** `~/.pgflight/control.sock` must be mode `0600`; it exposes SQL and params.
10. Performance claims are **target SLOs measured with pgbench**, not guarantees.

## Verification before finishing any task
```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

## Skills
- `.agents/skills/pg-wire-codec/SKILL.md`
- `.agents/skills/sql-ast-analyzer/SKILL.md`
- `.agents/skills/tauri-ipc-bridge/SKILL.md`

## ADRs
`docs/adr/`: 001 pg_query vs sqlparser, 002 zero-copy vs row inspection, 003 shadow safety, 004 daemon vs embedded.
