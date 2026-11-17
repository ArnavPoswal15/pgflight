# pgflight Code Audit

**Date:** 2026-11-18  
**Revision:** post-remediation pass (invariants, ADRs, Query Flight Recorder plan)

Initial review: [Code audit + fixes](7d771df0-05fe-41aa-8a8e-0225fec3a063). Security pass: [Security Review](739a866a-6d7b-4e92-8eff-6002adad1590).

### Summary

| Severity | Total | Fixed | Accepted | Open |
|----------|------:|------:|---------:|-----:|
| High | 4 | 4 | 0 | 0 |
| Medium | 10 | 7 | 1 | 2 |
| Low | 5 | 1 | 0 | 4 |
| Info | 2 | 0 | 0 | 2 |
| **All** | **21** | **12** | **1** | **8** |

## Scope

| Area | Coverage |
|------|----------|
| Logic & structure | Wire proxy expected-op queue, telemetry pipeline, recorder batch path, control protocol, analyzer fingerprint/N+1 |
| Code quality | Error handling (`thiserror`), mutex/`await` discipline, tests, workspace boundaries |
| Security | Control socket permissions, parameter/SQL redaction, client path resolution |
| Performance | Zero-copy forwarding, bounded channels, SQLite batching, event-hub flush |

**Reviewed paths:** `crates/pg-wire`, `pg-proxy`, `pg-analyzer`, `pg-storage`, `pgflight-control`, `apps/daemon`, `apps/cli`, `apps/desktop` (Tauri shell).

## Review Criteria

Aligned with `AGENTS.md` invariants 1–10, `docs/adr/` (001–004), and the Query Flight Recorder architecture:

1. Zero-copy hot path — no `DataRow` value deserialization; stream bodies above `STREAM_THRESHOLD`.
2. No heavy work on forwarding tasks (SQLite, `pg_query`, shadow).
3. Telemetry via bounded `try_send`; drop newest on overflow; no client backpressure.
4. `parking_lot::Mutex` on `ConnState` — never hold across `.await`.
5. Expected-op push before upstream write.
6. Async backend messages do not pop the expected-op queue.
7. Shadow safety (ADR-003) — not fully implemented (`pg-shadow` stub).
8. No `unwrap()` / `expect()` in production `crates/*` code.
9. Control socket mode `0600` and awareness of sensitive SQL/params.
10. Performance claims remain SLO targets (pgbench), not guarantees.

## Methodology

1. Static review of hot-path crates (`relay`, `conn_state`, `telemetry`, `frame`) against invariants.
2. Ripgrep scans for `unwrap`/`expect`, `try_send`, mutex usage, redaction flags.
3. Trace of daemon pipeline: proxy → `mpsc` → recorder worker → `FlightRecorder` → SQLite batch task → `EventHub` → control socket.
4. Protocol and persistence review (`pgflight-control`, `sqlite.rs`, ring buffer exports).
5. Targeted fixes for high/medium issues safe in one pass; `cargo fmt`, `clippy -D warnings`, `cargo test --workspace`.

## Findings

| ID | Severity | Category | Location | Issue | Recommendation | Status |
|----|----------|----------|----------|-------|----------------|--------|
| AUD-001 | High | Security | `pgflight-control/src/client.rs` | Default control socket path fell back to `/tmp/pgflight/control.sock` when `HOME` unset — world-writable directory. | Require `HOME`; surface `ControlError::NoHome`. | **fixed** |
| AUD-002 | Medium | Quality | `pg-analyzer/src/fingerprint.rs` | `expect("nz")` on LRU capacity in non-test code. | Use `const` `NonZeroUsize` fallback. | **fixed** |
| AUD-003 | Medium | Quality | `pg-analyzer/src/stats.rs` | `expect("histogram bounds")` in `Default`. | Store `Option<Histogram>`; degrade gracefully if init fails. | **fixed** |
| AUD-004 | Medium | Performance | `apps/daemon/src/event_hub.rs` | Each 100ms tick called `ring_delta(0)` (full ring scan) for cursor update. | Use `FlightRecorder::latest_ring_event_id()`. | **fixed** |
| AUD-005 | Medium | Correctness | `pg-storage/src/recorder.rs` | SQLite batch writer dropped pending rows when channel closed without final flush. | Flush `pending` on receiver close. | **fixed** |
| AUD-006 | Medium | Correctness | `pg-proxy/src/relay.rs` | Large frontend frames (`> STREAM_THRESHOLD`) skipped `note_execute_enqueued` for `Execute`/`Query`. | Mirror small-frame timing hooks on stream path. | **fixed** |
| AUD-007 | Medium | Correctness | `pg-proxy/src/relay.rs` | Large `Parse`/`Bind`/`Query` bodies not parsed — empty body passed to `push_frontend`; SQL/params missing from telemetry while expected-op queue remains consistent. | Peek/stream-parse message headers for SQL capture, or document limitation. | **accepted** — documented in `FEATURES_AND_LOGIC.md` |
| AUD-008 | Medium | Security | `apps/daemon/src/control.rs` | Unix socket mode `0600` but no peer credential check (any local user who can reach socket path if misconfigured). | Optional `getpeereid` / SO_PEERCRED gate. | open |
| AUD-009 | Low | Security | `apps/daemon/src/control.rs` | `Export` returns persisted rows without redaction filter at API layer (relies on storage redaction at insert). | Document; optionally strip `params_json` when `redact_params` in export path. | **fixed** — export emits timing/metadata only (no `raw_sql` / `params_json`) |
| AUD-010 | Low | Architecture | `crates/pg-shadow/src/lib.rs` | Shadow EXPLAIN/replayer stub; ADR-003 layers not enforced. | Implement AST guard + READ ONLY role before enabling shadow. | open |
| AUD-011 | Low | Correctness | `pg-analyzer/src/n1.rs` | `is_select_like` uses SQL prefix heuristic, not AST — false positives/negatives possible. | Tie to `pg_query` statement class when fingerprinting. | open |
| AUD-012 | Low | Performance | `pg-storage/src/recorder.rs` | `batch_writer_task` holds `parking_lot::Mutex` during synchronous SQLite work on tokio runtime (can block other tasks on same worker). | Offload inserts to `spawn_blocking` or dedicated thread. | open |
| AUD-013 | Low | Quality | `apps/daemon/src/analysis.rs` | Multiple mutex acquisitions per fingerprint (`cache`, `leaderboard`, `n1`, `pending_anomaly`) — acceptable off hot path but increases deadlock review surface. | Consider single analysis lock or structured state object. | open |
| AUD-014 | Info | Tests | `crates/pg-proxy/tests/phase_integration.rs` | Integration tests gated on env (`PGFLIGHT_IT_*`); no in-process unit tests for streaming large frames. | Add mock-stream tests for `relay` large-body path. | open |
| AUD-015 | Info | Quality | `apps/desktop/src-tauri` | `fetch_patterns` returns empty vec; cockpit UI not wired to subscribe stream. | Complete Phase 4 IPC per Tauri skill. | open |
| AUD-016 | High | Security | `config.rs`, `pg-storage` defaults | Plaintext params/SQL on disk when redaction off. | Default `redact_params=true`; dev uses `--no-redact-params`. | **fixed** |
| AUD-017 | High | Security | `sqlite.rs` | SQLite DB/world-readable via umask. | Set data dir `0700`, DB `0600` on open. | **fixed** |
| AUD-018 | High | Security | `upstream.rs`, `config.rs` | `--no-upstream-tls-verify` enables MITM. | Require `--insecure-upstream-tls`; startup error log. | **fixed** |
| AUD-019 | Medium | Security | `config.rs` | Non-loopback `listen` without guard. | Reject unless `--allow-network-bind`. | **fixed** |
| AUD-020 | Medium | Security | `tauri.conf.json` | CSP disabled (`null`). | Baseline `default-src 'self'` CSP. | **fixed** |
| AUD-021 | Medium | Security | Security review [739a866a](739a866a-6d7b-4e92-8eff-6002adad1590) | Control socket auth = UID only; subscribe streams canonical SQL. | Document threat model; optional peer cred gate (AUD-008). | open |

### Hot-path checklist (pass / notes)

| Invariant | Result |
|-----------|--------|
| Zero-copy `DataRow` | **Pass** — `datarow_field_count` only; large bodies streamed |
| No heavy work on relay | **Pass** |
| `try_send` telemetry | **Pass** — `pg-proxy/telemetry.rs`, storage `batch_tx` |
| Mutex not held across `.await` | **Pass** — relay locks scoped to sync sections |
| Push before write | **Pass** — state updated before `write_frame` / stream |
| Async backend messages | **Pass** — `BackendTag::is_async()` early return |
| Control socket `0600` | **Pass** — `serve_control_socket` sets permissions |
| Redaction at persistence | **Pass** — `FlightRecorder::append` strips params/raw when enabled |

## Remediation Summary

**Fixed / accepted:** AUD-001–006 ([audit pass](7d771df0-05fe-41aa-8a8e-0225fec3a063)), AUD-009, AUD-016–020 ([Security Review](739a866a-6d7b-4e92-8eff-6002adad1590)), AUD-007 documented limitation.

**Verification (required after changes):**

```bash
source $HOME/.cargo/env
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```