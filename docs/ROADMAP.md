# Roadmap (6 weeks)

## Phase 0: harness (days 1-2)
- [ ] `docker compose -f docker/docker-compose.yml up -d` (primary :5432, shadow :5434)
- [ ] Confirm `docker/n_plus_one.py` runs against the primary directly

## Phase 1: wire + pass-through daemon (Week 1)
- [ ] Startup decoder: SSLRequest/GSSENCRequest → `N`, CancelRequest, StartupMessage
- [ ] Header-peek forwarder with chunked streaming of large bodies
- [ ] `pgflightd` listener on :5433, `CancelRegistry` (Vec<u8> secrets)
- [ ] Control socket skeleton (mode 0600)
- **Accept:** `psql` + `sqlx` through :5433; `SELECT pg_sleep(10)` + Ctrl+C cancels

## Phase 2: expected-op queue + txn tracker (Week 2)
- [ ] Expected-op queue, push-before-write, drain-to-Sync on error
- [ ] Multi-statement `Q`, `I`, `s`, Describe/Close, async messages
- [ ] COPY pass-through, txn_id, idle-in-txn
- [ ] `queue_wait_us` vs `exec_us`
- **Accept:** pipelined sqlx, error mid-pipeline, `pg_dump` with zero desyncs

## Phase 3: analysis + storage (Week 3)
- [ ] ADR-001/002 finalized; `pg_query` fingerprint + LRU
- [ ] N+1 burst detector with config thresholds
- [ ] Ring buffer + SQLite WAL batch writer; retention + redaction
- **Accept:** n_plus_one.py yields an N+1 alert with count 50

## Phase 4: clients (Week 4)
- [ ] Daemon event stream with 100ms batching
- [ ] CLI `tail`/`status`; Tauri UI (tray, live stream, leaderboard)
- **Accept:** `pgbench -c 16 -T 15` through proxy; measure added p99 against the 0.3ms SLO

## Phase 5: shadow + replay (Week 5)
- [ ] Guard + SELECT-only role + READ ONLY wrapper
- [ ] EXPLAIN runner + heuristics
- [ ] `.pgrec` writer/reader; A/B replay with snapshot reset and warm-up
- **Accept:** drop/add index → A vs B diff shows Seq Scan removed

## Phase 6: hardening (Week 6)
- [ ] `cargo-fuzz` on pg-wire; criterion benches; pgbench overhead chart
- [ ] README with diagram, benchmarks, 60s demo
