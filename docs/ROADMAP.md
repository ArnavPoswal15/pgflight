# Roadmap (6 weeks)

## Phase 0: harness (days 1-2)
- [x] `docker compose -f docker/docker-compose.yml up -d` (primary :5432, shadow :5434)
- [x] Confirm `docker/n_plus_one.py` runs against the primary directly (`scripts/verify-phase0.sh`, `PGURL`)

## Phase 1: wire + pass-through daemon (Week 1)
- [x] Startup decoder: SSLRequest/GSSENCRequest → `N`, CancelRequest, StartupMessage
- [x] Header-peek forwarder with chunked streaming of large bodies
- [x] `pgflightd` listener on :5433, `CancelRegistry` (Vec<u8> secrets)
- [x] Control socket skeleton (mode 0600)
- **Accept:** `psql` + `sqlx` through :5433; `SELECT pg_sleep(10)` + Ctrl+C cancels

## Phase 2: expected-op queue + txn tracker (Week 2)
- [x] Expected-op queue, push-before-write, drain-to-Sync on error
- [x] Multi-statement `Q`, `I`, `s`, Describe/Close, async messages
- [x] COPY pass-through, txn_id, idle-in-txn
- [x] `queue_wait_us` vs `exec_us`
- **Accept:** pipelined sqlx, error mid-pipeline, `pg_dump` with zero desyncs

## Phase 3: Query Flight Recorder core (Week 3)
- [x] ADR-001/002 implemented; `pg_query` fingerprint + LRU (`pg-analyzer`)
- [x] N+1 burst detector (`N1Detector`); `FlightRecorder` ring + SQLite WAL (250ms)
- [x] `pgflightd --record`, `--redact-params`, `--data-dir`; `scripts/accept-phase3.sh`
- **Accept:** `n_plus_one.py` through :5433 → `anomalies.repeat_count >= 50` (needs Postgres + shop DB)

## Phase 4: cockpit clients (Week 4)
- [x] `pgflight-control` protocol; subscribe/status; EventHub 100ms `FlightLogBatch`
- [x] CLI `pgflight status|tail|ping|export`; Tauri v2 scaffold (Live Log + Patterns; Replay/Compare disabled)
- **Accept:** `pgbench -c 16 -T 15` direct vs :5433 — document p99 delta vs 0.3ms SLO (measurement)

## Phase 5: replay & plan compare (Week 5)
- [ ] Guard + SELECT-only role + READ ONLY wrapper
- [ ] EXPLAIN runner + heuristics
- [ ] `.pgrec` ZSTD writer/reader; A/B replay with snapshot reset and warm-up
- **Accept:** drop/add index → A vs B diff shows Seq Scan removed

## Phase 6: hardening (Week 6)
- [ ] `cargo-fuzz` on pg-wire; criterion benches; pgbench overhead chart
- [ ] README with diagram, benchmarks, 60s demo
