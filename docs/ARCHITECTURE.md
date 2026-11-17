# Architecture

## Planes
1. **Data plane** (`pgflightd` / `pg-proxy`): forwards frames. No disk I/O, no SQL parsing, no guard held across `.await`.
2. **Control/analysis plane**: consumes `RawQueryEvent` over bounded `mpsc` (cap 8192), fingerprints, detects anomalies, persists, runs shadow work.
3. **Presentation**: CLI and Tauri UI as clients of the daemon's Unix socket (mode 0600).

```
App ─► pgflightd :5433 ─► Postgres :5432
          │ try_send
          ▼
   analyzer ─► ring buffer (5k) ─► SQLite WAL (250ms batches)
          │                      └► event stream (100ms batches) ─► control.sock ─► CLI / Tauri
          └► shadow worker (semaphore: 2) ─► Shadow Postgres :5434
```

## Tasks per connection
- Task A: client → upstream. Parses frontend frames, **pushes expected-ops (briefly locks `ConnState`) before writing upstream**.
- Task B: upstream → client. Resolves expected-ops, counts rows, tracks txn state (briefly locks `ConnState`), `try_send`s events.
Both use `Arc<parking_lot::Mutex<ConnState>>`; guards are never held across `.await`.

## Framing
Small control frames are buffered; `DataRow`/`CopyData` and any body > 64KB are streamed in chunks, so memory per query is O(1) regardless of row size.

## Failure isolation
| Scenario | Behavior |
|---|---|
| SQLite spike | Channel fills; newest telemetry dropped, `dropped_events++`. Clients unaffected |
| 500k-row SELECT | Only counters increment |
| Ctrl+C | CancelRequest matched in `CancelRegistry`, forwarded out of band |
| Partial TCP read | Header peek returns `None` until enough bytes |
| Shadow unreachable | Explain fails with status card; proxy unaffected |
| UI crash | Daemon keeps proxying |

## Storage
```sql
PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;
CREATE TABLE fingerprints (fingerprint_id INTEGER PRIMARY KEY, canonical_sql TEXT NOT NULL, first_seen_ms INTEGER NOT NULL); -- u64 stored as bitcast i64
CREATE TABLE query_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT, connection_id INTEGER NOT NULL, txn_id INTEGER,
  fingerprint_id INTEGER NOT NULL REFERENCES fingerprints(fingerprint_id),
  raw_sql TEXT, params_json TEXT, truncated INTEGER DEFAULT 0,
  queue_wait_us INTEGER NOT NULL, exec_us INTEGER NOT NULL, rows_returned INTEGER NOT NULL,
  error_code TEXT, app_name TEXT, created_at_ms INTEGER NOT NULL);
CREATE INDEX idx_events_fp_time ON query_events(fingerprint_id, created_at_ms DESC);
CREATE INDEX idx_events_txn ON query_events(txn_id);
CREATE TABLE anomalies (
  id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, fingerprint_id INTEGER NOT NULL,
  parent_fingerprint INTEGER, repeat_count INTEGER NOT NULL, total_wasted_us INTEGER NOT NULL,
  confidence TEXT NOT NULL, created_at_ms INTEGER NOT NULL);
```
Retention pruner (`MAX_AGE_DAYS`, `MAX_DB_SIZE_MB`) and **redaction mode** (store canonical SQL only, no params) are required before any sharing of the DB.

## `.pgrec` format (zstd blocks)
Header 20 bytes: magic `PGREC001` (8) + flags u32 (4) + start_unix_us u64 (8).
Record: `delta_us u32`, `client_id u32`, `txn_id u64`, `kind u8` (query / session-state / txn-boundary), `sql_len u32 + sql`, `param_oids`, `params_len u32 + params`.
Session `SET` statements and BEGIN/COMMIT/ROLLBACK are recorded so transactions replay faithfully.

## Performance
"< 0.3ms p99 added latency" is a **target SLO**, verified with `pgbench` direct (:5432) vs proxied (:5433).
