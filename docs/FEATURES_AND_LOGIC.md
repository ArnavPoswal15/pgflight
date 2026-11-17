# Features and Logic

## 1. Transparent proxy
Startup handling for SSLRequest / GSSENCRequest (`N`), CancelRequest (routed) and StartupMessage (forward, typed mode). `K` registers cancel keys; `v` is forwarded.

## 2. Extended query and pipelining
Expected-op queue (see `pg-wire-codec` skill). Handles Parse/Bind/Describe/Execute/Close/Sync, multi-statement `Q`, `I`, `s`, mid-pipeline errors (drain to Sync), async `N`/`S`/`A`.
Params are captured up to 64KB with OIDs; larger are marked `truncated`.

## 3. Transactions
`Z` status `I→T` opens a `txn_id`; `T/E→I` closes it. Idle-in-transaction fires when idle > 2s with an empty queue.

## 4. Fingerprint leaderboard
`pg_query` fingerprint, LRU cache, per-fingerprint `exec_count`, `total_us`, `avg_us`, `p95_us` (hdrhistogram), `total_rows`.

## 5. N+1 detector
Frequency-based (ADR-002): `>= 5` same-fingerprint reads in one txn or with gaps `< gap_ms`. Preceding multi-row SELECT is an optional hint. Thresholds configurable.

## 6. Shadow EXPLAIN
1. AST guard (SELECT only; no INTO / FOR UPDATE / writable CTE / denylisted function).
2. SELECT-only role on shadow.
3. `BEGIN READ ONLY; SET LOCAL statement_timeout='3000ms'; EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ...; ROLLBACK;`
4. Replay the original `Parse` (same OIDs) and bind params typed; avoid string inlining.
5. Plan heuristics use `Actual Rows × Actual Loops`:
   - Seq Scan with total rows > `seq_scan_rows` (default 5000, configurable) → missing index hint
   - Sort Space Type Disk → work_mem spill
   - Actual ≫ Plan rows → stale stats
Shadow DB must be seeded with representative data and `ANALYZE`d.

## 7. Flight recorder and replay diff
Record → restore shadow to snapshot → warm-up → **Replay A (baseline)** → apply change (e.g. new index) → restore snapshot → **Replay B**. Repeat runs; compare p50/p95/p99 and top improved/regressed fingerprints. Never compare primary latencies against shadow latencies.

## 8. Clients
CLI and Tauri UI over `pgflightd`'s control socket: Live Stream, Fingerprints & N+1, Query Inspector / EXPLAIN tree, Replay & Benchmarks.
