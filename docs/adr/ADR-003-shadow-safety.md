# ADR-003: Shadow EXPLAIN safety layers
`EXPLAIN ANALYZE` executes the statement. Layers:
1. `pg_query` guard: `SelectStmt` only; no `intoClause`, no `lockingClause`, no writable CTEs.
2. Dedicated shadow role with SELECT privileges only (`default_transaction_read_only` alone is overridable via `BEGIN READ WRITE`).
3. `BEGIN READ ONLY; SET LOCAL statement_timeout; EXPLAIN ...; ROLLBACK;`
4. Function denylist: `nextval`, `setval`, `pg_sleep`, `pg_advisory_lock*`, `pg_terminate_backend`, `dblink*`, and others as found. Read-only transactions still permit some of these.
Shadow must be seeded with representative data and ANALYZEd. Replay A/B on the same host with snapshot reset between runs.
