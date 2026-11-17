# ADR-001: pg_query instead of sqlparser-rs
**Status:** Accepted
**Decision:** use `pg_query` (libpg_query, the real Postgres parser) for fingerprinting, normalization and the shadow guard.
**Why:** exact Postgres syntax fidelity, built-in fingerprint/normalize.
**Costs:** needs a C toolchain; slower cold builds; harder cross-compiling.
**To verify:** which Postgres version the pinned crate wraps; `IN (1,2)` vs `IN (1,2,3)` fingerprint equality; note that its fingerprint is not identical to `pg_stat_statements` queryid.
**Fallback:** `sqlparser` pinned to an exact version (its AST changes between releases).
