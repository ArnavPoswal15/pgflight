# ADR-002: Preserve zero-copy; N+1 by frequency
**Status:** proposed
**Decision:** never parse `DataRow`. N+1 detection uses same-fingerprint burst frequency within a txn or gap window. A preceding multi-row SELECT is a hint, not a `count ≈ rows` requirement (50 orders from 5 users yields 5 child queries).
**Tradeoff:** cannot match bound parameter values to parent-row keys, so some precision is lost. A future opt-in "deep mode" may parse rows for selected queries only.
**Related:** large bodies stream in chunks; telemetry `try_send` drops the newest event.
