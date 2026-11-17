# pg-wire-codec

## Framing
- Read 5-byte header: tag + i32 length (includes 4 length bytes).
- `body_len = len - 4`. Buffer bodies `<= 64 KiB`; stream larger with 64 KiB chunks.
- Never deserialize `DataRow` values; only field count if needed.

## Startup (pre-v3)
- 8-byte SSL/GSS probes; reply `N` on proxy leg.
- Cancel: 16-byte payload, route via `CancelRegistry`, do not relay as normal session.
- Startup packet: forward version + params upstream unchanged.

## Expected-op (Phase 2+)
- Push to queue **before** upstream write.
- Async backend `N`/`S`/`A`: forward, do not pop.
- `E` in extended mode: `drain_to_next_sync` then forward.
- `K`: register `(pid, secret)` for cancel.

## Tags
Use `FrontendTag` / `BackendTag` separately (colliding letters).