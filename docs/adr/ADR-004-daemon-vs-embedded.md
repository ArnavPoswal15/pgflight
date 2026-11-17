# ADR-004: Standalone daemon
**Decision:** `pgflightd` runs independently (launchd or detached); CLI and Tauri are clients over `~/.pgflight/control.sock` (0600).
**Why:** a UI crash or restart must not drop application DB connections; the CLI reuses the same API.
**Costs:** an RPC protocol to define and version; process lifecycle management.
