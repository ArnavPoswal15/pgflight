# apps/desktop (Phase 4)
Tauri v2 + React/TypeScript. Thin client of `pgflightd` over `~/.pgflight/control.sock`.
Do not start until `psql` and an ORM run queries and Ctrl+C cleanly through the proxy.
Scaffold with: `npm create tauri-app@latest` then add the crate to the workspace members.
