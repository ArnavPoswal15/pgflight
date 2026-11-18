# pg-wire fuzz targets

LibFuzzer targets for startup-phase decoding, frontend Parse/Bind bodies, backend header peek, and cancel requests.

## Prerequisites

- [cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz): `cargo install cargo-fuzz`
- **Nightly Rust** (required for `-Zsanitizer=address` builds). Default `cargo test` in the workspace does **not** use nightly.

## Run locally

From this directory (`crates/pg-wire/fuzz`):

```bash
cargo +nightly fuzz run startup_decode -- -max_total_time=60
cargo +nightly fuzz run parse_bind_parse -- -max_total_time=60
cargo +nightly fuzz run backend_header_peek -- -max_total_time=60
cargo +nightly fuzz run cancel_request -- -max_total_time=60
```

Or from the `pg-wire` crate root:

```bash
cargo +nightly fuzz run startup_decode
```

## Nightly CI / cron (optional)

A scheduled job can run each target with a time budget (e.g. 10–30 minutes per target) and archive `fuzz/artifacts/`. This is **not** part of the default GitLab `cargo test` pipeline.

## Targets

| Target | Exercises |
|--------|-----------|
| `startup_decode` | `parse_startup_pdu` (SSL/GSS/cancel/startup) |
| `parse_bind_parse` | `parse_parse` + `parse_bind` bodies |
| `backend_header_peek` | `peek_header` on arbitrary buffers |
| `cancel_request` | cancel encode/decode round-trip + random PDUs |