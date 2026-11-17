.PHONY: verify-phase0 harness fmt clippy test

harness: verify-phase0

verify-phase0:
	./scripts/verify-phase0.sh

fmt:
	cargo fmt --all

clippy:
	cargo clippy --workspace --all-targets -- -D warnings

test:
	cargo test --workspace