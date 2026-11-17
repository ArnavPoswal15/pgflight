#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=lib_pgflightd.sh
source "$ROOT/scripts/lib_pgflightd.sh"

LISTEN="${PGFLIGHT_LISTEN:-127.0.0.1:5433}"
UPSTREAM="${PGFLIGHT_UPSTREAM:-127.0.0.1:5432}"

docker compose -f "$ROOT/docker/docker-compose.yml" up -d --wait

trap stop_pgflightd_if_started EXIT
start_or_reuse_pgflightd "$ROOT" "$LISTEN" "$UPSTREAM"

export PGFLIGHT_PROXY="postgresql://postgres:postgres@${LISTEN}/shop"
cargo test -p pg-proxy -- --ignored phase2
cargo test -p pgflightd -- --ignored phase2

# Optional smoke: traffic shape for Phase 3 N+1 (not required for Phase 2 acceptance).
if python3 -c 'import psycopg' 2>/dev/null; then
  PGURL="$PGFLIGHT_PROXY" python3 "$ROOT/docker/n_plus_one.py"
  echo "==> n_plus_one.py through proxy OK (detector still Phase 3)"
else
  echo "==> skip n_plus_one.py (install: pip3 install 'psycopg[binary]')"
fi

echo "Phase 2 accept OK"