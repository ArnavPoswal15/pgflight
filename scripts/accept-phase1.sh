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

psql "postgresql://postgres:postgres@${LISTEN}/shop" -c "SELECT 1 AS phase1_ok"
echo "Phase 1 accept: psql through proxy OK"