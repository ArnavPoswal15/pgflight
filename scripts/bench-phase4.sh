#!/usr/bin/env bash
# Phase 4 optional acceptance: measure pgbench p99 direct vs proxied (target SLO < 0.3ms added).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=lib_pgflightd.sh
source "$ROOT/scripts/lib_pgflightd.sh"

LISTEN="${PGFLIGHT_LISTEN:-127.0.0.1:5433}"
UPSTREAM="${PGFLIGHT_UPSTREAM:-127.0.0.1:5432}"
DIRECT_URL="postgresql://postgres:postgres@${UPSTREAM}/shop"
PROXY_URL="postgresql://postgres:postgres@${LISTEN}/shop"

if ! command -v pgbench >/dev/null 2>&1; then
  echo "pgbench not found; install PostgreSQL client tools"
  exit 1
fi

docker compose -f "$ROOT/docker/docker-compose.yml" up -d --wait
trap stop_pgflightd_if_started EXIT
start_or_reuse_pgflightd "$ROOT" "$LISTEN" "$UPSTREAM"

echo "==> pgbench direct ($UPSTREAM)"
pgbench -i -s 1 "$DIRECT_URL" >/dev/null
pgbench -c 16 -T 15 -P 1 "$DIRECT_URL" | tee /tmp/pgflight-bench-direct.txt

echo "==> pgbench proxied ($LISTEN)"
pgbench -c 16 -T 15 -P 1 "$PROXY_URL" | tee /tmp/pgflight-bench-proxy.txt

echo "==> Compare p99 latency in the two outputs above (target: < 0.3ms added on proxy path)."
echo "Phase 4 bench script finished (manual SLO check)."