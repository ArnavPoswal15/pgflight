#!/usr/bin/env bash
# Phase 0 harness gate: compose healthy, psql smoke, n_plus_one against primary.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE=(docker compose -f "$ROOT/docker/docker-compose.yml")

echo "==> Starting compose (with healthchecks)..."
"${COMPOSE[@]}" up -d --wait

echo "==> psql smoke on :5432..."
if command -v psql >/dev/null 2>&1; then
  psql "postgresql://postgres:postgres@127.0.0.1:5432/shop" -c "SELECT 1 AS ok"
else
  echo "psql not installed; using docker exec"
  docker exec "$(docker compose -f "$ROOT/docker/docker-compose.yml" ps -q postgres-primary)" \
    psql -U postgres -d shop -c "SELECT 1 AS ok"
fi

echo "==> n_plus_one.py (primary; override with PGURL)..."
PGURL="${PGURL:-postgresql://postgres:postgres@127.0.0.1:5432/shop}"
export PGURL
python3 "$ROOT/docker/n_plus_one.py"

echo "==> Phase 0 verify OK"