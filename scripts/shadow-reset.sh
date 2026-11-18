#!/usr/bin/env bash
# Deterministic shadow DB reset: baseline shop data + SELECT-only grants.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_FILE="${PGFLIGHT_COMPOSE_FILE:-${ROOT}/docker/docker-compose.yml}"
SHADOW_ADMIN_URL="${SHADOW_ADMIN_URL:-postgresql://postgres:postgres@127.0.0.1:5434/shop}"
WARMUP_ITERS=0
RECREATE_VOLUME=0

usage() {
  cat <<'EOF'
Usage: shadow-reset.sh [--warmup N] [--recreate-volume]

  Default: DROP SCHEMA public + re-apply docker/seed.sql and shadow-grants.sql
           against postgres-shadow (:5434).

  --warmup N           Run shadow-warmup.sh N times after reset (default: 0).
  --recreate-volume    Stop shadow service, remove its container/volume, compose up.

Environment:
  SHADOW_ADMIN_URL     Superuser URL on shadow (default postgres@127.0.0.1:5434/shop).
  PGFLIGHT_COMPOSE_FILE  Path to docker-compose.yml.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --warmup)
      WARMUP_ITERS="${2:?--warmup requires a number}"
      shift 2
      ;;
    --recreate-volume)
      RECREATE_VOLUME=1
      shift
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

psql_admin() {
  psql "$SHADOW_ADMIN_URL" -v ON_ERROR_STOP=1 "$@"
}

wait_shadow() {
  local i
  for i in $(seq 1 30); do
    if psql_admin -c 'SELECT 1' >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "error: shadow Postgres not reachable at ${SHADOW_ADMIN_URL}" >&2
  exit 1
}

reset_schema() {
  echo "==> Resetting shadow schema (DROP public + seed)..."
  psql_admin <<'SQL'
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO public;
SQL
  psql_admin -f "${ROOT}/docker/seed.sql"
  if ! psql_admin -tAc "SELECT 1 FROM pg_roles WHERE rolname = 'pgflight_shadow'" | grep -q 1; then
    psql_admin -f "${ROOT}/docker/shadow-init.sql"
  else
    psql_admin -c "ALTER ROLE pgflight_shadow SET default_transaction_read_only = on;" >/dev/null
    psql_admin -c "ALTER ROLE pgflight_shadow SET statement_timeout = '30s';" >/dev/null
  fi
  psql_admin -f "${ROOT}/docker/shadow-grants.sql"
  echo "==> Shadow reset complete."
}

recreate_volume() {
  echo "==> Recreating postgres-shadow container and volume..."
  docker compose -f "$COMPOSE_FILE" stop postgres-shadow
  docker compose -f "$COMPOSE_FILE" rm -f -v postgres-shadow
  docker compose -f "$COMPOSE_FILE" up -d postgres-shadow
  wait_shadow
  echo "==> Shadow volume recreated (initdb hooks applied)."
}

if [[ "$RECREATE_VOLUME" -eq 1 ]]; then
  recreate_volume
else
  wait_shadow
  reset_schema
fi

if [[ "$WARMUP_ITERS" -gt 0 ]]; then
  "${ROOT}/scripts/shadow-warmup.sh" "$WARMUP_ITERS"
fi