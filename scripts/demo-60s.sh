#!/usr/bin/env bash
# ~60s demo: compose, pgflightd (--record), N+1 script through proxy, one status line.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=lib_pgflightd.sh
source "$ROOT/scripts/lib_pgflightd.sh"

LISTEN="${PGFLIGHT_LISTEN:-127.0.0.1:5433}"
UPSTREAM="${PGFLIGHT_UPSTREAM:-127.0.0.1:5432}"
PROXY_URL="postgresql://postgres:postgres@${LISTEN}/shop"
DAEMON_BIN="${ROOT}/target/release/pgflightd"
CLI_BIN="${ROOT}/target/release/pgflight"

skip() {
  echo "$1"
  exit 0
}

if ! command -v docker >/dev/null 2>&1; then
  skip "docker not found; skipping demo (exit 0)."
fi

if ! docker info >/dev/null 2>&1; then
  skip "docker daemon not running; skipping demo (exit 0)."
fi

trap stop_pgflightd_if_started EXIT

echo "==> Docker compose (primary + shadow)"
docker compose -f "$ROOT/docker/docker-compose.yml" up -d --wait

if [[ ! -x "$DAEMON_BIN" ]]; then
  echo "==> Building release pgflightd..."
  cargo build -p pgflightd --release
fi

if lsof -nP -iTCP:"${LISTEN##*:}" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "==> pgflightd already on ${LISTEN}"
  PGFLIGHTD_PID=""
else
  echo "==> Starting pgflightd with --record"
  "$DAEMON_BIN" --listen "$LISTEN" --upstream "$UPSTREAM" --record &
  PGFLIGHTD_PID=$!
  sleep 1
fi

echo "==> N+1 demo through proxy"
export PGURL="$PROXY_URL"
if command -v python3 >/dev/null 2>&1; then
  python3 "$ROOT/docker/n_plus_one.py"
else
  echo "python3 not found; run: PGURL=$PROXY_URL python3 docker/n_plus_one.py"
fi

if [[ -x "$CLI_BIN" ]] || cargo build -p pgflight --release 2>/dev/null; then
  echo "==> pgflight status (one line)"
  "$CLI_BIN" status 2>/dev/null | head -5 || true
else
  echo "==> (pgflight CLI not built; daemon is up on ${LISTEN})"
fi

echo "==> Demo finished (~60s path). Stop pgflightd if this script started it."