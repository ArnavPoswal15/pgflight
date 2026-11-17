#!/usr/bin/env bash
# Phase 3 acceptance: N+1 through :5433 → anomalies.repeat_count >= 50
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=scripts/lib_pgflightd.sh
source "${ROOT}/scripts/lib_pgflightd.sh"

LISTEN="${PGFLIGHT_LISTEN:-127.0.0.1:5433}"
UPSTREAM="${PGFLIGHT_UPSTREAM:-127.0.0.1:5432}"
DATA_DIR="${PGFLIGHT_DATA_DIR:-${HOME}/.pgflight-accept-phase3}"
export PGFLIGHT_DATA_DIR="${DATA_DIR}"
mkdir -p "${DATA_DIR}"

trap stop_pgflightd_if_started EXIT

BIN="${ROOT}/target/release/pgflightd"
if [[ ! -x "$BIN" ]]; then
  cargo build -p pgflightd --release
fi

if lsof -nP -iTCP:"${LISTEN##*:}" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "error: port ${LISTEN} in use; stop other pgflightd or set PGFLIGHT_LISTEN" >&2
  exit 1
fi

echo "==> Starting pgflightd for phase 3 acceptance..."
"$BIN" --listen "$LISTEN" --upstream "$UPSTREAM" --data-dir "$DATA_DIR" &
PGFLIGHTD_PID=$!
sleep 1

export PGURL="postgresql://postgres:postgres@${LISTEN}/shop"
if [[ -f "${ROOT}/docker/n_plus_one.py" ]]; then
  python3 "${ROOT}/docker/n_plus_one.py"
else
  echo "warn: docker/n_plus_one.py missing; skipping workload" >&2
fi

sleep 2
DB="${DATA_DIR}/pgflight.sqlite"
echo "==> Checking SQLite anomalies..."
sqlite3 "$DB" "SELECT kind, repeat_count, fingerprint_id FROM anomalies ORDER BY id DESC LIMIT 5;"

COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM anomalies WHERE kind='n_plus_one' AND repeat_count >= 50;")
if [[ "${COUNT}" -ge 1 ]]; then
  echo "==> Phase 3 acceptance PASSED (repeat_count >= 50)"
  exit 0
fi
echo "==> Phase 3 acceptance FAILED (no anomaly with repeat_count >= 50)" >&2
exit 1