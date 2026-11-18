#!/usr/bin/env bash
# Phase 5 acceptance: record N+1 via :5433 → shadow replay A/B → plan compare (Seq Scan → index).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=scripts/lib_pgflightd.sh
source "${ROOT}/scripts/lib_pgflightd.sh"

usage() {
  cat <<'EOF'
Usage: accept-phase5.sh [--help]

Phase 5 end-to-end acceptance (requires Docker + psql + python3):

  1. docker compose up (primary :5432, shadow :5434)
  2. pgflightd on :5433 with shadow-url + .pgrec recording
  3. n_plus_one workload through the proxy
  4. shadow-reset, drop users PK, replay A (EXPLAIN)
  5. CREATE INDEX on shadow users(id), shadow-reset, replay B (EXPLAIN)
  6. Assert plan compare shows Seq Scan → index improvement

Skips with exit 0 when Docker is not available.

Environment:
  PGFLIGHT_DATA_DIR       Default ~/.pgflight-accept-phase5
  PGFLIGHT_LISTEN         Default 127.0.0.1:54313 (avoids dev :5433)
  PGFLIGHT_UPSTREAM       Default 127.0.0.1:5432
  PGFLIGHT_CONTROL_SOCK   Default $DATA_DIR/control.sock
  PGFLIGHT_COMPOSE_FILE   Default docker/docker-compose.yml
  SHADOW_ADMIN_URL        Default postgres@127.0.0.1:5434/shop
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
  echo "==> skip: Docker unavailable (accept-phase5 not run)"
  exit 0
fi

COMPOSE_FILE="${PGFLIGHT_COMPOSE_FILE:-${ROOT}/docker/docker-compose.yml}"
LISTEN="${PGFLIGHT_LISTEN:-127.0.0.1:54313}"
UPSTREAM="${PGFLIGHT_UPSTREAM:-127.0.0.1:5432}"
DATA_DIR="${PGFLIGHT_DATA_DIR:-${HOME}/.pgflight-accept-phase5}"
CONTROL_SOCK="${PGFLIGHT_CONTROL_SOCK:-${DATA_DIR}/control.sock}"
SHADOW_ADMIN_URL="${SHADOW_ADMIN_URL:-postgresql://postgres:postgres@127.0.0.1:5434/shop}"
SHADOW_URL="${SHADOW_URL:-postgresql://pgflight_shadow:pgflight_shadow@127.0.0.1:5434/shop}"

mkdir -p "${DATA_DIR}"
rm -f "${DATA_DIR}"/pgflight-*.pgrec "${DATA_DIR}"/pgflight.sqlite "${DATA_DIR}"/pgflight.sqlite-*
export PGFLIGHT_DATA_DIR="${DATA_DIR}"
trap stop_pgflightd_if_started EXIT

wait_primary() {
  local i
  for i in $(seq 1 60); do
    if psql "postgresql://postgres:postgres@127.0.0.1:5432/shop" -c 'SELECT 1' >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "error: primary Postgres not ready on :5432" >&2
  exit 1
}

echo "==> docker compose up (primary + shadow)..."
docker compose -f "$COMPOSE_FILE" up -d postgres-primary postgres-shadow
wait_primary
"${ROOT}/scripts/shadow-reset.sh"

TARGET_DIR="${CARGO_TARGET_DIR:-${ROOT}/target}"
echo "==> Building release pgflightd..."
cargo build -p pgflightd --release
BIN="${TARGET_DIR}/release/pgflightd"

if lsof -nP -iTCP:"${LISTEN##*:}" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "error: port ${LISTEN} in use" >&2
  exit 1
fi

echo "==> Starting pgflightd (shadow + pgrec)..."
"$BIN" \
  --listen "$LISTEN" \
  --upstream "$UPSTREAM" \
  --data-dir "$DATA_DIR" \
  --control-socket "$CONTROL_SOCK" \
  --shadow-url "$SHADOW_URL" &
PGFLIGHTD_PID=$!
sleep 2

export PGURL="postgresql://postgres:postgres@${LISTEN}/shop"
if ! python3 -c "import psycopg" >/dev/null 2>&1; then
  echo "==> Installing psycopg for n_plus_one workload..."
  python3 -m pip install --quiet 'psycopg[binary]'
fi
echo "==> Recording N+1 workload (repeat until pgrec block flush)..."
# ZSTD blocks flush at 64KiB of encoded records; a single n_plus_one pass is far smaller.
for _ in $(seq 1 40); do
  python3 "${ROOT}/docker/n_plus_one.py"
  PGREC="$(find "$DATA_DIR" -maxdepth 1 -name 'pgflight-*.pgrec' -print 2>/dev/null | head -1)"
  if [[ -n "${PGREC:-}" && "$(stat -f%z "$PGREC" 2>/dev/null || stat -c%s "$PGREC")" -gt 4096 ]]; then
    break
  fi
done
sleep 2

PGREC="$(find "$DATA_DIR" -maxdepth 1 -name 'pgflight-*.pgrec' -print 2>/dev/null | head -1)"
PGREC_SIZE="$(stat -f%z "$PGREC" 2>/dev/null || stat -c%s "$PGREC")"
if [[ -z "${PGREC:-}" || "$PGREC_SIZE" -le 40 ]]; then
  echo "error: .pgrec not flushed (size=${PGREC_SIZE:-0}) under ${DATA_DIR}" >&2
  exit 1
fi
echo "==> Using pgrec: ${PGREC} (${PGREC_SIZE} bytes)"

export PGFLIGHT_CONTROL_SOCK="$CONTROL_SOCK"
export ACCEPT_PGREC="$PGREC"

echo "==> Shadow prep: reset, drop users PK for seq-scan baseline..."
"${ROOT}/scripts/shadow-reset.sh"
psql "$SHADOW_ADMIN_URL" -v ON_ERROR_STOP=1 <<'SQL'
ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_user_id_fkey;
ALTER TABLE users DROP CONSTRAINT users_pkey;
SQL

echo "==> Replay A (EXPLAIN)..."
REPLAY_JSON="$(python3 <<'PY'
import json, os, socket

sock_path = os.environ["PGFLIGHT_CONTROL_SOCK"]
pgrec = os.environ["ACCEPT_PGREC"]

def request(req, read_lines=1):
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(120)
    s.connect(sock_path)
    s.sendall((json.dumps(req) + "\n").encode())
    out = []
    buf = b""
    while len(out) < read_lines:
        chunk = s.recv(65536)
        if not chunk:
            break
        buf += chunk
        while b"\n" in buf:
            line, buf = buf.split(b"\n", 1)
            if line.strip():
                out.append(json.loads(line))
    s.close()
    return out

lines = request(
    {
        "op": "replay_run",
        "pgrec_path": pgrec,
        "label": "A",
        "warmup_iters": 0,
        "explain": True,
    },
    1,
)
resp = lines[0]
if resp.get("op") == "error":
    raise SystemExit(resp.get("message", resp))
if resp.get("op") != "replay_result":
    raise SystemExit(f"unexpected response: {resp}")
print(json.dumps(resp))
PY
)"
REPLAY_A_ID="$(echo "$REPLAY_JSON" | python3 -c 'import json,sys; print(json.load(sys.stdin)["replay"]["replay_id"])')"
echo "    replay A id: ${REPLAY_A_ID}"

echo "==> CREATE INDEX users(id), shadow-reset (add index via seed PK), replay B..."
psql "$SHADOW_ADMIN_URL" -v ON_ERROR_STOP=1 -c 'CREATE INDEX IF NOT EXISTS users_id_idx ON users (id);'
"${ROOT}/scripts/shadow-reset.sh"

REPLAY_B_JSON="$(python3 <<'PY'
import json, os, socket

sock_path = os.environ["PGFLIGHT_CONTROL_SOCK"]
pgrec = os.environ["ACCEPT_PGREC"]

def request(req):
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(120)
    s.connect(sock_path)
    s.sendall((json.dumps(req) + "\n").encode())
    buf = b""
    while b"\n" not in buf:
        chunk = s.recv(65536)
        if not chunk:
            raise SystemExit("short read from control socket")
        buf += chunk
    line = buf.split(b"\n", 1)[0]
    s.close()
    return json.loads(line)

resp = request(
    {
        "op": "replay_run",
        "pgrec_path": pgrec,
        "label": "B",
        "warmup_iters": 1,
        "explain": True,
    }
)
if resp.get("op") == "error":
    raise SystemExit(resp.get("message", resp))
print(json.dumps(resp))
PY
)"
REPLAY_B_ID="$(echo "$REPLAY_B_JSON" | python3 -c 'import json,sys; print(json.load(sys.stdin)["replay"]["replay_id"])')"
echo "    replay B id: ${REPLAY_B_ID}"

echo "==> Plan compare A vs B..."
export REPLAY_A_ID REPLAY_B_ID
export REPLAY_A_JSON="$REPLAY_JSON"
export REPLAY_B_JSON="$REPLAY_B_JSON"
COMPARE_JSON="$(python3 <<'PY'
import json, os, socket

sock_path = os.environ["PGFLIGHT_CONTROL_SOCK"]
a_id = os.environ["REPLAY_A_ID"]
b_id = os.environ["REPLAY_B_ID"]

def request(req):
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(60)
    s.connect(sock_path)
    s.sendall((json.dumps(req) + "\n").encode())
    buf = b""
    while b"\n" not in buf:
        chunk = s.recv(65536)
        if not chunk:
            raise SystemExit("short read from control socket")
        buf += chunk
    line = buf.split(b"\n", 1)[0]
    s.close()
    return json.loads(line)

base_plans = json.loads(os.environ.get("REPLAY_A_JSON", "{}")).get("plans") or []
var_plans = json.loads(os.environ.get("REPLAY_B_JSON", "{}")).get("plans") or []
if base_plans and var_plans:
    req = {
        "op": "compare_replay",
        "baseline_plans": base_plans,
        "variant_plans": var_plans,
    }
else:
    req = {
        "op": "compare_replay",
        "baseline": {"replay_id": a_id, "label": "A"},
        "variant": {"replay_id": b_id, "label": "B"},
    }
resp = request(req)
if resp.get("op") == "error":
    raise SystemExit(resp.get("message", resp))
print(json.dumps(resp))
PY
)"

echo "$COMPARE_JSON" | python3 -m json.tool

export COMPARE_JSON
python3 <<'PY'
import json, os

cmp = json.loads(os.environ["COMPARE_JSON"])
improved = cmp.get("improved") or []
diffs = cmp.get("diff_nodes") or []

seq_to_index = False
for entry in improved:
    s = entry.get("summary", "")
    if "Seq Scan" in s and ("Index" in s or "improved" in s):
        seq_to_index = True
        break

if not seq_to_index:
    for node in diffs:
        b = node.get("baseline") or ""
        v = node.get("variant") or ""
        if b == "Seq Scan" and v.startswith("Index"):
            seq_to_index = True
            break

if not seq_to_index:
    print("error: compare did not show Seq Scan → index improvement", file=sys.stderr)
    print(json.dumps(cmp, indent=2), file=sys.stderr)
    sys.exit(1)

print("==> Phase 5 acceptance PASSED (Seq Scan → index improvement)")
PY