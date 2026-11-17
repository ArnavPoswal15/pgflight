# shellcheck shell=bash
# Reuse an already-running pgflightd on LISTEN, or start a release binary.

start_or_reuse_pgflightd() {
  local root="$1"
  local listen="$2"
  local upstream="$3"
  local port="${listen##*:}"

  if lsof -nP -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "==> pgflightd already listening on ${listen}; reusing existing process"
    PGFLIGHTD_PID=""
    return 0
  fi

  local bin="${root}/target/release/pgflightd"
  if [[ ! -x "$bin" ]]; then
    echo "==> Building release pgflightd..."
    cargo build -p pgflightd --release
  fi

  echo "==> Starting pgflightd (${listen} -> ${upstream})..."
  "$bin" --listen "$listen" --upstream "$upstream" &
  PGFLIGHTD_PID=$!
  sleep 1

  if ! kill -0 "$PGFLIGHTD_PID" 2>/dev/null; then
    echo "error: pgflightd exited immediately" >&2
    exit 1
  fi
}

stop_pgflightd_if_started() {
  if [[ -n "${PGFLIGHTD_PID:-}" ]]; then
    kill "$PGFLIGHTD_PID" 2>/dev/null || true
  fi
}