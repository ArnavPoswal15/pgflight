#!/usr/bin/env bash
# Optional warm-up loop for shadow replay jobs (planner/cache stabilization).
set -euo pipefail

ITERS="${1:-3}"
SHADOW_URL="${SHADOW_URL:-postgresql://pgflight_shadow:pgflight_shadow@127.0.0.1:5434/shop}"
WARMUP_SQL="${WARMUP_SQL:-SELECT count(*) FROM orders}"

if [[ ! "$ITERS" =~ ^[0-9]+$ ]] || [[ "$ITERS" -lt 1 ]]; then
  echo "error: iterations must be a positive integer (got: ${ITERS})" >&2
  exit 1
fi

echo "==> Shadow warm-up: ${ITERS} iteration(s) as pgflight_shadow..."
for ((i = 1; i <= ITERS; i++)); do
  psql "$SHADOW_URL" -v ON_ERROR_STOP=1 -c "$WARMUP_SQL" >/dev/null
done
echo "==> Warm-up done."