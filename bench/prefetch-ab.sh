#!/usr/bin/env bash
# A/B-замер предзагрузки, аналог bench/prefetch-ab.ps1 для Linux/macOS.
#   RUNS=3 DURATION=2m ./bench/prefetch-ab.sh
# Итог по каждому прогону печатает сам k6; JSON лежит в bench/results/.
set -euo pipefail

RUNS="${RUNS:-3}"
BOOKS="${BOOKS:-2000}"
RATE="${RATE:-40}"
WARMUP="${WARMUP:-30s}"
DURATION="${DURATION:-2m}"
THINK_MS="${THINK_MS:-150}"
PROXY_URL="${PROXY_URL:-http://localhost:8080}"
# Ограничение свежести: upstream отдаёт карточки с max-age, и без потолка
# записи не успевали бы истечь за прогон.
export CACHE_MAX_TTL="${CACHE_MAX_TTL:-20s}"

root="$(cd "$(dirname "$0")/.." && pwd)"
results="$root/bench/results"
mkdir -p "$results"

for run in $(seq 1 "$RUNS"); do
  if (( run % 2 == 1 )); then modes="false true"; else modes="true false"; fi
  for mode in $modes; do
    export PREFETCH_ENABLED="$mode"
    docker compose -f "$root/docker-compose.yml" up -d --force-recreate --no-deps --wait proxy
    label="prefetch-$mode-run$run"
    k6 run --quiet \
      -e PROXY_URLS="$PROXY_URL" -e BOOKS="$BOOKS" -e RATE="$RATE" -e WARMUP="$WARMUP" \
      -e DURATION="$DURATION" -e THINK_MS="$THINK_MS" -e LABEL="$label" \
      -e RESULT_FILE="$results/$label.json" \
      "$root/k6/prefetch-chain.js"
  done
done
