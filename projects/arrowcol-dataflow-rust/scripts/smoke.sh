#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
project="$root/projects/arrowcol-dataflow-rust"
work="$project/.temp/smoke"
target_dir="${CARGO_TARGET_DIR:-$root/target}"
[[ "$target_dir" = /* ]] || target_dir="$root/$target_dir"
binary="$target_dir/debug/arrowcol-dataflow-rust"
rm -rf "$work"
mkdir -p "$work"
pid=0

cleanup() {
  if [[ "$pid" != 0 ]] && kill -0 "$pid" 2>/dev/null; then
    kill -TERM "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
  rm -rf "$work"
}
trap cleanup EXIT

# Starts one memory-backed topology, captures every admin document into
# "$work/<name>/", and leaves the collector running as "$pid".
start_collector() {
  local name="$1"
  local config="$2"
  local admin="$3"
  local output="$work/$name"
  mkdir -p "$output"
  ARROWCOL_WAL_PATH="$output/quiver" \
    "$binary" --num-cores 1 --config "$project/$config" >"$output/collector.log" 2>&1 &
  pid=$!

  local ready=false
  for _ in $(seq 1 60); do
    if curl --fail --silent "http://$admin/readyz" >"$output/ready.json"; then
      ready=true
      break
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      cat "$output/collector.log" >&2
      exit 1
    fi
    sleep 0.25
  done
  if [[ "$ready" != true ]]; then
    cat "$output/collector.log" >&2
    exit 1
  fi

  curl --fail --silent "http://$admin/healthz" >"$output/health.json"
  curl --fail --silent "http://$admin/v1/capabilities" >"$output/capabilities.json"
  curl --fail --silent "http://$admin/v1/pressure" >"$output/pressure.json"
  curl --fail --silent "http://$admin/metrics" >"$output/metrics.txt"
}

# Requires a bounded graceful exit after SIGTERM.
stop_collector() {
  local name="$1"
  kill -TERM "$pid"
  for _ in $(seq 1 80); do
    if ! kill -0 "$pid" 2>/dev/null; then
      wait "$pid"
      pid=0
      return 0
    fi
    sleep 0.25
  done
  cat "$work/$name/collector.log" >&2
  exit 1
}

start_collector best-effort config.smoke.yaml 127.0.0.1:32311
grep -F '"delivery_guarantee":"best_effort"' "$work/best-effort/ready.json" >/dev/null
grep -F '"delivery_guarantee":"best_effort"' "$work/best-effort/capabilities.json" >/dev/null
grep -F '"completion_mode":"best_effort_admission"' "$work/best-effort/capabilities.json" >/dev/null
grep -F '"receipt_polling":false' "$work/best-effort/capabilities.json" >/dev/null
grep -F '"at_least_once":false' "$work/best-effort/capabilities.json" >/dev/null
grep -F '"native_quiver":{"supported":true,"enabled":false}' "$work/best-effort/capabilities.json" >/dev/null
grep -F '"quiver_state":"disabled"' "$work/best-effort/pressure.json" >/dev/null
grep -F 'arrowcol_delivery_guarantee_info{delivery_guarantee="best_effort"' "$work/best-effort/metrics.txt" >/dev/null
stop_collector best-effort

start_collector durable config.smoke-quiver.yaml 127.0.0.1:32411
grep -F '"delivery_guarantee":"durable_admission"' "$work/durable/ready.json" >/dev/null
grep -F '"delivery_guarantee":"durable_admission"' "$work/durable/capabilities.json" >/dev/null
grep -F '"completion_mode":"durable_admission"' "$work/durable/capabilities.json" >/dev/null
grep -F '"receipt_polling":false' "$work/durable/capabilities.json" >/dev/null
grep -F '"at_least_once":true' "$work/durable/capabilities.json" >/dev/null
grep -F '"native_quiver":{"supported":true,"enabled":true}' "$work/durable/capabilities.json" >/dev/null
grep -F '"quiver_state":"ready"' "$work/durable/pressure.json" >/dev/null
grep -F '"native_quiver_buffered_batches":0' "$work/durable/pressure.json" >/dev/null
grep -F 'arrowcol_delivery_guarantee_info{delivery_guarantee="durable_admission"' "$work/durable/metrics.txt" >/dev/null
stop_collector durable
# The durable buffer created its per-core write-ahead log under the configured path.
test -d "$work/durable/quiver/core_0/wal"

rm -rf "$work"
trap - EXIT
