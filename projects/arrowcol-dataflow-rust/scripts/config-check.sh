#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
project="$root/projects/arrowcol-dataflow-rust"
work="$project/.temp/config-check"
target_dir="${CARGO_TARGET_DIR:-$root/target}"
[[ "$target_dir" = /* ]] || target_dir="$root/$target_dir"
binary="$target_dir/debug/arrowcol-dataflow-rust"
rm -rf "$work"
mkdir -p "$work"
trap 'rm -rf "$work"' EXIT

for config in config.yaml config.smoke.yaml config.quiver.yaml config.smoke-quiver.yaml; do
  "$binary" --validate-and-exit --config "$project/$config"
done

# The durable-admission topology must keep receiver:arrow_flight -> processor:durable_buffer
# -> exporter:arrow_deltalake and must enable native Quiver on the receiver.
grep -F 'native_quiver: true' "$project/config.quiver.yaml" >/dev/null
grep -F 'type: processor:durable_buffer' "$project/config.quiver.yaml" >/dev/null
grep -F 'wal_flush_interval: "${env:ARROWCOL_WAL_FLUSH_INTERVAL:-0s}"' "$project/config.quiver.yaml" >/dev/null

# A STANDARD-only processor (processor:batch accepts only OTAP/OTLP pdata) must be
# rejected by DFE wiring contracts when it receives Arrowcol pluggable Arrow pdata.
if "$binary" --validate-and-exit \
  --config "$project/config.incompatible-otap.yaml" \
  >"$work/incompatible-otap.log" 2>&1; then
  echo "OTAP-only batch processor wiring unexpectedly passed" >&2
  exit 1
fi
log="$work/incompatible-otap.log"
reports() {
  grep -F 'IncompatiblePdataWiring' "$log" >/dev/null &&
    grep -F "node: \"$1\"" "$log" >/dev/null &&
    grep -F "incoming: PdataRepresentationSet($2)" "$log" >/dev/null &&
    grep -F "accepted: PdataRepresentationSet($3)" "$log" >/dev/null
}
# Wiring validation visits edges in hash order, so it reports either side of the
# STANDARD-only processor: pluggable Arrow into otap_batch, or its STANDARD output
# into the pluggable-only Delta exporter.
if ! reports otap_batch PLUGGABLE_ARROW STANDARD && ! reports delta STANDARD PLUGGABLE_ARROW; then
  echo "OTAP-only batch processor wiring failed for an unexpected reason:" >&2
  cat "$log" >&2
  exit 1
fi
