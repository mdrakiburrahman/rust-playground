#!/usr/bin/env bash
# Functional native Delta/QAP check against the pinned local Kusto emulator.
#
# usage: kusto-qap-it.sh all|schemas|prune|start|init|mutate|vacuum|schemas-write|check [expose]|stop
#   all     start, init, check expose, mutate, check, vacuum, check, stop
#   schemas start, all 13 canonical sqlserver.sql tables with boundary values, check expose, stop
#   prune   DELTA_MAINTENANCE_REF=<core commit> required. Builds delta-maintenance from exactly that
#           commit, then on the marked disposable fixture (UNSAFE zero VACUUM/log retention):
#           init, mutate, prune-prep (receipt txns, watermark PURGE, OPTIMIZE), check expose;
#           cycle 1 (VACUUM LITE + Kernel checkpoint, then VACUUM + prune-log), prune-assert-1,
#           emulator restart + check expose (cold reread of the pruned log); prune-post, check;
#           cycle 2 (floor-based proof), prune-assert-2, restart + check expose, stop.
#           Every check phase (fallback + cached + oracle, all tables) must finish within 300s.
#   init    two partitioned Delta tables, two appends each
#   mutate  dataChange=true partition PURGE + append (sessions); row delete + dataChange=false
#           OPTIMIZE (availability groups)
#   vacuum  VACUUM(0h) so superseded files disappear from storage
#   check   runs the ignored `native_qap_converges_on_emulator` test (fallback, cached and
#           independent-oracle multisets at the exact Delta version); `expose` first creates the
#           external tables and their MaxAge=1m acceleration policy
#
# Requires Docker whose daemon sees the repository at the same path (devcontainer docker-outside-of-docker).
# Local runs are functional correctness/recovery evidence only, not production QAP performance.
set -euo pipefail

REPO=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
IT="$REPO/.temp/kusto-qap-it"
IMAGE=mcr.microsoft.com/azuredataexplorer/kustainer-linux@sha256:51e2250da2faf292de903f871a073eb37d3f2641cf63381b183879d21ebb1013
NET=kusto-qap-it
KUSTO=kusto-qap-it
SELF=$(hostname)

write() {
  (cd "$REPO" && npx --no-install tsx \
    projects/loadtest/loadgen/scripts/kusto_qap_fixture.ts "$IT/direct" "$1" \
    projects/loadtest/schemas/sqlserver.sql) ||
    return 1
  docker run --rm -v "$IT:/t" alpine chmod -R a+rwX /t
}

check() {
  local ip
  ip=$(docker inspect -f "{{(index .NetworkSettings.Networks \"$NET\").IPAddress}}" "$KUSTO")
  for _ in $(seq 1 90); do
    curl -sf -m 3 -X POST "http://$ip:8080/v1/rest/mgmt" -H 'Content-Type: application/json' \
      -d '{"csl":".show version"}' >/dev/null && break
    sleep 2
  done
  (cd "$REPO" && cargo test -q -p arrowcol-loadgen --no-run &&
    env KUSTO_IT_ENDPOINT="http://$ip:8080" KUSTO_IT_ROOT="$IT/direct" ${1:+KUSTO_IT_EXPOSE=1} \
      timeout "${KUSTO_IT_PHASE_SECS:-900}" \
      cargo test -q -p arrowcol-loadgen native_qap_converges_on_emulator -- --ignored --nocapture)
}

# Builds delta-maintenance from the exact requested commit in a detached worktree.
maintenance_bin() {
  [ -n "${DELTA_MAINTENANCE_REF:-}" ] || {
    echo "DELTA_MAINTENANCE_REF=<fixed core commit> is required for prune" >&2
    return 2
  }
  local sha src
  sha=$(git -C "$REPO" rev-parse --verify "${DELTA_MAINTENANCE_REF}^{commit}")
  src="$REPO/.temp/delta-maintenance-$sha"
  [ -d "$src" ] || git -C "$REPO" worktree add -q --detach "$src" "$sha"
  git -C "$src" submodule update -q --init --recursive
  (cd "$src" && CARGO_TARGET_DIR="$REPO/.temp/delta-maintenance-target" cargo build -q -p delta-maintenance)
  DM="$REPO/.temp/delta-maintenance-target/debug/delta-maintenance"
  echo "PROVENANCE core $sha native $(git -C "$REPO" rev-parse HEAD) $(git -C "$REPO" diff --quiet HEAD -- && echo clean || echo dirty)"
}

# One VACUUM LITE + checkpoint pass, then a VACUUM LITE + prune-log pass, on the marked disposable root.
maintain() {
  local args=() table
  for table in sqlserver_active_sessions sqlserver_availability_group_states; do
    touch "$IT/direct/telemetry/sqlserver/$table/.arrowcol-disposable-test-root"
    args+=(--table "$table")
  done
  sleep 1
  args+=(--root "file://$IT/direct/telemetry/sqlserver/" --apply --anchor-ms "$(date +%s%3N)"
    --vacuum-retention-secs 0 --log-retention-secs 0 --checkpoint-every-commits 1 --unsafe-short-vacuum-for-tests)
  TMPDIR="$IT" "$DM" "${args[@]}" --operation vacuum --operation checkpoint | tee "$IT/maintenance-$1-checkpoint.out"
  TMPDIR="$IT" "$DM" "${args[@]}" --operation vacuum --operation prune-log | tee "$IT/maintenance-$1-prune.out"
}

stop() {
  docker network disconnect "$NET" "$SELF" >/dev/null 2>&1 || true
  docker rm -f "$KUSTO" >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
}

start() {
  stop
  docker network create "$NET" >/dev/null
  docker network connect "$NET" "$SELF"
  docker run --rm -v "$REPO/.temp:/t" alpine rm -rf /t/kusto-qap-it
  mkdir -p "$IT/direct" && chmod 777 "$IT/direct"
  docker run -d --name "$KUSTO" --network "$NET" -e ACCEPT_EULA=Y -m 4g \
    -v "$IT/direct:/delta/direct:ro" "$IMAGE" >/dev/null
}

case "${1:-}" in
all)
  trap stop EXIT
  start
  write init && check expose
  write mutate && check
  write vacuum && check
  ;;
schemas)
  trap stop EXIT
  start
  write schemas && check expose
  ;;
prune)
  maintenance_bin
  export KUSTO_IT_PHASE_SECS=300
  trap stop EXIT
  start
  # One step per line: set -e does not stop on a failure inside an && list.
  write init
  write mutate
  write prune-prep
  check expose
  maintain 1
  write prune-assert-1
  docker restart "$KUSTO" >/dev/null
  check expose
  write prune-post
  check
  maintain 2
  write prune-assert-2
  docker restart "$KUSTO" >/dev/null
  check expose
  ;;
start) start ;;
init | mutate | vacuum | schemas-write | prune-prep | prune-post | prune-assert-[12]) write "${1%-write}" ;;
check) check "${2:-}" ;;
stop) stop ;;
*)
sed -n '2,23p' "$0"
  exit 2
  ;;
esac
