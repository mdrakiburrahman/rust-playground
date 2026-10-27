#!/usr/bin/env bash
# Functional native Delta/QAP check against the pinned local Kusto emulator.
#
# usage: kusto-qap-it.sh all|schemas|start|init|mutate|vacuum|schemas-write|check [expose]|stop
#   all     start, init, check expose, mutate, check, vacuum, check, stop
#   schemas start, all 13 canonical sqlserver.sql tables with boundary values, check expose, stop
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
WRITER=python:3.12-slim
PACKAGES="deltalake==1.6.6 pyarrow==26.0.0 pandas==3.1.0 numpy==2.5.4"
NET=kusto-qap-it
KUSTO=kusto-qap-it
SELF=$(hostname)

write() {
  docker run --rm -v "$IT/direct:/d" -v "$REPO/projects/loadtest/loadgen/scripts/kusto_qap_fixture.py:/w.py:ro" \
    -v "$REPO/projects/loadtest/schemas/sqlserver.sql:/sqlserver.sql:ro" \
    -v kusto-qap-it-pip:/root/.cache/pip "$WRITER" \
    sh -c "pip install -q --disable-pip-version-check --root-user-action=ignore $PACKAGES >/dev/null && python /w.py /d $1 /sqlserver.sql" ||
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
  (cd "$REPO" && env KUSTO_IT_ENDPOINT="http://$ip:8080" KUSTO_IT_ROOT="$IT/direct" ${1:+KUSTO_IT_EXPOSE=1} \
    cargo test -q -p arrowcol-loadgen native_qap_converges_on_emulator -- --ignored --nocapture)
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
start) start ;;
init | mutate | vacuum | schemas-write) write "${1%-write}" ;;
check) check "${2:-}" ;;
stop) stop ;;
*)
sed -n '2,16p' "$0"
  exit 2
  ;;
esac
