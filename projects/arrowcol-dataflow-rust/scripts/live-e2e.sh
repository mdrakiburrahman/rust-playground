#!/usr/bin/env bash
set -Eeuo pipefail

WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
INFRA_ROOT="$WORKSPACE_ROOT/projects/arrowcol-infra"
DIAGNOSTIC_ROOT="$WORKSPACE_ROOT/projects/arrowcol-dataflow-rust/.diagnostics"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
DIAGNOSTICS="$DIAGNOSTIC_ROOT/$RUN_ID"
mkdir -p "$DIAGNOSTICS"
workspace_container=""
infra_network=""
network_connected=false
infra_started=false
catalog_pid=0
target_dir="${CARGO_TARGET_DIR:-$WORKSPACE_ROOT/target}"
[[ "$target_dir" = /* ]] || target_dir="$WORKSPACE_ROOT/$target_dir"
catalog_binary="$target_dir/debug/arrowcol-catalog-rust"
catalog_port="${ARROWCOL_LIVE_CATALOG_PORT:-38817}"

# shellcheck source=../../arrowcol-infra/scripts/lib.sh
source "$INFRA_ROOT/scripts/lib.sh"

wait_for_port() {
  local host="$1"
  local port="$2"
  local label="$3"
  local attempts=60
  while (( attempts > 0 )); do
    if (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; then
      exec 3>&-
      exec 3<&-
      return 0
    fi
    attempts=$((attempts - 1))
    sleep 1
  done
  echo "Timed out waiting for $label at $host:$port" >&2
  return 1
}

cleanup() {
  local status="$1"
  if [[ "$catalog_pid" != 0 ]]; then
    if kill -0 "$catalog_pid" 2>/dev/null; then
      kill -INT "$catalog_pid" 2>/dev/null || true
      for _ in $(seq 1 80); do
        kill -0 "$catalog_pid" 2>/dev/null || break
        sleep 0.25
      done
      if kill -0 "$catalog_pid" 2>/dev/null; then
        echo "Catalog did not drain within 20 seconds" >&2
        status=1
        kill -KILL "$catalog_pid" 2>/dev/null || true
      fi
    fi
    wait "$catalog_pid" 2>/dev/null || status=1
  fi
  if [[ "$infra_started" == true ]]; then
    compose ps --all >"$DIAGNOSTICS/compose-ps.txt" 2>&1 || true
    compose logs --no-color --timestamps --tail 1000 >"$DIAGNOSTICS/compose.log" 2>&1 || true
  fi
  if [[ "$network_connected" == true ]]; then
    docker network disconnect "$infra_network" "$workspace_container" >/dev/null 2>&1 || true
  fi
  if [[ "$infra_started" == true ]]; then
    "$INFRA_ROOT/scripts/stack.sh" down >"$DIAGNOSTICS/down.log" 2>&1 || status=1
  fi
  printf 'exit_status=%s\n' "$status" >"$DIAGNOSTICS/result.txt"
  printf 'diagnostics=%s\n' "$DIAGNOSTICS"
  return "$status"
}

on_exit() {
  local status=$?
  trap - EXIT
  cleanup "$status" || status=$?
  exit "$status"
}
trap on_exit EXIT

if [[ ! "$catalog_port" =~ ^[0-9]+$ ]] || (( catalog_port < 1 || catalog_port > 65535 )); then
  echo "ARROWCOL_LIVE_CATALOG_PORT must be a TCP port" >&2
  exit 64
fi
if [[ ! -x "$catalog_binary" ]]; then
  echo "Missing catalog binary; run arrowcol-catalog-rust:build first" >&2
  exit 64
fi

if [[ ! -f "$INFRA_ROOT/.env" || ! -s "$INFRA_ROOT/secrets/arrowcol-app-password.txt" ]]; then
  "$INFRA_ROOT/scripts/bootstrap-secrets.sh" >"$DIAGNOSTICS/bootstrap.log" 2>&1
fi

infra_started=true
if ! "$INFRA_ROOT/scripts/stack.sh" up >"$DIAGNOSTICS/up.log" 2>&1; then
  sleep 10
  "$INFRA_ROOT/scripts/stack.sh" up >"$DIAGNOSTICS/up-retry.log" 2>&1
fi

sql_host=127.0.0.1
sql_port="${SQLSERVER_PORT:-14333}"
workspace_container="$(hostname)"
if docker inspect "$workspace_container" >/dev/null 2>&1; then
  sql_container="$(compose ps -q sqlserver)"
  infra_network="$(
    docker inspect \
      --format '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{"\n"}}{{end}}' \
      "$sql_container" \
      | head -n 1
  )"
  if ! docker inspect --format '{{json .NetworkSettings.Networks}}' "$workspace_container" \
    | grep -Fq "\"$infra_network\""; then
    docker network connect "$infra_network" "$workspace_container" >/dev/null
    network_connected=true
  fi
  sql_host=sqlserver
  sql_port=1433
fi

wait_for_port "$sql_host" "$sql_port" "SQL Server"
# The owned local catalog uses explicit local roots; inherited Azure reset-generation
# and publisher-fence configuration is not changed or bypassed.
env -u ARROWCOL_CATALOG_DDL_DIR "$catalog_binary" --listen "127.0.0.1:$catalog_port" \
  --state-dir "$DIAGNOSTICS/catalog-state" \
  --warehouse-root "$DIAGNOSTICS/warehouse" >"$DIAGNOSTICS/catalog.log" 2>&1 &
catalog_pid=$!
catalog_ready=false
for _ in $(seq 1 60); do
  if ! kill -0 "$catalog_pid" 2>/dev/null; then
    cat "$DIAGNOSTICS/catalog.log" >&2
    exit 1
  fi
  if "$catalog_binary" --listen "127.0.0.1:$catalog_port" --healthcheck >/dev/null 2>&1; then
    catalog_ready=true
    break
  fi
  sleep 1
done
if [[ "$catalog_ready" != true ]]; then
  echo "Timed out waiting for owned remote Schema Catalog" >&2
  exit 1
fi

(
  cd "$WORKSPACE_ROOT"
  ARROWCOL_RUN_LIVE_TESTS=1 \
  ARROWCOL_SQL_HOST="$sql_host" \
  ARROWCOL_SQL_PORT="$sql_port" \
  ARROWCOL_CATALOG_ENDPOINT="http://127.0.0.1:$catalog_port" \
  ARROWCOL_SQL_PASSWORD_FILE="$INFRA_ROOT/secrets/arrowcol-app-password.txt" \
  RUST_BACKTRACE=1 \
    cargo test --locked --package arrowcol-dataflow-components \
      live_remote_catalog_delta -- --ignored --nocapture --test-threads=1
) 2>&1 | tee "$DIAGNOSTICS/live-e2e.log"
