#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
INFRA_ROOT="$WORKSPACE_ROOT/projects/arrowcol-infra"
# shellcheck source=../../../projects/arrowcol-infra/scripts/lib.sh
source "$INFRA_ROOT/scripts/lib.sh"

require_runtime_configuration

workspace_container=""
infra_network=""

cleanup() {
  local status=$?
  if [[ -n "$workspace_container" && -n "$infra_network" ]]; then
    docker network disconnect "$infra_network" "$workspace_container" >/dev/null 2>&1 || true
  fi
  "$INFRA_ROOT/scripts/stack.sh" down >/dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT

"$INFRA_ROOT/scripts/stack.sh" up

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
  docker network connect "$infra_network" "$workspace_container" >/dev/null 2>&1 || true
  sql_host=sqlserver
  sql_port=1433
fi

ARROWCOL_SQLSERVER_TEST=1 \
ARROWCOL_SQLSERVER_HOST="$sql_host" \
ARROWCOL_SQLSERVER_PORT="$sql_port" \
ARROWCOL_APP_PASSWORD_FILE="$(resolve_project_path "${ARROWCOL_APP_PASSWORD_FILE:-./secrets/arrowcol-app-password.txt}")" \
ARROWCOL_MIGRATOR_PASSWORD_FILE="$(resolve_project_path "${ARROWCOL_MIGRATOR_PASSWORD_FILE:-./secrets/arrowcol-migrator-password.txt}")" \
  cargo test --locked --package arrowcol-routing --test live_sql -- --ignored --test-threads=1

echo "Arrowcol routing SQL Server live integration passed"
