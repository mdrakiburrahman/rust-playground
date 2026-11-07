#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

cleanup() {
  "$SCRIPT_DIR/stack.sh" down >/dev/null 2>&1 || true
}
trap cleanup EXIT

require_runtime_configuration
"$SCRIPT_DIR/config-check.sh"
"$SCRIPT_DIR/stack.sh" up

assert_healthy() {
  local service="$1"
  local container_id health

  container_id="$(compose ps -q "$service")"
  if [[ -z "$container_id" ]]; then
    echo "Service has no container: $service" >&2
    return 1
  fi

  health="$(docker inspect --format '{{.State.Health.Status}}' "$container_id")"
  if [[ "$health" != "healthy" ]]; then
    echo "Service is not healthy: $service ($health)" >&2
    return 1
  fi
}

verify_sql_objects() {
  compose exec -T sqlserver \
    /usr/local/bin/sqlcmd-secret \
    -S tcp:127.0.0.1,1433 \
    -U sa \
    -b \
    -r 1 \
    -l 30 \
    -d master \
    -i /opt/arrowcol/verification/verify.sql
}

assert_healthy sqlserver
verify_sql_objects

echo "Re-running every initialization path to prove idempotency"
compose run --rm --no-deps database-bootstrap
compose run --rm --no-deps arrowcol-migrate

assert_healthy sqlserver
verify_sql_objects

echo "Arrowcol routing SQL Server integration verification passed"
