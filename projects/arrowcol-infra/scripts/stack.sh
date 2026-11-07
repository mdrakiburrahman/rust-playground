#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

action="${1:-}"
if [[ $# -gt 0 ]]; then
  shift
fi

case "$action" in
  up)
    require_runtime_configuration
    if [[ $# -gt 0 ]]; then
      compose up \
        --detach \
        --build \
        --wait \
        --wait-timeout "${STACK_WAIT_SECONDS:-600}" \
        "$@"
    else
      compose up \
        --detach \
        --build \
        --wait \
        --wait-timeout "${STACK_WAIT_SECONDS:-600}" \
        sqlserver
      compose run --rm --no-deps database-bootstrap
      compose run --rm --no-deps arrowcol-migrate
    fi
    ;;
  down)
    compose down \
      --remove-orphans \
      --timeout "${STACK_STOP_SECONDS:-30}" \
      "$@"
    ;;
  logs)
    compose logs --follow --tail 200 "$@"
    ;;
  reset)
    compose down \
      --volumes \
      --remove-orphans \
      --timeout "${STACK_STOP_SECONDS:-30}" \
      "$@"
    ;;
  clean)
    compose down \
      --volumes \
      --rmi local \
      --remove-orphans \
      --timeout "${STACK_STOP_SECONDS:-30}" \
      "$@"
    ;;
  *)
    echo "Usage: $0 {up|down|logs|reset|clean} [compose arguments...]" >&2
    exit 64
    ;;
esac
