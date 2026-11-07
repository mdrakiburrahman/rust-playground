#!/usr/bin/env bash

set -Eeuo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKSPACE_ROOT="$(cd "$PROJECT_ROOT/../.." && pwd)"
ENV_FILE="${ARROWCOL_INFRA_ENV_FILE:-$PROJECT_ROOT/.env}"
EXAMPLE_ENV_FILE="$PROJECT_ROOT/.env.example"
COMPOSE_FILE="$PROJECT_ROOT/compose.yaml"

load_local_env() {
  if [[ ! -f "$ENV_FILE" ]]; then
    echo "Missing $ENV_FILE" >&2
    echo "Run: $PROJECT_ROOT/scripts/bootstrap-secrets.sh" >&2
    return 64
  fi

  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
}

resolve_project_path() {
  local path="$1"
  if [[ "$path" = /* ]]; then
    printf '%s' "$path"
  else
    printf '%s/%s' "$PROJECT_ROOT" "${path#./}"
  fi
}

required_secret_paths() {
  printf '%s\n' \
    "$(resolve_project_path "${SQLSERVER_SA_PASSWORD_FILE:-./secrets/sqlserver-sa-password.txt}")" \
    "$(resolve_project_path "${ARROWCOL_APP_PASSWORD_FILE:-./secrets/arrowcol-app-password.txt}")" \
    "$(resolve_project_path "${ARROWCOL_MIGRATOR_PASSWORD_FILE:-./secrets/arrowcol-migrator-password.txt}")"
}

require_runtime_configuration() {
  load_local_env

  local secret
  while IFS= read -r secret; do
    if [[ ! -s "$secret" ]]; then
      echo "Missing or empty Docker secret: $secret" >&2
      echo "Run: $PROJECT_ROOT/scripts/bootstrap-secrets.sh" >&2
      return 64
    fi
  done < <(required_secret_paths)
}

compose() {
  local env_file="$ENV_FILE"
  if [[ ! -f "$env_file" ]]; then
    env_file="$EXAMPLE_ENV_FILE"
  fi

  docker compose \
    --project-directory "$PROJECT_ROOT" \
    --env-file "$env_file" \
    -f "$COMPOSE_FILE" \
    "$@"
}
