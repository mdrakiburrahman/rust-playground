#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

docker compose version >/dev/null

docker compose \
  --project-directory "$PROJECT_ROOT" \
  --env-file "$EXAMPLE_ENV_FILE" \
  -f "$COMPOSE_FILE" \
  config --quiet

while IFS= read -r script; do
  bash -n "$script"
done < <(find "$PROJECT_ROOT/scripts" "$PROJECT_ROOT/docker" -type f -name '*.sh' -print | sort)

mapfile -t migrations < <(find "$PROJECT_ROOT/migrations" -maxdepth 1 -type f -name 'V*.sql' -print | sort)
if [[ "${#migrations[@]}" -ne 11 ]]; then
  echo "Expected exactly 11 versioned migrations, found ${#migrations[@]}" >&2
  exit 1
fi

expected_version=1
for migration in "${migrations[@]}"; do
  expected_name="$(printf 'V%04d__' "$expected_version")"
  if [[ "$(basename "$migration")" != "$expected_name"* ]]; then
    echo "Migration order is not contiguous at: $migration" >&2
    exit 1
  fi
  grep -Fq '$(MIGRATION_CHECKSUM)' "$migration"
  expected_version=$((expected_version + 1))
done

for migration in "${migrations[@]}"; do
  # Scalar digest conversions are not binary storage declarations.
  if sed -E 's/\bCONVERT[[:space:]]*\([[:space:]]*varbinary[[:space:]]*\([[:space:]]*max[[:space:]]*\)[[:space:]]*,/CONVERT_SCALAR(/Ig' "$migration" |
    grep -niE '(^|[^[:alnum:]_])(varbinary[[:space:]]*\([[:space:]]*max[[:space:]]*\)|image)([^[:alnum:]_]|$)'; then
    echo "Application migrations must not define Arrow payload byte storage: $migration" >&2
    exit 1
  fi
done

if grep -nE '^[A-Z0-9_]*PASSWORD=' "$EXAMPLE_ENV_FILE"; then
  echo ".env.example must contain secret file paths, not passwords" >&2
  exit 1
fi

git -C "$WORKSPACE_ROOT" check-ignore -q \
  projects/arrowcol-infra/secrets/config-check-probe.txt

grep -Fxq '.env' "$PROJECT_ROOT/.dockerignore"
grep -Fxq 'secrets/' "$PROJECT_ROOT/.dockerignore"
grep -q '@sha256:' "$PROJECT_ROOT/docker/sqlserver/Dockerfile"

echo "Compose configuration and static infrastructure checks passed"
