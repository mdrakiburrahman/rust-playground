#!/usr/bin/env bash
set -euo pipefail

image="ghcr.io/mdrakiburrahman/rust-playground/arrow-tenant-stream:dev"

docker run --rm --platform linux/amd64 "$image" --help |
  grep -F -- '--checkpoint' >/dev/null

docker run --rm --platform linux/amd64 --entrypoint /bin/sh "$image" -eu -c '
  test "$(id -u)" -eq 10001
  test "$(id -g)" -eq 10001
  test -w /var/lib/tenant-stream
  test "$AZURE_CONFIG_DIR" = /var/lib/tenant-stream-azure
  test -w "$AZURE_CONFIG_DIR"
  test "$(stat -c %a "$AZURE_CONFIG_DIR")" = 700
  test "$(stat -c %u:%g "$AZURE_CONFIG_DIR")" = 10001:10001
  test -z "$(ls -A "$AZURE_CONFIG_DIR")"
  touch "$AZURE_CONFIG_DIR/write-probe"
  rm "$AZURE_CONFIG_DIR/write-probe"
  test "$(az version --query "\"azure-cli\"" --output tsv)" = 2.91.0
'

healthcheck="$(docker image inspect --format '{{json .Config.Healthcheck.Test}}' "$image")"
grep -F '/metrics' <<<"$healthcheck" >/dev/null
grep -F '9094' <<<"$healthcheck" >/dev/null
test "$(docker image inspect --format '{{.Config.StopSignal}}' "$image")" = SIGTERM
